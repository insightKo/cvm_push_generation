#!/usr/bin/env python3
"""ДВЕ модели подбора товаров для массового промо (волна 2 недели) + сравнение.

Согласовано 18.08.2026. Одна выгрузка чеков — два независимых ранжира,
результаты обеих моделей смотрим рядом:

  МОДЕЛЬ «СКОРЫ» — строится на скорах БД (I_PRODUCT_SCORE):
      порядок отбора = целевой скор x CONTACTS (скор x охват);
      частота -> FREQ_SCORE, средний чек -> CHECK_SCORE, LTV -> LOYALTY_SCORE.
      Скоры показывают лучшие товары сами по себе; оценочный слой считает
      прогноз ВДОЛЬ этого порядка и на порядок не влияет.

  МОДЕЛЬ «ОЦЕНКА» — строится на оценках из чеков:
      жадная максимизация покрытия (submodular max-coverage, lazy greedy):
      каждый следующий SKU добавляется по предельному вкладу в цель
      (частота -> уникальные доп. визиты; чек -> доп. ТО; LTV -> доп. ТО x
      LOYALTY_SCORE) с учётом пересечения аудиторий на сэмпле клиент x товар:
      P(клиент придёт) = 1 − Π(1 − u_j), u_j = V_j / CONTACTS_j.

  Лист «Сравнение» — размер наборов, пересечение, суммарные прогнозы обеих
  моделей и списки SKU, которые взяла только одна из них.

Общий прогнозный слой (для обеих моделей, всё оценивается из чеков,
история промо-кампаний не используется):
  Э1. Эластичность β по SKU — OLS log(Q, штуки) ~ log(цена единицы) + день
      недели на дневных агрегатах 12 недель; редким SKU — сжатие к категории:
      β* = w·β_sku + (1−w)·β_cat, w = n/(n+K).
  Э2. Межпокупочный цикл T (медиана) -> доля прироста, уходящая в НОВЫЕ
      ВИЗИТЫ: φ = min(1, 14/T).
  Отклик на скидку d: ΔQ = CHECKS x 14/42 x ((1−d)^β* − 1)
      (CHECKS за 6-недельное окно скоров масштабируются к базе «за волну»);
      визиты V = ΔQ x φ, добавка в чек A = ΔQ x (1−φ);
      Доп. ТО = V x PAIRED_BUDGET + A x цена; стоимость = (база+ΔQ) x цена x d;
      PL = 0.30 x Доп. ТО − стоимость.
      Скидка SKU = максимальная из сетки с PL >= 0; иначе argmax PL + пометка.

Ценовые сегменты: база категории делится на 3 части кластеризацией
распределения PRICE_INDEX (k-средних, k=3) — границы находят данные,
не экспертные пороги. Лимит категории пропорционален её размеру
(max-per-cat-share от активных SKU, мин. 1, потолок max-per-cat);
внутри категории <= max-per-seg SKU на ценовой сегмент.

Фильтры гигиены (статус-причина, ничего не выпадает молча): нет цены /
меньше половины магазинов (SHOPS_2W) / мало покупателей за 2 недели
(CONTACTS_2W) / прошлая волна (ротация). Товары с
круглогодичной скидкой помечаются (Э4), опционально исключаются.

Вход (готовят output/I_PRODUCT_SCORE_refresh.sql и output/model_extracts.sql):
  data/I_PRODUCT_SCORE_NEW.xlsx  — скоры (лист «I_PRODUCT_SCORE») и маппинг
                                   с названиями (лист «выгрузка»)
  data/model_data.csv            — дневные агрегаты SKU (Э1)
  output/product_cycle.csv       — межпокупочные циклы (Э2)
  output/client_sample.csv       — сэмпл клиент x товар 2%% (Э3)
  output/perm_discount.csv       — круглогодичные скидки (Э4)
  data/ассортимент.xlsx          — PAIRED_BUDGET и запасные цены/названия
  data/promo_skus.json           — SKU текущих промо

Оффлайн (ID_ORGANIZATION=1) и екомм (3) анализируются раздельно.
Запуск: python output/promo_product_model.py --goal freq
Модель ничего не отправляет и сама не режет — срез на ревью кривой.
"""
from __future__ import annotations

import argparse
import heapq
import json
import subprocess
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
MARGIN = 0.30           # маржа из CVM offline (forecast.py)
WAVE_DAYS = 14          # длина промо-волны
SCORE_DAYS = 42         # окно скоров = горизонт клиентских метрик I_CVM_CONTACT (6 недель)
WAVE_SHARE = WAVE_DAYS / SCORE_DAYS
BETA_CLIP = (-10.0, -0.1)

GOAL_COL = {"freq": "FREQ_SCORE", "check": "CHECK_SCORE", "ltv": "LOYALTY_SCORE"}
GOAL_NAME = {"freq": "частота (трафик)", "check": "средний чек", "ltv": "LTV"}
ORG_LABEL = {1: "оффлайн", 3: "екомм"}
SEG_ORDER = ["дешёвый", "средний", "премиальный"]
IN_SET = "В НАБОРЕ"


# ---------------------------------------------------------------- утилиты ----

def norm_id(v) -> str:
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.endswith("_0"):
        s = s[:-2]
    return s


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    return pd.read_csv(path, sep=None, engine="python")


# Сигнатуры блоков текстового дампа SSMS (Results to Text): все выгрузки
# model_extracts.sql могут лежать одним файлом — блоки ищутся по заголовкам
DUMP_SIGNATURES = {
    ("ID_ORGANIZATION", "ID_PRODUCT", "DATA", "Q", "PRICE"): "daily",
    ("ID_ORGANIZATION", "ID_PRODUCT", "T_MEDIAN", "REPEAT_BUYERS"): "cycles",
    ("ID_ORGANIZATION", "ID_PRODUCT", "WEEKS", "DISC_WEEKS", "AVG_DISC"): "perm",
    ("ID_ORGANIZATION", "ID_CONTACT", "ID_PRODUCT"): "sample",
    ("ID_ORGANIZATION", "TOTAL_CHECKS", "TOTAL_CLIENTS", "TOTAL_TO"): "totals",
}

# Имена колонок выгрузок — для CSV без заголовка (обычный экспорт из SSMS)
EXTRACT_COLS = {
    "daily": ["ID_ORGANIZATION", "ID_PRODUCT", "DATA", "Q", "PRICE"],
    "cycles": ["ID_ORGANIZATION", "ID_PRODUCT", "T_MEDIAN", "REPEAT_BUYERS"],
    "perm": ["ID_ORGANIZATION", "ID_PRODUCT", "WEEKS", "DISC_WEEKS", "AVG_DISC"],
    "sample": ["ID_ORGANIZATION", "ID_CONTACT", "ID_PRODUCT"],
    "totals": ["ID_ORGANIZATION", "TOTAL_CHECKS", "TOTAL_CLIENTS", "TOTAL_TO"],
}


def load_extract(path: Path, kind: str) -> pd.DataFrame:
    """CSV выгрузки: с заголовком или без него (безголовый экспорт SSMS)."""
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        first = f.readline()
    if "ID_" in first.upper() or "TOTAL" in first.upper():
        return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    df = pd.read_csv(path, sep=None, engine="python", header=None, encoding="utf-8-sig")
    df.columns = EXTRACT_COLS[kind][:len(df.columns)]
    return df


def parse_ssms_dump(path: Path) -> dict[str, pd.DataFrame]:
    """Разбирает вывод SSMS «Results to Text», где все выгрузки склеены в один
    файл: блоки находятся по заголовкам, разделители/итоги/битые (обрезанные)
    строки отбрасываются."""
    blocks: dict[str, list] = {}
    cols: tuple | None = None
    name: str | None = None
    dropped = 0
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if set(line) <= {"-", " "}:      # линия-разделитель под заголовком
                continue
            parts = tuple(line.split())
            if parts in DUMP_SIGNATURES:
                name = DUMP_SIGNATURES[parts]
                cols = parts
                blocks.setdefault(name, [])
                continue
            if name is None or cols is None:
                continue                      # преамбула («Окно оценки» и т.п.)
            if line.startswith("(") and "row" in line:
                continue                      # (N rows affected)
            if len(parts) == len(cols):
                blocks[name].append(parts)
            else:
                dropped += 1                  # обрезанные/служебные строки
    out: dict[str, pd.DataFrame] = {}
    for blk, rows in blocks.items():
        blk_cols = next(k for k, v in DUMP_SIGNATURES.items() if v == blk)
        df = pd.DataFrame(rows, columns=list(blk_cols))
        for c in df.columns:
            if c != "DATA":
                df[c] = pd.to_numeric(df[c].replace("NULL", np.nan), errors="coerce")
        out[blk] = df
    if dropped:
        print(f"SSMS-дамп: отброшено битых/служебных строк: {dropped}")
    return out


def wmean(g: pd.DataFrame, val: str, w: str) -> float:
    d = g.dropna(subset=[val])
    if d.empty:
        return np.nan
    ww = d[w].fillna(0)
    if ww.sum() > 0:
        return float((d[val] * ww).sum() / ww.sum())
    return float(d[val].mean())


def load_assortment(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Лист1")
    df["SKU"] = df["ID_PRODUCT_EXTERNAL"].map(norm_id)
    rows = []
    for sku, g in df.groupby("SKU"):
        rows.append({
            "SKU": sku,
            "Название": g["PRODUCT_NAME"].iloc[0],
            "CAT_EXT": g["ID_CATEGORY_5_ext"].iloc[0],
            "SHOPS": g["COUNT_SHOPS"].sum(),
            "AVG_PRICE": wmean(g, "AVG_PRICE", "COUNT_CHECK"),
            "REAL_SALE": wmean(g, "REAL_SALE", "COUNT_CHECK"),
            "PAIRED_BUDGET": wmean(g, "PAIRED_BUDGET", "COUNT_CHECK"),
        })
    return pd.DataFrame(rows)


def prev_wave_ids(path: Path, org_label: str) -> set[str]:
    """SKU прошлой волны — объединение наборов обеих моделей (листы «Скоры/Оценка»)."""
    xl = pd.ExcelFile(path)
    sheets = [s for s in xl.sheet_names
              if s in (f"Скоры ({org_label})", f"Оценка ({org_label})",
                       f"Модель ({org_label})", "Модель")]
    ids: set[str] = set()
    for sheet in sheets:
        df = xl.parse(sheet)
        if "Статус" in df.columns:
            df = df[df["Статус"] == IN_SET]
        col = "SKU" if "SKU" in df.columns else df.columns[0]
        ids |= set(df[col].map(norm_id))
    return ids


def kmeans3(x: np.ndarray, iters: int = 100) -> np.ndarray:
    """1D k-средних, k=3: границы сегментов находит распределение данных,
    а не назначенный порог. Возвращает метки 0/1/2 по возрастанию центров."""
    c = np.quantile(x, [1 / 6, 3 / 6, 5 / 6]).astype(float)
    lab = np.zeros(len(x), int)
    for _ in range(iters):
        lab = np.abs(x[:, None] - c[None, :]).argmin(1)
        new_c = np.array([x[lab == k].mean() if (lab == k).any() else c[k] for k in range(3)])
        if np.allclose(new_c, c):
            break
        c = new_c
    order = np.argsort(c)
    remap = np.empty(3, int)
    remap[order] = np.arange(3)
    return remap[lab]


# ------------------------------------------------- Э1: эластичность спроса ----

def fit_sku_elasticity(g: pd.DataFrame, min_days: int, min_prices: int) -> tuple[float | None, int]:
    g = g[(g["Q"] > 0) & (g["PRICE"] > 0)]
    n = len(g)
    if n < min_days or g["PRICE"].round(2).nunique() < min_prices:
        return None, n
    y = np.log(g["Q"].to_numpy(float))
    x = np.log(g["PRICE"].to_numpy(float))
    dates = pd.to_datetime(g["DATA"])
    # линейный тренд обязателен: без него сезонный рост объёма (нектарины в
    # августе) приписывается падению цены и β раздувается
    trend = (dates - dates.min()).dt.days.to_numpy(float)
    trend = trend / max(trend.max(), 1)
    dw = dates.dt.dayofweek
    # скидка работает сильнее в пт/сб (вводная Елены): отдельный наклон цены
    # для этих дней; в итоговую β он входит с весом доли объёма пт+сб
    wk = ((dw == 4) | (dw == 5)).to_numpy(float)
    dow = pd.get_dummies(dw, drop_first=True)
    X = np.column_stack([np.ones(n), x, x * wk, trend] +
                        ([dow.to_numpy(float)] if len(dow.columns) else []))
    try:
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return None, n
    q = g["Q"].to_numpy(float)
    wk_share = float(q[wk == 1].sum() / q.sum()) if q.sum() > 0 else 0.0
    beta_eff = float(coef[1]) + float(coef[2]) * wk_share
    return beta_eff, n


def wmedian(values: np.ndarray, weights: np.ndarray) -> float:
    order = np.argsort(values)
    v, w = values[order], weights[order]
    cum = np.cumsum(w)
    return float(v[np.searchsorted(cum, 0.5 * cum[-1])])


def estimate_elasticity(daily: pd.DataFrame, cat_of: dict, min_days: int,
                        min_prices: int, shrink_k: float):
    """β для СЕТЕВОГО инкремента оценивается на КАТЕГОРИЙНОМ уровне.

    SKU-эластичность ловит переключение клиентов между товарами категории:
    объём промо-SKU растёт в разы (β до −6), но деньги перетекают с соседней
    полки — сеть инкремента почти не получает. Агрегация категории схлопывает
    переключение внутрь, и категорийная β отражает честный прирост спроса.
    SKU-β остаётся справочной колонкой («сила переключения»).

    Возвращает (est, cat_beta, glob): est по SKU, cat_beta — словарь β категорий
    (для товаров без строк в дневных данных), glob — глобальный запасной."""
    daily = daily.copy()
    daily["CAT_EXT"] = daily["ID_PRODUCT"].map(cat_of)

    sku_rows = []
    for pid, g in daily.groupby("ID_PRODUCT"):
        b, n = fit_sku_elasticity(g, min_days, min_prices)
        sku_rows.append({"ID_PRODUCT": pid, "BETA_SKU": b, "N_DAYS": n})
    est = pd.DataFrame(sku_rows)
    if est.empty:
        return (pd.DataFrame(columns=["ID_PRODUCT", "BETA", "BETA_SRC", "BETA_SKU", "N_DAYS"]),
                {}, np.nan)

    # Категорийные дневные ИНДЕКСЫ (не суммы!): в одной категории смешаны
    # весовые (кг) и упаковки (шт) — складывать количества нельзя. Объём и цена
    # каждого SKU нормируются на его же средние (безразмерные отношения),
    # категория агрегируется взвешенно по выручке SKU:
    #   Q_idx(день) = Σ w_i·(q_i/сред.q_i) / Σ w_i;  P_idx(день) — аналогично
    d = daily[(daily["Q"] > 0) & (daily["PRICE"] > 0)].copy()
    base = d.groupby("ID_PRODUCT").agg(
        QBAR=("Q", "mean"), PBAR=("PRICE", "mean"))
    rev = (d["Q"] * d["PRICE"]).groupby(d["ID_PRODUCT"]).sum().rename("REV")
    d = d.merge(base, left_on="ID_PRODUCT", right_index=True) \
         .merge(rev, left_on="ID_PRODUCT", right_index=True)
    d["RQ"] = d["Q"] / d["QBAR"] * d["REV"]
    d["RP"] = d["PRICE"] / d["PBAR"] * d["REV"]
    cd = (d.groupby(["CAT_EXT", "DATA"], as_index=False)
          .agg(RQ=("RQ", "sum"), RP=("RP", "sum"), W=("REV", "sum")))
    cd["Q"] = cd["RQ"] / cd["W"]        # индекс объёма категории (безразмерный)
    cd["PRICE"] = cd["RP"] / cd["W"]    # индекс цены категории (безразмерный)
    cat_rows = []
    for cat, g in cd.groupby("CAT_EXT"):
        b, n = fit_sku_elasticity(g, min_days, min_prices)
        cat_rows.append({"CAT_EXT": cat, "BETA_CAT": b, "N_CAT": n})
    cats = pd.DataFrame(cat_rows)

    okc = cats[cats["BETA_CAT"].notna() & (cats["BETA_CAT"] < 0)]
    glob = wmedian(okc["BETA_CAT"].to_numpy(float), okc["N_CAT"].to_numpy(float)) \
        if len(okc) else np.nan
    # хвосты категорийных β — по распределению самих оценок (P5–P95)
    lo, hi = (np.nanquantile(okc["BETA_CAT"], [0.05, 0.95]) if len(okc) >= 20 else BETA_CLIP)
    cat_beta: dict = {}
    for _, r in cats.iterrows():
        b = r["BETA_CAT"]
        if pd.isna(b) or b >= 0:
            b = glob
        else:
            w = r["N_CAT"] / (r["N_CAT"] + shrink_k)
            b = w * b + (1 - w) * glob
        if pd.notna(b):
            cat_beta[r["CAT_EXT"]] = float(np.clip(b, max(lo, BETA_CLIP[0]),
                                                   min(hi, BETA_CLIP[1])))

    est["CAT_EXT"] = est["ID_PRODUCT"].map(cat_of)
    est["BETA"] = est["CAT_EXT"].map(cat_beta)
    est["BETA"] = est["BETA"].fillna(glob)
    est["BETA_SRC"] = "категория (сетевая)"

    # максимальная допустимая глубина углубления — из ИСТОРИИ цен категории:
    # насколько цена реально проваливалась в прошлых промо (P05 к медиане по SKU,
    # P90 по категории). Сахару, который глубоко не дисконтился, глубокая
    # скидка предлагаться не будет
    dip = (d.groupby("ID_PRODUCT")["PRICE"]
           .agg(lambda p: 1 - np.quantile(p, 0.05) / np.quantile(p, 0.5)
                if np.quantile(p, 0.5) > 0 else np.nan)
           .clip(0, 0.9))
    dip_cat = dip.groupby(dip.index.map(cat_of)).quantile(0.9)
    cat_dmax = dip_cat.to_dict()
    glob_dmax = float(dip.median()) if dip.notna().any() else 0.0
    return est[["ID_PRODUCT", "BETA", "BETA_SRC", "BETA_SKU", "N_DAYS"]], cat_beta, glob, cat_dmax, glob_dmax


# ------------------------------------------- Э2: цикл покупки и конверсия ----

def visit_share(t_median: pd.Series, cat: pd.Series) -> pd.Series:
    """φ = min(1, 14/T); пропуски -> медиана φ категории -> глобальная медиана."""
    phi = (WAVE_DAYS / t_median).clip(upper=1.0)
    cat_med = phi.groupby(cat).transform("median")
    phi = phi.fillna(cat_med)
    return phi.fillna(phi.median())


# -------------------------------- отклик на скидку и скидка модели -----------

def demand_response(df: pd.DataFrame, grid: list[float], retention: float,
                    k_dedup: float = 1.0) -> pd.DataFrame:
    """Скидка SKU = max из сетки с PL >= 0; иначе argmax PL + пометка.

    Ретеншн (Елена, 18.08.2026): retention (80%) клиентов базы и так придут —
    полную корзину приносят только НОВЫЕ визиты V x (1 − retention);
    остальной прирост покупок инкрементален только ценой самого товара."""
    best_d, best_pl, marginal = [], [], []
    dq_c, v_c, a_c, dto_c, cost_c, delta_c, grid_c = [], [], [], [], [], [], []
    for _, r in df.iterrows():
        if pd.isna(r["PRICE_EFF"]) or pd.isna(r["BETA"]):
            best_d.append(np.nan); best_pl.append(np.nan); marginal.append(False)
            dq_c.append(np.nan); v_c.append(np.nan); a_c.append(np.nan)
            dto_c.append(np.nan); cost_c.append(np.nan); delta_c.append(np.nan)
            grid_c.append(None)
            continue
        wave_checks = r["CHECKS"] * WAVE_SHARE  # база «за волну» из 6-недельных CHECKS
        # ИНКРЕМЕНТАЛЬНАЯ скидка: товар УЖЕ продаётся со средней реальной скидкой
        # d_cur (DISCOUNT_2W) — промо-скидка d частично её замещает, а не
        # добавляется. Сеть доплачивает и спрос реагирует только на разницу:
        #   δ = 1 − (1−d)/(1−d_cur), не ниже 0. Иначе расходы задваиваются.
        d_cur = r["DISCOUNT_2W"] if ("DISCOUNT_2W" in r and pd.notna(r["DISCOUNT_2W"])) else 0.0
        d_cur = min(max(float(d_cur), 0.0), 0.9)
        dmax = r["DELTA_MAX"] if ("DELTA_MAX" in r and pd.notna(r["DELTA_MAX"])) else 1.0
        variants = []
        for d in grid:
            delta = max(0.0, 1 - (1 - d) / (1 - d_cur))
            if delta > dmax + 1e-9:
                continue      # глубже, чем категория когда-либо дисконтилась
            dq = wave_checks * ((1 - delta) ** r["BETA"] - 1)
            # НОВЫЕ визиты = прирост x (1−retention): частотный клиент и так придёт,
            # его прирост ложится в существующий чек (φ здесь НЕ участвует — у
            # частотных товаров φ=1 объявлял бы весь прирост визитами с корзинами)
            v_new = dq * (1 - retention)
            a = dq - v_new                            # покупки в визитах, которые и так были бы
            # деньги — ТОЛЬКО прирост самих товаров; корзинные допущения убраны
            # (не согласованы). v_new остаётся прогнозом трафика, в ТО не входит
            dto = dq * r["PRICE_EFF"]
            cost = (wave_checks + dq) * r["PRICE_EFF"] * delta
            variants.append((d, dq, v_new, a, dto, cost, MARGIN * dto - cost, delta))
        if not variants:  # ни одной допустимой глубины (история цен не позволяет)
            best_d.append(np.nan); best_pl.append(np.nan); marginal.append(False)
            dq_c.append(np.nan); v_c.append(np.nan); a_c.append(np.nan)
            dto_c.append(np.nan); cost_c.append(np.nan); delta_c.append(np.nan)
            grid_c.append(None)
            continue
        # приоритет выбора (вводные Елены 18.08):
        #   1) МИНИМАЛЬНАЯ углубляющая глубина, если она маржинальна (PL >= 0);
        #   2) иначе — действующая скидка (δ=0, доплаты нет, товар в наборе);
        #   3) углубление в минус — только если действующей скидки нет
        #      (такой товар пойдёт под замену)
        deepening = [t for t in variants if t[7] > 0]
        deep_ok = [t for t in deepening if t[6] >= 0]
        flat = [t for t in variants if t[7] == 0]
        if deep_ok:
            pick = min(deep_ok, key=lambda t: t[7])
        elif flat:
            pick = max(flat, key=lambda t: t[0])
        else:
            pick = min(deepening, key=lambda t: t[7]) if deepening \
                else max(variants, key=lambda t: (t[6], t[0]))
        d, dq, v, a, dto, cost, pl, delta = pick
        best_d.append(d); best_pl.append(pl); marginal.append(pl >= 0)
        dq_c.append(dq); v_c.append(v); a_c.append(a); dto_c.append(dto); cost_c.append(cost)
        delta_c.append(delta); grid_c.append(variants)
    out = df.copy()
    out["Скидка модели"] = best_d
    out["Скидка доп. (инкремент)"] = delta_c
    out["_grid"] = grid_c        # вся сетка вариантов — для догона до целевого доп. ТО
    out["_dto_i"] = dto_c        # независимые доп. ТО и PL выбранного варианта
    out["_pl_i"] = best_pl
    out["ΔQ покупок"] = dq_c
    out["Доп. визиты (независ.)"] = v_c
    out["Добавка в чек"] = a_c
    out["Доп. ТО ₽"] = dto_c
    out["Стоимость скидки ₽"] = cost_c
    out["PL ₽"] = best_pl
    out["_маржинальный"] = marginal
    return out


# ------------------------------------------------ покрытие на сэмпле ---------

class Coverage:
    """Покрытие клиентов на сэмпле: survival s_i = Π(1−u_j) по выбранным SKU."""

    def __init__(self, pairs: pd.DataFrame, scale: float):
        self.scale = scale
        self.clients_of: dict = defaultdict(list)
        self.survival: dict = {}
        for cid, pid in zip(pairs["ID_CONTACT"].to_numpy(), pairs["ID_PRODUCT"].to_numpy()):
            self.clients_of[pid].append(cid)
            self.survival[cid] = 1.0

    def has(self, pid) -> bool:
        return pid in self.clients_of

    def gain(self, pid, u: float) -> float:
        return u * sum(self.survival[c] for c in self.clients_of.get(pid, ())) * self.scale

    def add(self, pid, u: float) -> None:
        for c in self.clients_of.get(pid, ()):
            self.survival[c] *= (1 - u)


def u_of_row(r) -> float:
    if r["CONTACTS"] > 0 and pd.notna(r["Доп. визиты (независ.)"]):
        return float(np.clip(r["Доп. визиты (независ.)"] / r["CONTACTS"], 0, 1))
    return 0.0


def indep_visits(r) -> float:
    return r["Доп. визиты (независ.)"] if pd.notna(r["Доп. визиты (независ.)"]) else 0.0


def make_cat_cap(df: pd.DataFrame, args):
    """Лимит категории пропорционален её размеру: доля от активных SKU,
    минимум 1, потолок max-per-cat («5 из большой и 1 из маленькой»).
    Считается по LIMIT_CAT: фрукты-овощи (ветка 60) укрупнены до подветок,
    иначе супер-трафиковые мелкие категории заполоняют набор."""
    size_of = df.groupby("LIMIT_CAT")["ID_PRODUCT"].nunique().to_dict()

    def cat_cap(cat) -> int:
        return min(args.max_per_cat,
                   max(1, int(args.max_per_cat_share * size_of.get(cat, 0))))
    return cat_cap


# ------------------------------------------- МОДЕЛЬ «СКОРЫ»: отбор -----------

def select_by_scores(df: pd.DataFrame, cov: Coverage | None, args) -> tuple[list, dict]:
    """Порядок отбора = целевой скор x CONTACTS. Покрытие считает уникальные
    доп. визиты вдоль порядка — прогноз, на порядок не влияет."""
    cat_cap = make_cat_cap(df, args)
    cand = df[df["Статус"] == IN_SET].sort_values("RANK_VAL", ascending=False)
    cat_cnt: dict = defaultdict(int)
    seg_cnt: dict = defaultdict(int)
    order: list = []
    uniq_gain: dict = {}
    for i, r in cand.iterrows():
        cat, seg = r["LIMIT_CAT"], r["Ценовой сегмент"]
        if cat_cnt[cat] >= cat_cap(cat):
            df.at[i, "Статус"] = "вне: лимит категории"
            continue
        if seg_cnt[(cat, seg)] >= args.max_per_seg:
            df.at[i, "Статус"] = "вне: лимит сегмента в категории"
            continue
        u = u_of_row(r)
        if cov is not None and cov.has(r["ID_PRODUCT"]):
            uniq_gain[i] = cov.gain(r["ID_PRODUCT"], u)
            cov.add(r["ID_PRODUCT"], u)
        else:
            uniq_gain[i] = indep_visits(r)
        order.append(i)
        cat_cnt[cat] += 1
        seg_cnt[(cat, seg)] += 1
    return order, uniq_gain


# ------------------------------------------ МОДЕЛЬ «ОЦЕНКА»: отбор -----------

def select_by_estimate(df: pd.DataFrame, cov: Coverage | None, args) -> tuple[list, dict]:
    """Lazy greedy по предельному вкладу в цель с учётом пересечения аудиторий:
    частота -> уникальные доп. визиты; чек -> доп. ТО; LTV -> доп. ТО x LOYALTY."""
    cat_cap = make_cat_cap(df, args)
    cand = df[df["Статус"] == IN_SET]
    u_of, weight = {}, {}
    for i, r in cand.iterrows():
        u_of[i] = u_of_row(r)
        paired = r["PAIRED_BUDGET"] if pd.notna(r["PAIRED_BUDGET"]) else r["PRICE_EFF"]
        if args.goal == "freq":
            weight[i] = 1.0
        elif args.goal == "check":
            weight[i] = paired if pd.notna(paired) else 0.0
        else:  # ltv
            loyal = r["LOYALTY_SCORE"] if pd.notna(r["LOYALTY_SCORE"]) else 0.0
            weight[i] = (paired if pd.notna(paired) else 0.0) * loyal

    def marginal(i) -> float:
        r = df.loc[i]
        base = (cov.gain(r["ID_PRODUCT"], u_of[i])
                if cov is not None and cov.has(r["ID_PRODUCT"]) else indep_visits(r))
        return base * weight[i]

    heap = [(-marginal(i), 0, i) for i in cand.index]
    heapq.heapify(heap)
    version = defaultdict(int)
    cat_cnt: dict = defaultdict(int)
    seg_cnt: dict = defaultdict(int)
    order: list = []
    uniq_gain: dict = {}
    while heap:
        neg, ver, i = heapq.heappop(heap)
        if ver != version[i]:
            continue
        cat, seg = df.at[i, "LIMIT_CAT"], df.at[i, "Ценовой сегмент"]
        if cat_cnt[cat] >= cat_cap(cat):
            df.at[i, "Статус"] = "вне: лимит категории"
            continue
        if seg_cnt[(cat, seg)] >= args.max_per_seg:
            df.at[i, "Статус"] = "вне: лимит сегмента в категории"
            continue
        g = marginal(i)
        if heap and g < -heap[0][0] - 1e-12:      # lazy: оценка устарела
            version[i] += 1
            heapq.heappush(heap, (-g, version[i], i))
            continue
        r = df.loc[i]
        uniq_gain[i] = (cov.gain(r["ID_PRODUCT"], u_of[i])
                        if cov is not None and cov.has(r["ID_PRODUCT"]) else indep_visits(r))
        if cov is not None:
            cov.add(r["ID_PRODUCT"], u_of[i])
        order.append(i)
        cat_cnt[cat] += 1
        seg_cnt[(cat, seg)] += 1
    return order, uniq_gain


def portfolio_adjust(df: pd.DataFrame, order: list, sample: pd.DataFrame | None,
                     retention: float) -> dict:
    """Портфельный прогноз набора — БЕЗ двойного счёта пересечений в чеке.

    Клиент покупает несколько товаров набора одним походом (~700 ₽ чек):
    сумма по-товарных приростов задваивает один и тот же чек. Поэтому прирост
    считается на НАБОРЕ целиком: средневзвешенные скидка d̄ и эластичность β̄,
    единый uplift = (1−d̄)^β̄ − 1; доп. ТО = база ТО набора x uplift,
    по SKU распределяется весами их индивидуального отклика.
    Визиты: прирост чеков-вхождений x (1−retention), дедуплицированный на
    среднее число товаров набора у одного покупателя (из сэмпла).
    Возвращает {index -> доп. визиты} для finalize; правит Доп. ТО ₽ и PL ₽."""
    sel = df.loc[order]
    base_to = pd.to_numeric(sel["CHECKS_2W"], errors="coerce") * sel["PRICE_EFF"]
    d = sel["Скидка доп. (инкремент)"]   # платим и получаем отклик только на разницу с текущей скидкой
    beta = sel["BETA"]
    # портфель считается ТОЛЬКО по углубляемым (δ>0): действующие скидки не
    # разбавляют средневзвешенные d̄/β̄ — у них нет ни доплаты, ни прироста
    okm = base_to.notna() & (base_to > 0) & beta.notna() \
        & (pd.to_numeric(d, errors="coerce") > 0)
    if not order:
        return {}
    in_order = df.index.isin(order)
    # дедуп визитов: товаров набора на одного покупателя (по сэмплу клиент x товар)
    k = 1.0
    if sample is not None and len(sample):
        pids = set(sel["ID_PRODUCT"].dropna())
        sp = sample[sample["ID_PRODUCT"].isin(pids)]
        if len(sp):
            k = max(1.0, len(sp) / sp["ID_CONTACT"].nunique())

    checks_w = pd.to_numeric(sel["CHECKS_2W"], errors="coerce").fillna(0)
    beta_own = pd.to_numeric(sel["BETA"], errors="coerce")
    beta_fill = float(beta_own.median()) if beta_own.notna().any() else -1.0

    df.loc[in_order, "Доп. ТО ₽"] = 0.0
    item_to = pd.Series(0.0, index=sel.index)
    visits = pd.Series(0.0, index=sel.index)

    if okm.any():
        w_to = base_to[okm]
        d_bar = float((d[okm] * w_to).sum() / w_to.sum())
        beta_bar = float((beta[okm] * w_to).sum() / w_to.sum())
        uplift = (1 - d_bar) ** beta_bar - 1
        add_to_total = float(w_to.sum() * uplift)
        raw = w_to * ((1 - d[okm]) ** beta[okm] - 1)
        share = raw / raw.sum() if raw.sum() > 0 else w_to / w_to.sum()
        item_to.loc[share.index] = add_to_total * share
        # прирост чеков углубляемых — от их инкрементальной скидки
        entries = checks_w * ((1 - d.fillna(0)) ** beta_own.fillna(beta_bar) - 1)
        visits = entries * (1 - retention) / k

    # позиции с ДЕЙСТВУЮЩЕЙ скидкой (δ=0): трафик и ТО считаются ВЕЗДЕ
    # (вводная Елены 19.08) — сколько спроса генерирует их текущая скидка
    # по той же эластичности; доплата 0, PL 0 (финансирует текущее ценообразование)
    flat_idx = sel.index[~okm]
    if len(flat_idx):
        d_cur = pd.to_numeric(sel.loc[flat_idx, "DISCOUNT_2W"], errors="coerce").clip(0, 0.9).fillna(0)
        b_f = beta_own.loc[flat_idx].fillna(beta_fill)
        supported = (checks_w.loc[flat_idx] * (1 - (1 - d_cur) ** (-b_f))).clip(lower=0)
        item_to.loc[flat_idx] = supported * pd.to_numeric(
            sel.loc[flat_idx, "PRICE_EFF"], errors="coerce").fillna(0)
        visits.loc[flat_idx] = supported * (1 - retention) / k

    df.loc[sel.index, "Доп. ТО ₽"] = item_to
    df.loc[in_order, "PL ₽"] = (
        MARGIN * pd.to_numeric(df.loc[in_order, "Доп. ТО ₽"], errors="coerce").fillna(0)
        - pd.to_numeric(df.loc[in_order, "Стоимость скидки ₽"], errors="coerce").fillna(0))
    # PL действующих принудительно 0: их скидку волна не оплачивает,
    # приписывать себе их маржу нельзя
    df.loc[flat_idx, "PL ₽"] = 0.0
    df.loc[flat_idx, "Стоимость скидки ₽"] = 0.0
    return dict(zip(sel.index, visits))


# ------------------------------------------------ сборка листа модели --------

def finalize(df: pd.DataFrame, order: list, uniq_gain: dict) -> pd.DataFrame:
    in_set = df.index.isin(order)
    df["Ранг"] = np.nan
    for rank, i in enumerate(order, 1):
        df.at[i, "Ранг"] = rank
    df["Доп. визиты (уник.)"] = pd.Series(uniq_gain)

    sel = df.loc[order]
    for src, dst in [("Доп. визиты (уник.)", "Доп. визиты накопл."),
                     ("Доп. ТО ₽", "Доп. ТО накопл. ₽"),
                     ("Стоимость скидки ₽", "Скидка накопл. ₽"),
                     ("PL ₽", "PL накопл. ₽"),
                     ("CONTACTS", "Охват (Σ покупателей) накопл.")]:
        df[dst] = np.nan
        df.loc[order, dst] = sel[src].cumsum()
    neg = [i for i in order if pd.notna(df.at[i, "PL ₽"]) and df.at[i, "PL ₽"] < 0]
    if neg:
        i = neg[0]
        df.at[i, "Пометка"] = (str(df.at[i, "Пометка"]) + "; ").lstrip("; ") + "← граница маржинальности"

    cols = ["Ранг", "SKU", "ID_PRODUCT", "Название", "CAT_EXT", "Категория (название)", "ID_CATEGORY_5",
            "Ценовой сегмент", "FREQ_SCORE", "CHECK_SCORE", "LOYALTY_SCORE", "PRICE_INDEX",
            "RANK_VAL", "CONTACTS", "CHECKS", "REPEAT",
            "SHOPS_2W", "CONTACTS_2W", "CHECKS_2W", "PRICE_2W", "DISCOUNT_2W",
            "CAT_PRODUCTS_2W", "Скидка круглогодичная", "DELTA_MAX",
            "LIMIT_CAT", "BETA", "BETA_SRC", "N_DAYS", "T_MEDIAN", "PHI",
            "Скидка группы (реальная 2 нед.)", "Скидка модели", "Скидка доп. (инкремент)",
            "ΔQ покупок", "Доп. визиты (независ.)", "Доп. визиты (уник.)", "Добавка в чек",
            "PRICE_EFF", "Доп. ТО ₽", "Стоимость скидки ₽", "PL ₽",
            "Доп. визиты накопл.", "Доп. ТО накопл. ₽", "Скидка накопл. ₽",
            "PL накопл. ₽", "Охват (Σ покупателей) накопл.", "Статус", "Пометка"]
    return pd.concat([
        df[in_set].sort_values("Ранг"),
        df[~in_set].sort_values("RANK_VAL", ascending=False),
    ])[[c for c in cols if c in df.columns]]


def snap_discount(x: float, grid: list[float]) -> float:
    """Ближайшее значение сетки; ниже минимума сетки не опускаемся."""
    if pd.isna(x):
        return grid[0]
    x = max(x, grid[0])
    return min(grid, key=lambda g: abs(g - x))


def build_promo_list(res: pd.DataFrame, grid: list[float]) -> pd.DataFrame:
    """Чистый список товаров под промо: набор, сортировка по трафик-рангу,
    скидка + прогноз на волну (скидка действует все 2 недели волны).
    Без цен — это лист для передачи. Скидка = скидка модели (эластичность),
    пока её нет — реальная скидка группы к сетке."""
    # сортировка листа — по ПРОГНОЗНОМУ ТРАФИКУ строки (топ = кто больше всего
    # везёт визитов, включая поддержку действующими скидками)
    s = res[res["Статус"] == IN_SET].copy()
    s = s.sort_values("Доп. визиты (уник.)", ascending=False)
    s["Ранг"] = range(1, len(s) + 1)
    fallback = s["Скидка группы (реальная 2 нед.)"].map(lambda x: snap_discount(x, grid))
    disc = s["Скидка модели"].fillna(fallback)
    # тип строки: углубление (сеть доплачивает разницу) или действующая скидка
    # (доплаты нет, скидка коммуницируется); для действующих показывается
    # ФАКТИЧЕСКИЙ текущий процент, а не ступень сетки
    delta = pd.to_numeric(s["Скидка доп. (инкремент)"], errors="coerce")
    is_deep = delta > 0
    # «Скидка, %»: у углублений — номинал модели; у действующих (без доплаты) —
    # 10% (вводная Елены 19.08); фактический текущий процент (в т.ч. сезонные
    # переоценки, записанные кассой как скидка) — в листе «Скоры», DISCOUNT_2W
    pct = pd.Series(np.where(is_deep, disc * 100, 10), index=s.index)
    out = pd.DataFrame({
        "№": s["Ранг"].astype(int),
        "SKU": s["SKU"],
        "Товар": s["Название"],
        "Категория": s.get("Категория (название)", pd.Series("", index=s.index)),
        "Скидка, %": pct.round(0).astype("Int64"),
        "Тип скидки": np.where(is_deep, "углубление (доплата сети)", "действующая (без доплаты)"),
        # прогноз на волну 2 недели: доп. ТО/трафик/PL требуют Э1 (эластичность);
        # скидка без отклика = базовые покупки волны x цена x скидка (нижняя оценка)
        "Доп. трафик (визиты)": s.get("Доп. визиты (уник.)"),
        "Доп. ТО, ₽": s.get("Доп. ТО ₽"),
        "Скидка, ₽": pd.to_numeric(s.get("Стоимость скидки ₽"), errors="coerce").fillna(
            pd.to_numeric(s.get("CHECKS_2W"), errors="coerce")
            * pd.to_numeric(s.get("PRICE_2W"), errors="coerce") * disc),
        "PL, ₽": s.get("PL ₽"),
    })
    for c in ["Доп. трафик (визиты)", "Доп. ТО, ₽", "Скидка, ₽", "PL, ₽"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").round(0)
    # нулевой прогнозный трафик (покрытие-SKU с копеечным приростом) не показываем
    out.loc[out["Доп. трафик (визиты)"] == 0, "Доп. трафик (визиты)"] = np.nan
    # строки ИТОГО в листе НЕТ намеренно: при суммировании колонки она удваивала
    # результат; итоги волны — на листе «Прогноз»
    return out


def write_coverage_sql(path: Path, sets: dict[int, list]) -> None:
    """SQL для ТОЧНОГО расчёта покрытия набора в БД (а не по сэмплу):
    доля уникальных клиентов ВСЕЙ активной базы, купивших >=1 SKU набора,
    и доля чеков с товарами набора — за 2 недели и за окно скоров (6 недель).
    Прогнать в Manzana после утверждения среза (список SKU = набор «Скоры»)."""
    lines = [
        "-- Точное покрытие набора массового промо — считается в БД по всей базе.",
        "-- Сгенерировано моделью promo_product_model.py; список SKU = набор «Скоры».",
        "-- После среза набора лишние SKU можно просто удалить из вставки #set.",
        "set nocount on;",
        "declare @idc int = 1;",
        "",
        "drop table if exists #set;",
        "create table #set (ID_ORGANIZATION int, ID_PRODUCT bigint);",
    ]
    for org, ids in sets.items():
        for i in range(0, len(ids), 1000):
            chunk = ids[i:i + 1000]
            values = ", ".join(f"({org}, {int(p)})" for p in chunk)
            lines.append(f"insert into #set (ID_ORGANIZATION, ID_PRODUCT) values {values};")
    lines += [
        "",
        "declare @maxd date = (select max(DATA) from I_CHECK (nolock) where ID_COMPANY = @idc);",
        "declare @d_end date = dateadd(day, -((datediff(day, '19000101', @maxd) + 1) % 7), @maxd);",
        "declare @d2 date = dateadd(day, -13, @d_end);   -- последние 2 недели",
        "declare @d6 date = dateadd(day, -41, @d_end);   -- окно скоров, 6 недель",
        "",
        "-- не-фродовая база",
        "drop table if exists #ok;",
        "select ID_CONTACT, ID_ORGANIZATION",
        "into #ok",
        "from I_CVM_CONTACT (nolock)",
        "where ID_COMPANY = @idc and ID_CONTACT <> 0 and SEGMENT <> 0",
        "group by ID_CONTACT, ID_ORGANIZATION;",
        "",
        "-- чеки базы за 6 недель + флаг «в чеке есть товар набора»",
        "drop table if exists #cheq;",
        "select h.ID_ORGANIZATION, h.ID_CHECK, h.ID_CONTACT, h.DATA,",
        "       HIT = max(iif(t.ID_PRODUCT is not null, 1, 0))",
        "into #cheq",
        "from I_CHECKHEADER as h (nolock)",
        "     join #ok as o on h.ID_CONTACT = o.ID_CONTACT and h.ID_ORGANIZATION = o.ID_ORGANIZATION",
        "     left join I_CHECK as c (nolock)",
        "          on c.ID_CHECK = h.ID_CHECK and c.ID_CONTACT = h.ID_CONTACT",
        "          and c.ID_COMPANY = h.ID_COMPANY and c.DATA = h.DATA",
        "     left join #set as t",
        "          on t.ID_PRODUCT = c.ID_PRODUCT and t.ID_ORGANIZATION = h.ID_ORGANIZATION",
        "where h.ID_COMPANY = @idc and h.ID_CONTACT <> 0",
        "  and h.DATA between @d6 and @d_end",
        "group by h.ID_ORGANIZATION, h.ID_CHECK, h.ID_CONTACT, h.DATA;",
        "",
        "-- покрытие: клиенты и чеки, за 2 недели и за 6 недель",
        "select [Окно] = N'2 недели', ID_ORGANIZATION",
        "     , [Клиентов базы]      = count(distinct ID_CONTACT)",
        "     , [Клиентов с набором] = count(distinct iif(HIT = 1, ID_CONTACT, null))",
        "     , [Доля клиентов]      = cast(count(distinct iif(HIT = 1, ID_CONTACT, null)) * 100.0",
        "                                / count(distinct ID_CONTACT) as decimal(5, 1))",
        "     , [Чеков базы]         = count(*)",
        "     , [Чеков с набором]    = sum(HIT)",
        "     , [Доля чеков]         = cast(sum(HIT) * 100.0 / count(*) as decimal(5, 1))",
        "from #cheq where DATA >= @d2",
        "group by ID_ORGANIZATION",
        "union all",
        "select N'6 недель', ID_ORGANIZATION",
        "     , count(distinct ID_CONTACT)",
        "     , count(distinct iif(HIT = 1, ID_CONTACT, null))",
        "     , cast(count(distinct iif(HIT = 1, ID_CONTACT, null)) * 100.0",
        "          / count(distinct ID_CONTACT) as decimal(5, 1))",
        "     , count(*), sum(HIT), cast(sum(HIT) * 100.0 / count(*) as decimal(5, 1))",
        "from #cheq",
        "group by ID_ORGANIZATION",
        "order by [Окно], ID_ORGANIZATION;",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def build_forecast(res: pd.DataFrame, promo: pd.DataFrame,
                   totals_row: dict | None, coverage_sql: str) -> pd.DataFrame:
    """Сводный прогноз волны. Показываются только считаемые строки —
    несчитаемое (нет данных) в лист не попадает."""
    s = res[res["Статус"] == IN_SET]
    n_all = len(res)
    to_all = (pd.to_numeric(res["CHECKS_2W"], errors="coerce")
              * pd.to_numeric(res["PRICE_EFF"], errors="coerce")).sum()
    to_set = (pd.to_numeric(s["CHECKS_2W"], errors="coerce")
              * pd.to_numeric(s["PRICE_EFF"], errors="coerce")).sum()
    add_v = pd.to_numeric(s["Доп. визиты (уник.)"], errors="coerce").sum()
    add_to = pd.to_numeric(s["Доп. ТО ₽"], errors="coerce").sum()
    pl = pd.to_numeric(s["PL ₽"], errors="coerce").sum()
    disc = pd.to_numeric(promo.iloc[:-1]["Скидка, ₽"], errors="coerce").sum()  # без строки ИТОГО
    has_est = pd.to_numeric(s["Доп. ТО ₽"], errors="coerce").notna().any()

    cat_col = "LIMIT_CAT" if "LIMIT_CAT" in res.columns else "CAT_EXT"
    cats_all = res[cat_col].replace("", np.nan).dropna().nunique()
    cats_set = s[cat_col].replace("", np.nan).dropna().nunique()
    # покрытие с учётом УЖЕ действующих скидок: группа закрыта, если в ней
    # есть реальная скидка >= минимальной ступени сетки — волна её не дублирует
    disc_g = pd.to_numeric(res["DISCOUNT_2W"], errors="coerce").groupby(res[cat_col]).max()
    real_cov = set(disc_g.index[disc_g >= 0.10]) - {""}
    union_cov = len(real_cov | set(s[cat_col].replace("", np.nan).dropna()))
    rows = [
        ("SKU в наборе", len(s)),
        ("Доля набора от активного ассортимента, %", round(100 * len(s) / n_all, 1)),
        ("Доля набора в ТО активного ассортимента, %",
         round(100 * to_set / to_all, 1) if to_all else None),
        ("Доля категорий со скидкой, %",
         round(100 * union_cov / cats_all, 1) if cats_all else None),
        ("Скидка волны, ₽", round(disc)),
    ]
    if has_est:
        rows += [
            ("Общий доп. трафик (уникальные визиты)", round(add_v)),
            ("Общий доп. ТО, ₽", round(add_to)),
            ("PL, ₽", round(pl)),
        ]
        if totals_row:
            avg0 = totals_row["TOTAL_TO"] / totals_row["TOTAL_CHECKS"]
            avg1 = (totals_row["TOTAL_TO"] + add_to) / (totals_row["TOTAL_CHECKS"] + add_v)
            rows.append(("Влияние на средний чек сети, ₽", round(avg1 - avg0, 2)))
    return pd.DataFrame(rows, columns=["Показатель", "Значение"])


def set_totals(res: pd.DataFrame) -> dict:
    s = res[res["Статус"] == IN_SET]
    return {
        "SKU в наборе": len(s),
        "Доп. визиты (уник.)": s["Доп. визиты (уник.)"].sum(),
        "Доп. ТО ₽": s["Доп. ТО ₽"].sum(),
        "Стоимость скидки ₽": s["Стоимость скидки ₽"].sum(),
        "PL ₽": s["PL ₽"].sum(),
        "Охват (Σ покупателей)": s["CONTACTS"].sum(),
    }


def comparison_sheet(res_a: pd.DataFrame, res_b: pd.DataFrame) -> pd.DataFrame:
    """Сравнение наборов двух моделей: итоги + чем они разошлись."""
    a = res_a[res_a["Статус"] == IN_SET]
    b = res_b[res_b["Статус"] == IN_SET]
    sa, sb = set(a["SKU"]), set(b["SKU"])
    inter = sa & sb
    ta, tb = set_totals(res_a), set_totals(res_b)
    rows = [("Показатель", "Модель «Скоры»", "Модель «Оценка»")]
    rows += [(k, round(ta[k]), round(tb[k])) for k in ta]
    rows.append(("Пересечение наборов, SKU",
                 f"{len(inter)} ({len(inter) / max(1, min(len(sa), len(sb))):.0%} от меньшего)", ""))
    rows.append(("", "", ""))
    rows.append(("Только в «Скорах» (топ-30 по рангу)", "", "Только в «Оценке» (топ-30 по рангу)"))
    only_a = a[~a["SKU"].isin(sb)].nsmallest(30, "Ранг")
    only_b = b[~b["SKU"].isin(sa)].nsmallest(30, "Ранг")
    la = [f"{r.SKU} · {getattr(r, 'Название', '')}" for r in only_a.itertuples()]
    lb = [f"{r.SKU} · {getattr(r, 'Название', '')}" for r in only_b.itertuples()]
    for x, y in zip(la + [""] * (len(lb) - len(la)) if len(la) < len(lb) else la,
                    lb + [""] * (len(la) - len(lb)) if len(lb) < len(la) else lb):
        rows.append((x, "", y))
    return pd.DataFrame(rows[1:], columns=list(rows[0]))


# ----------------------------------------------------------------- прогон ----

def process_org(df: pd.DataFrame, ass: pd.DataFrame, est: pd.DataFrame,
                cat_beta: dict, glob_beta: float, cat_dmax: dict, glob_dmax: float,
                cycles: pd.DataFrame, sample: pd.DataFrame | None,
                perm: pd.DataFrame | None,
                prev_ids: set, args, grid: list):
    goal_col = GOAL_COL[args.goal]

    df = df.merge(ass.rename(columns={"CAT_EXT": "CAT_EXT_ass", "Название": "Название_ass"}),
                  on="SKU", how="left")
    # категория и название: БД первичны, ассортимент — запасной
    if "CAT_EXT" not in df.columns:
        df["CAT_EXT"] = df["ID_CATEGORY_5_ext"] if "ID_CATEGORY_5_ext" in df.columns else np.nan
    df["CAT_EXT"] = df["CAT_EXT"].fillna(df["CAT_EXT_ass"])
    name_db = df["Название БД"] if "Название БД" in df.columns else pd.Series(np.nan, index=df.index)
    df["Название"] = name_db.fillna(df["Название_ass"])
    df = df.merge(est, on="ID_PRODUCT", how="left")
    # товары без строк в дневных данных: категорийная β, затем глобальная
    df["BETA"] = df["BETA"].fillna(df["CAT_EXT"].map(cat_beta)).fillna(glob_beta)
    df["BETA_SRC"] = df["BETA_SRC"].fillna("категория (сетевая)")
    # потолок углубления из истории цен категории (сахар глубже 5% не бывал —
    # глубже модель не предложит)
    df["DELTA_MAX"] = df["CAT_EXT"].map(cat_dmax).fillna(glob_dmax)
    df = df.merge(cycles[["ID_PRODUCT", "T_MEDIAN"]].drop_duplicates("ID_PRODUCT"),
                  on="ID_PRODUCT", how="left")
    df["PHI"] = visit_share(df["T_MEDIAN"], df["CAT_EXT"])
    df["REPEAT"] = df["CHECKS"] / df["CONTACTS"]
    # цена — свежая из БД (PRICE_2W); ассортиментная — только запасной вариант
    df["PRICE_EFF"] = (df["PRICE_2W"].fillna(df["AVG_PRICE"])
                       if "PRICE_2W" in df.columns else df["AVG_PRICE"])
    # размер категории «для клиентов»: активные SKU категории ext
    df["CAT_PRODUCTS_2W"] = df.groupby("CAT_EXT")["ID_PRODUCT"].transform("nunique")
    # лимитная категория (для лимитов отбора И покрытия):
    #  - фрукты-овощи (ветка 60) укрупняются до подветок (6001 овощи, 6002 фрукты,
    #    6003 экзотика, ...) — сезонные нектарины/абрикосы/персики = одна группа;
    #  - остальные группируются по НАЗВАНИЮ категории: в справочнике встречаются
    #    два кода с одним именем («БАТОНЫ, БАГЕТЫ ИНД» и его (К)-дубль) — склеиваем
    cat_s = df["CAT_EXT"].apply(lambda v: str(int(v)) if pd.notna(v) else "")
    name_s = (df.get("Категория (название)", pd.Series("", index=df.index)).astype(str)
              .str.replace(r"\s*\(К\)\s*$", "", regex=True).str.strip().str.upper())
    df["LIMIT_CAT"] = np.where(cat_s.str.startswith("60"), cat_s.str[:4],
                               np.where((name_s != "") & (name_s != "NAN"), name_s, cat_s))
    # РАНЖИР МОДЕЛИ «СКОРЫ»: целевой скор x охват
    df["RANK_VAL"] = df[goal_col].fillna(0) * df["CONTACTS"]

    # круглогодичная скидка (Э4)
    if perm is not None and len(perm):
        df = df.merge(perm[["ID_PRODUCT", "WEEKS", "DISC_WEEKS", "AVG_DISC"]].drop_duplicates("ID_PRODUCT"),
                      on="ID_PRODUCT", how="left")
        df["Скидка круглогодичная"] = ((df["DISC_WEEKS"] / df["WEEKS"] >= args.perm_weeks_share)
                                       & (df["AVG_DISC"] >= args.perm_min_disc)).fillna(False)
    else:
        df["Скидка круглогодичная"] = False

    # ценовой сегмент: реальное деление базы категории на 3 части (k-средних)
    seg_source = "PRICE_INDEX" if "PRICE_INDEX" in df.columns else "PRICE_EFF"
    df["Ценовой сегмент"] = ""
    for cat, g in df.groupby("CAT_EXT"):
        vals = g[seg_source].dropna()
        if len(vals) < 3 or vals.nunique() < 3:
            continue
        labels = kmeans3(vals.to_numpy(float))
        df.loc[vals.index, "Ценовой сегмент"] = [SEG_ORDER[k] for k in labels]
    # категории, где товаров мало для своей кластеризации (весовые бананы и т.п.):
    # сегмент — по положению фактической цены единицы на ОБЩЕЙ ценовой шкале
    # организации (k-средних по log-ценам всех товаров; профиль покупателей тут
    # не годится — универсальные товары покупают все, и аудитория «премиальнеет»)
    empty = (df["Ценовой сегмент"] == "") & df["PRICE_EFF"].notna() & (df["PRICE_EFF"] > 0)
    base = df.loc[df["PRICE_EFF"].notna() & (df["PRICE_EFF"] > 0), "PRICE_EFF"]
    if empty.any() and len(base) >= 3 and base.nunique() >= 3:
        lab_all = kmeans3(np.log(base.to_numpy(float)))
        seg_all = pd.Series([SEG_ORDER[k] for k in lab_all], index=base.index)
        df.loc[empty, "Ценовой сегмент"] = seg_all[df.index[empty]]

    # фильтры гигиены — на свежих полях из БД
    shops_col = "SHOPS_2W" if "SHOPS_2W" in df.columns else "SHOPS"
    max_shops = df[shops_col].max() if shops_col == "SHOPS_2W" else ass["SHOPS"].max()
    min_shops = args.min_shops_share * max_shops
    df["Статус"] = IN_SET
    df["Пометка"] = ""
    # служебные категории из справочника: пакеты на кассе (640214xx),
    # акции лояльности/дисконтные карты (7101xx) — скидка на них не промо
    prefixes = tuple(p.strip() for p in args.stop_cat_prefixes.split(",") if p.strip())
    if prefixes:
        catstr = df["CAT_EXT"].apply(lambda v: str(int(v)) if pd.notna(v) else "")
        df.loc[catstr.str.startswith(prefixes), "Статус"] = "вне: стоп-категория (пакеты/лояльность/табак)"
    ok = df["Статус"] == IN_SET
    df.loc[ok & df["PRICE_EFF"].isna(), "Статус"] = "вне: нет цены"
    ok = df["Статус"] == IN_SET
    df.loc[ok & (df[shops_col].fillna(0) < min_shops), "Статус"] = "вне: меньше половины магазинов"
    if "CONTACTS_2W" in df.columns:
        ok = df["Статус"] == IN_SET
        df.loc[ok & (df["CONTACTS_2W"].fillna(0) < args.min_contacts_2w),
               "Статус"] = "вне: мало покупателей за 2 недели"
    ok = df["Статус"] == IN_SET
    df.loc[ok & df["SKU"].isin(prev_ids), "Статус"] = "вне: прошлая волна (ротация)"
    if args.price_segment != "all":
        ok = df["Статус"] == IN_SET
        df.loc[ok & (df["Ценовой сегмент"] != args.price_segment), "Статус"] = "вне: фильтр ценового сегмента"
    if args.exclude_perm_discount:
        ok = df["Статус"] == IN_SET
        df.loc[ok & df["Скидка круглогодичная"], "Статус"] = "вне: круглогодичная скидка"

    # «неинтересные» категории — вне волны (вводная Елены 18.08): группы с совсем
    # малыми продажами. Порог не экспертный: k-средних (3 кластера) по log ТО
    # групп, нижний кластер отсекается; из базы покрытия они тоже уходят
    grp_to = ((pd.to_numeric(df["CHECKS_2W"], errors="coerce") * df["PRICE_EFF"])
              .groupby(df["LIMIT_CAT"]).sum())
    grp_to = grp_to[grp_to.index != ""]
    small_cats: set = set(grp_to.index[grp_to <= 0])
    pos = grp_to[grp_to > 0]
    if len(pos) >= 10 and pos.nunique() >= 3:
        lab = kmeans3(np.log10(pos.to_numpy(float)))
        small_cats |= set(pos.index[lab == 0])
    ok = df["Статус"] == IN_SET
    df.loc[ok & df["LIMIT_CAT"].isin(small_cats), "Статус"] = "вне: мелкая категория (мало продаж)"

    # прогнозный слой (общий для обеих моделей)
    # K заранее — по кандидатам после фильтров (тот же сэмпл клиент x товар):
    # выбор скидки и портфельный PL считают корзину визита одинаково
    k0 = 1.0
    if sample is not None and len(sample):
        cand_ids = set(df.loc[df["Статус"] == IN_SET, "ID_PRODUCT"].dropna())
        sp0 = sample[sample["ID_PRODUCT"].isin(cand_ids)]
        if len(sp0):
            k0 = max(1.0, len(sp0) / sp0["ID_CONTACT"].nunique())
    df = demand_response(df, grid, args.retention, k0)
    # ПРИРОСТ — дополнительный вес ранжира (вводная Елены 18.08): ранжир =
    # трафик-скор x охват x прирост покупок при выбранной скидке.
    # Товар с δ=0 (скидка уже действует, прироста нет) уходит в хвост.
    uplift_i = (1 - df["Скидка доп. (инкремент)"].fillna(0)) ** df["BETA"].fillna(0) - 1
    df["RANK_VAL"] = df[goal_col].fillna(0) * df["CONTACTS"] * uplift_i.clip(lower=0)
    bad = (df["Статус"] == IN_SET) & df["BETA"].notna() & (~df["_маржинальный"].astype(bool))
    df.loc[bad, "Пометка"] = "немаржинальный при любой скидке сетки"
    pf = (df["Статус"] == IN_SET) & df["Скидка круглогодичная"]
    df.loc[pf, "Пометка"] = (df.loc[pf, "Пометка"] + "; ").str.lstrip("; ") + "круглогодичная скидка"
    if "DISCOUNT_2W" in df.columns and df["DISCOUNT_2W"].notna().any():
        df["Скидка группы (реальная 2 нед.)"] = df.groupby("CAT_EXT")["DISCOUNT_2W"].transform("mean")
    else:
        df["Скидка группы (реальная 2 нед.)"] = df["CAT_EXT"].map(ass.groupby("CAT_EXT")["REAL_SALE"].mean())

    # ДВЕ МОДЕЛИ на общих данных. В каждой:
    #   1) отбор -> портфельный прогноз;
    #   2) SKU с отрицательным PL выкидываются и ЗАМЕНЯЮТСЯ следующими по рангу
    #      (итерации до сходимости) — вводная Елены 18.08;
    #   3) гарантия покрытия категорий: в непокрытую категорию добавляется её
    #      лучший покупаемый SKU (CONTACTS_2W >= порога) поверх фильтра
    #      магазинов — цель «скидка в >=90% категорий»; без покупаемых SKU
    #      категория остаётся без скидки. Добавленные для покрытия из
    #      PL-выбраковки исключены (их минус — цена покрытия).
    stop_status = "вне: стоп-категория (пакеты/лояльность/табак)"

    def run_model(select_fn):
        banned: set = set()
        dfx, order = df.copy(), []
        for _ in range(12):
            dfx = df.copy()
            if banned:
                m = dfx["ID_PRODUCT"].isin(banned) & (dfx["Статус"] == IN_SET)
                dfx.loc[m, "Статус"] = "вне: немаржинальный (заменён)"
            cov = (Coverage(sample, 1.0 / args.sample_share)
                   if sample is not None and len(sample) else None)
            order, _g = select_fn(dfx, cov, args)
            # замене подлежат «совсем в минус»: SKU, немаржинальные при ЛЮБОЙ
            # скидке сетки (даже минимальной) — на их место встают следующие
            sel = dfx.loc[order]
            losers = set(sel["ID_PRODUCT"][~sel["_маржинальный"].astype(bool)].dropna())
            if not losers:
                break
            banned |= losers
        def apply_variant(i, var):
            d_, dq_, v_, a_, dto_, cost_, pl_, delta_ = var
            dfx.at[i, "Скидка модели"] = d_
            dfx.at[i, "Скидка доп. (инкремент)"] = delta_
            dfx.at[i, "ΔQ покупок"] = dq_
            dfx.at[i, "Доп. визиты (независ.)"] = v_
            dfx.at[i, "Добавка в чек"] = a_
            dfx.at[i, "Стоимость скидки ₽"] = cost_
            dfx.at[i, "_dto_i"] = dto_
            dfx.at[i, "_pl_i"] = pl_

        def best_positive_var(g):
            cand = [v for v in g if v[7] > 0 and v[4] > 0]  # δ>0 и доп. ТО>0
            return max(cand, key=lambda v: v[6]) if cand else None

        # товары с действующей скидкой ОСТАЮТСЯ в наборе (их скидки пойдут в
        # коммуникацию); доплата сети по ним нулевая, тип виден в промо-листе

        # покрытие до цели (>=90%) — по УКРУПНЁННЫМ группам (LIMIT_CAT).
        # Группа с уже действующей реальной скидкой >= минимальной ступени
        # сетки считается покрытой ФАКТОМ — нулевые строки для неё не нужны
        disc_g = pd.to_numeric(df["DISCOUNT_2W"], errors="coerce").groupby(df["LIMIT_CAT"]).max()
        real_cov = set(disc_g.index[disc_g >= min(grid)]) - {""}
        covered = set(dfx.loc[order, "LIMIT_CAT"].dropna()) | real_cov
        interesting = (df.loc[~df["LIMIT_CAT"].isin(small_cats), "LIMIT_CAT"]
                       .replace("", np.nan).dropna().nunique())
        target = int(np.ceil(args.cat_coverage * interesting))
        addable = dfx[(dfx["LIMIT_CAT"] != "") & ~dfx["LIMIT_CAT"].isin(covered)
                      & ~dfx["LIMIT_CAT"].isin(small_cats)
                      & (pd.to_numeric(dfx["CONTACTS_2W"], errors="coerce") > 0)
                      & dfx["PRICE_EFF"].notna()
                      & (dfx["Статус"] != stop_status)
                      & (dfx["Статус"] != "вне: прошлая волна (ротация)")
                      & (dfx["Статус"] != "вне: мелкая категория (мало продаж)")]
        best = (addable.sort_values("CONTACTS_2W", ascending=False)
                .drop_duplicates("LIMIT_CAT"))
        for i in best.index:
            if len(covered) >= target:
                break
            g = dfx.at[i, "_grid"]
            var = best_positive_var(g) if isinstance(g, list) else None
            if var is None:
                continue
            apply_variant(i, var)
            dfx.at[i, "Статус"] = IN_SET
            dfx.at[i, "Пометка"] = "добавлен для покрытия категории"
            order.append(i)
            covered.add(dfx.at[i, "LIMIT_CAT"])

        # ДОГОН до целевого доп. ТО (вводная Елены: минимум 200 млн): скидки
        # углубляются шагами с максимальной прибавкой ТО на рубль потери PL
        # (сначала «бесплатные» шаги с ростом PL, затем самые эффективные)
        portfolio_adjust(dfx, order, sample, args.retention)
        port = float(pd.to_numeric(dfx.loc[order, "Доп. ТО ₽"], errors="coerce").fillna(0).sum())
        indep = float(pd.to_numeric(dfx.loc[order, "_dto_i"], errors="coerce").fillna(0).sum())
        ratio = port / indep if indep > 0 else 1.0
        target_to = args.target_add_to / max(ratio, 1e-9)

        def best_step(i):
            g = dfx.at[i, "_grid"]
            if not isinstance(g, list):
                return None
            cur_d = dfx.at[i, "Скидка модели"]
            cur_dto = dfx.at[i, "_dto_i"]
            cur_pl = dfx.at[i, "_pl_i"]
            bst = None
            for var in g:
                if var[0] <= cur_d or not (var[4] > cur_dto):
                    continue
                dpl = var[6] - cur_pl
                eff = float("inf") if dpl >= 0 else (var[4] - cur_dto) / (-dpl)
                key = (0 if dpl >= 0 else 1, -eff)
                if bst is None or key < bst[0]:
                    bst = (key, var)
            return bst

        heap: list = []
        for n_i, i in enumerate(order):
            b = best_step(i)
            if b:
                heapq.heappush(heap, (b[0], n_i, i, b[1][0]))
        while indep < target_to and heap:
            key, n_i, i, var_d = heapq.heappop(heap)
            b = best_step(i)
            if b is None:
                continue
            if b[0] != key or b[1][0] != var_d:      # оценка устарела — перевзвесить
                heapq.heappush(heap, (b[0], n_i, i, b[1][0]))
                continue
            indep += b[1][4] - dfx.at[i, "_dto_i"]
            apply_variant(i, b[1])
            nb = best_step(i)
            if nb:
                heapq.heappush(heap, (nb[0], n_i, i, nb[1][0]))

        gain = portfolio_adjust(dfx, order, sample, args.retention)
        return dfx, order, gain

    df_a, order_a, gain_a = run_model(select_by_scores)
    res_a = finalize(df_a, order_a, gain_a)

    if df["BETA"].notna().any():
        df_b, order_b, gain_b = run_model(select_by_estimate)
        res_b = finalize(df_b, order_b, gain_b)
        comp = comparison_sheet(res_a, res_b)
    else:  # без Э1 оценок нет — модель «Оценка» пропускается
        res_b, comp = None, None

    seg_a = res_a[res_a["Статус"] == IN_SET]["Ценовой сегмент"].value_counts()
    seg_b = (res_b[res_b["Статус"] == IN_SET]["Ценовой сегмент"].value_counts()
             if res_b is not None else pd.Series(dtype=int))
    own_beta = int((df["BETA_SRC"] == "SKU (сжатие)").sum())
    params = pd.DataFrame([
        ("Цель волны", GOAL_NAME[args.goal]),
        ("Модель «Скоры»: ранжир", f"{goal_col} x CONTACTS (скор x охват)"),
        ("Модель «Оценка»: отбор", "lazy greedy max-coverage по предельному вкладу в цель"),
        ("Скидка", "max d сетки с PL >= 0 (по-товарно, из эластичности)"),
        ("Ретеншн базы", f"{args.retention:.0%} придут и без промо — их скидка каннибализирована, "
                         f"корзину несут только {1 - args.retention:.0%} новых визитов"),
        ("Сетка скидок", args.discount_grid),
        ("Маржа", MARGIN),
        ("Окно скоров / волна", f"{SCORE_DAYS} дн. / {WAVE_DAYS} дн. (коэф. {WAVE_SHARE:.2f})"),
        ("Эластичность: SKU с собственной β", f"{own_beta} из {int(df['BETA'].notna().sum())}"),
        ("Эластичность: медианная β",
         round(float(df["BETA"].median()), 2) if df["BETA"].notna().any() else "—"),
        ("Сжатие к категории K", args.shrink_k),
        ("Медианный цикл T, дни",
         round(float(df["T_MEDIAN"].median()), 1) if df["T_MEDIAN"].notna().any() else "—"),
        ("Медианная φ (в новые визиты)", round(float(df["PHI"].median()), 2)),
        ("Покрытие", f"сэмпл клиент x товар, доля {args.sample_share}"
                     if sample is not None and len(sample) else "нет сэмпла — визиты без пересечений"),
        ("Сегментация цены", f"k-средних (3 кластера) по {seg_source} внутри категории — границы из данных"),
        ("Лимит категории", f"{args.max_per_cat_share:.0%} её активных SKU, мин. 1, потолок {args.max_per_cat}"),
        ("Лимит SKU на сегмент в категории", args.max_per_seg),
        ("Порог магазинов", f">= {min_shops:.0f} из {max_shops:.0f} "
                            f"({'SHOPS_2W из БД' if shops_col == 'SHOPS_2W' else 'ассортимент.xlsx — запасной'})"),
        ("Порог покупателей за 2 недели",
         args.min_contacts_2w if "CONTACTS_2W" in df.columns else "нет данных CONTACTS_2W"),
        ("Стоп-категории (префиксы ext)", args.stop_cat_prefixes),
        ("Круглогодичная скидка", f"скидка >= {args.perm_min_disc:.0%} в >= {args.perm_weeks_share:.0%} недель; "
                                  f"{'исключаются' if args.exclude_perm_discount else 'только пометка'}"),
        ("Набор «Скоры»: дешёвый/средний/премиальный",
         " / ".join(str(int(seg_a.get(s, 0))) for s in SEG_ORDER)),
        ("Набор «Оценка»: дешёвый/средний/премиальный",
         " / ".join(str(int(seg_b.get(s, 0))) for s in SEG_ORDER)),
    ], columns=["Параметр", "Значение"])
    return res_a, res_b, comp, params


def main() -> None:
    ap = argparse.ArgumentParser(description="Две модели подбора товаров для массового промо + сравнение")
    ap.add_argument("--goal", choices=list(GOAL_COL), default="freq")
    ap.add_argument("--orgs", default="1,3", help="1 — оффлайн, 3 — екомм (раздельно)")
    ap.add_argument("--score", default=str(BASE / "data" / "I_PRODUCT_SCORE_NEW.xlsx"))
    ap.add_argument("--score-sheet", default="I_PRODUCT_SCORE")
    ap.add_argument("--mapping", default=str(BASE / "data" / "I_PRODUCT_SCORE_NEW.xlsx"),
                    help="маппинг external + названия (лист «выгрузка» того же файла)")
    ap.add_argument("--mapping-sheet", default="выгрузка")
    ap.add_argument("--elasticity", default=str(BASE / "data" / "e0.csv"),
                    help="Э1 — дневные агрегаты SKU (эластичность)")
    ap.add_argument("--cycles", default=str(BASE / "data" / "e1.csv"),
                    help="Э2 — межпокупочные циклы")
    ap.add_argument("--sample", default=str(BASE / "data" / "e3.csv"),
                    help="Э3 — сэмпл клиент x товар (пока копируется — возьмётся из model_data.csv)")
    ap.add_argument("--sample-share", type=float, default=0.02)
    ap.add_argument("--assortment", default=str(BASE / "data" / "ассортимент.xlsx"))
    ap.add_argument("--prev-wave", default=None)
    ap.add_argument("--perm-discount", default=str(BASE / "data" / "e2.csv"),
                    help="Э4 — товары с постоянной (круглогодичной) скидкой")
    ap.add_argument("--totals", default=str(BASE / "data" / "e4.csv"),
                    help="Э5 — базовые показатели сети (чеки/клиенты/ТО за 2 недели)")
    ap.add_argument("--retention", type=float, default=0.80,
                    help="доля клиентов базы, которые придут и без промо (вводная Елены 18.08); "
                         "корзину несут только (1−retention) новых визитов")
    ap.add_argument("--perm-weeks-share", type=float, default=0.9)
    ap.add_argument("--perm-min-disc", type=float, default=0.05)
    ap.add_argument("--exclude-perm-discount", action="store_true")
    # 640214 пакеты на кассе; 7101 акции лояльности/карты;
    # 610901/610904/610906 сигареты/электронные/стики — скидки на табак ЗАПРЕЩЕНЫ (Елена, 18.08.2026)
    ap.add_argument("--stop-cat-prefixes", default="640214,7101,610901,610904,610906",
                    help="префиксы ID_CATEGORY_5_ext служебных категорий: 640214 — пакеты "
                         "на кассе, 7101 — акции лояльности/дисконтные карты")
    ap.add_argument("--price-segment", choices=["all"] + SEG_ORDER, default="all")
    ap.add_argument("--cat-coverage", type=float, default=0.90,
                    help="целевая доля категорий со скидкой (Елена, 18.08)")
    ap.add_argument("--target-add-to", type=float, default=300_000_000,
                    help="целевой ТОВАРНЫЙ доп. ТО волны, ₽ (Елена, 19.08: 200-400 млн); "
                         "догон углубляет скидки в пределах исторических потолков")
    ap.add_argument("--max-per-cat", type=int, default=5,
                    help="потолок SKU на категорию ext")
    ap.add_argument("--max-per-cat-share", type=float, default=0.10,
                    help="лимит категории = доля от числа её активных SKU (мин. 1)")
    ap.add_argument("--max-per-seg", type=int, default=2)
    ap.add_argument("--min-shops-share", type=float, default=0.5)
    ap.add_argument("--min-contacts-2w", type=int, default=1000)
    ap.add_argument("--discount-grid", default="0.10,0.15,0.20,0.25,0.30,0.35,0.40",
                help="номиналы; фактическая глубина каждой ступени ограничена историей цен категории")
    ap.add_argument("--min-days", type=int, default=14)
    ap.add_argument("--min-price-points", type=int, default=3)
    ap.add_argument("--shrink-k", type=float, default=30.0)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    grid = sorted(float(x) for x in args.discount_grid.split(","))
    orgs = [int(x) for x in args.orgs.split(",")]

    score = pd.read_excel(args.score, sheet_name=args.score_sheet)
    for c in ["FREQ_SCORE", "LOYALTY_SCORE", "CHECK_SCORE", "PRICE_INDEX",
              "CONTACTS", "CHECKS", "SHOPS_2W", "CONTACTS_2W", "CHECKS_2W",
              "PRICE_2W", "DISCOUNT_2W"]:
        if c in score.columns:
            score[c] = pd.to_numeric(score[c], errors="coerce")
    for c in ["FREQ_SCORE", "LOYALTY_SCORE", "CHECK_SCORE", "PRICE_INDEX"]:
        if c in score.columns:
            score[c] = score[c].clip(0, 1)
    if "DISCOUNT_2W" in score.columns:
        score["DISCOUNT_2W"] = score["DISCOUNT_2W"].clip(0, 1)

    mp = Path(args.mapping)
    mapping = (pd.read_excel(mp, sheet_name=args.mapping_sheet)
               if mp.suffix.lower() in (".xlsx", ".xls") else read_table(mp))
    mapping["SKU"] = mapping["ID_PRODUCT_EXTERNAL"].map(norm_id)
    mapping = mapping.rename(columns={"PRODUCT_NAME": "Название БД",
                                      "I_CATEGORY_5": "Категория (название)"})
    map_cols = ["ID_PRODUCT", "SKU"]
    for c in ("ID_CATEGORY_5_ext", "Название БД", "Категория (название)"):
        if c in mapping.columns:
            map_cols.append(c)
    if "PRICE_INDEX" in mapping.columns and "PRICE_INDEX" not in score.columns:
        map_cols.append("PRICE_INDEX")

    ass = load_assortment(Path(args.assortment))

    # Выгрузки: чистые CSV (с заголовком или без — e0..e3) либо SSMS-дамп со
    # всеми блоками одним файлом — определяем по первой строке
    el_path = Path(args.elasticity)
    with open(el_path, encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline()
    dump: dict[str, pd.DataFrame] = {}
    if len([x for x in first_line.replace(";", ",").split(",") if x.strip()]) < 3:
        dump = parse_ssms_dump(el_path)
        print("Разобран SSMS-дамп:", {k: f"{len(v):,}" for k, v in dump.items()})
        daily_all = dump.get("daily")
        if daily_all is None:
            print("ВНИМАНИЕ: нет Э1 (дневные агрегаты) — эластичности не будет:\n"
                  "  считается только модель «Скоры», прогнозные колонки пустые до выгрузки Э1 (e0)")
            daily_all = pd.DataFrame(columns=["ID_ORGANIZATION", "ID_PRODUCT", "DATA", "Q", "PRICE"])
    else:
        daily_all = load_extract(el_path, "daily")
    cycles_all = dump.get("cycles")
    if cycles_all is None:
        cycles_all = load_extract(Path(args.cycles), "cycles") if Path(args.cycles).exists() else \
            pd.DataFrame(columns=["ID_ORGANIZATION", "ID_PRODUCT", "T_MEDIAN"])
    sample_all = dump.get("sample")
    if sample_all is None and Path(args.sample).exists():
        sample_all = load_extract(Path(args.sample), "sample")
    if sample_all is None and (BASE / "data" / "model_data.csv").exists():
        sample_all = parse_ssms_dump(BASE / "data" / "model_data.csv").get("sample")
        if sample_all is not None:
            print("Сэмпл клиент x товар взят из data/model_data.csv (дамп)")
    if sample_all is None:
        print("ВНИМАНИЕ: нет сэмпла клиент x товар — пересечения аудиторий не учитываются")
    perm_all = dump.get("perm")
    if perm_all is None:
        perm_all = load_extract(Path(args.perm_discount), "perm") \
            if Path(args.perm_discount).exists() else None
    if perm_all is None:
        print("ВНИМАНИЕ: нет данных Э4 — круглогодичные скидки не помечаются")
    totals_all = dump.get("totals")
    if totals_all is None:
        totals_all = load_extract(Path(args.totals), "totals") if Path(args.totals).exists() else None
    if totals_all is None:
        print("ВНИМАНИЕ: нет Э5 (base_totals) — влияние на средний чек сети не посчитать")

    next_monday = date.today() + timedelta(days=(7 - date.today().weekday()) % 7 or 7)
    out = Path(args.out) if args.out else BASE / "output" / f"массовое_промо_волна_{next_monday}.xlsx"

    sheets: list[tuple[str, pd.DataFrame]] = []
    promo_sheets: list[tuple[str, pd.DataFrame]] = []   # рабочие списки — первыми в файле
    cov_name = f"coverage_check_{next_monday}.sql"      # точное покрытие — скриптом в БД
    cov_sets: dict[int, list] = {}
    for org in orgs:
        label = ORG_LABEL.get(org, str(org))
        part = score[(score["ID_ORGANIZATION"] == org) & (score["CONTACTS"] > 0)].copy()
        if part.empty:
            print(f"[{label}] в файле скоров нет строк ID_ORGANIZATION={org} — пропуск")
            continue
        part = part.merge(mapping[map_cols].drop_duplicates("ID_PRODUCT"), on="ID_PRODUCT", how="left")
        if "CAT_EXT" not in part.columns and "ID_CATEGORY_5_ext" in part.columns:
            part["CAT_EXT"] = part["ID_CATEGORY_5_ext"]
        cat_of = dict(zip(part["ID_PRODUCT"], part.get("CAT_EXT", part.get("ID_CATEGORY_5_ext"))))
        daily = daily_all[daily_all["ID_ORGANIZATION"] == org]
        est, cat_beta, glob_beta, cat_dmax, glob_dmax = estimate_elasticity(
            daily, cat_of, args.min_days, args.min_price_points, args.shrink_k)
        cycles = cycles_all[cycles_all["ID_ORGANIZATION"] == org]
        sample = sample_all[sample_all["ID_ORGANIZATION"] == org] if sample_all is not None else None
        perm = perm_all[perm_all["ID_ORGANIZATION"] == org] if perm_all is not None else None
        prev_ids = prev_wave_ids(Path(args.prev_wave), label) if args.prev_wave else set()

        res_a, res_b, comp, params = process_org(
            part, ass, est, cat_beta, glob_beta, cat_dmax, glob_dmax,
            cycles, sample, perm, prev_ids, args, grid)
        promo = build_promo_list(res_a, grid)
        promo_sheets.append((f"Промо ({label})", promo))
        trow = None
        if totals_all is not None:
            tr = totals_all[totals_all["ID_ORGANIZATION"] == org]
            if len(tr):
                trow = tr.iloc[0].to_dict()
        promo_sheets.append((f"Прогноз ({label})",
                             build_forecast(res_a, promo, trow, f"output/{cov_name}")))
        cov_sets[org] = [int(p) for p in
                         res_a.loc[res_a["Статус"] == IN_SET, "ID_PRODUCT"].dropna()]
        sheets.append((f"Скоры ({label})", res_a))
        if res_b is not None:
            sheets.append((f"Оценка ({label})", res_b))
            sheets.append((f"Сравнение ({label})", comp))
        sheets.append((f"Параметры ({label})", params))
        na = int((res_a["Статус"] == IN_SET).sum())
        if res_b is not None:
            nb = int((res_b["Статус"] == IN_SET).sum())
            inter = len(set(res_a[res_a["Статус"] == IN_SET]["SKU"])
                        & set(res_b[res_b["Статус"] == IN_SET]["SKU"]))
            print(f"[{label}] «Скоры»: {na} SKU · «Оценка»: {nb} SKU · пересечение: {inter}")
        else:
            print(f"[{label}] «Скоры»: {na} SKU · «Оценка» пропущена (нет Э1)")

    if not sheets:
        raise SystemExit("Нет данных ни по одной организации — проверь файл скоров")
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        for name, frame in promo_sheets + sheets:
            frame.to_excel(xw, sheet_name=name[:31], index=False)
            ws = xw.sheets[name[:31]]
            # форматирование: ширины колонок, жирная шапка, разделители тысяч —
            # поячеечно (в смешанных колонках вроде «Значение» числа тоже форматируются)
            for j, col in enumerate(frame.columns, 1):
                width = max(len(str(col)) + 2,
                            int(frame[col].astype(str).str.len().quantile(0.9)) + 2
                            if len(frame) else 10)
                ws.column_dimensions[ws.cell(row=1, column=j).column_letter].width = min(width, 45)
                head = ws.cell(row=1, column=j)
                head.font = head.font.copy(bold=True)
                for i in range(2, len(frame) + 2):
                    c = ws.cell(row=i, column=j)
                    if isinstance(c.value, (int, float)) and abs(c.value) >= 1000:
                        c.number_format = "#,##0"
            ws.freeze_panes = "A2"
    # macOS вешает quarantine на файлы из песочницы — Excel отказывается их открывать
    subprocess.run(["xattr", "-d", "com.apple.quarantine", str(out)], capture_output=True)
    if cov_sets:
        write_coverage_sql(BASE / "output" / cov_name, cov_sets)
        print(f"Скрипт точного покрытия (прогнать в БД): output/{cov_name}")
    print(f"Результат: {out}")


if __name__ == "__main__":
    main()
