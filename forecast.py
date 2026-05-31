"""Прогноз метрик акций на основе исторических аналогов из CVM offline.

Формула из CVM offline: PL = Доп ТО × 0.30 − Скидка
(проверено на 10+ октябрьских акциях 2025: PL сходится с копейками).
"""
from __future__ import annotations

import re
import statistics
from collections import defaultdict
from typing import Iterable

import pandas as pd

MARGIN = 0.30  # маржа из CVM offline для расчёта PL
CATEGORY_NARROW_BREADTH = "NARROW"
CATEGORY_WIDE_BREADTH = "WIDE"


def _num(s) -> float | None:
    if s is None:
        return None
    t = str(s).strip().replace("\xa0", "").replace(" ", "").replace("%", "")
    if not t or t.lower() in ("nan", "none"):
        return None
    t = t.replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def _pct(s) -> float | None:
    """Парсит ячейку процента: '5%'→0.05, '0.05'→0.05, '1%'→0.01, '50'→0.5."""
    if s is None:
        return None
    t = str(s).strip().replace("\xa0", "").replace(" ", "")
    if not t or t.lower() in ("nan", "none"):
        return None
    has_pct = "%" in t
    t = t.replace("%", "").replace(",", ".")
    try:
        v = float(t)
    except ValueError:
        return None
    if has_pct or v > 1:
        return v / 100
    return v


def _int(s) -> int | None:
    v = _num(s)
    if v is None:
        return None
    return int(v)


def _discount_rub(row: dict) -> float | None:
    """В CVM offline две колонки 'Скидка': условие (%) и рубли.
    После дедупа рублёвая — 'Скидка_1'. Пробуем оба варианта."""
    for k in ("Скидка_1", "Скидка"):
        v = _num(row.get(k))
        if v is not None and v > 100:  # рубли как метрика — обычно сотни-тысячи
            return v
        if k == "Скидка_1" and v is not None:
            return v
    return None


def _seg_group(seg: str) -> str:
    s = (seg or "").lower()
    has_active = "актив" in s or "нов" in s
    has_churn = "отток" in s or "спящ" in s
    if has_active and has_churn:
        return "ALL_SEG"
    if has_active:
        return "ACTIVE_NEW"
    if has_churn:
        return "CHURN_SLEEP"
    return "NICHE"


def _mech_type(name: str, desc: str, mech: str, bonus: str) -> str:
    text = " ".join([name or "", desc or "", mech or ""]).lower()
    bonus_s = str(bonus or "").strip()
    has_threshold = bool(re.search(r"чек\s*от|при\s+покупке|на\s+чек", text))

    if "дарим" in text and ("монет" in text or "бонус" in text or "балл" in text):
        return "BONUS_THRESHOLD" if has_threshold else "BONUS_GIFT"
    if "кешбэк" in text or "кэшбэк" in text or "cashback" in text:
        return "CASHBACK_PCT"
    if "купон" in text:
        return "COUPON"
    if "%" in (name or "") and has_threshold:
        return "DISCOUNT_THRESHOLD"
    if "%" in (name or ""):
        return "DISCOUNT_PCT"
    if bonus_s and bonus_s.replace("р", "").replace(".", "").replace(",", "").isdigit():
        return "BONUS_GIFT"
    return "OTHER"


def _cat_breadth(category: str) -> str:
    c = (category or "").strip().lower()
    if c in ("", "все товары", "все", "список товаров"):
        return CATEGORY_WIDE_BREADTH
    return CATEGORY_NARROW_BREADTH


def classify(row: dict) -> tuple[str, str, str]:
    return (
        _seg_group(row.get("Сегмент", "")),
        _mech_type(
            row.get("Название промо", ""),
            row.get("Описание акции", ""),
            row.get("Механика", ""),
            row.get("Бонусы", ""),
        ),
        _cat_breadth(row.get("Категория", "")),
    )


def build_analogue_db(df: pd.DataFrame) -> dict[tuple, list[dict]]:
    """Группа -> список заполненных исторических акций с per-client-метриками."""
    analogues: dict[tuple, list[dict]] = defaultdict(list)
    for _, r in df.iterrows():
        rd = r.to_dict()
        clients = _int(rd.get("Примерное количество клиентов"))
        otklik = _pct(rd.get("отклик"))
        dop_to = _num(rd.get("Доп ТО (план), р."))
        if not clients or not otklik or not dop_to:
            continue
        disc = _discount_rub(rd) or 0.0
        key = classify(rd)
        analogues[key].append({
            "num": str(rd.get("НОМЕР", "")).strip(),
            "name": str(rd.get("Название промо", "")).strip(),
            "otklik": otklik,
            "dop_to_pc": dop_to / clients,
            "disc_pc": disc / clients,
            "clients": clients,
        })
    return analogues


def _existing_plan(row: dict) -> dict | None:
    """Если в CVM offline уже стоят отклик+ДопТО — берём как план."""
    otklik = _pct(row.get("отклик"))
    dop_to = _num(row.get("Доп ТО (план), р."))
    if otklik is None or dop_to is None:
        return None
    disc = _discount_rub(row) or 0.0
    pl = _num(row.get("PL"))
    if pl is None:
        pl = dop_to * MARGIN - disc
    return {
        "source": "план",
        "otklik": otklik,
        "dop_to": dop_to,
        "disc": disc,
        "pl": pl,
        "analogue_n": 0,
        "analogue_ids": [],
    }


def _match_analogues(key: tuple, analogues: dict) -> tuple[list[dict], str]:
    """Поиск аналогов с расширением группы при нехватке."""
    seg, mech, cat = key

    exact = analogues.get(key, [])
    if len(exact) >= 2:
        return exact, "точный (сегмент+механика+категория)"

    same_seg_mech: list[dict] = list(exact)
    for k, v in analogues.items():
        if k == key:
            continue
        if k[0] == seg and k[1] == mech:
            same_seg_mech += v
    if len(same_seg_mech) >= 2:
        return same_seg_mech, "сегмент+механика"

    same_mech: list[dict] = list(same_seg_mech)
    for k, v in analogues.items():
        if k[1] == mech and k not in {(seg, mech, c) for c in (CATEGORY_WIDE_BREADTH, CATEGORY_NARROW_BREADTH)}:
            same_mech += v
    if len(same_mech) >= 2:
        return same_mech, "только механика"

    same_seg: list[dict] = list(same_seg_mech)
    for k, v in analogues.items():
        if k[0] == seg:
            same_seg += v
    if same_seg:
        return same_seg, "только сегмент"

    flat = [m for vs in analogues.values() for m in vs]
    return flat, "глобальная медиана"


def forecast_row(row: dict, analogues: dict) -> dict:
    plan = _existing_plan(row)
    if plan:
        return plan

    key = classify(row)
    matches, level = _match_analogues(key, analogues)
    if not matches:
        return {
            "source": "нет аналогов",
            "otklik": None, "dop_to": None, "disc": None, "pl": None,
            "analogue_n": 0, "analogue_ids": [],
            "clients_est": None,
        }

    clients = _int(row.get("Примерное количество клиентов"))
    clients_estimated = False
    if not clients:
        # Оценка клиентов = медиана из аналогов того же уровня матчинга
        client_pool = [m["clients"] for m in matches if m.get("clients")]
        if not client_pool:
            return {
                "source": "нет клиентов и нет оценки",
                "otklik": None, "dop_to": None, "disc": None, "pl": None,
                "analogue_n": 0, "analogue_ids": [],
                "clients_est": None,
            }
        clients = int(statistics.median(client_pool))
        clients_estimated = True

    otklik = statistics.median([m["otklik"] for m in matches])
    dop_to_pc = statistics.median([m["dop_to_pc"] for m in matches])
    disc_pc = statistics.median([m["disc_pc"] for m in matches])

    dop_to = clients * dop_to_pc
    disc = clients * disc_pc
    pl = dop_to * MARGIN - disc

    src = f"прогноз: {level} ({len(matches)} аналогов)"
    if clients_estimated:
        src += f"; клиенты оценены ({clients:,})".replace(",", " ")

    return {
        "source": src,
        "otklik": otklik,
        "dop_to": dop_to,
        "disc": disc,
        "pl": pl,
        "analogue_n": len(matches),
        "analogue_ids": [m["num"] for m in matches[:5]],
        "clients_est": clients if clients_estimated else None,
    }


def forecast_dataframe(promos_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
    """Считает прогноз по списку акций. history_df — источник аналогов."""
    analogues = build_analogue_db(history_df)
    out_rows = []
    for _, r in promos_df.iterrows():
        d = r.to_dict()
        f = forecast_row(d, analogues)
        real_clients = _int(d.get("Примерное количество клиентов"))
        clients_out = real_clients if real_clients else f.get("clients_est")
        out_rows.append({
            "НОМЕР": str(d.get("НОМЕР", "")).strip(),
            "Название промо": d.get("Название промо", ""),
            "Сегмент": d.get("Сегмент", ""),
            "Старт акции": d.get("Старт акции", ""),
            "Окончание акции": d.get("Окончание акции", ""),
            "Канал": d.get("Каналы коммуникации", ""),
            "Категория": d.get("Категория", ""),
            "Бонус/скидка": d.get("Бонусы", "") or d.get("Скидка", "") or "",
            "Кол-во клиентов": clients_out,
            "Отклик, %": round(f["otklik"] * 100, 2) if f["otklik"] is not None else None,
            "Доп ТО, ₽": round(f["dop_to"]) if f["dop_to"] is not None else None,
            "Скидка, ₽": round(f["disc"]) if f["disc"] is not None else None,
            "PL, ₽": round(f["pl"]) if f["pl"] is not None else None,
            "Источник": f["source"],
            "Аналоги": ", ".join(f.get("analogue_ids", [])),
        })
    return pd.DataFrame(out_rows)


# ─── Идеи акций ──────────────────────────────────────────────────────────────

SEASONAL_HINTS: dict[int, list[tuple[str, str]]] = {
    1: [("шампанское/новогодний алкоголь", "после-НГ распродажа"), ("сладости", "сладкое для каникул")],
    2: [("сладости/цветы", "14 февраля"), ("бритвы/гели для бритья", "23 февраля"), ("сыры", "масленица")],
    3: [("цветы/конфеты", "8 марта"), ("куличи/творог", "Пасха/пост"), ("крупы/овощи", "Великий пост")],
    4: [("куличи/яйца", "Пасха"), ("дача/семена", "открытие сезона"), ("шашлык/маринады", "майские")],
    5: [("шашлык/уголь/соусы", "майские"), ("мороженое", "жара"), ("газировка/вода", "жара")],
    6: [("мороженое/лимонад", "лето"), ("шашлык", "пикники"), ("детские товары", "1 июня")],
    7: [("мороженое/напитки", "лето"), ("корм для дачных питомцев", "дачный сезон")],
    8: [("школьные товары", "начало учебного года"), ("консервация/огурцы/банки", "заготовки")],
    9: [("школьные товары", "сентябрь"), ("чай/кофе", "осень"), ("суповые наборы", "холода")],
    10: [("Хеллоуин", "тыквы/сладости"), ("горячие напитки", "осень"), ("консервы/каши", "холода")],
    11: [("Чёрная пятница", "масштабные распродажи"), ("кофе/чай", "холода")],
    12: [("новогодние товары/мандарины", "НГ"), ("шампанское/икра", "НГ-стол"), ("подарки/сладости", "НГ")],
}


def _to_date(val, year_hint: int = 2026):
    from datetime import datetime, date
    if val is None or str(val).strip() in ("", "nan", "None"):
        return None
    s = str(val).strip().rstrip(".")
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.strptime(s, "%d.%m").replace(year=int(year_hint)).date()
    except ValueError:
        return None


def segment_coverage(promos_df: pd.DataFrame) -> dict[str, int]:
    coverage: dict[str, int] = defaultdict(int)
    for _, r in promos_df.iterrows():
        for s in ("Активные", "Новые", "Спящие", "Отток", "Мамы", "Кофе"):
            if s.lower() in str(r.get("Сегмент", "")).lower():
                coverage[s] += 1
    return dict(coverage)


def top_historical_by_pl_per_client(history_df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Топ исторических акций по PL/клиент — это шаблоны для копирования."""
    rows = []
    for _, r in history_df.iterrows():
        clients = _int(r.get("Примерное количество клиентов"))
        dop_to = _num(r.get("Доп ТО (план), р."))
        pl = _num(r.get("PL"))
        if not clients or pl is None or not dop_to:
            continue
        rows.append({
            "НОМЕР": str(r.get("НОМЕР", "")).strip(),
            "Название": r.get("Название промо", ""),
            "Сегмент": r.get("Сегмент", ""),
            "Категория": r.get("Категория", ""),
            "Клиенты": clients,
            "PL, ₽": round(pl),
            "PL/клиент, ₽": round(pl / clients, 2),
            "Доп ТО/клиент, ₽": round(dop_to / clients, 2),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("PL/клиент, ₽", ascending=False).head(n)


def gap_ideas(promos_df: pd.DataFrame, month_num: int) -> list[str]:
    """Эвристики по пробелам в плане месяца."""
    ideas: list[str] = []
    coverage = segment_coverage(promos_df)
    for seg in ("Активные", "Спящие", "Отток", "Новые"):
        if coverage.get(seg, 0) == 0:
            ideas.append(
                f"⚠️ В месяце нет ни одной акции для сегмента **{seg}** — "
                f"добавьте хотя бы 1 акцию (исторически для каждого сегмента 3–5 акций в месяц)."
            )
    if month_num in SEASONAL_HINTS:
        cat_in_plan = " ".join(str(promos_df.get("Категория", pd.Series([])).fillna("").astype(str).str.lower().tolist()))
        for cat_key, reason in SEASONAL_HINTS[month_num]:
            cat_main = cat_key.split("/")[0].strip()
            if cat_main.lower() not in cat_in_plan:
                ideas.append(f"🌿 Сезонная идея: добавьте акцию на **{cat_key}** — {reason}.")
    return ideas
