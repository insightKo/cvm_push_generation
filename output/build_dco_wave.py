# -*- coding: utf-8 -*-
"""Набор ДЦО (трафик, ОФФЛАЙН) — выборка из последней волны массового промо.

Берём готовый прогон promo_product_model.py (массовое_промо_волна_*.xlsx),
канал оффлайн, и оставляем товары с УГЛУБЛЕНИЕМ скидки (сеть доплачивает
разницу) — по прогнозному доп. трафику. Углублений в волне всего 73, поэтому
до 100 позиций набор добирается только действующими скидками (без доплаты
сети) — включается флагом TOPUP.
Листы и колонки — как в исходном файле волны.
"""
from pathlib import Path
import subprocess

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent
SRC = BASE / "массовое_промо_волна_2026-08-24.xlsx"
OUT = BASE / "ДЦО_волна_трафик_4.xlsx"

TOP_N = 100
TOPUP = True         # добить набор до TOP_N товарами, отсечёнными лимитом категории
LABEL = "оффлайн"
DEEP = "углубление (доплата сети)"


def forecast(res: pd.DataFrame, sel: pd.DataFrame) -> pd.DataFrame:
    """Повторяет build_forecast() модели на выбранном подмножестве набора."""
    s = res[res["SKU"].isin(set(sel["SKU"]))]
    n_all = len(res)
    to_all = (pd.to_numeric(res["CHECKS_2W"], errors="coerce")
              * pd.to_numeric(res["PRICE_EFF"], errors="coerce")).sum()
    to_set = (pd.to_numeric(s["CHECKS_2W"], errors="coerce")
              * pd.to_numeric(s["PRICE_EFF"], errors="coerce")).sum()
    # итоги — по самому листу «Промо»: в нём и набор, и добор
    add_v = pd.to_numeric(sel["Доп. трафик (визиты)"], errors="coerce").sum()
    add_to = pd.to_numeric(sel["Доп. ТО, ₽"], errors="coerce").sum()
    pl = pd.to_numeric(sel["PL, ₽"], errors="coerce").sum()
    disc = pd.to_numeric(sel["Скидка, ₽"], errors="coerce").sum()

    cat_col = "LIMIT_CAT" if "LIMIT_CAT" in res.columns else "CAT_EXT"
    cats_all = res[cat_col].replace("", np.nan).dropna().nunique()
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
        ("Общий доп. трафик (уникальные визиты)", round(add_v)),
        ("Общий доп. ТО, ₽", round(add_to)),
        ("PL, ₽", round(pl)),
    ]
    return pd.DataFrame(rows, columns=["Показатель", "Значение"])


def topup(res: pd.DataFrame, have: pd.DataFrame, n: int) -> pd.DataFrame:
    """Добор товаров с углублением, которые модель отсекла ЛИМИТОМ КАТЕГОРИИ
    (по охвату магазинов и марже они прошли). Берём только PL >= 0, рейтинг —
    по доп. визитам. ВАЖНО: у них считаются НЕЗАВИСИМЫЕ визиты — пересечение с
    уже набранными SKU той же категории не вычтено, это верхняя оценка."""
    c = res[(res["Статус"].isin(["вне: лимит категории",
                                 "вне: лимит сегмента в категории"]))].copy()
    c["d"] = pd.to_numeric(c["Скидка доп. (инкремент)"], errors="coerce")
    c["V"] = pd.to_numeric(c["Доп. визиты (независ.)"], errors="coerce")
    c["PL"] = pd.to_numeric(c["PL ₽"], errors="coerce")
    c = c[(c["d"] > 0) & (c["PL"] >= 0)].sort_values("V", ascending=False).head(n)
    return pd.DataFrame({
        "SKU": c["SKU"],
        "Товар": c["Название"],
        "Категория": c["Название категории"] if "Название категории" in c else c["Категория (название)"],
        "Скидка, %": (pd.to_numeric(c["Скидка модели"], errors="coerce") * 100).round(0).astype("Int64"),
        "Тип скидки": DEEP,
        "Доп. трафик (визиты)": c["V"].round(0),
        "Доп. ТО, ₽": pd.to_numeric(c["Доп. ТО ₽"], errors="coerce").round(0),
        "Скидка, ₽": pd.to_numeric(c["Стоимость скидки ₽"], errors="coerce").round(0),
        "PL, ₽": c["PL"].round(0),
    })


def main() -> None:
    xl = pd.ExcelFile(SRC)
    promo = xl.parse(f"Промо ({LABEL})")
    # лист «Промо» модель строит из res_a — это лист «Скоры», НЕ «Оценка»
    res = xl.parse(f"Скоры ({LABEL})")

    by_traffic = lambda d: d.sort_values("Доп. трафик (визиты)", ascending=False,
                                         na_position="last")
    sel = by_traffic(promo[promo["Тип скидки"] == DEEP]).head(TOP_N)
    if TOPUP and len(sel) < TOP_N:
        sel = pd.concat([sel, topup(res, sel, TOP_N - len(sel))], ignore_index=True)
    sel = by_traffic(sel).copy()
    sel["№"] = range(1, len(sel) + 1)

    # «Категория» — ext-код категории 5-го уровня (в нём работает ДЦО), а не
    # cat5-название: одно и то же имя (напр. «МЕППИНГ») висит на разных кодах.
    # Название оставлено рядом отдельной колонкой для читаемости.
    ext = res.set_index("SKU")["CAT_EXT"].astype(str)
    sel = sel.rename(columns={"Категория": "Название категории"})
    sel["Категория"] = sel["SKU"].map(ext)
    # «Тип скидки» в лист не выводим: в наборе только углубления, колонка
    # одинаковая во всех строках
    sel = sel[["№", "SKU", "Товар", "Категория", "Название категории", "Скидка, %",
               "Доп. трафик (визиты)", "Доп. ТО, ₽", "Скидка, ₽", "PL, ₽"]]

    sheets = [(f"Промо ({LABEL})", sel), (f"Прогноз ({LABEL})", forecast(res, sel))]
    with pd.ExcelWriter(OUT, engine="openpyxl") as xw:
        for name, frame in sheets:
            frame.to_excel(xw, sheet_name=name, index=False)
            ws = xw.sheets[name]
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
    subprocess.run(["xattr", "-d", "com.apple.quarantine", str(OUT)], capture_output=True)

    print(sheets[1][1].to_string(index=False))
    print(f"Результат: {OUT}")


if __name__ == "__main__":
    main()
