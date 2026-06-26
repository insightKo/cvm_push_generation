"""Построить индекс категорий из ассортимента для подбора cat5 (CVM offline).

Источник: data/ассортимент.xlsx (есть ID_CATEGORY_5/4/3_ext + PRODUCT_NAME, но НЕТ
названий категорий). Сопоставлять «категорию акции → cat5» по названиям отдельных товаров
ненадёжно (слово «фрукты» не встречается в «БАНАНЫ ВЕС», зато встречается в конфетах
«с фруктами»). Поэтому работаем на уровне ГРУПП cat4: у каждой cat4 есть характерные
товары (БАНАНЫ, ЯБЛОКИ, ОГУРЦЫ…), и вся группа разворачивается в свои cat5.

Результат: data/cat5_catalog.json со структурой:
  {
    "cat4": { "<cat4>": {"cat3":int, "n":int, "names":[...], "cat5":[<cat5>,...]} },
    "cat5_names": { "<cat5>": [названия товаров] }   # для проверки/отображения
  }

Запуск разовый (или при обновлении ассортимента):  python build_cat5_catalog.py
"""
import json
from pathlib import Path

import pandas as pd

SRC = Path(__file__).resolve().parent / "data" / "ассортимент.xlsx"
OUT = Path(__file__).resolve().parent / "data" / "cat5_catalog.json"
TOP_NAMES_CAT4 = 8   # характерных товаров на группу cat4
TOP_NAMES_CAT5 = 4   # названий на cat5 (для проверки)


def _norm(s: str) -> str:
    return " ".join(str(s).strip().split())


def _top_names(grp: pd.DataFrame, n: int) -> list[str]:
    names, seen = [], set()
    for nm in grp["PRODUCT_NAME"].astype(str):
        key = _norm(nm).upper()
        if key in seen:
            continue
        seen.add(key)
        names.append(_norm(nm))
        if len(names) >= n:
            break
    return names


def main() -> None:
    df = pd.read_excel(SRC)
    df = df.dropna(subset=["ID_CATEGORY_5_ext", "ID_CATEGORY_4_ext"])
    if "COUNT_CHECK" in df.columns:
        df = df.sort_values("COUNT_CHECK", ascending=False)

    cat4_index: dict[str, dict] = {}
    for cat4, grp in df.groupby("ID_CATEGORY_4_ext"):
        cat5_codes = [str(int(c)) for c in grp["ID_CATEGORY_5_ext"].dropna().unique()]
        row0 = grp.iloc[0]
        cat4_index[str(int(cat4))] = {
            "cat3": int(row0["ID_CATEGORY_3_ext"]) if pd.notna(row0.get("ID_CATEGORY_3_ext")) else None,
            "n": int(len(grp)),
            "names": _top_names(grp, TOP_NAMES_CAT4),
            "cat5": cat5_codes,
        }

    cat5_names: dict[str, list[str]] = {}
    for cat5, grp in df.groupby("ID_CATEGORY_5_ext"):
        cat5_names[str(int(cat5))] = _top_names(grp, TOP_NAMES_CAT5)

    OUT.write_text(
        json.dumps({"cat4": cat4_index, "cat5_names": cat5_names}, ensure_ascii=False, indent=1),
        encoding="utf-8",
    )
    print(f"cat4 групп: {len(cat4_index)} | cat5: {len(cat5_names)} → {OUT}")


if __name__ == "__main__":
    main()
