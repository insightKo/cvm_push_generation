"""Разовый батч-фикс июльских акций в черновике условий.

Для каждой акции из cache/conditions_draft.json:
  • проставляет год кампании во все даты (срок сгорания, текст купона);
  • дозаполняет cat5-«Категории» (resolve_cat5_codes) и ПРОВЕРЯЕТ их скиллом verify_cat5_codes;
  • перепроверяет deeplink в «Кнопке» (только постоянные разделы каталога).

Пишет обратно в черновик и кладёт сводку в output/july_review.xlsx — НЕ трогает Google Sheets.
Запуск:  python output/patch_july.py
"""
import json
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import ai_generator as g  # noqa: E402
import app  # noqa: E402
from config import SPREADSHEET_ID  # noqa: E402

DRAFT = ROOT / "cache" / "conditions_draft.json"
REVIEW = ROOT / "output" / "july_review.xlsx"


def _num(x) -> str:
    return str(x).replace(".0", "").strip()


def main() -> None:
    draft = json.loads(DRAFT.read_text(encoding="utf-8"))
    results = draft["results"]

    # Входные поля акций из листа (Год/Месяц/даты/категория/канал) по НОМЕРу
    df = app.load_cvm_data(SPREADSHEET_ID)
    df.columns = [c.strip() for c in df.columns]
    df["_num"] = df["НОМЕР"].apply(_num)
    by_num = {r["_num"]: r for _, r in df.iterrows()}

    review_rows = []
    for res in results:
        num = _num(res.get("__promo_num", ""))
        src = by_num.get(num, {})
        promo = {
            "Название промо": res.get("__promo_name") or src.get("Название промо", ""),
            "Год": str(src.get("Год", "")).strip(),
            "Месяц": str(src.get("Месяц", "")).strip(),
            "Старт акции": str(src.get("Старт акции", "")).strip(),
            "Окончание акции": str(src.get("Окончание акции", "")).strip(),
            "Категория": res.get("Категория") or src.get("Категория", ""),
            "Описание акции": res.get("Описание акции", ""),
            "Каналы коммуникации": str(src.get("Каналы коммуникации", "")).strip(),
        }

        # 1) год кампании во все даты
        g._fix_campaign_years(res, promo)

        # 2) cat5 — дозаполнить, если пусто, и проверить скиллом
        cat5_note = ""
        if not str(res.get("Категории", "")).strip():
            c5 = g.resolve_cat5_codes(promo)
            codes = c5.get("codes", [])
            if codes:
                ver = g.verify_cat5_codes(promo["Категория"], codes)
                ok = [c for c in codes if c in set(ver.get("ok", codes))]
                wrong = ver.get("wrong", [])
                res["Категории"] = "\n".join(ok)
                cat5_note = f"{len(ok)} cat5"
                if wrong:
                    cat5_note += f"; проверка убрала {len(wrong)}"
            elif c5.get("broad"):
                cat5_note = "широкая (все товары) — cat5 не нужны"
            else:
                cat5_note = "не подобрано"
        else:
            cat5_note = "уже было"

        # 3) deeplink — только постоянный раздел каталога
        dl_note = ""
        chan = promo["Каналы коммуникации"].lower()
        if "slip" in chan or "слип" in chan:
            dl_note = "slip — без кнопки"
        else:
            dl = g.find_best_deeplink(promo["Категория"]) or g.find_best_deeplink(promo["Название промо"])
            if dl and not g._is_evergreen_category(dl.get("category", "")):
                dl = None
            if not dl:
                dl = g.resolve_deeplink_ai(promo["Категория"], promo["Название промо"])
            if dl:
                btn = res.get("Кнопка", "")
                text = re.sub(r"\s*dixyapp://\S+", "", btn).strip() or "В каталог"
                res["Кнопка"] = f"{text} {dl['deeplink']}"
                dl_note = dl["category"]
            else:
                dl_note = "каталог"

        review_rows.append({
            "НОМЕР": num,
            "Название": promo["Название промо"][:50],
            "Категория": promo["Категория"][:30],
            "cat5": cat5_note,
            "deeplink → раздел": dl_note,
            "Срок сгорания": res.get("Срок сгорания бонусов", ""),
            "Купон заполнен": "да" if str(res.get("Текст на информационном купоне / слип-чеке", "")).strip() else "",
        })
        print(f"  {num}: год✓ | cat5: {cat5_note} | deeplink: {dl_note}")

    DRAFT.write_text(json.dumps(draft, ensure_ascii=False, indent=1), encoding="utf-8")
    pd.DataFrame(review_rows).to_excel(REVIEW, index=False)
    print(f"\nЧерновик обновлён. Сводка: {REVIEW}")


if __name__ == "__main__":
    main()
