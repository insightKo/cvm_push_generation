"""Исправить июльские акции в листе: коды в «Категория» (только скидка/кешбэк) + тематические кнопки.

— «Категория»: для скидочных/кешбэк-акций кладём ПОДОБРАННЫЕ КОДЫ (плоский список через \n).
  Коммуникации/тематические рассылки НЕ трогаем (там остаётся текст).
— «Кнопка»: чистый тематический текст + РЕАЛЬНЫЙ deeplink (берём URL из текущей кнопки).

Запуск:  python output/fix_july_category_button.py
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import gspread  # noqa: E402
from google.oauth2.service_account import Credentials  # noqa: E402
import ai_generator as g  # noqa: E402
from config import SPREADSHEET_ID, ANTHROPIC_API_KEY  # noqa: E402

DRAFT = ROOT / "cache" / "conditions_draft.json"


def _num(x) -> str:
    return str(x).replace(".0", "").strip()


def _is_communication(mech: str) -> bool:
    return "коммуникац" in (mech or "").lower()


def main() -> None:
    draft = json.loads(DRAFT.read_text(encoding="utf-8"))
    results = draft["results"]

    # 1) Тематические тексты кнопок одним запросом
    items = []
    for r in results:
        dl = re.search(r"name=([^&\n]+)", str(r.get("Кнопка", "")))
        items.append({
            "num": _num(r.get("__promo_num", "")),
            "name": str(r.get("__promo_name", "")),
            "section": dl.group(1) if dl else "",
        })
    listing = "\n".join(f'  {it["num"]}: акция «{it["name"]}» → раздел «{it["section"]}»' for it in items)
    prompt = (
        "Для каждой акции ДИКСИ придумай КОРОТКИЙ текст кнопки (2–4 слова), тематический и подходящий "
        "под акцию, в тоне «ты». БЕЗ «В каталог», без слова «скидка», без дублирования названия раздела.\n"
        "Примеры: реппеленты → «Защититься от насекомых»; фрукты → «Выбрать фрукты»; пиво → «За пивом»;\n"
        "вода и соки → «Утолить жажду»; печенье → «К чаю».\n\n"
        f"{listing}\n\n"
        'Ответь СТРОГО JSON: {"<номер>": "<текст кнопки>", ...}'
    )
    btn_text = g._parse_response(g._anthropic_message(prompt, ANTHROPIC_API_KEY, 1200))

    # 2) Применяем к черновику
    review = []
    for r in results:
        num = _num(r.get("__promo_num", ""))
        mech = str(r.get("Механика", ""))
        # коды в «Категория» — только для скидки/кешбэка
        cat_val = None
        if not _is_communication(mech):
            codes = str(r.get("Категории", "")).split()
            if codes:
                cat_val = "\n".join(codes)
                r["Категория"] = cat_val
        # тематическая кнопка + реальный deeplink
        dl_url = re.search(r"dixyapp://\S+", str(r.get("Кнопка", "")))
        text = str(btn_text.get(num, "")).strip()
        if dl_url and text:
            r["Кнопка"] = f"{text} {dl_url.group(0)}"
        review.append((num, "коды" if cat_val else "текст(не трогаю)", text, mech))

    DRAFT.write_text(json.dumps(draft, ensure_ascii=False, indent=1), encoding="utf-8")

    # 3) Пишем «Категория» (где коды) и «Кнопка» в лист
    cred = Credentials.from_service_account_file(
        str(ROOT / "credentials" / "service_account.json"),
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    ws = gspread.authorize(cred).open_by_key(SPREADSHEET_ID).worksheet("CVM offline")
    headers = ws.row_values(1)
    num_col = headers.index("НОМЕР") + 1
    row_by_num = {_num(v): i + 1 for i, v in enumerate(ws.col_values(num_col))}
    cat_c = headers.index("Категория") + 1
    btn_c = headers.index("Кнопка") + 1

    batch = []
    for r in results:
        num = _num(r.get("__promo_num", ""))
        srow = row_by_num.get(num)
        if not srow:
            continue
        if not _is_communication(str(r.get("Механика", ""))) and str(r.get("Категории", "")).split():
            batch.append({"range": gspread.utils.rowcol_to_a1(srow, cat_c), "values": [[r["Категория"]]]})
        batch.append({"range": gspread.utils.rowcol_to_a1(srow, btn_c), "values": [[r.get("Кнопка", "")]]})
    if batch:
        ws.batch_update(batch, value_input_option="USER_ENTERED")

    print("Готово. Сводка (номер | Категория | кнопка | механика):")
    for num, cat, text, mech in review:
        print(f"  {num} | {cat:18} | {text:28} | {mech}")
    print(f"\nЯчеек записано: {len(batch)}")


if __name__ == "__main__":
    main()
