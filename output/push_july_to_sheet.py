"""Записать исправленные июльские акции из черновика в лист «CVM offline».

Берёт cache/conditions_draft.json (уже пропатченный output/patch_july.py: год, cat5,
deeplink) и пишет поля в Google Sheets, сопоставляя строки по НОМЕРу. Проставляет
«Купон» = «да», если купон заполнен.

Запуск:  python output/push_july_to_sheet.py
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import gspread  # noqa: E402
from google.oauth2.service_account import Credentials  # noqa: E402
from config import SPREADSHEET_ID  # noqa: E402

DRAFT = ROOT / "cache" / "conditions_draft.json"
FIELDS = [
    "Описание акции", "Скидка", "Бонусы", "Механика", "Категория", "Категории",
    "Срок сгорания бонусов", "Название информационного купона для МП",
    "Текст на информационном купоне / слип-чеке", "Кнопка",
]


def _num(x) -> str:
    return str(x).replace(".0", "").strip()


def main() -> None:
    results = json.loads(DRAFT.read_text(encoding="utf-8"))["results"]

    cred_path = ROOT / "credentials" / "service_account.json"
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(str(cred_path), scopes=scopes)
    ws = gspread.authorize(creds).open_by_key(SPREADSHEET_ID).worksheet("CVM offline")

    headers = ws.row_values(1)
    num_col = headers.index("НОМЕР") + 1
    num_cells = ws.col_values(num_col)  # включая заголовок (строка 1)
    row_by_num = {_num(v): i + 1 for i, v in enumerate(num_cells)}  # 1-based строка листа

    batch = []
    written = 0
    for res in results:
        num = _num(res.get("__promo_num", ""))
        srow = row_by_num.get(num)
        if not srow:
            print(f"  ! {num}: строка не найдена в листе — пропуск")
            continue
        for field in FIELDS:
            if field in headers and res.get(field, "") != "":
                col = headers.index(field) + 1
                batch.append({"range": gspread.utils.rowcol_to_a1(srow, col), "values": [[res.get(field, "")]]})
        # Купон = да, если есть текст купона
        if str(res.get("Текст на информационном купоне / слип-чеке", "")).strip() and "Купон" in headers:
            col = headers.index("Купон") + 1
            batch.append({"range": gspread.utils.rowcol_to_a1(srow, col), "values": [["да"]]})
        written += 1
        print(f"  {num} → строка {srow}")

    if batch:
        ws.batch_update(batch, value_input_option="USER_ENTERED")
    print(f"\nЗаписано акций: {written}, ячеек: {len(batch)}")


if __name__ == "__main__":
    main()
