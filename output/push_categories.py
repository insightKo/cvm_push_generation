"""Записать cat5-коды в колонку «Категория» листа CVM offline — для июльских акций-офферов.

— Акции-офферы (скидка/кешбэк): «Категория» = плоский список cat5-кодов (через \n).
— Коммуникации/рассылки: cat5 НЕ нужны — «Категория» оставляем текстом, лишние коды чистим.

Запуск:  python output/push_categories.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import gspread  # noqa: E402
from google.oauth2.service_account import Credentials  # noqa: E402
from config import SPREADSHEET_ID  # noqa: E402

DRAFT = ROOT / "cache" / "conditions_draft.json"


def _num(x) -> str:
    return str(x).replace(".0", "").strip()


def _is_comm(mech: str) -> bool:
    return "коммуникац" in (mech or "").lower()


def main() -> None:
    draft = json.loads(DRAFT.read_text(encoding="utf-8"))
    results = draft["results"]

    offers = {}  # num -> коды для «Категория»
    for r in results:
        num = _num(r.get("__promo_num", ""))
        if _is_comm(str(r.get("Механика", ""))):
            r["Категории"] = ""  # коммуникациям cat5 не нужны
        else:
            codes = str(r.get("Категории", "") or r.get("Категория", "")).strip()
            r["Категория"] = codes  # коды идут в «Категория»
            r["Категории"] = codes
            if codes:
                offers[num] = codes
    DRAFT.write_text(json.dumps(draft, ensure_ascii=False, indent=1), encoding="utf-8")

    cred = Credentials.from_service_account_file(
        str(ROOT / "credentials" / "service_account.json"),
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    ws = gspread.authorize(cred).open_by_key(SPREADSHEET_ID).worksheet("CVM offline")
    headers = ws.row_values(1)
    num_col = headers.index("НОМЕР") + 1
    row_by_num = {_num(v): i + 1 for i, v in enumerate(ws.col_values(num_col))}
    kat_c = headers.index("Категория") + 1

    batch = []
    for num, codes in offers.items():
        srow = row_by_num.get(num)
        if srow:
            batch.append({"range": gspread.utils.rowcol_to_a1(srow, kat_c), "values": [[codes]]})
            print(f"  {num} → {len([c for c in codes.splitlines() if c.strip()])} cat5-кодов")
    if batch:
        ws.batch_update(batch, value_input_option="USER_ENTERED")
    print(f"\nЗаписано офферов: {len(batch)} (коммуникации не трогали)")


if __name__ == "__main__":
    main()
