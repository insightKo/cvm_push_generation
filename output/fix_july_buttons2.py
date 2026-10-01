"""Финальная правка кнопок июльских акций.

— Кнопка нужна ТОЛЬКО у акций-офферов (скидка/кешбэк), у коммуникаций/рассылок — НЕТ.
— Текст кнопки стандартный «В КАТАЛОГ» + реальный deeplink (URL из текущей кнопки).
— У коммуникаций кнопку очищаем.

Запуск:  python output/fix_july_buttons2.py
"""
import json
import re
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


def _is_communication(mech: str) -> bool:
    return "коммуникац" in (mech or "").lower()


def main() -> None:
    draft = json.loads(DRAFT.read_text(encoding="utf-8"))
    results = draft["results"]

    review = []
    for r in results:
        if _is_communication(str(r.get("Механика", ""))):
            r["Кнопка"] = ""  # коммуникациям кнопка не нужна
            review.append((_num(r.get("__promo_num", "")), "— (убрана)"))
        else:
            dl = re.search(r"dixyapp://\S+", str(r.get("Кнопка", "")))
            r["Кнопка"] = f"В КАТАЛОГ {dl.group(0)}" if dl else r.get("Кнопка", "")
            review.append((_num(r.get("__promo_num", "")), "В КАТАЛОГ"))

    DRAFT.write_text(json.dumps(draft, ensure_ascii=False, indent=1), encoding="utf-8")

    cred = Credentials.from_service_account_file(
        str(ROOT / "credentials" / "service_account.json"),
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    ws = gspread.authorize(cred).open_by_key(SPREADSHEET_ID).worksheet("CVM offline")
    headers = ws.row_values(1)
    num_col = headers.index("НОМЕР") + 1
    row_by_num = {_num(v): i + 1 for i, v in enumerate(ws.col_values(num_col))}
    btn_c = headers.index("Кнопка") + 1

    batch = []
    for r in results:
        srow = row_by_num.get(_num(r.get("__promo_num", "")))
        if srow:
            batch.append({"range": gspread.utils.rowcol_to_a1(srow, btn_c), "values": [[r.get("Кнопка", "")]]})
    if batch:
        ws.batch_update(batch, value_input_option="USER_ENTERED")

    print("Кнопки обновлены:")
    for num, st in review:
        print(f"  {num}: {st}")
    print(f"Ячеек: {len(batch)}")


if __name__ == "__main__":
    main()
