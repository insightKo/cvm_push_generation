"""«Примерное количество клиентов» общих акций ЖЦ ноября 2026 (101550–101556) → Google «CVM offline», точечно по НОМЕР.

Число — клиенты на отправку из I_PROMO_OFFER (CONTROL_GROUP = 0), как у прошлых акций (101482: 80 466 = список без КГ).
Строки серий (101550_1–_3 и т. д.) получают число своего базового списка. 101557 — списка ещё нет, не пишется.
Пишется только эта колонка, перед записью — бэкап листа.
Запуск: .venv/Scripts/python.exe output/lifecycle_nov_counts.py <counts.tsv: ID_PROMO\tклиентов> <папка_бэкапа>
"""
import datetime as dt, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402
from gspread.utils import rowcol_to_a1  # noqa: E402


def main():
    src, bdir = Path(sys.argv[1]), Path(sys.argv[2]); bdir.mkdir(parents=True, exist_ok=True)
    cnt = {a: int(b) for a, b in (l.split("\t") for l in src.read_text(encoding="utf-8").splitlines() if l[:1].isdigit())}
    assert set(cnt) == {str(n) for n in range(101550, 101557)}, sorted(cnt)
    wo = sc.get_spreadsheet().worksheet(sc.SHEET_CVM_OFFLINE)
    v = wo.get_all_values(value_render_option="FORMULA")
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    (bdir / f"backup_cvm_offline_counts_{stamp}.json").write_text(json.dumps(v, ensure_ascii=False), encoding="utf-8")
    col = v[0].index("Примерное количество клиентов") + 1
    upd, found = [], set()
    for i, r in enumerate(v[1:], start=2):
        num = str(r[1]).strip(); base = num.split("_")[0]
        if base in cnt:
            found.add(base); upd.append({"range": rowcol_to_a1(i, col), "values": [[cnt[base]]]})
    assert found == set(cnt), f"нет строк в CVM offline: {sorted(set(cnt) - found)}"
    wo.batch_update(upd, value_input_option="RAW")
    print("бэкап", stamp, "| ячеек", len(upd))


if __name__ == "__main__":
    main()
