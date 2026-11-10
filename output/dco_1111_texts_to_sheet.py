"""ДЦО ср 11.11.2026: заголовки и тексты пушей 101497–101522 → лист PUSH (строки, записанные dco_1111_to_sheet.py).
Шаблон Елены 10–11.11: заголовок «11.11. 🫰-X% на <товар>», текст «и ещё 1000 продуктов со скидками до 70%. Закупись нужными продуктами в ДИКСИ.»
Источник — output/ДЦО_2026-11-11_черновик.tsv. Перед записью — бэкап колонок N:P; номер промо в строке (L) сверяется с номером текста.
Запуск: .venv/Scripts/python.exe output/dco_1111_texts_to_sheet.py <папка_бэкапа> [--overwrite]
"""
import csv, datetime as dt, json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent; sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402
sys.stdout.reconfigure(encoding="utf-8")

rows = list(csv.reader(open(ROOT / "output/ДЦО_2026-11-11_черновик.tsv", encoding="utf-8"), delimiter="\t"))
h = rows[0]; it, ib = h.index("PUSH заголовок"), h.index("текст PUSH")
tx = {r[0]: (r[it], r[ib]) for r in rows[1:]}
assert len(tx) == 26

ws = sc.get_spreadsheet().worksheet(sc.SHEET_PUSH)
v = ws.get_all_values(value_render_option="FORMULA")
assert v[0][11] == "Номер промо"
hit = [(i, str(r[11]).strip()) for i, r in enumerate(v[1:], start=2) if len(r) > 11 and str(r[11]).strip() in tx]
assert [n for _, n in hit] == sorted(tx), hit
first, last = hit[0][0], hit[-1][0]
assert last - first == 25, (first, last)
bdir = Path(sys.argv[1]); bdir.mkdir(parents=True, exist_ok=True)
stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
(bdir / f"backup_push_N_P_{first}_{last}_{stamp}.json").write_text(
    json.dumps([r[13:16] for r in v[first - 1:last]], ensure_ascii=False), encoding="utf-8")
if "--overwrite" not in sys.argv:
    assert all(not str(r[13]).strip() and not str(r[15]).strip() for r in v[first - 1:last]), "в строках уже есть тексты (перезапись — --overwrite)"

data = []
for i, n in hit:
    t, b = tx[n]
    data += [{"range": f"N{i}", "values": [[t]]}, {"range": f"P{i}", "values": [[b]]}]
ws.batch_update(data, value_input_option="RAW")

chk = ws.get_values(f"L{first}:P{last}")
for r in chk:
    print(r[0], "|", r[2], "|", r[4], "|", r[3], r[5] if len(r) > 5 else "")
