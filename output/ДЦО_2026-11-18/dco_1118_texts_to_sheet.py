"""ДЦО ср 18.11.2026: заголовки и тексты пушей 101579–101643 → лист PUSH (строки, записанные dco_1118_to_sheet.py).
Шаблон Елены 10–11.11 (ДЦО 11.11): заголовок «18.11. 🫰-X% на <товар>», текст «и ещё 1000 продуктов со скидками до 70%.
Закупись нужными продуктами в ДИКСИ.» Источник — promos18.json (поле title). Перед записью — бэкап колонок N:P;
номер промо в строке (L) сверяется с номером текста. Пишется только после ОК Елены на тексты.
Запуск: .venv/Scripts/python.exe "output/ДЦО_2026-11-18/dco_1118_texts_to_sheet.py" <promos18.json> <папка_бэкапа> [--overwrite]
"""
import datetime as dt, json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]; sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402
sys.stdout.reconfigure(encoding="utf-8")

BODY = "и ещё 1000 продуктов со скидками до 70%. Закупись нужными продуктами в ДИКСИ."
args = [a for a in sys.argv[1:] if not a.startswith("--")]
P = json.load(open(args[0], encoding="utf-8"))
tx = {str(p["num"]): (p["title"], BODY) for p in P}
assert len(tx) == 65

ws = sc.get_spreadsheet().worksheet(sc.SHEET_PUSH)
v = ws.get_all_values(value_render_option="FORMULA")
assert v[0][11] == "Номер промо"
hit = [(i, str(r[11]).strip()) for i, r in enumerate(v[1:], start=2) if len(r) > 11 and str(r[11]).strip() in tx]
assert [n for _, n in hit] == sorted(tx), hit
first, last = hit[0][0], hit[-1][0]
assert last - first == 64, (first, last)
bdir = Path(args[1]); bdir.mkdir(parents=True, exist_ok=True)
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
