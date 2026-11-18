# ДЦО 18.11: маски «покупали категорию CVM» (путь конструктора MssqlSource.line — buy_live.sql, ревью sql-senior 19.10; так же 11.11).
# w6 — все категории групп CVM, в которые входят категории слотов (для шага «похожие» = та же группа справочника);
# w52 — категории слотов. Якорь — последний день витрины покупок. Только чтение; файлы остаются на ru (/tmp/dco1811/masks).
import sys, time, pathlib, datetime as dt, collections
sys.path.insert(0, "/srv/cvm_api")
import pyodbc
from app.config import Settings
from app.sqlstore import SqlStore
from app.contact_bi_buy import MssqlSource, Line, BIG, state_from_row
W = pathlib.Path("/tmp/dco1811"); out = W / "masks"; out.mkdir(parents=True, exist_ok=True)
s = Settings.from_env(); src = MssqlSource(s, SqlStore(s.sql_dir))
anchor = state_from_row(src.state()).anchor
print("anchor", anchor, flush=True)
slot_cats = {int(l.split("\t")[2]) for l in open(W / "slots18.tsv", encoding="utf-8") if l.strip() and not l.startswith("#")}
cn = pyodbc.connect(s.connection_string(), timeout=20, autocommit=True); cn.timeout = 60
cur = cn.cursor(); cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
cur.execute("select ID_CVM_CATEGORY, NODE_TYPE, ID_PARENT, STATUS from dbo.I_CVM_CATEGORY with (nolock)")
node = {int(r[0]): (int(r[1]), (int(r[2]) if r[2] is not None else None), r[3]) for r in cur.fetchall()}
def grp(i):
    seen = 0
    while i in node and node[i][0] != 1 and node[i][1] is not None and seen < 10:
        i = node[i][1]; seen += 1
    return i if i in node and node[i][0] == 1 else None
parents = {p for _, p, _ in node.values() if p is not None}
leaves = {i for i, (t, p, st) in node.items() if t != 1 and i not in parents and st == 1}
G = {grp(c) for c in slot_cats}
print("групп слотов", sorted(G), "категорий слотов", len(slot_cats), flush=True)
w6 = sorted(c for c in leaves if grp(c) in G) ; w6 = sorted(set(w6) | slot_cats)
with open(W / "cat_grp.tsv", "w") as f:
    for c in w6: f.write(f"{c}\t{grp(c)}\n")
todo = [(c, "w52") for c in sorted(slot_cats)] + [(c, "w6") for c in w6]
print("масок", len(todo), flush=True)
t0 = time.time()
for n, (c, per) in enumerate(todo, 1):
    f = out / f"t2_k{c}_{per}.bin"
    if f.exists(): continue
    for a in range(3):
        try:
            m = src.line(Line(2, int(c), per, 1, BIG), anchor); break
        except Exception as e:
            print("retry", c, per, str(e)[:150], flush=True); time.sleep(20)
    else:
        print("FAIL", c, per, flush=True); continue
    tmp = f.with_suffix(".tmp"); tmp.write_bytes(m.to_bytes()); tmp.rename(f)
    if n % 25 == 0: print(n, "из", len(todo), round(time.time() - t0), flush=True)
print("DONE", round(time.time() - t0), flush=True)
