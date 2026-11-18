# ДЦО 18.11: раздача пушей внутри кластера офлайна (присвоение 15.11), клиент один раз — правила раздачи 11.11 в границах кластера:
# 1) покупал за 6 нед. категорию товара своего кластера (акция действует в его округе) — каскадом от редкой в кластере к частой;
# 2) нет — товар кластера из группы справочника CVM, где у клиента больше всего купленных за 6 нед. категорий (ничья — более редкий);
# 3) нет и в группе — категория товара кластера за 52 нед., каскадом; 4) остаток — товар кластера по умолчанию (еда, место 1).
# Аудитория как 11.11: org 1, сегменты 1–3, пуш и приложение, город ЦФО/СЗФО (снимок конструктора); без кластера — не входят.
# Файл (id_promo, id_contact) остаётся на ru; наружу — только счётчики.
import sys, csv, time, json, pathlib, collections, pyodbc, duckdb
sys.path.insert(0, "/srv/cvm_api")
from app.config import Settings
from app.contact_bi_buy import Mask, push_mask
t0 = time.time(); W = pathlib.Path("/tmp/dco1811")
s = Settings.from_env()
cn = pyodbc.connect(s.connection_string(), timeout=20, autocommit=True); cn.timeout = 120
cur = cn.cursor(); cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
sql = open("/srv/cvm_api/sql/dco/district_cities.sql", encoding="utf-8").read()
cur.execute("\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--")))
cities = {1: [], 2: []}
for cid, did in cur.fetchall():
    if int(did) in cities: cities[int(did)].append(int(cid))
c1 = ",".join(map(str, cities[1])); c2 = ",".join(map(str, cities[2]))
S = []
for r in csv.reader(open(W / "slots18.tsv", encoding="utf-8"), delimiter="\t"):
    if not r or r[0].startswith("#"): continue
    S.append(dict(cl=int(r[0]), b=int(r[1]), cat=int(r[2]), pc=int(r[3]), ps=int(r[4]), nf=int(r[5]), dflt=int(r[6])))
cg = {int(a): int(b) for a, b in csv.reader(open(W / "cat_grp.tsv"), delimiter="\t")}
con = duckdb.connect()
con.execute("SET memory_limit='3GB'; SET threads=2")
snap = json.load(open("/srv/cvm_api/data/contact_bi/current.json"))["file"]
con.execute(f"ATTACH '/srv/cvm_api/data/contact_bi/{snap}' AS s (READ_ONLY)")
con.execute(f"ATTACH '{W}/cl.duckdb' AS q (READ_ONLY)")
con.execute(f"""CREATE TABLE e0 AS SELECT c.ckey, (c.ckey >> 32) * 4294967296 + (c.ckey & 4294967295) // 63 AS k, (c.ckey & 4294967295) % 63 AS sh,
               CASE WHEN c.k_city IN ({c1}) THEN 'ЦФО' ELSE 'СЗФО' END AS d, coalesce(cl.cl, 0)::INT AS cl, 0::BIGINT AS p, 0::BIGINT AS q
               FROM s.c AS c LEFT JOIN q.cl AS cl ON cl.id_contact = (c.ckey & 4294967295)::INT
               WHERE c.org = 1 AND c.k_segment IN (1, 2, 3) AND c.k_push_app = 1 AND c.k_city IN ({c1},{c2})""")
print("снимок", snap, "аудитория, без кластера", con.execute("SELECT count(*), count(*) FILTER (WHERE cl = 0) FROM e0").fetchone(), flush=True)
con.execute("CREATE TABLE e AS SELECT * FROM e0 WHERE cl BETWEEN 1 AND 10")
# слоты, действующие в округе: битовая маска по (кластер, округ)
ok = collections.defaultdict(int)
for x in S:
    if x["pc"]: ok[(x["cl"], "ЦФО")] |= 1 << x["b"]
    if x["ps"]: ok[(x["cl"], "СЗФО")] |= 1 << x["b"]
con.execute("CREATE TABLE dm (cl INT, d VARCHAR, ok BIGINT)")
con.executemany("INSERT INTO dm VALUES (?, ?, ?)", [(c, d, v) for (c, d), v in ok.items()])


def mask(c, per):
    f = W / "masks" / f"t2_k{c}_{per}.bin"
    if not f.exists():
        print("нет маски", c, per, flush=True); return False
    push_mask(con, "_m", Mask.from_bytes(f.read_bytes()).as_dict()); return True


bycat = collections.defaultdict(list)
for x in S: bycat[x["cat"]].append((x["cl"], 1 << x["b"]))
con.execute("CREATE TABLE t (cl INT, bit BIGINT)")
for cat, lst in bycat.items():
    con.execute("DELETE FROM t"); con.executemany("INSERT INTO t VALUES (?, ?)", lst)
    for per, col in (("w6", "p"), ("w52", "q")):
        if mask(cat, per):
            con.execute(f"""UPDATE e SET {col} = e.{col} | t.bit FROM _m f, t
                            WHERE f.K = e.k AND ((f.M >> e.sh) & 1) = 1 AND t.cl = e.cl""")
con.execute("UPDATE e SET p = e.p & dm.ok, q = e.q & dm.ok FROM dm WHERE dm.cl = e.cl AND dm.d = e.d")
print("слоты загружены", round(time.time() - t0), flush=True)
# редкость товара внутри кластера — число покупателей категории за 6 нед. среди аудитории кластера
R = []
for x in S:
    n = con.execute(f"SELECT count(*) FROM e WHERE cl = {x['cl']} AND (p >> {x['b']}) & 1 = 1").fetchone()[0]
    R.append((x["cl"], x["b"], n))
rk = {}
for c in range(1, 11):
    for i, (cl, b, n) in enumerate(sorted([r for r in R if r[0] == c], key=lambda r: (r[2], r[1]))):
        rk[(cl, b)] = i
print("покупателей слота", R, flush=True)
con.execute("CREATE TABLE r (cl INT, b INT, rk INT, grp INT)")
con.executemany("INSERT INTO r VALUES (?, ?, ?, ?)", [(x["cl"], x["b"], rk[(x["cl"], x["b"])], cg[x["cat"]]) for x in S])
first = lambda col: f"(SELECT r.b FROM r WHERE r.cl = e.cl AND ({col} >> r.b) & 1 = 1 ORDER BY r.rk LIMIT 1)"
con.execute(f"CREATE TABLE a AS SELECT e.ckey, e.cl, e.d, {first('e.p')} AS b, 1 AS step FROM e WHERE e.p <> 0")
con.execute("CREATE TABLE l AS SELECT e.ckey, e.k, e.sh, e.cl, e.d, e.q, dm.ok FROM e JOIN dm USING (cl, d) WHERE e.p = 0")
print("шаг 1", con.execute("SELECT count(*) FROM a").fetchone()[0], "остаток", con.execute("SELECT count(*) FROM l").fetchone()[0], flush=True)
# шаг 2: купленные за 6 нед. категории по группам справочника
con.execute("CREATE TABLE h (ckey BIGINT, grp INT)")
for cat, g in cg.items():
    if mask(cat, "w6"):
        con.execute(f"INSERT INTO h SELECT l.ckey, {g} FROM l JOIN _m f ON f.K = l.k AND ((f.M >> l.sh) & 1) = 1")
print("группы загружены", round(time.time() - t0), flush=True)
con.execute("""INSERT INTO a SELECT l.ckey, l.cl, l.d, arg_max(r.b, x.n * 100 - r.rk), 2
               FROM l JOIN (SELECT ckey, grp, count(*) AS n FROM h GROUP BY ckey, grp) x USING (ckey)
               JOIN r ON r.cl = l.cl AND r.grp = x.grp AND (l.ok >> r.b) & 1 = 1
               GROUP BY l.ckey, l.cl, l.d""")
con.execute("CREATE TABLE l3 AS SELECT * FROM l WHERE ckey NOT IN (SELECT ckey FROM a)")
con.execute(f"INSERT INTO a SELECT e.ckey, e.cl, e.d, {first('e.q')}, 3 FROM l3 AS e WHERE e.q <> 0")
dflt = {x["cl"]: x["b"] for x in S if x["dflt"]}
con.execute("CREATE TABLE df (cl INT, b INT)"); con.executemany("INSERT INTO df VALUES (?, ?)", list(dflt.items()))
con.execute("INSERT INTO a SELECT l3.ckey, l3.cl, l3.d, df.b, 4 FROM l3 JOIN df USING (cl) WHERE l3.ckey NOT IN (SELECT ckey FROM a)")
pm = [(x["cl"], x["b"], "ЦФО", x["pc"]) for x in S] + [(x["cl"], x["b"], "СЗФО", x["ps"]) for x in S]
con.execute("CREATE TABLE pm (cl INT, b INT, d VARCHAR, id_promo INT)"); con.executemany("INSERT INTO pm VALUES (?, ?, ?, ?)", pm)
bad = con.execute("SELECT count(*) FROM a LEFT JOIN pm USING (cl, b, d) WHERE pm.id_promo IS NULL OR pm.id_promo = 0").fetchone()[0]
con.execute("""CREATE TABLE o AS SELECT pm.id_promo, (a.ckey & 4294967295)::INT AS id_contact, a.cl, a.d, a.step
               FROM a JOIN pm USING (cl, b, d) WHERE pm.id_promo <> 0""")
tot = con.execute("SELECT count(*), count(DISTINCT id_contact) FROM o").fetchone()
con.execute(f"COPY (SELECT id_promo, id_contact FROM o ORDER BY id_promo, id_contact) TO '{W}/dco18.csv' (HEADER false)")
print("ВСЕГО", tot, "без акции", bad, "аудитория кластеров", con.execute("SELECT count(*) FROM e").fetchone()[0], flush=True)
print("ШАГИ", con.execute("SELECT step, count(*) FROM o GROUP BY 1 ORDER BY 1").fetchall())
for row in con.execute("""SELECT id_promo, count(*) n, count(*) FILTER (WHERE d='ЦФО') cfo, count(*) FILTER (WHERE d='СЗФО') szfo,
                          count(*) FILTER (WHERE step=1) s1, count(*) FILTER (WHERE step=2) s2, count(*) FILTER (WHERE step=3) s3,
                          count(*) FILTER (WHERE step=4) s4, string_agg(DISTINCT cl::VARCHAR, ',') cls FROM o GROUP BY 1 ORDER BY 1""").fetchall():
    print("P\t" + "\t".join(map(str, row)))
for row in con.execute("SELECT cl, count(*), count(*) FILTER (WHERE step=1), count(*) FILTER (WHERE step=4) FROM o GROUP BY 1 ORDER BY 1").fetchall():
    print("CL\t" + "\t".join(map(str, row)))
print("DONE", round(time.time() - t0), flush=True)
