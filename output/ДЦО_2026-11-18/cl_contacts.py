# ДЦО 18.11: численность аудитории пуша по кластерам офлайна (только чтение; наружу — только счётчики).
# 1) MCI: кластер клиента последнего готового присвоения офлайна (как в output/CVM_101538_101549_v2.sql и
#    аудитории_кластеры_запрос.sql 10.11): I_CLUSTER_CONTACT — CCI 148 млн строк, читается только окно @cl_date (~7,5 млн),
#    две колонки потоком без группировки (одна строка на клиента — CVM_CLUSTER_ASSIGN.sql), порции в CSV, duckdb грузит один раз.
# 2) Проверки: журнал присвоения не менялся за время чтения; прочитано столько строк, сколько записано (ROWS_); у клиента один кластер.
# 3) duckdb на ru: соединение со снимком конструктора (сегмент, пуш+приложение, город) — аудитория как 11.11:
#    org 1, k_segment 1–3, k_push_app = 1, город в ЦФО/СЗФО. Файлы клиентов остаются на ru (/tmp/dco1811).
# Ревью sql-senior 17.11: правки 1–5 внесены.
import sys, csv, time, json, pathlib, pyodbc, duckdb
sys.path.insert(0, "/srv/cvm_api")
from app.config import Settings
t0 = time.time(); W = pathlib.Path("/tmp/dco1811"); W.mkdir(exist_ok=True)
s = Settings.from_env()
cn = pyodbc.connect(s.connection_string(), timeout=30, autocommit=True); cn.timeout = 900
cur = cn.cursor(); cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")

cur.execute("""
set nocount on;
declare @cl_log_max int = (select max(ID) from I_CLUSTER_ASSIGN_LOG (nolock));
select top (1) LOG_MAX = @cl_log_max, l.REFERENCE_DATE, l.MODEL_VERSION, l.ROWS_
from I_CLUSTER_ASSIGN_LOG as l (nolock)
where l.ID_ORGANIZATION = 1 and l.STATUS = 'OK'
	and not exists (select 1 from I_CLUSTER_ASSIGN_LOG as r (nolock)
					where r.ID_ORGANIZATION = l.ID_ORGANIZATION and r.REFERENCE_DATE = l.REFERENCE_DATE
						and r.MODEL_VERSION = l.MODEL_VERSION and r.STATUS = 'RUN' and r.ID > l.ID)
order by l.REFERENCE_DATE desc, l.ID desc;""")
row = cur.fetchone()
if row is None:
    print("СТОП: нет готового присвоения", flush=True); sys.exit(1)
log_max, cl_date, cl_ver, rows_ = row
print("присвоение", cl_date, cl_ver, "строк в журнале", rows_, "журнал до", log_max, flush=True)

cur.execute("""
select ID_CONTACT, CLUSTER_DETAIL
from I_CLUSTER_CONTACT (nolock)
where ID_ORGANIZATION = 1 and REFERENCE_DATE = cast(? as date) and MODEL_VERSION = cast(? as varchar(32))
option (maxdop 4);""", str(cl_date), cl_ver)
n = 0
with open(W / "cl.csv", "w", newline="") as f:
    wr = csv.writer(f)
    while True:
        rs = cur.fetchmany(200000)
        if not rs: break
        wr.writerows(rs); n += len(rs)
print("прочитано", n, round(time.time() - t0), flush=True)

cur.execute("""select n = count(*) from I_CLUSTER_ASSIGN_LOG (nolock)
               where ID_ORGANIZATION = 1 and REFERENCE_DATE = cast(? as date) and MODEL_VERSION = cast(? as varchar(32)) and ID > ?""",
            str(cl_date), cl_ver, log_max)
if cur.fetchone()[0]:
    print("СТОП: присвоение перезаписывалось во время чтения", flush=True); sys.exit(1)
if rows_ is not None and n != int(rows_):
    print("СТОП: прочитано", n, "а записано", rows_, flush=True); sys.exit(1)

con = duckdb.connect(str(W / "cl.duckdb"))
con.execute("SET memory_limit='3GB'; SET threads=2")
con.execute(f"CREATE OR REPLACE TABLE raw AS SELECT * FROM read_csv('{W}/cl.csv', header=false, columns={{'id_contact':'INTEGER','c':'TINYINT'}})")
con.execute("CREATE OR REPLACE TABLE cl AS SELECT id_contact, min(c) AS cl, max(c) AS cl_max FROM raw GROUP BY 1")
multi = con.execute("SELECT count(*) FROM cl WHERE cl < cl_max").fetchone()[0]
print("клиентов с кластером", con.execute("SELECT count(*) FROM cl").fetchone()[0], "несколько кластеров", multi, flush=True)

sql = open("/srv/cvm_api/sql/dco/district_cities.sql", encoding="utf-8").read()
cur.execute("\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--")))
cities = {1: [], 2: []}
for cid, did in cur.fetchall():
    if int(did) in cities: cities[int(did)].append(int(cid))
c1 = ",".join(map(str, cities[1])); c2 = ",".join(map(str, cities[2]))
snap = json.load(open("/srv/cvm_api/data/contact_bi/current.json"))["file"]
con.execute(f"ATTACH '/srv/cvm_api/data/contact_bi/{snap}' AS s (READ_ONLY)")
con.execute(f"""CREATE OR REPLACE TABLE e AS
    SELECT c.ckey, (c.ckey & 4294967295)::INT AS id_contact, c.k_segment AS seg,
           CASE WHEN c.k_city IN ({c1}) THEN 'ЦФО' ELSE 'СЗФО' END AS d, coalesce(cl.cl, 0) AS cl
    FROM s.c AS c LEFT JOIN cl ON cl.id_contact = (c.ckey & 4294967295)::INT
    WHERE c.org = 1 AND c.k_segment IN (1, 2, 3) AND c.k_push_app = 1 AND c.k_city IN ({c1},{c2})""")
print("снимок", snap, flush=True)
for r in con.execute("SELECT cl, d, seg, count(*) FROM e GROUP BY ALL ORDER BY ALL").fetchall():
    print("\t".join(map(str, r)))
print("DONE", round(time.time() - t0), flush=True)
