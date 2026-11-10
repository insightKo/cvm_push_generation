import pyodbc
from app.config import Settings
cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30); cur = cn.cursor()
cur.execute("""select t.name, p.rows from sys.tables t join sys.partitions p on p.object_id = t.object_id and p.index_id in (0,1)
 where t.name like '%CVM[_]CATEGOR%'""")
for r in cur.fetchall(): print(r)
for t in ('I_CVM_CATEGORY', 'I_CVM_CATEGORY_PRODUCT', 'CVM_CATEGORY_PRODUCT'):
    cur.execute("select c.name from sys.columns c where c.object_id = object_id(?)", 'dbo.' + t)
    print(t, [r[0] for r in cur.fetchall()])
