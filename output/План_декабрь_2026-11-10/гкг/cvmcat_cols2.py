import pyodbc
from app.config import Settings
cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30); cur = cn.cursor()
for t in ('I_CVM_CATEGORY_MAP', 'I_CVM_CATEGORY_RULE'):
    cur.execute("select c.name from sys.columns c where c.object_id = object_id(?) order by column_id", 'dbo.' + t)
    print(t, [r[0] for r in cur.fetchall()])
    cur.execute(f"select top 3 * from dbo.{t} (nolock)")
    for r in cur.fetchall(): print('  ', r)
cur.execute("""select top 5 c.name from sys.columns c join sys.tables t on t.object_id = c.object_id where t.name = 'I_PRODUCT' and c.name like '%CVM%'""")
print('I_PRODUCT cvm cols', cur.fetchall())
