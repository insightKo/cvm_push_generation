# Делает раннер для чтения через cvm-api на ru: SQL из файла (после ревью sql-senior) → python-скрипт, печатает строки результата JSON.
import json, sys
sql = open(sys.argv[1], encoding='utf-8').read()
out = sys.argv[2]
open(out, 'w', encoding='utf-8').write(f'''import pyodbc, json, decimal, datetime
from app.config import Settings
cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30, autocommit=True); cn.timeout = 1500
cur = cn.cursor()
cur.execute({json.dumps(sql, ensure_ascii=False)})
while cur.description is None:
    if not cur.nextset(): break
cols = [d[0] for d in cur.description]
def cv(v):
    if isinstance(v, decimal.Decimal): return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)): return v.isoformat()
    return v
for r in cur.fetchall():
    print(json.dumps({{c: cv(v) for c, v in zip(cols, r)}}, ensure_ascii=False))
''')
