# План декабря: скоры категорий по кластерам и справочник кластеров (малые витрины, только SELECT, nolock).
import sys, json, datetime, decimal
import pyodbc
from app.config import Settings
cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30)
cn.timeout = 180
cur = cn.cursor()
def conv(v):
    if isinstance(v, decimal.Decimal): return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)): return v.isoformat()
    return v
def dump(t, sql, lim=20000):
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    n = 0
    while True:
        rs = cur.fetchmany(5000)
        if not rs or n >= lim: break
        for r in rs:
            d = {'t': t}; d.update({c: conv(v) for c, v in zip(cols, r)})
            print(json.dumps(d, ensure_ascii=False)); n += 1
    print(json.dumps({'t': 'meta', 'block': t, 'rows': n}), file=sys.stderr)



dump('score', """select CLUSTER, CLUSTER_NAME, ID_CVM_CATEGORY, ID_CVM_GROUP, CATEGORY_NAME, FREQ_INDEX, CHECK_INDEX, FREQ_INDEX_LO, CHECK_INDEX_LO,
 FREQ_RANK, CHECK_RANK, CONTACTS, CHECKS, C_ALL, DATE_FROM, DATE_TO, CALC_DT from dbo.I_CLUSTER_CATEGORY_SCORE (nolock) where ID_COMPANY = 1 and ID_ORGANIZATION = 1""")
