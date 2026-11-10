# Типичная цена покупки категории CVM: коды категорий из I_CLUSTER_CATEGORY_SCORE (кластер 0, малая витрина, nolock)
# × снимок «Товары» (duckdb, PRICE2W = оборот / количество за 2 недели, CHECKS_WEEK) — медиана по чекам. Только чтение.
import json, glob, os, statistics
import pyodbc, duckdb
from app.config import Settings
cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30); cn.timeout = 120
cur = cn.cursor()
cur.execute("""select ID_CVM_CATEGORY, CATEGORY_NAME, EXT_CODES, EXT_CODES_PART from dbo.I_CLUSTER_CATEGORY_SCORE (nolock)
               where ID_COMPANY = 1 and ID_ORGANIZATION = 1 and CLUSTER = 0""")
cats = cur.fetchall()
os.chdir('/srv/cvm_api')
f = sorted(glob.glob('data/products/products_*.duckdb'))[-1]
c = duckdb.connect(f, read_only=True)
prod = c.execute("""select d.ID_PRODUCT, d.EXT, d.C2, d.C3, d.C4, d.C5, a.PRICE2W, a.CHECKS_WEEK, a.IN_SALE, a.SHOPS2W
                    from dict d join activity a on a.ID_PRODUCT = d.ID_PRODUCT where a.IN_SALE and a.PRICE2W > 0""").fetchall()
by_code = {}
for p in prod:
    for code in p[2:6]:
        if code: by_code.setdefault(str(code), []).append(p)
def wmedian(items):
    items = sorted(items); tot = sum(w for _, w in items); acc = 0
    for v, w in items:
        acc += w
        if acc >= tot / 2: return v
for cid, name, codes, part in cats:
    seen = {}
    for code in (codes or '').replace(';', ',').split(','):
        code = code.strip()
        for p in by_code.get(code, []): seen[p[0]] = p
    items = [(p[6], p[7] or 0) for p in seen.values() if (p[7] or 0) > 0]
    print(json.dumps({'id': cid, 'name': name, 'codes': codes, 'part': part, 'sku': len(seen),
                      'median_price': round(wmedian(items), 1) if items else None,
                      'checks_week': round(sum(w for _, w in items))}, ensure_ascii=False))
print(json.dumps({'snapshot': f}))
