# Типичная цена покупки категории CVM (справочник CVM: I_CVM_CATEGORY_MAP + I_CVM_CATEGORY_RULE, малые, nolock)
# по снимку «Товары» (duckdb): медиана PRICE2W, взвешенная по CHECKS_WEEK, только товары в продаже. Только чтение.
import json, glob, os, re
import pyodbc, duckdb
from app.config import Settings
cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30); cn.timeout = 120
cur = cn.cursor()
cur.execute("select ID_CATEGORY_5_ext, MAP_METHOD, ID_CVM_CATEGORY from dbo.I_CVM_CATEGORY_MAP (nolock) where STATUS = 1")
mp = {}
for code, meth, cid in cur.fetchall(): mp[str(code).strip()] = (meth, cid)
cur.execute("select ID_CATEGORY_5_ext, RULE_ORDER, WORDS_ANY, WORDS_NONE, ID_CVM_CATEGORY from dbo.I_CVM_CATEGORY_RULE (nolock) order by ID_CATEGORY_5_ext, RULE_ORDER")
rules = {}
for code, o, wa, wn, cid in cur.fetchall(): rules.setdefault(str(code).strip(), []).append((o, wa, wn, cid))
cur.execute("select ID_CVM_CATEGORY, NAME from dbo.I_CVM_CATEGORY (nolock)")
names = dict(cur.fetchall())
samp = [r for v in rules.values() for r in v if r[1] and (',' in r[1] or '|' in r[1] or ';' in r[1])][:3]
print(json.dumps({'rule_sample': [list(map(str, s)) for s in samp]}, ensure_ascii=False))
def words(s):
    return [w.strip().upper() for w in re.split(r'[|,;]', s or '') if w.strip()]
os.chdir('/srv/cvm_api')
f = sorted(glob.glob('data/products/products_*.duckdb'))[-1]
c = duckdb.connect(f, read_only=True)
prod = c.execute("""select d.ID_PRODUCT, d.C5, upper(d.NAME), a.PRICE2W, a.CHECKS_WEEK from dict d join activity a on a.ID_PRODUCT = d.ID_PRODUCT
                    where a.IN_SALE and a.PRICE2W > 0 and a.CHECKS_WEEK > 0""").fetchall()
cat_items = {}
unm = 0
for pid, c5, nm, price, chw in prod:
    c5 = str(c5 or '').strip(); cid = None
    if c5 in rules:
        for o, wa, wn, rc in rules[c5]:
            if (not wa or any(w in nm for w in words(wa))) and not any(w in nm for w in words(wn)):
                cid = rc; break
    if cid is None and c5 in mp: cid = mp[c5][1]
    if cid is None: unm += 1; continue
    cat_items.setdefault(cid, []).append((price, chw))
def wmedian(items):
    items = sorted(items); tot = sum(w for _, w in items); acc = 0
    for v, w in items:
        acc += w
        if acc >= tot / 2: return v
for cid, items in cat_items.items():
    print(json.dumps({'id': cid, 'name': names.get(cid), 'sku': len(items), 'median_price': round(wmedian(items), 1),
                      'checks_week': round(sum(w for _, w in items))}, ensure_ascii=False))
print(json.dumps({'snapshot': f, 'unmapped_products': unm, 'products': len(prod)}))
