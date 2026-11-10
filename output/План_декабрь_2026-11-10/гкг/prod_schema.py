import duckdb, glob, os
os.chdir('/srv/cvm_api')
f = sorted(glob.glob('data/products/products_*.duckdb'))[-1]
print(f)
c = duckdb.connect(f, read_only=True)
for (t,) in c.execute('show tables').fetchall():
    print('TABLE', t, c.execute(f'select count(*) from "{t}"').fetchone())
    print(c.execute(f'describe "{t}"').fetchall())
src = open('app/products_snapshot.py', encoding='utf-8').read()
i = src.find('PRICE2W'); print(src[max(0, i-1500):i+600])
