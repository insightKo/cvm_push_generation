# Чтение слоёв эффекта dbo.I_CVM_EFFECT_CG (только SELECT, nolock) для разбора таргетинга (план декабря 2026, аналитик).
# Запуск: ssh kz 'ssh ru "docker exec -i cvm-api python -"' < cg_read.py > cg_layers.jsonl
# Строки — JSON {"t": <блок>, ...}. Блоки: cnt (число строк по периоду и каналу), cols (колонки витрины по одной строке),
# cg (слои «итого», входящие в итог акции, акции со стартом 01.10.2025–31.07.2026).
import sys, json, datetime, decimal
import pyodbc
from app.config import Settings

cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30)
cn.timeout = 300
cur = cn.cursor()
D1, D2 = '2025-10-01', '2026-08-01'


def conv(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


def dump(t, sql, params=()):
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    n = 0
    while True:
        rows = cur.fetchmany(5000)
        if not rows:
            break
        for r in rows:
            d = {'t': t}
            d.update({c: conv(v) for c, v in zip(cols, r)})
            print(json.dumps(d, ensure_ascii=False))
            n += 1
    print(json.dumps({'t': 'meta', 'block': t, 'rows': n}), file=sys.stderr)
    sys.stdout.flush()
    return cols


# 1. сколько строк попадёт в выгрузку (сначала число строк — потом данные)
cur.execute("""select PERIOD_ID, CHANNEL_ID, count(*) as N
               from dbo.I_CVM_EFFECT_CG with (nolock)
               where ID_COMPANY = 1 and START_DATE >= ? and START_DATE < ?
               group by PERIOD_ID, CHANNEL_ID""", (D1, D2))
cnt = cur.fetchall()
total3 = 0
for p, c, n in cnt:
    print(json.dumps({'t': 'cnt', 'PERIOD_ID': p, 'CHANNEL_ID': c, 'N': n}))
    if p == 3:
        total3 += n
if total3 > 250000:
    print(json.dumps({'t': 'stop', 'why': f'строк итого {total3} > 250000'}))
    sys.exit(0)

# 2. какие колонки есть в витрине (одна строка одной акции, без системного каталога)
cols = dump('cols', "select top (1) * from dbo.I_CVM_EFFECT_CG with (nolock) where ID_PROMO = 101075 and PERIOD_ID = 3")

# 3. слои «итого», входящие в итог акции
want = ['ID_PROMO', 'ID_ORGANIZATION', 'CHANNEL_ID', 'SEGMENT_ID', 'LAYER_NO', 'WEEK_FROM', 'WEEK_TO',
        'X_FROM', 'X_TO', 'X_BAND', 'CLIENTS_TG', 'CLIENTS_CG', 'TO_BEFORE_TG', 'TO_BEFORE_CG',
        'TO_PERIOD_TG', 'TO_PERIOD_CG', 'ADD_TO', 'ADD_TO_SE', 'CLIENTS_TG_ALL', 'CLIENTS_CG_ALL']
sel = ', '.join(c for c in want if c in cols)
dump('cg', f"""select {sel}
               from dbo.I_CVM_EFFECT_CG with (nolock)
               where ID_COMPANY = 1 and START_DATE >= ? and START_DATE < ? and PERIOD_ID = 3 and IN_TOTAL = 1""", (D1, D2))
cn.close()
