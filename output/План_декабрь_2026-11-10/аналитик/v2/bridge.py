# Мост «сумма Доп ТО акций → ГКГ (Активные + Отток)» по месяцам.
# Доп ТО акции разносится по дням: «в акцию» — равномерно по дням старт…финиш, «после» (итого − в акцию) — по 4 дням после финиша.
# Скидка ЦГ — по дням акции. Ошибка — доля дисперсии по доле дней (f² · SE²), акции независимы.
import json
import numpy as np, pandas as pd
from load2 import load_all

D = r"C:/Projects/cvm_push_generation/output/План_декабрь_2026-11-10/"
A, _ = load_all()
X = A[~A.excl.eq('КГ несопоставима')].copy()
X['add_in'] = X.add_in.fillna(0); X['add'] = X['add'].fillna(0); X['se'] = X.se.fillna(0)
X['add_after'] = np.where(X.after_no, 0, X['add'] - X.add_in)
X.loc[X.after_no, 'add_in'] = X.loc[X.after_no, 'add']


def month_split(start, fin):
    days = pd.date_range(start, fin, freq='D')
    s = pd.Series(1.0 / len(days), index=days.strftime('%Y%m'))
    return s.groupby(level=0).sum()


rows = []
for _, r in X.iterrows():
    fin = month_split(r.start, r.fin)
    aft = month_split(r.fin + pd.Timedelta(days=1), r.fin + pd.Timedelta(days=4)) if not r.after_no else pd.Series(dtype=float)
    tot = abs(r.add_in) + abs(r.add_after)
    for ym in sorted(set(fin.index) | set(aft.index)):
        a_in = r.add_in * fin.get(ym, 0); a_af = r.add_after * aft.get(ym, 0)
        f = (abs(a_in) + abs(a_af)) / tot if tot > 0 else fin.get(ym, 0)
        rows.append(dict(ID=r.ID, ym=ym, add=a_in + a_af, var=(f * r.se) ** 2, disc=r.disc * fin.get(ym, 0), src=r.src, ch=r.ch,
                         y426=(a_in + a_af) if r.ID == 101426 else 0.0))
M = pd.DataFrame(rows)
S = M.groupby('ym').agg(n=('ID', 'nunique'), S=('add', 'sum'), var=('var', 'sum'), disc=('disc', 'sum'), s426=('y426', 'sum')).reset_index()
S['seS'] = np.sqrt(S['var'])

# ГКГ
g = [json.loads(l) for l in open(D + 'гкг/gcg_read2.jsonl', encoding='utf-8')]
G = pd.DataFrame([r for r in g if 'YEAR_MONTH' in r and r.get('t') != 'layer'])
G['ym'] = G.YEAR_MONTH.astype(str)
G['agg'] = G['ТО ЦГ'] - G['ТО ЦГ ДО'] * G['KOEF']
GG = G.groupby('ym').agg(G=('Доп ТО р', 'sum'), Gagg=('agg', 'sum'), seG=('SE', lambda s: np.sqrt((s ** 2).sum()) if s.notna().all() else np.nan),
                         calc=('Дата расчёта', 'max'), src=('Источник', 'first')).reset_index()
B = GG.merge(S, on='ym', how='left')
B['D'] = B.G - B.S                       # «база»: ГКГ минус сумма Доп ТО акций (до скидки)
B['Dn'] = B.G - (B.S - B.disc)           # то же, если акции перевести в ТО после скидки (как ГКГ)
B['seD'] = np.sqrt(B.seG ** 2 + B.seS ** 2)
pd.set_option('display.width', 250)
out = B.copy()
for c in ['G', 'Gagg', 'seG', 'S', 'seS', 'disc', 's426', 'D', 'Dn', 'seD']:
    out[c] = out[c] / 1e6
print(out[['ym', 'src', 'calc', 'G', 'Gagg', 'seG', 'n', 'S', 'seS', 's426', 'disc', 'D', 'Dn', 'seD']].round(1).to_string(index=False))
print('\nсуммы акций по месяцам (все месяцы выгрузок)')
print((S.assign(S=S.S / 1e6, seS=S.seS / 1e6, disc=S.disc / 1e6, s426=S.s426 / 1e6))[['ym', 'n', 'S', 'seS', 'disc', 's426']].round(1).to_string(index=False))
B.to_pickle('bridge.pkl'); M.to_pickle('bridge_rows.pkl')
