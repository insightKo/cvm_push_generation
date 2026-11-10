# v2: коэффициенты четырёх семейств по двум источникам (выгрузка 10.11 + снимок авг–окт), единица — акция целиком (список ЦГ),
# ₽ Доп ТО итого на клиента СПИСКА ЦГ. Эмпирический Байес — функции v1 (../eb.py): eb2 (акция → ячейка → семейство), eb3 (+ уровень механики).
import sys, re
import numpy as np, pandas as pd
sys.path.insert(0, '..')
from eb import eb2, eb3, sig_label

A = pd.read_pickle('A.pkl')
O = A[(A.ch == 'офлайн') & A.ok].copy()
CLUSTERS = {'Большая закупка продуктов', 'Большая закупка с нон-фудом', 'Вино, крепкое, кофе', 'За хлебом и водой', 'Много нон-фуда',
            'Много нон-фуда, Большая закупка с нон-фудом', 'Перекусы на ходу', 'Регулярные за продуктами', 'Редкие', 'Ультра-частые',
            'Частые с большой корзиной'}


def listkind(h):
    if h in CLUSTERS: return 'кластер'
    hl = h.lower()
    if hl in ('отток', 'отток спящие'): return 'Отток+Спящие (список)'
    if hl == 'спящие': return 'Спящие'
    if hl == 'случайные': return 'Случайные'
    if hl == 'случайные отток спящие': return 'Случайные+Отток+Спящие'
    if hl in ('активные', 'активные. омни'): return 'Активные'
    if hl in ('все оффлайн',): return 'ВСЕ офлайн'
    if 'алкоголь' in hl and 'отток' in hl: return 'Отток (алкоголь)'
    return 'Аффинитивные: ' + h


O['lk'] = O.hdr.map(listkind)
O.loc[O.ID.isin([101225]), 'lk'] = 'ВСЕ офлайн'
O.loc[O.ID.isin([101250]), 'lk'] = 'ВСЕ офлайн'      # шапка «Отток», 3,7 млн — широкий 1-дневный список (v1)
O.loc[O.ID.isin([101391]), 'lk'] = 'Отток (6,0 млн)'   # шапка «Отток», 6,02 млн — шире списков Отток+Спящие (1,0–1,4 млн)
num = lambda s: float(re.sub(r'[^\d]', '', s)) if s else np.nan


def show(C, F, par, u, title):
    print('\n=====', title, {k: v for k, v in par.items() if not isinstance(v, dict)})
    for _, c in C.sort_values('post', ascending=False).iterrows():
        g = u[u.kc == c.kc]
        print(f"{c.kc[:70]:70s} n={c.n:2d} N={c.N/1e3:8.0f}т сырой {c.r:7.2f} ±{np.sqrt(c.v0):6.2f}  → {c.post:7.2f} [{c.lo:7.2f}; {c.hi:7.2f}]  B={c.get('B', c.get('Bc', 1)):.2f}  "
              + ', '.join(f'{i}:{y:.1f}' for i, y in zip(g.ID, g.y)))


res = {}
# ---------------- а) монеты без порога (начисление), волны 3–11 дней и месячные
a = O[O.mech.eq('Начисление монет')].copy()
a['act'] = np.where(a.name.str.contains('Активируй|Акцептные', case=False), 'с «Активируй/Акцептные» в названии', 'без «Активируй» в названии')
a['size'] = a.name.str.extract(r'(\d+)\s*(?:монет|балл)')[0].astype(float).map(lambda v: '' if pd.isna(v) else f'{v:.0f} монет')
a['dk'] = np.where(a.dur >= 15, 'месяц (27–34 дн.)', 'волна (3–11 дн.)')
a['arm'] = np.where(a.N < 20000, 'A/B/C-плечо 10 тыс.', 'массовый список')
w = a[a.dk.str.startswith('волна')]
C, F, par, u = eb3(w, ['lk', 'act', 'size', 'arm'], ['lk'], ['dk'])
show(C, F, par, u, 'а) монеты без порога, волны: ячейка список×активация×размер×плечо → семейство список')
res['a_wave'] = (C, F, par, u)
# семейство по типу списка (сжатое к среднему волн)
print(F[['kf', 'nf', 'rf', 'vf', 'pf', 'vpf', 'Bf']].assign(sd=lambda x: np.sqrt(x.vpf), sdr=lambda x: np.sqrt(x.vf)).round(2).to_string())
print(a[a.dk.str.startswith('месяц')][['ID', 'name', 'lk', 'dur', 'N', 'y', 's', 'd', 'sig', 'prelim']].round(2).to_string())

# ---------------- б) «Вернём N р. монетами с покупки категории»
CT = {101445: 'сладкое', 101447: 'сладкое', 101448: 'сладкое', 101450: 'сладкое', 101466: 'сладкое',
      101467: 'напитки', 101469: 'напитки', 101472: 'напитки', 101473: 'напитки', 101474: 'напитки',
      101460: 'нон-фуд', 101461: 'нон-фуд', 101462: 'нон-фуд', 101463: 'нон-фуд', 101464: 'нон-фуд', 101465: 'нон-фуд', 101471: 'нон-фуд',
      101449: 'еда', 101451: 'еда', 101452: 'еда', 101453: 'еда', 101454: 'еда', 101455: 'еда', 101456: 'еда', 101457: 'еда',
      101458: 'еда', 101459: 'еда', 101468: 'еда', 101470: 'еда', 101446: 'еда', 101058: 'еда', 101064: 'еда', 101104: 'еда'}
b = O[O.mech.eq('Кешбэк X р. с покупки категории')].copy()
b['ct'] = b.ID.map(CT)
assert b.ct.notna().all(), b[b.ct.isna()][['ID', 'name']]
b['fam'] = 'Вернём N р. за категорию'
C, F, par, u = eb2(b, ['ct'], ['fam'])
show(C, F, par, u, 'б) Вернём N р. за категорию: акция → тип категории → семейство')
res['b'] = (C, F, par, u)
C2, F2, par2, u2 = eb2(b.assign(cl=b.hdr.where(b.lk.eq('кластер'), 'Активные (2025)')), ['cl'], ['fam'])
show(C2, F2, par2, u2, 'б2) то же по группам кластеров')
res['b_cl'] = (C2, F2, par2, u2)
print(F[['kf', 'nf', 'rf', 'vf']].assign(sd=lambda x: np.sqrt(x.vf)).round(2).to_string())

# ---------------- в) кешбэк % на категорию
c = O[O.mech.isin(['Кешбэк X% с покупки категории', 'Кешбэк X% с покупки товаров или группы товаров'])].copy()
c['pct'] = c.name.str.extract(r'(\d+)\s*%')[0].astype(float)
c['alc'] = c.name.str.contains('алкогол|пив|вин|крепк', case=False)
c['kind'] = np.where(c.dur >= 15, 'долгий (24–49 дн.)', np.where(c.alc, 'алкоголь: ' + np.where(c.lk.str.contains('Отток|Случайные'), 'Отток/Случайные', 'Активные'), c.lk))
c['pb'] = pd.cut(c.pct, [0, 15, 20, 30, 50, 100], labels=['10–15 %', '20 %', '30 %', '50 %', '100 %']).astype(str).replace('nan', 'без % в названии')
C, F, par, u = eb3(c, ['kind', 'pb'], ['kind'], ['mechc'] if False else ['ch'])
show(C, F, par, u, 'в) кешбэк % на категорию: ячейка вид списка × % → вид списка → все')
print(F[['kf', 'nf', 'rf', 'vf', 'pf', 'vpf', 'Bf']].assign(sd=lambda x: np.sqrt(x.vpf)).round(2).to_string())
res['c'] = (C, F, par, u)

# ---------------- г) купоны
d = O[O.mech.eq('Купон X р. на покупку')].copy()
def ctype(r):
    n = r['name'].lower()
    if r.ID == 101380: return 'условный (2 покупки от 1000 → купон 300)'
    if r.ID in (101394, 101395, 101396): return '100 р. на категорию алкоголя, 7 дней'
    if 'следующую' in n: return '100 р. на следующую покупку'
    if re.search(r'от\s*\d', n): return 'N р. с порогом чека'
    return '50 р. без порога'
d['ct'] = d.apply(ctype, axis=1)
C, F, par, u = eb3(d, ['ct', 'lk'], ['ct'], ['ch'])
show(C, F, par, u, 'г) купоны: ячейка тип × список → тип → все')
print(F[['kf', 'nf', 'rf', 'vf', 'pf', 'vpf', 'Bf']].assign(sd=lambda x: np.sqrt(x.vpf)).round(2).to_string())
res['d'] = (C, F, par, u)
pd.to_pickle(res, 'fam_res.pkl')
pd.to_pickle(dict(a=a, b=b, c=c, d=d), 'fam_units.pkl')
