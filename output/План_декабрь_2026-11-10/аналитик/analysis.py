# Пункты 2–6: зависимость от выгоды, сезонность, повтор, таргетинг по слоям, потолок декабря.
import numpy as np, pandas as pd
from eb import dl_tau2
pd.set_option('display.width', 230); pd.set_option('display.max_rows', 300); pd.set_option('display.max_colwidth', 70)
a = pd.read_pickle('promos.pkl'); U = pd.read_pickle('units.pkl'); cg = pd.read_pickle('cg.pkl'); R = pd.read_pickle('coef.pkl')
ok = a[a.ok].set_index('ID')


def wls(y, X, v):
    """Взвешенный МНК, веса 1/v; возвращает коэффициенты и ошибки."""
    W = 1 / v
    XtW = X.T * W
    A = np.linalg.inv(XtW @ X)
    b = A @ (XtW @ y)
    return b, np.sqrt(np.diag(A))


def show(ids, cols=('Название', 'ym', 'dur', 'N', 'y', 's', 'disc', 'pl')):
    x = ok.loc[[i for i in ids if i in ok.index], list(cols)].copy()
    x['y/скидка'] = x.y / x.disc.replace(0, np.nan)
    x['Название'] = x['Название'].str.slice(0, 45)
    return x.round(2)


print('=== П2. ЗАВИСИМОСТЬ ОТ ВЫГОДЫ И ПОРОГА')
print('-- A/B/C 18–21.12.2025 (случайные плечи одного списка): монеты без порога')
print(show([101133, 101134, 101135, 101136, 101137, 101138, 101139, 101140]).to_string())
for ids, nm in (([101133, 101134, 101135], 'Активные'), ([101136, 101137, 101138], 'Отток')):
    x = ok.loc[ids]; X = np.c_[np.ones(3), x.ben.values / 100]
    b, se = wls(x.y.values, X, x.s.values ** 2)
    print(f'  {nm}: наклон {b[1]:.2f} ± {1.96 * se[1]:.2f} ₽ на клиента на +100 монет; скидка на +100 монет '
          f'{np.polyfit(x.ben.values / 100, x.disc.values, 1)[0]:.2f} ₽')
print('-- Лестница «Активируй скидку N р. с 10N р.», Активные (3 волны + 101147)')
lad = [101147, 101159, 101160, 101161, 101162, 101181, 101182, 101183, 101184, 101244, 101245, 101246, 101247]
L = show(lad); L['ben'] = ok.loc[lad, 'ben']
# ТО «до» на клиента ЦГ (42 дня) по слоям Активных — чем отличались списки ступеней
c2 = cg[(cg.CHANNEL_ID == 1) & (cg.SEGMENT_ID == 2)].groupby('ID_PROMO').agg(X=('TO_BEFORE_TG', 'sum'), N=('CLIENTS_TG', 'sum'))
L['ТО до 42 дн. на клиента'] = (c2.X / c2.N).reindex(lad).round(0)
print(L.to_string())
x = ok.loc[lad]; tw, _, _ = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in x.groupby('ben')])
X = np.c_[np.ones(len(x)), x.ben.values / 100]
b, se = wls(x.y.values, X, x.s.values ** 2 + tw)
print(f'  наклон {b[1]:.2f} ± {1.96 * se[1]:.2f} ₽ на клиента на +100 ₽ выгоды (порог растёт вместе с выгодой), τ между волнами {np.sqrt(tw):.2f}')
for ben, g in x.groupby('ben'):
    print(f'  {ben:.0f} ₽: Σ/Σ {(g.y * g.N).sum() / g.N.sum():.2f} ₽, скидка {(g.disc * g.N).sum() / g.N.sum():.2f} ₽, волны: ' +
          ', '.join(f'{i} {ok.loc[i, "y"]:.1f}' for i in g.index))
print('-- Монеты без порога: 50 (Активные) / 100 / 200 (Отток+Спящие)')
for ids, nm in (([101154, 101173, 101210, 101265, 101324], 'Активные 50'), ([101060, 101266], 'Отток+Спящие 100'),
                ([101155, 101174, 101208, 101249], 'Отток+Спящие 200')):
    g = ok.loc[ids]
    print(f'  {nm}: Σ/Σ {(g.y * g.N).sum() / g.N.sum():.2f} ₽, скидка {(g.disc * g.N).sum() / g.N.sum():.2f} ₽, '
          f'PL {(g.pl * g.N).sum() / g.N.sum():.2f} ₽, доля погасивших {((g.disc * g.N).sum() / g.N.sum()) / g.ben.iloc[0]:.1%}')
print('-- Вернём N р. с покупки от X')
print(show([101048, 101051, 101272, 101069, 101075, 101080, 101070, 101076, 101081, 101071, 101077, 101082, 101072, 101078, 101083, 101098, 101073, 101074, 101079, 101084]).to_string())
print('-- Целевые монеты на чек от X')
print(show([101141, 101143, 101145, 101148, 101142, 101144, 101108]).to_string())
print('-- Купоны N р. (слип/пуш)')
print(show([101054, 101055, 101056, 101105, 101106, 101107, 101129, 101186, 101204, 101225, 101278, 101202, 101273]).to_string())
print('-- Кешбэк % с чека и скидка % на чек')
print(show([101271, 110014, 101053, 101149, 101086, 110003, 110008, 110015, 110106]).to_string())

print('\n=== П3. СЕЗОННОСТЬ: ноябрь–декабрь 2025 против остальных месяцев (офлайн-акции, единицы акция × сегмент)')
off = U[(U.ch == 'офлайн') & ~U.fam.str.startswith('Коммуникация') & (U.dcl != '15+ дн.')].copy()
off['nd'] = off.ym.isin(['2025-11', '2025-12']).astype(int)
off['cell'] = off.tseg + ' | ' + off.fam + ' | ' + off.dcl
both = off.groupby('cell').nd.agg(['min', 'max'])
cells = both[(both['min'] == 0) & (both['max'] == 1)].index
x = off[off.cell.isin(cells)].copy()
tw, _, _ = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in x.groupby('cell')])
D = pd.get_dummies(x.cell).astype(float).values
X = np.c_[D, x.nd.values]
b, se = wls(x.y.values, X, x.s.values ** 2 + tw)
lvl = (x[x.nd == 0].y * x[x.nd == 0].N).sum() / x[x.nd == 0].N.sum()
print(f'  ячеек с обоими периодами {len(cells)}, единиц {len(x)} (ноя–дек {x.nd.sum()}); сдвиг ноя–дек {b[-1]:.2f} ± {1.96 * se[-1]:.2f} ₽ на клиента; '
      f'уровень тех же ячеек в другие месяцы {lvl:.2f} ₽; τ {np.sqrt(tw):.2f}')
t = x.groupby(['cell', 'nd']).apply(lambda g: pd.Series({'n': g.ID.nunique(), 'N': g.N.sum(), 'y': (g.y * g.N).sum() / g.N.sum()}), include_groups=False).unstack()
print(t.round(2).to_string())
# по акциям целиком (итог «Все каналы»), те же семейства по шапке
print('-- по шапке, итог акции: ноя–дек против остальных')
pa = ok[(ok.ch == 'офлайн') & ~ok.fam.str.startswith('Коммуникация') & (ok.dcl != '15+ дн.')].copy()
pa['nd'] = pa.ym.isin(['2025-11', '2025-12']).astype(int)
pa['cell'] = pa.seg + ' | ' + pa.fam
bb = pa.groupby('cell').nd.agg(['min', 'max']); cc = bb[(bb['min'] == 0) & (bb['max'] == 1)].index
y = pa[pa.cell.isin(cc)]
tw2, _, _ = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in y.groupby('cell')])
X = np.c_[pd.get_dummies(y.cell).astype(float).values, y.nd.values]
b2, se2 = wls(y.y.values, X, y.s.values ** 2 + tw2)
print(f'  ячеек {len(cc)}, акций {len(y)} (ноя–дек {y.nd.sum()}): сдвиг {b2[-1]:.2f} ± {1.96 * se2[-1]:.2f} ₽')
print(y.groupby(['cell', 'nd']).apply(lambda g: f'{len(g)} акц., {(g.y * g.N).sum() / g.N.sum():.2f} ₽', include_groups=False).unstack().to_string())

print('\n=== П4. ПОВТОР')
series = {'Активные: 50 монет без порога': [101154, 101173, 101210, 101265, 101324],
          'Отток+Спящие: 100–200 монет без порога': [101060, 101155, 101174, 101208, 101249, 101266],
          'Активные: скидка 100 с 1000': [101147, 101159, 101181, 101244], 'Активные: скидка 150 с 1500': [101160, 101182, 101245],
          'Активные: скидка 200 с 2000': [101161, 101183, 101246], 'Активные: скидка 300 с 3000': [101162, 101184, 101247],
          'Отток+Спящие: вернём 300 с 1000 (еженедельно)': [101069, 101075, 101080], 'Спящие: вернём 300 с 1000 (еженедельно)': [101070, 101076, 101081],
          'Случайные: вернём 300 с 1000 (еженедельно)': [101071, 101077, 101082], 'Активные: вернём 300 с 1000 (еженедельно)': [101072, 101078, 101083],
          'Новые: вернём 300 с 1000 (еженедельно)': [101073, 101079, 101084],
          'Активные: 20 % кешбэк на категорию (почти еженедельно)': ok[(ok.seg == 'Активные') & (ok.fam == 'Кешбэк X% на категорию') & (ok.ben == 20)].index.tolist(),
          'Отток+Спящие: 50 % кешбэк на категорию': ok[(ok.seg == 'Отток+Спящие (список 2026)') & (ok.fam == 'Кешбэк X% на категорию') & (ok.ben == 50)].index.tolist()}
rows = []
for nm, ids in series.items():
    ids = [i for i in ids if i in ok.index]
    g = ok.loc[ids].sort_values('start').copy()
    g['k'] = np.arange(1, len(g) + 1); g['gap'] = g.start.diff().dt.days
    print(f'-- {nm}: ' + '; '.join(f'{i} {r.start:%d.%m.%y} {r.y:.1f}±{1.96 * r.s:.1f}' for i, r in g.iterrows()))
    for i, r in g.iterrows():
        rows.append(dict(series=nm, ID=i, k=r.k, gap=r.gap, y=r.y, s=r.s, N=r.N))
S = pd.DataFrame(rows)
tw, _, _ = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in S.groupby('series')])
X = np.c_[pd.get_dummies(S.series).astype(float).values, S.k.values - 1]
b, se = wls(S.y.values, X, S.s.values ** 2 + tw)
print(f'  наклон по номеру повтора (внутри серии): {b[-1]:.2f} ± {1.96 * se[-1]:.2f} ₽ на клиента за каждый следующий повтор; τ {np.sqrt(tw):.2f}')
S2 = S.dropna(subset=['gap'])
X = np.c_[pd.get_dummies(S2.series).astype(float).values, np.log(S2.gap.values)]
b, se = wls(S2.y.values, X, S2.s.values ** 2 + tw)
print(f'  по паузе от прошлой акции серии: {b[-1]:.2f} ± {1.96 * se[-1]:.2f} ₽ на клиента на ×2,7 паузы (ln дней)')
# наложение: сколько других акций офлайн шли в те же дни у той же шапки
offp = ok[(ok.ch == 'офлайн') & ~ok.fam.str.startswith('Коммуникация')].copy()
def overl(r):
    m = (offp.index != r.name) & (offp.start <= r.fin) & (offp.fin >= r.start) & (offp.seg == r.seg)
    return int(m.sum())
offp['ov'] = offp.apply(overl, axis=1)
x = offp[offp.seg.isin(['Активные', 'Отток+Спящие (список 2026)']) & (offp.dcl == '2–14 дн.')].copy()
x['cell'] = x.seg + ' | ' + x.fam
tw, _, _ = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in x.groupby('cell')])
X = np.c_[pd.get_dummies(x.cell).astype(float).values, (x.ov > 0).astype(float).values]
b, se = wls(x.y.values, X, x.s.values ** 2 + tw)
print(f'  наложение: акций {len(x)}, из них шли одновременно с другой акцией той же шапки {int((x.ov > 0).sum())}; '
      f'разница эффекта «с наложением − без» {b[-1]:.2f} ± {1.96 * se[-1]:.2f} ₽ на клиента')
print(x[x.ov > 0][['seg', 'fam', 'ov', 'y']].assign(n=x.Название.str.slice(0, 40)).round(2).to_string())

print('\n=== П5. ТАРГЕТИНГ ВНУТРИ СЕГМЕНТА (слои офлайн-базы, итог)')
L = cg[(cg.CHANNEL_ID == 1)].merge(ok[['seg', 'fam', 'dcl', 'ben']], left_on='ID_PROMO', right_index=True)
L = L[L.CLIENTS_TG > 0].copy()
L['xpc'] = L.TO_BEFORE_TG / L.CLIENTS_TG
# терциль суммы «до» внутри акции × сегмента × недели — по накопленной доле клиентов
L = L.sort_values(['ID_PROMO', 'SEGMENT_ID', 'WEEK_FROM', 'xpc'])
L['cum'] = L.groupby(['ID_PROMO', 'SEGMENT_ID', 'WEEK_FROM']).CLIENTS_TG.cumsum() / L.groupby(['ID_PROMO', 'SEGMENT_ID', 'WEEK_FROM']).CLIENTS_TG.transform('sum')
L['terc'] = np.where(L.cum <= 1 / 3 + 1e-9, 'T1 низ', np.where(L.cum <= 2 / 3 + 1e-9, 'T2 середина', 'T3 верх'))
L['wk'] = L.WEEK_FROM.astype(str) + ('' if True else '')
FAM = {'Монеты без порога, список Активных': (L.fam == 'Начисление N монет без порога') & (L.seg == 'Активные') & (L.dcl == '2–14 дн.'),
       'Монеты без порога, список Оттока+Спящих': (L.fam == 'Начисление N монет без порога') & (L.seg == 'Отток+Спящие (список 2026)'),
       'Скидка N р. на чек, Активные': (L.fam == 'Скидка N р. на чек от X') & (L.seg == 'Активные'),
       'Вернём 300 с 1000, Отток+Спящие и Спящие': (L.fam == 'Вернём N р. монетами с чека от X') & L.seg.isin(['Отток+Спящие (список 2026)', 'Спящие']),
       'Кешбэк 20 % на категорию, Активные': (L.fam == 'Кешбэк X% на категорию') & (L.seg == 'Активные') & (L.ben == 20),
       'Кешбэк 50 % на категорию, Отток+Спящие': (L.fam == 'Кешбэк X% на категорию') & (L.seg == 'Отток+Спящие (список 2026)')}
agg = lambda g: pd.Series({'акций': g.ID_PROMO.nunique(), 'клиентов': g.CLIENTS_TG.sum(), '₽ на клиента': g.ADD_TO.sum() / g.CLIENTS_TG.sum(),
                           '±': 1.96 * np.sqrt((g.ADD_TO_SE ** 2).sum()) / g.CLIENTS_TG.sum(), 'ТО до на клиента': g.TO_BEFORE_TG.sum() / g.CLIENTS_TG.sum()})
for nm, m in FAM.items():
    x = L[m]
    print(f'-- {nm}')
    for sid, h in x.groupby('SEGMENT_ID'):
        if h.CLIENTS_TG.sum() < 20000:
            continue
        tot = agg(h)
        print(f'   сегмент {sid}: всего {tot["клиентов"]:.0f} клиентов, {tot["₽ на клиента"]:.2f} ± {tot["±"]:.2f} ₽')
        bw = h.groupby('WEEK_FROM').apply(agg, include_groups=False); bw['доля клиентов'] = bw['клиентов'] / tot['клиентов']
        print(bw.round(2).to_string())
        bt = h.groupby('terc').apply(agg, include_groups=False); bt['доля клиентов'] = bt['клиентов'] / tot['клиентов']
        print(bt.round(2).to_string())
pd.to_pickle(L, 'layers.pkl')
