# Коэффициенты «Доп ТО на клиента ЦГ» → коэффициенты_эффекта_аналитик.csv
import numpy as np, pandas as pd
from eb import eb3, sig_label

OUT = r"C:/Projects/cvm_push_generation/output/План_декабрь_2026-11-10/коэффициенты_эффекта_аналитик.csv"
U = pd.read_pickle('units.pkl')
a = pd.read_pickle('promos.pkl')
U['comm'] = U.fam.str.startswith('Коммуникация')
# покрытие: доля клиентов списка ЦГ, попавших в оценку (В оценке ЦГ / Клиентов ЦГ), по типу списка
ok = a[a.ok]
cvh = ok.groupby('seg').apply(lambda g: g.Ne.sum() / g.N.sum(), include_groups=False).to_dict()
COV = {'Новые': cvh['Новые'], 'Активные (вкл. ВЦ/LFL)': cvh['Активные'], 'Отток': cvh['Отток+Спящие (список 2026)'],
       'Спящие': cvh['Спящие'], 'Случайные': cvh['Случайные']}
SUSPECT = {101075: 'КГ 2,7 % вместо 10 %', 101076: 'КГ 1,9 % вместо 10 %'}

rows, allC = [], []
for ch in ['офлайн', 'онлайн']:
    for comm in [False, True]:
        x = U[(U.ch == ch) & (U.comm == comm)]
        C, F, par, uu = eb3(x, ['ch', 'tseg', 'fam', 'dcl', 'ben', 'thr'], ['ch', 'tseg', 'fam', 'dcl'], ['ch', 'fam', 'dcl'])
        C['grp'] = f'{ch} {"коммуникации" if comm else "акции"}'
        allC.append((C, par))
        for _, c in C.iterrows():
            g = uu[uu.kc == c.kc]
            ids = sorted(g.ID.unique())
            pr = a.set_index('ID').loc[ids]
            tseg = g.tseg.iloc[0]
            cov = COV.get(tseg, cvh.get(tseg, 0.97))
            disc = (pr.disc * pr.N).sum() / pr.N.sum()
            post, lo, hi, raw = c.post * cov, c.lo * cov, c.hi * cov, c.r * cov
            pl = (post * 0.3 - disc) / 1.15
            nsig = int(pr.sig_n.sum())
            durs = f'{int(pr.dur.min())}–{int(pr.dur.max())}' if pr.dur.min() != pr.dur.max() else f'{int(pr.dur.min())}'
            com = []
            sus = [f'{i}: {SUSPECT[i]}' for i in ids if i in SUSPECT]
            if pr.nocamp.any() and not comm:
                com.append('скидка не найдена в кампаниях Manzana у ' + ', '.join(str(i) for i in pr.index[pr.nocamp]) + ' — скидка и PL занижены/неизвестны')
            if sus:
                com.append('КГ ниже плана: ' + '; '.join(sus))
            if c.Bc < 1 or c.Bf < 1:
                com.append(f'сжатие: вес своей оценки {c.Bc if c.ncell > 1 else c.Bf:.2f}')
            if g.src.iloc[0] == 'слои':
                com.append('сегмент — давность на старте по слоям акции')
                if tseg in ('Отток', 'Спящие') and (g.hdr.isin(['Отток+Спящие (список 2026)', 'Отток (алкоголь)'])).any():
                    com.append('в списках Отток+Спящие 2026 Доп ТО вернувшихся до старта (на старте уже Активные) разложен поровну на клиента Оттока и Спящих')
            if c.post > c.r + 1:
                com.append(f'оценка поднята к среднему семейства: свой факт {c.r * cov:.2f} ₽')
            # годность
            if comm and 'тематическая' in c.kc:
                fit = 'нет'; com.append('тематические рассылки: разброс между повторами больше эффекта (разбор 101402: шум вокруг нуля)')
            elif c.post <= 0 or c.r <= 0:
                fit = 'нет'
            elif c.lo > 0 and len(ids) >= 2 and not sus:
                fit = 'да'
            else:
                fit = 'осторожно'
            if fit == 'да' and c.post > c.r + 1:
                fit = 'осторожно'
            if fit == 'да' and c.N < 100000:
                fit = 'осторожно'; com.append('только малые тесты (< 100 тыс. клиентов)')
            if c.N < 5000:
                continue
            ben = '' if pd.isna(pr.ben.iloc[0]) else (f'{pr.ben.iloc[0]:.0f} %' if ('%' in c.kc or 'Скидка X%' in c.kc) else f'{pr.ben.iloc[0]:.0f} ₽')
            thr = '' if pd.isna(pr.thr.iloc[0]) else f'{pr.thr.iloc[0]:.0f}'
            rows.append({'канал': ch, 'сегмент': tseg, 'семейство механики': g.fam.iloc[0], 'выгода, ₽ или %': ben, 'порог чека, ₽': thr,
                         'длительность, дн.': durs, '₽ на клиента (прогнозный, после сжатия)': round(post, 2),
                         'ДИ от': round(lo, 2), 'ДИ до': round(hi, 2), '₽ на клиента сырой': round(raw, 2),
                         'скидка ₽ на клиента': round(disc, 2), 'PL ₽ на клиента': round(pl, 2), 'n акций': len(ids),
                         'ID аналогов': ', '.join(str(i) for i in ids), 'значимость': f'{sig_label(c.z)}; значимых акций {nsig} из {len(ids)}',
                         'годится в прогноз (да/нет/осторожно)': fit, 'комментарий': '; '.join(com),
                         '_post': post, '_clients': c.N})
R = pd.DataFrame(rows).sort_values(['канал', '_post'], ascending=[False, False])
R.drop(columns=['_post', '_clients']).to_csv(OUT, sep=';', index=False, encoding='utf-8-sig')
R.to_pickle('coef.pkl')
pd.to_pickle(allC, 'eb_all.pkl')
pd.set_option('display.width', 250); pd.set_option('display.max_rows', 300); pd.set_option('display.max_colwidth', 60)
print(COV)
for C, par in allC:
    print(C.grp.iloc[0], {k: v for k, v in par.items() if not isinstance(v, dict)})
print(R[['канал', 'сегмент', 'семейство механики', 'выгода, ₽ или %', 'порог чека, ₽', 'длительность, дн.', '₽ на клиента (прогнозный, после сжатия)',
         'ДИ от', 'ДИ до', '₽ на клиента сырой', 'скидка ₽ на клиента', 'PL ₽ на клиента', 'n акций', 'годится в прогноз (да/нет/осторожно)']].to_string(index=False))
