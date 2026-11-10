# Статистика моста: база B = ГКГ − Σ акций, наклон ГКГ на Σ акций, требуемая сумма акций для 600 млн и вероятность.
import numpy as np, pandas as pd
from math import erf, sqrt
Phi = lambda z: 0.5 * (1 + erf(z / sqrt(2)))
B = pd.read_pickle('bridge.pkl').set_index('ym')
m = lambda x: x / 1e6
G = m(B.G); S = m(B.S); disc = m(B.disc); Sa = S - disc; seG = m(B.seG); seS = m(B.seS)
# Пересчёт ГКГ без копий чеков 2^44 выполнен 10.11 10:00–10:04 (обменник CVM_GCG_DEDUP_202609_202610_20261110.sql, коммит cccf778);
# в gcg_read2.jsonl (чтение раньше пересчёта) — версия с копиями. Итоги пересчёта — память project_check_duplicates_2pow44 (10.11):
# окт Активные 107,2 (было 95,8), Отток 41,2 (было 47,6); сен Активные 162,7 (было 163,6); сен Отток после пересчёта в записи нет — оставлен 48,5.
DEDUP = {'202610': 107.2 + 41.2, '202609': 162.7 + m(B.loc['202609', 'G'] - 0) * 0 + 48.47}
Gd = G.copy(); Gd['202610'] = DEDUP['202610']; Gd['202609'] = DEDUP['202609']
variants = {
 'как в чтении 10.11 (сен–окт с копиями)': dict(G=G.copy()),
 'сен–окт без копий (пересчёт 10.11)':     dict(G=Gd),
 'без копий, авг по KOEF строки 56,3':      dict(G=Gd.where(Gd.index != '202608', m(B.Gagg))),
}
months_new = ['202607', '202608', '202609', '202610']
months_all = ['202603', '202604', '202605', '202606'] + months_new
print('месяц  ГКГ  ±SE   Σакций ±SE  скидка  Σакций−скидка  D=ГКГ−Σ  Dn=ГКГ−(Σ−скидка)')
for ym in months_all + ['202611']:
    print(ym, f'{G[ym]:7.1f} {seG[ym]:6.1f} {S[ym]:7.1f} {seS[ym]:5.1f} {disc[ym]:6.1f} {Sa[ym]:8.1f} {G[ym]-S[ym]:8.1f} {G[ym]-Sa[ym]:8.1f}')
for name, v in variants.items():
    g = v['G']
    for lab, Sx in (('до скидки', S), ('после скидки', Sa)):
        D = (g - Sx)
        # июль–октябрь: простое среднее (у июля нет ошибки) и взвешенное по август–октябрь
        d4 = D[months_new]; b4 = d4.mean(); sd4 = d4.std(ddof=1); se4 = sd4 / np.sqrt(4)
        w = 1 / (seG[months_new[1:]] ** 2 + seS[months_new[1:]] ** 2)
        d3 = D[months_new[1:]]; bw = (w * d3).sum() / w.sum(); sew = np.sqrt(1 / w.sum())
        Q = (w * (d3 - bw) ** 2).sum()
        dall = D[months_all]
        # наклон ГКГ на Σ акций
        xs, ys = Sx[months_all].values, g[months_all].values
        X = np.column_stack([np.ones(8), xs]); bb, res, *_ = np.linalg.lstsq(X, ys, rcond=None)
        e = ys - X @ bb; s2 = (e ** 2).sum() / 6; cov = s2 * np.linalg.inv(X.T @ X)
        xs4, ys4 = Sx[months_new].values, g[months_new].values
        X4 = np.column_stack([np.ones(4), xs4]); b4r = np.linalg.lstsq(X4, ys4, rcond=None)[0]
        e4 = ys4 - X4 @ b4r; c4 = (e4 ** 2).sum() / 2 * np.linalg.inv(X4.T @ X4)
        print(f'\n[{name}; акции {lab}] D июль–окт: {", ".join(f"{x:.1f}" for x in d4)}; среднее {b4:.1f}, sd {sd4:.1f}, ошибка среднего {se4:.1f}')
        print(f'   взвешенно авг–окт: {bw:.1f} ± {sew:.1f} (Q = {Q:.2f} при 2 ст. св.: разброс {"в пределах ошибки ГКГ" if Q < 5.99 else "больше ошибки"})')
        print(f'   март–июнь (ГКГ Манзаны, простая разница): {", ".join(f"{x:.1f}" for x in D[months_all[:4]])}; среднее {D[months_all[:4]].mean():.1f}')
        print(f'   наклон ГКГ на Σ акций, март–окт: {bb[1]:.2f} ± {np.sqrt(cov[1,1]):.2f} (своб. член {bb[0]:.1f}); июль–окт: {b4r[1]:.2f} ± {np.sqrt(c4[1,1]):.2f}')
        print(f'   отношение ΣГКГ/Σакций июль–окт: {g[months_new].sum() / Sx[months_new].sum():.2f}; по месяцам: {", ".join(f"{g[k]/Sx[k]:.2f}" for k in months_new)}')
# вероятность 600 при разных суммах акций декабря (после скидки), база — среднее июль–окт, вариант «окт ÷ 1,0885», акции после скидки
g = variants['сен–окт без копий (пересчёт 10.11)']['G']; D = g - Sa
b = D[months_new].mean(); seb = D[months_new].std(ddof=1) / 2
seG_dec = seG['202610']       # ошибка ГКГ декабря ≈ октябрьской (гипотеза)
print(f'\nбаза b = {b:.1f} ± {seb:.1f}; ошибка ГКГ месяца (окт) {seG_dec:.1f}')
for sS in (0.0, 30.0):
    sig = np.sqrt(seG_dec ** 2 + seb ** 2 + sS ** 2)
    need50 = 600 - b; need80 = 600 - b + 0.8416 * sig; need90 = 600 - b + 1.2816 * sig
    print(f' ошибка прогноза акций {sS:.0f}: σ = {sig:.1f}; нужно Σ(Доп ТО − скидка) акций: 50 % — {need50:.0f}, 80 % — {need80:.0f}, 90 % — {need90:.0f}')
    for Sd in (100, 200, 300, 400, 500, 600, 700):
        print(f'   Σ(Доп ТО − скидка) = {Sd}: ГКГ {b+Sd:.0f} (95 % {b+Sd-1.96*sig:.0f}…{b+Sd+1.96*sig:.0f}), P(ГКГ ≥ 600) = {1-Phi((600-b-Sd)/sig):.0%}')
