# Разбор gcg_oct_outliers.jsonl: итог октября по каждому правилу выбросов (+ с фродом / >5 чеков до нарезки).
import json, sys, math
from collections import defaultdict

SRC = sys.argv[1] if len(sys.argv) > 1 else 'gcg_oct_outliers.jsonl'
rows = [json.loads(l) for l in open(SRC, encoding='utf-8') if l.startswith('{')]
V = [r for r in rows if r['t'] == 'V']
F = ('N', 'BUY', 'SY', 'SX', 'SYY', 'SXX', 'SXY')


def acc(filt):
    out = defaultdict(lambda: {'TG': [0.0] * 7, 'CG': [0.0] * 7})
    for r in V:
        if filt(r):
            v = out[(r['BATCH_ID'], r['SEGMENT'], r['CELL'])][r['GRP']]
            for i, f in enumerate(F):
                v[i] += r[f] or 0
    return out


def cell_est(t, c):
    nt, _, yt, xt, yyt, xxt, xyt = t
    nc, _, yc, xc, yyc, xxc, xyc = c
    if nt < 2 or nc < 2 or xc == 0:
        return None
    k = yc / xc

    def var(n, y, x, yy, xx, xy):
        sd = y - k * x
        return max(yy - 2 * k * xy + k * k * xx - sd * sd / n, 0) / (n - 1)
    se = nt * math.sqrt(var(nt, yt, xt, yyt, xxt, xyt) / nt + ((xt / nt) / (xc / nc)) ** 2 * var(nc, yc, xc, yyc, xxc, xyc) / nc)
    return yt - xt * k, se


def total(cells):
    res = {2: [0.0, 0.0], 5: [0.0, 0.0]}
    neg = 0
    for k, g in cells.items():
        e = cell_est(g['TG'], g['CG'])
        if e is None:
            continue
        res[k[1]][0] += e[0]; res[k[1]][1] += e[1] ** 2
        neg += e[0] < 0
    a = res[2][0] + res[5][0]; se = math.sqrt(res[2][1] + res[5][1])
    return res[2][0], res[5][0], a, se, neg


NAMES = {'base': 'как сейчас (процедура)', 'jump': 'отклонение от своего «ДО» вверх',
         'jump2': 'отклонение от своего «ДО» в обе стороны', 'cap': 'обрезка суммы месяца до границы',
         'pool': 'граница месяца по ЦГ+ГКГ', 'k2': 'κ = 2 (жёстче)', 'base_g': 'справочно: ТО до скидки'}
print(f"{'правило':<44}{'Акт':>8}{'Отток':>8}{'ИТОГО':>8}{'95% ДИ':>18}{'яч<0':>6}")
for var in ('base', 'jump', 'jump2', 'cap', 'pool', 'k2', 'base_g'):
    for suf, ex in (('', lambda r: False), (' − фрод − >5 до', lambda r: r['F_FRAUD'] == 1 or r['F_PRE5'] == 1)):
        a, o, t, se, neg = total(acc(lambda r, var=var, ex=ex: r['VAR'] == var and not ex(r)))
        print(f"{NAMES[var] + suf:<44}{a / 1e6:8.1f}{o / 1e6:8.1f}{t / 1e6:8.1f}   {(t - 1.96 * se) / 1e6:6.1f}…{(t + 1.96 * se) / 1e6:6.1f}{neg:>6}")
for r in rows:
    if r['t'] == 'CNT':
        print(r)
