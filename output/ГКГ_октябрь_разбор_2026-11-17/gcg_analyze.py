# Разбор выгрузки gcg_oct_decomp.jsonl: повтор итога витрины, вклад признаков, сценарии исключения.
import json, sys, math
from collections import defaultdict

SRC = sys.argv[1] if len(sys.argv) > 1 else 'gcg_oct_decomp.jsonl'
rows = [json.loads(l) for l in open(SRC, encoding='utf-8') if l.startswith('{')]
by_t = defaultdict(list)
for r in rows:
    by_t[r['t']].append(r)

F = ('N', 'BUY', 'SY', 'SX', 'SYY', 'SXX', 'SXY')


def acc(rs, key, filt=lambda r: True):
    """суммы по (ячейка, группа) → dict[(b,s,c)][grp] = [N,...]"""
    out = defaultdict(lambda: {'TG': [0.0] * 7, 'CG': [0.0] * 7})
    for r in rs:
        if not filt(r):
            continue
        k = key(r)
        v = out[k][r['GRP']]
        for i, f in enumerate(F):
            v[i] += r[f] or 0
    return out


def cell_est(t, c):
    """Доп ТО и SE ячейки по формуле процедуры"""
    nt, _, yt, xt, yyt, xxt, xyt = t
    nc, _, yc, xc, yyc, xxc, xyc = c
    if nt < 2 or nc < 2 or xc == 0:
        return None
    k = yc / xc
    add = yt - xt * k

    def var(n, y, x, yy, xx, xy):
        sd = y - k * x
        ss = yy - 2 * k * xy + k * k * xx - sd * sd / n
        return max(ss, 0) / (n - 1)
    vt = var(nt, yt, xt, yyt, xxt, xyt)
    vc = var(nc, yc, xc, yyc, xxc, xyc)
    mxt, mxc = xt / nt, xc / nc
    se = nt * math.sqrt(vt / nt + (mxt / mxc) ** 2 * vc / nc)
    return add, se


def total(cells, seg_filter=None):
    res = defaultdict(lambda: [0.0, 0.0, 0, 0])
    for k, g in cells.items():
        if seg_filter and not seg_filter(k):
            continue
        e = cell_est(g['TG'], g['CG'])
        if e is None:
            res[k[1]][2] += 1   # пропущенная ячейка
            continue
        res[k[1]][0] += e[0]
        res[k[1]][1] += e[1] ** 2
        res[k[1]][3] += g['TG'][0]
    out = {}
    for s, (a, v, miss, n) in res.items():
        out[s] = (a, math.sqrt(v), miss, n)
    a = sum(x[0] for x in out.values()); se = math.sqrt(sum(x[1] ** 2 for x in out.values()))
    out['ИТОГО'] = (a, se, sum(x[2] for x in out.values()), sum(x[3] for x in out.values()))
    return out


def show(title, tot):
    seg = {2: 'Активные', 5: 'Отток', 'ИТОГО': 'ИТОГО'}
    parts = []
    for s in (2, 5, 'ИТОГО'):
        if s in tot:
            a, se, miss, n = tot[s]
            parts.append(f"{seg[s]} {a / 1e6:7.1f} ± {1.96 * se / 1e6:5.1f}")
    print(f"{title:<60} " + ' | '.join(parts))


A = by_t['A']
cellkey = lambda r: (r['BATCH_ID'], r['SEGMENT'], r['CELL'])
base = acc(A, cellkey, lambda r: r['OUTF'] == 0)
show('ПОВТОР процедуры (выбросы исключены)', total(base))
show('без исключения выбросов', total(acc(A, cellkey)))

# заражение ГКГ: сколько клиентов ГКГ в списках октября (отправка, не КГ акции)
print('\nГКГ в списках акций октября (CONTROL_GROUP = 0):')
for name, f in (('любая акция', lambda r: r['EXPO'] > 0), ('101426 к отправке', lambda r: r['IN426'] == 1),
                ('101426 в КГ акции', lambda r: r['IN426'] == 2), ('ПП 6ПП', lambda r: r['PP'] == 1), ('ДЦО 30.10', lambda r: r['DCO'] == 1)):
    for grp in ('CG', 'TG'):
        n = sum(r['N'] for r in A if r['GRP'] == grp and f(r))
        N = sum(r['N'] for r in A if r['GRP'] == grp)
        print(f"   {name:<20} {'ГКГ' if grp == 'CG' else 'ЦГ '} {n:>10,.0f} из {N:>10,.0f} ({100 * n / N:5.2f} %)")

# сценарии исключения (обе группы), выбросы — как в процедуре
print('\nСценарии исключения клиентов из обеих групп:')
SC = [
    ('фрод-лист I_FRAUD_CONTACT', lambda r: r['F_FRAUD'] == 1),
    ('>5 чеков в день ДО нарезки (105 дн.)', lambda r: r['F_PRE5'] == 1),
    ('>5 чеков в день в октябре (после воздействия!)', lambda r: r['F_OCT5'] == 1),
    ('фрод + >5 ДО', lambda r: r['F_FRAUD'] == 1 or r['F_PRE5'] == 1),
]
for name, ex in SC:
    show('минус ' + name, total(acc(A, cellkey, lambda r, ex=ex: r['OUTF'] == 0 and not ex(r))))
    n = {g: sum(r['N'] for r in A if r['OUTF'] == 0 and r['GRP'] == g and ex(r)) for g in ('TG', 'CG')}
    print(f"{'':<60}   исключено ЦГ {n['TG']:,.0f} / ГКГ {n['CG']:,.0f}")

# вклад признаков (страты по признаку до/без воздействия: свой KOEF в ячейке × признак)
for t, fld, names in (('LC', 'LC', None), ('CLU', 'CLU', None), ('PUSH', 'PUSH', None)):
    print(f'\nВклад по признаку {fld} (ячейка × значение, свой KOEF):')
    vals = sorted({r[fld] for r in by_t[t]})
    for v in vals:
        cells = acc(by_t[t], cellkey, lambda r, v=v: r['OUTF'] == 0 and r[fld] == v)
        tt = total(cells)
        nT = sum(r['N'] for r in by_t[t] if r['OUTF'] == 0 and r[fld] == v and r['GRP'] == 'TG')
        nC = sum(r['N'] for r in by_t[t] if r['OUTF'] == 0 and r[fld] == v and r['GRP'] == 'CG')
        show(f'{fld}={v}  ЦГ {nT:,.0f} ГКГ {nC:,.0f}', tt)

print('\nВклад по слоям (как в витрине):')
cells = base
for k in sorted(cells):
    e = cell_est(cells[k]['TG'], cells[k]['CG'])
    if e and (k[1] == 2 or k[2] <= 6):
        print(f"   партия {k[0]} {'Акт' if k[1] == 2 else 'Отт'} ячейка {k[2]:>2}: {e[0] / 1e6:7.1f} ± {1.96 * e[1] / 1e6:5.1f}")
