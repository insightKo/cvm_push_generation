# Разметка единиц из названия товара → строки для dbo.I_CVM_PRODUCT_UNIT (SOURCE = 1, по названию).
# Вход (TSV без шапки): ID_PRODUCT, название, ID категории, категория, единица полки (кг/л/шт), без шкалы (0/1), группа.
# Необязательно: продажи (CSV ID_PRODUCT, TURNOVER, QUANTITY, …) — цена за штуку = TURNOVER / QUANTITY; нужна, чтобы
#   выбрать толкование там, где название двусмысленно (короб «90Г Х16» или мультипак «0,5Л*6»; «5ШТ 150Г» — всего или на штуку).
# Выход (TSV с шапкой): ID_PRODUCT, UNIT_NAME, QTY_PER_PIECE, PIECES, IS_MULTIPACK, QTY_TOTAL, QTY_ALT_TOTAL, IS_WEIGHT_NAME,
#   STATUS, NOTE. Количество — в единице названия UNIT_NAME (обычно = единице полки); QTY_ALT_TOTAL — вторая единица
#   с двойной этикетки («680Г/720МЛ»). Перевод кг ↔ л — в модели ценового индекса (ds-senior 23.10.2026).
#   STATUS: ok — размечено; manual — нужна ручная разметка (причина в NOTE); skip — полка без шкалы.
# Правила (подход согласован Еленой 19.10.2026):
#   - объём с единицей (Г, КГ, МЛ, Л, ШТ, РУЛ, ПАК …), граммы → кг, миллилитры → литры;
#   - хвост «/ 34.00 ГР» — вес упаковки целиком, главнее остального названия;
#   - мультипак — только «число × объём с единицей»; голое «Х48» — короб, не мультипак; «5* 0,5Л» — звёзды коньяка;
#   - «АЛК.4,7МЛ» и проценты — крепость/жирность, не объём;
#   - полки «за штуку» — число штук из названия, нет числа — 1 штука;
#   - «ВЕС», «ВЕСОВ», «/КГ» — весовой: цена за единицу = цена за кг, объём из названия не берётся;
#   - граммы на полке «за литр» и миллилитры на полке «за кг» — размечаются в своей единице (UNIT_NAME), плотность
#     не выдумывается: множитель полки оценивает модель индекса из данных (решение 23.10.2026 вместо ручной разметки);
#   - двусмысленное название — толкование, при котором цена за единицу ближе к медиане однозначных товаров полки.
import csv
import math
import re
import statistics
import sys
from collections import defaultdict

MASS = {'КГ': 1.0, 'KG': 1.0, 'Г': 0.001, 'ГР': 0.001, 'ГРАММ': 0.001, 'G': 0.001, 'GR': 0.001}
VOL = {'Л': 1.0, 'ЛТ': 1.0, 'L': 1.0, 'МЛ': 0.001, 'ML': 0.001}
PCS = ['ШТ', 'ШТУК', 'PCS', 'РУЛ', 'РУЛОН', 'РУЛОНА', 'РУЛОНОВ', 'ПАК', 'ПАКЕТ', 'ПАКЕТОВ', 'ПАКЕТИКОВ', 'ПАР', 'ЯИЦ',
       'ЯЙЦА', 'ТАБЛ', 'ТАБ', 'КАПС', 'СТИК', 'СТИКОВ', 'САШЕ', 'ЛИСТ', 'ЛИСТОВ', 'ПОДГ', 'ПОДГУЗ', 'ПЛАСТИН']   # «ПЛАСТ» — пластик, не пластинки
UNITS = sorted(list(MASS) + list(VOL) + PCS, key=len, reverse=True)
U = '(' + '|'.join(re.escape(u) for u in UNITS) + r')(?![А-ЯA-Z])'
NUM = r'(\d+(?:[.,]\d+)?)'
X = r'(?:\s*[XХ×]\s*|\s*\*(?!\s))'                # «*» с пробелом после — звёзды коньяка, не умножение
RE_YEAR = re.compile(r'(?<![\d.,])(?:19|20)\d\d\s*Г(?:ОД)?\.?(?![А-ЯA-Z])')   # 2006Г — год, не граммы
RE_ALC = re.compile(r'АЛК\.?\s*\d+(?:[.,]\d+)?\s*(?:%|МЛ|ОБ\.?)?|\d+(?:[.,]\d+)?\s*%')
RE_TAIL = re.compile(r'/\s*(\d+\.\d+)\s*(КГ|ГР|Г|МЛ|Л|ШТ)(?![А-ЯA-Z])')
RE_RANGE = re.compile(NUM + r'\s*[-–]\s*' + NUM + r'\s*' + U)
RE_MULTI1 = re.compile(r'(?<![\d.,])(\d{1,3})(?:\s*(?:ШТ|ПАК|САШЕ|СТИК)\.?)?' + X + NUM + r'\s*' + U)  # 6Х0,5Л, 15ШТ*2 Г
RE_MULTI2 = re.compile(NUM + r'\s*' + U + X + r'(\d{1,3})(?![\d.,])(\s*' + U + ')?')  # 0,5Л*6 / 0,5Л*4ШТ
RE_QTY = re.compile(r'(?<![\d.,])' + NUM + r'\s*' + U)
RE_ART = re.compile(r'(?<=[А-ЯA-Z])\d{4,}(?=\s*ШТ)')   # «P5060911ШТ», «ЗЕВАДЕЛЮКС1844430ШТ» — артикул, не штуки
RE_WEIGHT = re.compile(r'(?<![А-ЯA-Z])(ВЕС|ВЕСОВ\w*|НА ВЕС)(?![А-ЯA-Z])|/\s*КГ(?![А-ЯA-Z])|\(КГ\)')


def num(s):
    return float(s.replace(',', '.'))


def kind(u):
    return 'кг' if u in MASS else 'л' if u in VOL else 'шт'


def factor(u):
    return MASS.get(u) or VOL.get(u) or 1.0


def cand(q, p, mp, note):
    return dict(q=q, pcs=p, mp=mp, w=0, note=note)


def kind_cands(n, singles, k):
    """Толкования объёма в единице k (кг или л); None — в названии нет объёма в этой единице."""
    out = []
    for m in RE_MULTI1.finditer(n):
        if kind(m.group(3)) == k:
            per = num(m.group(2)) * factor(m.group(3))
            out = [cand(per, int(m.group(1)), 1, 'мультипак'), cand(per, 1, 0, 'объём (короб?)')]
            break
    if not out:
        for m in RE_MULTI2.finditer(n):
            if kind(m.group(2)) == k:
                per, c = num(m.group(1)) * factor(m.group(2)), int(m.group(3))
                if m.group(4):                       # 0,5Л*4ШТ — явный мультипак
                    out = [cand(per, c, 1, 'мультипак')]
                else:                                # «150Г Х52» — чаще короб поставщика; «0,5Л*6» — мультипак; решит цена
                    out = [cand(per, 1, 0, 'объём, число после — короб'), cand(per, c, 1, 'мультипак')]
                break
    if out:
        return out
    rng = RE_RANGE.search(n)
    if rng and kind(rng.group(3)) == k:
        a, b = num(rng.group(1)), num(rng.group(2))
        return [cand((a + b) / 2 * factor(rng.group(3)), 1, 0, 'диапазон, середина')]
    vals = sorted({round(q, 6) for kk, q in singles if kk == k})
    if len(vals) > 1:
        return dict(st='manual', note='несколько объёмов: ' + ','.join(f'{x:g}' for x in vals))
    if vals:
        pcs = sorted({int(p) for kk, p in singles if kk == 'шт' and p >= 2})
        if len(pcs) == 1:                            # «5ШТ 150Г» — обычно всего; «(5ШТ/50Г)» — на штуку; решит цена
            p = pcs[0]
            return [cand(vals[0] / p, p, 1, 'объём на всё, штук'), cand(vals[0], p, 1, 'объём на штуку × штук')]
        return [cand(vals[0], 1, 0, 'объём из названия')]
    return None


def parse(name, shelf, group=''):
    """dict(c=[толкования, первое — предпочтительное], unit=единица названия, alt=количество во второй единице)
    или dict(st='manual', note=…). Количество — в единице названия; перевод в единицу полки (кг ↔ л) оценивает
    модель ценового индекса из данных (двойные этикетки, семьи товара, группа дерева) — решение ds-senior 23.10.2026."""
    n = ' ' + name.upper().replace('Ё', 'Е') + ' '
    n = RE_ALC.sub(' ¦ ', n)
    n = RE_YEAR.sub(' ¦ ', n)
    if group == 'Алкоголь':                          # алкоголь в граммах не продаётся: «3Г» — выдержка
        n = re.sub(r'(?<![\d.,])\d+(?:[.,]\d+)?\s*(?:Г|ГР|ГОДА|ЛЕТ)\.?(?![А-ЯA-Z])', ' ¦ ', n)
    if shelf in ('кг', 'л') and RE_WEIGHT.search(n):
        return dict(c=[dict(q=1.0, pcs=1, mp=0, w=1, note='весовой')], unit=shelf, alt=None)
    tail = RE_TAIL.search(n)
    if tail and (shelf == 'шт' or kind(tail.group(2)) == 'шт'):   # «200МЛ / 200.00 ШТ» — хвост не про объём
        n = n[:tail.start()] + ' ' + n[tail.end():]
        tail = None
    n = RE_ART.sub(' ¦ ', n)
    singles = [(kind(m.group(2)), num(m.group(1)) * factor(m.group(2))) for m in RE_QTY.finditer(n)]
    if shelf == 'шт':
        m1 = [(int(m.group(1)), kind(m.group(3))) for m in RE_MULTI1.finditer(n)]
        cnt = sorted({int(q) for k, q in singles if k == 'шт' and q >= 1 and q == int(q)})   # «2773,1ШТ» — артикул,1 шт
        if len(cnt) > 1:
            return dict(st='manual', note='несколько чисел штук: ' + ','.join(map(str, cnt)))
        if cnt:
            return dict(c=[cand(1.0, cnt[0], 0, 'штук из названия')], unit='шт', alt=None)
        if m1 and m1[0][0] > 1:                      # 20*12Г на полке «за штуку» — 20 стиков; или короб — решит цена
            return dict(c=[cand(1.0, m1[0][0], 1, 'штук в мультипаке'), cand(1.0, 1, 0, 'одна штука (короб)')],
                        unit='шт', alt=None)
        return dict(c=[cand(1.0, 1, 0, 'одна штука')], unit='шт', alt=None)
    other = 'л' if shelf == 'кг' else 'кг'
    if tail:
        k = kind(tail.group(2))
        q = num(tail.group(1)) * factor(tail.group(2))
        pcs = [p for kk, p in singles if kk == 'шт' and p >= 2]
        p = int(pcs[0]) if len(set(pcs)) == 1 and pcs else 1
        alt = {round(x, 6) for kk, x in singles if kk == (other if k == shelf else shelf)}
        return dict(c=[cand(q / p, p, 1 if p > 1 else 0, 'вес упаковки из хвоста')], unit=k,
                    alt=alt.pop() if len(alt) == 1 else None)
    for k, o in ((shelf, other), (other, shelf)):
        r = kind_cands(n, singles, k)
        if r is None:
            continue
        if isinstance(r, dict):
            return r
        alt = {round(x, 6) for kk, x in singles if kk == o}          # двойная этикетка «680Г/720МЛ»
        return dict(c=r, unit=k, alt=alt.pop() if len(alt) == 1 else None)
    return dict(st='manual', note='нет объёма в названии')


def load_prices(path):
    price = {}
    if not path:
        return price
    for r in csv.DictReader(open(path)):
        t, q = float(r['TURNOVER'] or 0), float(r['QUANTITY'] or 0)
        if t > 0 and q > 0:
            price[r['ID_PRODUCT']] = t / q
    return price


def main(src, dst, sales=None):
    price = load_prices(sales)
    rows = [r for r in csv.reader(open(src), delimiter='\t')]
    parsed = {}
    for r in rows:
        pid, name, cat, unit, ns = r[0], r[1], r[2], r[4], r[5]
        parsed[pid] = 'skip' if ns in ('1', 'True', 'да') else parse(name, unit, r[6] if len(r) > 6 else '')
    # медиана цены за единицу по «полка × единица названия» — только однозначные товары с продажами
    per_cat = defaultdict(list)
    for r in rows:
        x = parsed[r[0]]
        if isinstance(x, dict) and 'c' in x and len(x['c']) == 1 and r[0] in price and not x['c'][0]['w']:
            q = x['c'][0]['q'] * x['c'][0]['pcs']
            if q > 0:
                per_cat[(r[2], x['unit'])].append(math.log(price[r[0]] / q))
    med = {c: statistics.median(v) for c, v in per_cat.items() if len(v) >= 5}
    out = csv.writer(open(dst, 'w', newline=''), delimiter='\t')
    out.writerow(['ID_PRODUCT', 'UNIT_NAME', 'QTY_PER_PIECE', 'PIECES', 'IS_MULTIPACK', 'QTY_TOTAL', 'QTY_ALT_TOTAL',
                  'IS_WEIGHT_NAME', 'STATUS', 'NOTE'])
    for r in rows:
        pid, cat = r[0], r[2]
        x = parsed[pid]
        if x == 'skip':
            out.writerow([pid, '', '', '', 0, '', '', 0, 'skip', 'полка без шкалы'])
            continue
        if 'st' in x:
            out.writerow([pid, '', '', '', 0, '', '', 0, 'manual', x['note']])
            continue
        c = [v for v in x['c'] if v['q'] > 0 and v['pcs'] > 0]
        if not c:
            out.writerow([pid, '', '', '', 0, '', '', 0, 'manual', 'нулевой объём'])
            continue
        best, how = c[0], ''
        if len(c) > 1:
            m = med.get((cat, x['unit']))
            if pid in price and m is not None:
                best = min(c, key=lambda v: abs(math.log(price[pid] / (v['q'] * v['pcs'])) - m))
                how = ' (выбрано по цене полки)'
            else:
                how = ' (без продаж — первое толкование)'
        q, p = best['q'], best['pcs']
        alt = x['alt'] * p if x['alt'] and best['mp'] and x['alt'] < q * p * 0.5 else x['alt']   # «6Х(680Г/720МЛ)»
        unit_note = '' if x['unit'] == r[4] else f'; единица названия {x["unit"]} ≠ полки {r[4]}'
        out.writerow([pid, x['unit'], round(q, 6), p, best['mp'], round(q * p, 6), round(alt, 6) if alt else '',
                      best['w'], 'ok', best['note'] + how + unit_note])


if __name__ == '__main__':
    main(*sys.argv[1:4])
