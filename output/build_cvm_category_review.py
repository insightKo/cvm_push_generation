# Свод предложений «объединить / разбить» по категориям справочника CVM (разбор по группам A–E) — на решение Елене.
import json
import re
import sys
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Font

OUT = Path(__file__).resolve().parent
DST = OUT / 'объединение_и_разбивка_категорий_CVM.xlsx'
ACT = {'merge': 'Объединить', 'split': 'Разбить'}


def main(src_dir):
    wb = openpyxl.load_workbook(OUT / 'справочник_категорий_CVM.xlsx', read_only=True)
    cats = {r[0]: r for r in list(wb['Категории'].iter_rows(values_only=True))[1:]}
    group_order = [r[1] for r in list(wb['Группы'].iter_rows(values_only=True))[1:]]

    props = []
    for k in 'ABCDE':
        props += json.load(open(Path(src_dir) / f'proposals_{k}.json', encoding='utf-8'))
    props.sort(key=lambda p: (group_order.index(p['group']) if p['group'] in group_order else 99,
                              -(p.get('turnover_share_pct') or 0)))

    # какие категории задевает предложение: свои from_ids + ID, упомянутые в правилах
    touched = []
    for p in props:
        ids = set(p['from_ids'])
        for c in p['new_categories']:
            ids |= {int(x) for x in re.findall(r'\b1[0-7]\d{2}\b', str(c.get('rule') or '')) if int(x) in cats}
        touched.append(ids)

    def clean(rule):
        r = str(rule or '')
        r = re.sub(r'\\b|\.\?|-\?|\\', '', r)
        r = re.sub(r'\(?(существующ[^)]*|id \d+[^)]*)\)?', '', r)
        r = re.sub(r'\bid\s*\d+', '', r).replace(' / ', ', ').replace('|', ', ')
        r = re.sub(r'\s*→\s*в\s*[»«]*\s*$|\s*→\s*в\s*(?=[.;])', '', r)
        return re.sub(r'\s+', ' ', r).strip(' ;,.→')

    existing = {r[3] for r in cats.values()}
    UNIT = {'кг': 'за кг', 'л': 'за литр', 'шт': 'за штуку'}

    def describe(p):
        """(действие, текст): «Объединить» / «Вынуть часть товаров» / «Разбить»."""
        src = [cats[x][3] for x in p['from_ids'] if x in cats]
        news = p['new_categories']
        unit = lambda c: UNIT.get(c['unit'], c['unit'])
        if p['type'] == 'noscale':
            return 'Убрать шкалу', 'Полки остаются как есть, у всех убирается ценовая шкала (решено 19.10).'
        if p['type'] == 'merge':
            c = news[0]
            return 'Объединить', (f"Категории из колонки «Было» сливаются в одну полку «{c['name']}» "
                                  f"({c['products']} тов., {unit(c)}).")
        listed = {c['name'] for c in news}
        rest_elsewhere = any(re.match(r'остат|остальн', clean(c.get('rule'))) and c['name'] not in src for c in news)
        gone = [x for x in src if x not in listed and (rest_elsewhere or len(src) == 1)]
        stay = [x for x in src if x not in gone]
        lines = []
        for c in news:
            rule = clean(c.get('rule'))
            if c['name'] in src:
                continue
            where = (f"в имеющуюся полку «{c['name']}»" if c['name'] in existing
                     else f"новая полка «{c['name']}» ({unit(c)})")
            lines.append(f"→ {where} — {c['products']} тов." + (f" Какие: {rule}." if rule else ''))
        if stay:
            names = ', '.join(f'«{x}»' for x in stay) if len(stay) <= 4 else f'все {len(stay)} категорий из колонки «Было»'
            kept = [c for c in news if c['name'] in stay]
            lines.append(f"Остаются {names} — со всеми остальными товарами" +
                         (' (' + '; '.join(f"«{c['name']}» — {c['products']} тов." for c in kept) + ')' if kept and len(kept) <= 4 else '') + '.')
        for x in gone:
            lines.append(f"«{x}» убирается — все её товары расходятся по полкам выше.")
        return ('Разбить' if gone else 'Вынуть часть товаров'), '\n'.join(lines)

    rows = []
    for i, p in enumerate(props):
        src = [f'{cats[x][3]} ({cats[x][2]})' if x in cats else str(x) for x in p['from_ids']]
        was = '\n'.join(src) if len(src) <= 8 else f'{len(src)} категорий:\n' + '\n'.join(src)
        action, will = describe(p)
        cross = [str(j + 1) for j in range(len(props)) if j != i and touched[i] & touched[j]]
        rows.append((i + 1, p['group'], action, was, will, p['why'], '\n'.join(p['evidence']),
                     p.get('turnover_share_pct'), ', '.join(cross) or None, 'да (19.10)' if p['type'] == 'noscale' else None))

    out = openpyxl.Workbook()
    ws = out.active
    ws.title = 'Предложения'
    head = ['№', 'Группа', 'Действие', 'Было (категории сейчас)', 'Что изменится (→ куда уходят товары)',
            'Почему', 'Примеры товаров', 'Доля оборота, %', 'Задевает те же категории, что №', 'Решение']
    ws.append(head)
    for r in rows:
        ws.append(list(r))
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical='top')
    for i, w in enumerate([5, 18, 12, 38, 60, 50, 46, 10, 14, 14]):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i + 1)].width = w
    ws.freeze_panes = 'D2'
    ws.auto_filter.ref = f'A1:J{len(rows) + 1}'

    ws2 = out.create_sheet('Сводка')
    acts = ['Объединить', 'Вынуть часть товаров', 'Разбить', 'Убрать шкалу']
    ws2.append(['Группа'] + acts)
    for g in group_order:
        ws2.append([g] + [sum(1 for r in rows if r[1] == g and r[2] == a) for a in acts])
    ws2.append(['Итого'] + [sum(1 for r in rows if r[2] == a) for a in acts])
    for cell in ws2[1]:
        cell.font = Font(bold=True)
    ws2.column_dimensions['A'].width = 24
    out.save(DST)
    print(DST.name, len(rows), sum(1 for r in rows if r[8]))


if __name__ == '__main__':
    main(sys.argv[1])
