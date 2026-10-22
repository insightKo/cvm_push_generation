# Итоговый справочник категорий CVM после согласования (19.10.2026): дерево + переносы товаров + два круга проверки.
# Вход: справочник_категорий_CVM.xlsx (коды, правила разрезов, доли оборота), справочник_CVM_дерево_итог.json,
#       справочник_CVM_переносы_товаров.json, справочник_CVM_определения_категорий.json, выгрузка I_PRODUCT (csv).
# Выход: справочник_категорий_CVM.xlsx в том же формате (его читает build_cvm_category_sql.py).
# Порядок пересборки: build_cvm_category_reference.py → build_cvm_category_final.py → build_cvm_category_sql.py.
# Товар получает категорию: код целиком → правило слов → иначе поштучно; поштучная привязка — только там,
# где код и правила дают не ту категорию. Сверка в конце: каждый товар разрешается ровно в согласованную категорию.
import collections
import csv
import json
import re
import sys
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Font

import build_cvm_category_reference as ref

OUT = Path(__file__).resolve().parent
SRC = OUT / 'справочник_категорий_CVM.xlsx'
UNMAPPED = 'Несопоставленные'
NO_SCALE_GROUPS = {'Табак', 'Одежда и обувь', 'Служебное'}


def main(products_csv):
    wb = openpyxl.load_workbook(SRC, read_only=True)
    rd = lambda s: list(wb[s].iter_rows(values_only=True))[1:]
    old_groups = [r[1] for r in rd('Группы')]
    old_cats = {r[3]: r for r in rd('Категории')}               # имя → строка (ID, ID группы, группа, имя, ед., без шкалы, ...)
    old_codes = {str(r[0]): r for r in rd('Маппинг кодов')}
    old_rules = collections.defaultdict(list)
    for r in rd('Правила разрезов'):
        old_rules[str(r[0])].append(r)
    old_manual = {r[0]: r[5] for r in rd('Товарный маппинг')}
    tree = {t['category']: t for t in json.load(open(OUT / 'справочник_CVM_дерево_итог.json', encoding='utf-8'))}
    moves = {m['product_id']: m['to'] for m in json.load(open(OUT / 'справочник_CVM_переносы_товаров.json', encoding='utf-8'))}
    defs = json.load(open(OUT / 'справочник_CVM_определения_категорий.json', encoding='utf-8'))
    old_name_by_id = {r[0]: r[3] for r in old_cats.values()}

    # 1. товары: категория до переносов (старая логика) и после (согласованная)
    prods, ext_name = {}, {}
    with open(products_csv, encoding='utf-8') as f:
        for row in csv.reader(f):
            if row[0] == 'P':
                pid = int(row[1])
                o = prods.get(pid, ('', ''))
                prods[pid] = (max(o[0], row[2]), max(o[1], row[3]))
            elif row[0] == 'E':
                ext_name.setdefault(row[1], row[2])

    def old_cat(pid, name, code):
        if pid in old_manual:
            return old_manual[pid]
        m = old_codes.get(code)
        if m and m[2] == 'целиком':
            return m[4]
        if m and m[2] == 'по словам':
            return ref.apply_cut(code, name)[1]
        return UNMAPPED

    final = {pid: moves.get(pid) or old_cat(pid, n, c) for pid, (n, c) in prods.items()}
    live = collections.Counter(final.values())
    live[UNMAPPED] += 0

    # 2. куда ушли товары убранных категорий — для перепривязки кодов и правил
    gone_to = collections.defaultdict(collections.Counter)
    for pid, (n, c) in prods.items():
        oc = old_cat(pid, n, c)
        if oc not in live:
            gone_to[oc][final[pid]] += 1
    remap = {oc: cnt.most_common(1)[0][0] for oc, cnt in gone_to.items()}

    def live_target(name):
        seen = set()
        while name not in live and name in remap and name not in seen:
            seen.add(name)
            name = remap[name]
        return name if name in live else UNMAPPED

    # 3. дерево: живые категории, единица и шкала
    def unit_scale(name):
        if name in old_cats:
            return old_cats[name][4], old_cats[name][5] == 'да'
        t = tree[name]
        return t['unit'], bool(t.get('no_scale'))

    cat_group = {}
    for name in live:
        cat_group[name] = old_cats[name][2] if name in old_cats else tree[name]['group']
    share = {name: (old_cats[name][8] or 0) if name in old_cats else 0 for name in live}
    gshare = collections.Counter()
    for name, g in cat_group.items():
        gshare[g] += share[name]
    groups = [g for g in old_groups if g in gshare] + sorted(g for g in gshare if g not in old_groups)
    gid = {g: i + 1 for i, g in enumerate(groups)}
    cid, nxt = {}, 1001
    for g in groups:
        for name in sorted((n for n in live if cat_group[n] == g), key=lambda n: (-share[n], n)):
            cid[name] = nxt
            nxt += 1

    # 4. коды: целиком → на живую категорию (убранная — туда, куда ушло большинство её товаров);
    #    правила слов — цели тоже на живые категории
    by_code = collections.defaultdict(collections.Counter)
    for pid, (n, c) in prods.items():
        by_code[c][final[pid]] += 1
    code_rows, rules_out, new_codes = [], [], []
    for code, r in old_codes.items():
        method = r[2]
        target = None
        if method == 'целиком':
            target = live_target(r[4])
            if target == UNMAPPED and by_code.get(code):
                target = by_code[code].most_common(1)[0][0]
        code_rows.append((code, r[1] or ext_name.get(code), method, cid[target] if target else None, target))
        if method == 'по словам':
            for rr in old_rules[code]:
                t = live_target(old_name_by_id.get(rr[4], rr[5]))
                rules_out.append((code, rr[1], rr[2], rr[3], cid[t], t))
    for code in sorted(c for c in by_code if c and c not in old_codes):     # коды, появившиеся после сборки дерева
        t = by_code[code].most_common(1)[0][0]
        code_rows.append((code, ext_name.get(code), 'целиком', cid[t], t))
        new_codes.append((code, ext_name.get(code), t))

    # 5. разрешение по кодам и правилам — поштучно только расхождения
    codes_now = {r[0]: r for r in code_rows}
    rules_now = collections.defaultdict(list)
    for r in rules_out:
        rules_now[r[0]].append(r)

    def resolve(name, code):
        m = codes_now.get(code)
        if m and m[2] == 'целиком':
            return m[4]
        if m and m[2] == 'по словам':
            for _, _, any_w, none_w, _, t in sorted(rules_now[code], key=lambda x: x[1]):
                inc = [] if str(any_w).startswith('—') else [w.strip() for w in str(any_w).split(',')]
                exc = [w.strip() for w in str(none_w).split(',')] if none_w else []
                if exc and ref.word_hit(name, exc):
                    continue
                if not inc or ref.word_hit(name, inc):
                    return t
        return UNMAPPED

    manual = []
    for pid, (n, c) in prods.items():
        if resolve(n, c) != final[pid]:
            manual.append((pid, n, 'согласованная привязка', cid[final[pid]], cat_group[final[pid]], final[pid],
                           'вручную', 'проверка 19.10', None))
    manual_ids = {r[0] for r in manual}
    bad = [pid for pid, (n, c) in prods.items()
           if (final[pid] if pid in manual_ids else resolve(n, c)) != final[pid]]
    assert not bad, bad[:5]

    # 6. запись в том же формате
    out = openpyxl.Workbook()
    bold = Font(bold=True)

    def sheet(title, head, rows, widths):
        ws = out.create_sheet(title)
        ws.append(head)
        for c in ws[1]:
            c.font = bold
            c.alignment = Alignment(wrap_text=True, vertical='top')
        for row in rows:
            ws.append(list(row))
        for i, w in enumerate(widths):
            ws.column_dimensions[openpyxl.utils.get_column_letter(i + 1)].width = w
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:{openpyxl.utils.get_column_letter(len(head))}{len(rows) + 1}'

    out.remove(out.active)
    sheet('Группы', ['ID группы', 'Группа', 'Категорий', 'Доля оборота'],
          [(gid[g], g, sum(1 for n in live if cat_group[n] == g), round(gshare[g], 6)) for g in groups], [10, 28, 11, 13])
    cat_rows = []
    for name in sorted(live, key=lambda n: cid[n]):
        unit, ns = unit_scale(name)
        g = cat_group[name]
        if g in NO_SCALE_GROUPS and not (g == 'Табак' and name in ('Зажигалки', 'Спички', 'Табачные аксессуары')):
            ns = True
        elif g == 'Детские товары' and ref.NO_SCALE_FOOTWEAR.search(name):
            ns = True
        codes_of = ', '.join(r[0] for r in code_rows if r[4] == name)
        cat_rows.append((cid[name], gid[g], g, name, unit, 'да' if ns else None, 'активна', live[name],
                         round(share[name], 6), codes_of, defs.get(name)))
    sheet('Категории', ['ID категории', 'ID группы', 'Группа', 'Категория', 'Единица', 'Без шкалы', 'Статус',
                        'Товаров', 'Доля оборота', 'Ext-коды целиком', 'Определение'],
          cat_rows, [12, 10, 22, 36, 9, 10, 9, 9, 12, 40, 80])
    sheet('Маппинг кодов', ['Ext-код', 'Ext-название', 'Как относится', 'ID категории', 'Категория'],
          code_rows, [16, 44, 14, 12, 36])
    sheet('Правила разрезов', ['Ext-код', 'Порядок', 'Если в названии любое из', 'И нет ни одного из', 'ID категории', 'Категория'],
          rules_out, [12, 9, 44, 30, 12, 36])
    sheet('Товарный маппинг', ['ID товара', 'Название', 'Откуда', 'ID категории', 'Группа', 'Категория', 'Источник',
                               'Как отнесено', 'Проверить'], manual, [11, 50, 16, 12, 20, 32, 13, 30, 30])
    sheet('Новые коды ДИКСИ', ['Ext-код', 'Ext-название', 'Категория по товарам'], new_codes, [16, 44, 36])
    out.save(SRC)
    ns = sum(1 for r in cat_rows if r[5])
    print('групп', len(groups), 'категорий', len(cat_rows), 'без шкалы', ns, 'кодов', len(code_rows),
          'правил', len(rules_out), 'поштучно', len(manual), 'новых кодов', len(new_codes), 'товаров', len(prods))


if __name__ == '__main__':
    main(sys.argv[1])
