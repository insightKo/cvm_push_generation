"""Расчёт для КД (согласование с коммерцией) — акции со скидками и монетами ноября 2026 → output/расчет_КД_ноябрь.xlsx.

Формат и формулы — как в расчётах КД июня–октября 2026:
  Доп ТО = клиенты × отклик × 900 ₽; скидка = клиенты × отклик × выгода на откликнувшегося;
  PL = (Доп ТО × 30% − скидка) / 1,15.
Строки — «CVM offline» (Google, только чтение): месяц 11, Настройка «да», механика не «Коммуникация»
(ДЦО-коммуникации не входят: это скидки самой коммерции).

Клиенты — списки I_PROMO_OFFER без КГ (чтение 06.11.2026). Списков ещё нет у 101560–101563 и 101557 —
по базе с пушем на 06.11.2026 (I_CVM_CONTACT орг. 1 × I_CONTACT HAS_PUSH = 1, TOKENS = 1, минус I_GLOBAL_CG),
за вычетом КГ как в списках ноября: сегменты 1–3 — 5 %, 4–6 — 10 %.

Отклик:
  скидка 20 % на категорию (Активные, Новые, 1 день) — 0,05 %, кешбэк 50 % — 0,15 % (нормативы расчётов КД);
  монеты и скидка на чек (нормативов в расчётах КД нет) — доля ЦГ, получившая выгоду, по факту последней
  такой же механики на том же сегменте: I_CVM_EFFECT (CHANNEL_ID = 0), «Скидка ЦГ, ₽» ÷ «Клиентов ЦГ» ÷ номинал.
Средняя стоимость закупки — AVG_PRICE, взвешенная по COUNT_CHECK (data/ассортимент.xlsx), как в октябре;
SKU в подборке: коды категорий — SKU с COUNT_SHOPS ≥ 300, список SKU — число SKU списка.
Коды категорий у акций по списку SKU — cat5 товаров списка (ассортимент.xlsx, недостающие — I_PRODUCT).

Запуск: .venv/Scripts/python.exe output/build_kd_november.py <miss_cat5.json из I_PRODUCT>
"""
import collections
import datetime as dt
import json
import sys
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402

OUT = ROOT / 'output' / 'расчет_КД_ноябрь.xlsx'

# списки I_PROMO_OFFER, CONTROL_GROUP = 0 (06.11.2026)
LISTS = {
    101538: 13872, 101539: 348239, 101540: 16739, 101541: 13872, 101542: 348239, 101543: 16739,
    101544: 13872, 101545: 348239, 101546: 16739, 101547: 13872, 101548: 348239, 101549: 16739,
    101550: 852081, 101551: 962805, 101552: 572912, 101553: 302706, 101554: 817284, 101555: 555762,
    101556: 614016,
}
# база с пушем по сегментам I_CVM_CONTACT, минус I_GLOBAL_CG (06.11.2026)
SEG = {1: 31572, 2: 779453, 3: 2059171, 4: 908150, 5: 868960, 6: 687583}
CG = {1: 0.05, 2: 0.05, 3: 0.05, 4: 0.10, 5: 0.10, 6: 0.10}


def seg_clients(segs):
    return round(sum(SEG[s] * (1 - CG[s]) for s in segs))


# факт аналогов I_CVM_EFFECT: (Скидка ЦГ, ₽; Клиентов ЦГ; номинал)
A_CLUSTER_COIN = [(109112.14, 49533, 100), (105540.20, 47549, 100), (52922.37, 32756, 100)]  # 101445–101447 Перекусы на ходу, 22–25.10
A_ACTIVATE_ACTIVE = [(1805054.34, 2598199, 50)]   # 101378 «Активируй 50 монет на любые покупки», Активные, 10–13.09
A_COINS_PURCHASE = [(355943.51, 1541429, 100)]    # 101358 «Вернём 100 монет за любую покупку», Активные, 13–16.08 (у Оттока такой механики с затратами нет)
A_GIFT_SLEEP = [(91826.26, 227003, 150)]          # 101062 «Дарим 150 монет», Спящие, 23–26.10.2025
A_CHECK_1500 = {                                  # «Активируй N р. на чек от 1500р.» по сегментам
    'act': [(1051253.86, 627619, 150)],           # 101245 Активные, 21–31.05 (у Новых своей нет — как Активные)
    'rnd': [(384993.27, 676750, 300)],            # 101146 Случайные, 24–30.12.2025
    'out': [(343356.24, 1362172, 150)],           # 101351 Отток, 20–31.08
    'slp': [(482341.03, 398551, 300)],            # 101130 Отток и Спящие, 11–28.12.2025
}


def rate(an):
    """Доля ЦГ, получившая выгоду: Σ скидки ÷ Σ (клиентов × номинал), округление до 0,01 п.п."""
    return round(sum(c for c, _, _ in an) / sum(n * v for _, n, v in an), 4)


def rate_check_1500():
    parts = {'act': seg_clients([1, 2, 3]), 'rnd': seg_clients([4]), 'out': seg_clients([5]), 'slp': seg_clients([6])}
    resp = sum(parts[k] * sum(c for c, _, _ in A_CHECK_1500[k]) / sum(n * v for _, n, v in A_CHECK_1500[k]) for k in parts)
    return round(resp / sum(parts.values()), 4)


CATEGORY_WORDS = {
    '101538': 'сэндвичи и бургеры', '101539': 'бананы', '101540': 'мясные снеки', '101541': 'роллы и онигири',
    '101542': 'хурма', '101543': 'чипсы', '101544': 'лапша и паста', '101545': 'виноград', '101546': 'газировка',
    '101547': 'пицца и газировка', '101548': 'шоколад и батончики', '101549': 'семечки',
    '101560': 'средства для мытья посуды', '101561': 'средства для ухода за полостью рта',
    '101562': 'дезодоранты и гели для душа', '101563': 'шампуни и бальзамы для волос',
    '101554_1': 'шоколад и шоколадные батончики', '101554_2': 'чипсы, сухарики и кола',
    '101554_3': 'торты, пирожные и чёрный чай в пакетиках',
}


def num(s):
    return float(str(s).replace('%', '').replace('\xa0', '').replace(' ', '').replace(',', '.'))


def dmy(s):
    d, m = s.strip().strip('.').split('.')[:2]
    return dt.datetime(2026, int(m), int(d))


def load_assort(miss_path):
    wb = openpyxl.load_workbook(ROOT / 'data' / 'ассортимент.xlsx', read_only=True)
    by_sku, by_cat = collections.defaultdict(list), collections.defaultdict(list)
    for r in wb['Лист1'].iter_rows(min_row=2, values_only=True):
        if not r[2]:
            continue
        code = str(r[2])
        by_sku[code[:-2] if code.endswith('_0') else code].append(r)
        by_cat[str(r[3])].append(r)
    miss = {(a[:-2] if a.endswith('_0') else a): b for a, b, _ in json.loads(Path(miss_path).read_text(encoding='utf-8'))}
    return by_sku, by_cat, miss


def avg_price(rows):
    cc = sum(r[7] or 0 for r in rows)
    return round(sum((r[10] or 0) * (r[7] or 0) for r in rows) / cc) if cc else None


def main():
    by_sku, by_cat, miss = load_assort(sys.argv[1])
    v = sc.get_spreadsheet().worksheet(sc.SHEET_CVM_OFFLINE).get_all_values()
    h = v[0]
    c = {k: h.index(k) for k in ('Настройка', 'НОМЕР', 'Название промо', 'Сегмент', 'Год', 'Месяц', 'Старт акции',
                                  'Окончание акции', 'Каналы коммуникации', 'Механика', 'Категория', 'Бонусы')}
    c_disc = h.index('Скидка')  # первая «Скидка» — условие (20%, 250)

    rows = []
    for r in v[1:]:
        if (r[c['Год']].strip() != '2026' or r[c['Месяц']].strip() != '11' or r[c['Настройка']].strip() != 'да'
                or r[c['Механика']] == 'Коммуникация'):
            continue
        num_s = r[c['НОМЕР']].strip()
        base = int(num_s.split('_')[0])
        mech = r[c['Механика']]
        codes = [x.strip() for x in r[c['Категория']].replace('\n', ',').split(',') if x.strip()]
        row = dict(n=int(num_s) if num_s.isdigit() else num_s, name=r[c['Название промо']], seg=r[c['Сегмент']],
                   start=dmy(r[c['Старт акции']]), end=dmy(r[c['Окончание акции']]), ch=r[c['Каналы коммуникации']],
                   cat=CATEGORY_WORDS.get(num_s, CATEGORY_WORDS.get(str(base), 'любая покупка')),
                   codes='', avg=None, sku=None, l_eq_k=False)
        if base in LISTS:
            row['h'] = LISTS[base]
        elif base in (101560, 101561, 101562, 101563):
            row['h'] = seg_clients([1, 2, 3])
        elif base == 101557:
            row['h'] = seg_clients([1, 2, 3, 4, 5, 6])
        else:
            raise SystemExit(f'нет численности для {num_s}')

        if codes and all(x.isdigit() and len(x) == 8 for x in codes):        # коды категорий cat5
            ar = [x for k in codes for x in by_cat[k]]
            row.update(codes=', '.join(codes), avg=avg_price(ar), sku=len({x[2] for x in ar if (x[6] or 0) >= 300}))
        elif codes:                                                          # список SKU
            ar = [x for k in codes for x in by_sku.get(k, [])]
            c5 = {str(x[3]) for x in ar} | {miss[k] for k in codes if k not in by_sku and k in miss}
            nf = [k for k in codes if k not in by_sku and k not in miss]
            if nf:
                raise SystemExit(f'{num_s}: нет cat5 у {nf[:5]}')
            row.update(codes=', '.join(sorted(c5)), avg=avg_price(ar), sku=len(codes))

        if mech == 'Скидка X% на категорию':
            p = num(r[c_disc]) / 100
            row.update(i=0.0005, k=f'=L{{r}}', l=f'={p:.0%}*H{{r}}*I{{r}}*O{{r}}', l_eq_k=True)
        elif mech == 'Кешбэк X% с покупки категории':
            p = num(r[c['Бонусы']]) / 100
            row.update(i=0.0015, k=f'=L{{r}}', l=f'={p:.0%}*H{{r}}*I{{r}}*O{{r}}', l_eq_k=True)
        elif mech == 'Кешбэк X р. с покупки категории':
            row.update(i=rate(A_CLUSTER_COIN), k=f'=H{{r}}*I{{r}}*{num(r[c["Бонусы"]]):.0f}', l='=K{r}', l_eq_k=True)
        elif mech == 'Начисление монет':
            an = A_GIFT_SLEEP if base == 101556 else A_ACTIVATE_ACTIVE
            row.update(i=rate(an), k=f'=H{{r}}*I{{r}}*{num(r[c["Бонусы"]]):.0f}', l=None)
        elif mech == 'Кешбэк X монет за покупку':
            row.update(i=rate(A_COINS_PURCHASE), k=f'=H{{r}}*I{{r}}*{num(r[c["Бонусы"]]):.0f}', l=None)
        elif mech == 'Скидка X р. на чек':
            row.update(i=rate_check_1500(), k=f'=H{{r}}*I{{r}}*{num(r[c_disc]):.0f}', l=None)
        else:
            raise SystemExit(f'{num_s}: механика без правила расчёта — {mech}')
        rows.append(row)

    rows.sort(key=lambda x: (x['start'], x['end'], str(x['n'])))

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'ноябрь'
    head = ['НОМЕР', 'Название промо', 'Сегмент', 'Старт акции', 'Окончание акции', 'Канал', 'Категория',
            'Кол-во клиентов', 'Отклик, %', 'Доп ТО, р.', 'Скидка, р.', 'Скидка в категории, р.', 'PL, р.',
            'Коды категорий', 'Средняя стоимость закупки, ₽', 'SKU в подборке']
    ws.append(head)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical='top')
    for i, x in enumerate(rows, start=2):
        ws.append([x['n'], x['name'], x['seg'], x['start'], x['end'], x['ch'], x['cat'], x['h'], x['i'],
                   f'=H{i}*I{i}*900', x['k'].format(r=i), x['l'].format(r=i) if x['l'] else None,
                   f'=(J{i}*0.3-K{i})/1.15', x['codes'] or None, x['avg'], x['sku']])
        for col in (4, 5):
            ws.cell(i, col).number_format = 'DD.MM.YYYY'
        for col in (8, 10, 11, 12, 13):
            ws.cell(i, col).number_format = '#,##0'
        ws.cell(i, 9).number_format = '0.00%'
        for col in range(1, 17):
            ws.cell(i, col).alignment = Alignment(vertical='top', wrap_text=col in (2, 3, 7))
    widths = {'A': 11, 'B': 44, 'C': 44, 'D': 12, 'E': 12, 'F': 8, 'G': 26, 'H': 12, 'I': 9, 'J': 13, 'K': 12,
              'L': 13, 'M': 12, 'N': 30, 'O': 13, 'P': 10}
    for k, w in widths.items():
        ws.column_dimensions[k].width = w
    ws.freeze_panes = 'B2'
    ws.auto_filter.ref = f'A1:{get_column_letter(len(head))}{len(rows) + 1}'
    wb.save(OUT)
    print('строк', len(rows), '→', OUT)


if __name__ == '__main__':
    main()
