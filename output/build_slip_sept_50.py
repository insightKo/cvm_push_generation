# -*- coding: utf-8 -*-
"""Купоны на slip — сентябрь 2026: купон 50 р. на любую покупку (101391), выдача 09.09–30.09.
Формат и формулы — как августовский «Купоны на slip — 05.08-31.08.docx» (101332):
  Доп ТО = (отклик − каннибализация) × клиентов × INCR, где INCR = 459 ₽ инкремента на откликнувшегося (ср. чек 850 ₽ × 54%);
  Скидка = отклик × клиентов × 50 ₽;  PL = Доп ТО × 30% − Скидка.
Аудитория — лист «Параметры сегментов» таблицы CVM offline (дамп 07.09.2026): офлайн и омни клиенты
сегментов Новые, Активные, Активные LFL, Отток, Спящие БЕЗ PUSH и APP, минус фрод без PUSH.
Расход бумаги — по августовской норме 150 тыс ₽ на 6,45 млн клиентов."""
import json
from docx import Document
from docx.shared import Pt

RESP, CANN, COUPON, INCR, MARGIN = 0.03, 0.01, 50, 459, 0.30
PAPER_PER_CLIENT = 150_000 / 6_450_000

d = json.load(open('output/cvm_offline_dump_2026-09-07.json'))
seg = d['Параметры сегментов']
def num(s): return int(str(s).replace('\xa0', '').replace(' ', '') or 0)
rows, cur = {}, None
for r in seg:
    v = list(r.values())
    if v[1] in ('Фрод', 'Новые', 'Активные', 'Активные LFL', 'Случайные', 'Отток', 'Спящие', 'Аффинитивные'):
        cur = v[1]
        if v[3]: rows[(cur, 'all')] = (num(v[3]), num(v[4]))
    elif v[1] in ('omni', 'offline', 'e-commerce') and cur:
        rows[(cur, v[1])] = (num(v[3]), num(v[4]))
aud = 0
detail = []
for s in ('Новые', 'Активные', 'Активные LFL', 'Отток', 'Спящие'):
    for ch in ('offline', 'omni'):
        tot, app = rows.get((s, ch), (0, 0))
        aud += tot - app; detail.append((s, ch, tot - app))
fraud = rows[('Фрод', 'all')][0] - rows[('Фрод', 'all')][1]
clients = aud - fraud

dop_to = (RESP - CANN) * clients * INCR
disc = RESP * clients * COUPON
pl = dop_to * MARGIN - disc
paper = clients * PAPER_PER_CLIENT
mln = lambda x: f"{x/1e6:.2f}".replace('.', ',')

doc = Document(); st = doc.styles['Normal'].font; st.name = 'Calibri'; st.size = Pt(11)
def p(text, bold=False, size=11):
    par = doc.add_paragraph(); r = par.add_run(text); r.bold = bold; r.font.size = Pt(size); return par
p('Акции с использованием канала: слип-чек. 09.09-30.09.2026', bold=True, size=14)
p('Целевая аудитория: офлайн-клиенты ДИКСИ, для которых слип-чек используется как основной канал персональной коммуникации (без приложения и PUSH).')
p('Цели:')
doc.add_paragraph('дополнительный трафик по всем офлайн-сегментам (купон 50р. на любую покупку)', style='List Bullet')
doc.add_paragraph('возврат за повторной покупкой в течение 7 дней после выдачи слип-чека', style='List Bullet')
p('Даты проведения акций: 09.09.-30.09.2026')
p('Механика: за период акции после любой покупки клиенту выдаётся слип-чек с QR-купоном на 50р. (применяется при предъявлении купона и карты на 1 покупку, не более 2 применений в сутки; не действует на социально значимые товары, алкоголь, табак, лотерейные билеты и промотовары по жёлтым ценникам). Выдача купонов прекращается 23.09.2026 — купон действует 7 дней.')
p(f'Затраты на коммуникацию на чеках: {round(paper/1000)} тыс ₽')
p('Планируемый эффект: ')
p(f'+{mln(dop_to)} млн ₽ (Доп ТО); PL +{mln(pl)} млн ₽', bold=True)
headers = ['Название промо', 'Старт', 'Финиш', 'Сегмент МС', 'Клиентов, млн', 'отклик, %', 'Каннибал., %', 'Доп ТО, млн ₽', 'Скидка, млн ₽', 'PL, млн ₽']
t = doc.add_table(rows=1, cols=len(headers)); t.style = 'Light Grid Accent 1'
for i, h in enumerate(headers):
    c = t.rows[0].cells[i].paragraphs[0].add_run(h); c.bold = True; c.font.size = Pt(8)
vals = ['101391 Купон 50р. на любую покупку', '09.09.2026', '30.09.2026', 'Все офлайн-сегменты без APP (Активные, Новые, Отток, Спящие)',
        mln(clients), '3%', '1%', mln(dop_to), mln(disc), mln(pl)]
cells = t.add_row().cells
for i, v in enumerate(vals):
    run = cells[i].paragraphs[0].add_run(v); run.font.size = Pt(8)
out = 'output/Купоны на slip — 09.09-30.09.docx'
doc.save(out); print('SAVED:', out)
for s, ch, n in detail: print(f'  {s:14} {ch:8} без PUSH/APP: {n:>10,}'.replace(',', ' '))
print(f'  фрод без PUSH: -{fraud:,}'.replace(',', ' '))
print(f'Клиентов: {clients:,} | Доп ТО: {dop_to:,.0f} | Скидка: {disc:,.0f} | PL: {pl:,.0f} | Бумага: {paper:,.0f}'.replace(',', ' '))
