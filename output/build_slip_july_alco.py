# -*- coding: utf-8 -*-
"""Сборка файла 'Купоны на slip — июль (алкоголь)' по аналогии с мартовским,
формат расчёта — со скрина (вкладка 'купоны 01.01-11.01'), категорийный."""
from docx import Document
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH

NBSP = " "
def rub(x):
    return f"{int(round(x)):,}".replace(",", NBSP)

# --- константы расчёта (со скрина / существующих вкладок) ---
CHECK = 1000      # средний чек ДИКСИ, ₽ (как в формуле Доп ТО на скрине)
COEF  = 0.54      # доля инкремента (Активные/Новые)
MARGIN= 0.30      # фронт-маржа
VAT   = 1.15      # налоговый коэффициент
COUPON= 100       # номинал купона, ₽
RESP  = 0.03      # отклик (по аналогии со скрином: напитки = 3%)
CANN  = 0.01      # каннибализация (со скрина = 1%)

promos = [
    # номер, название, категория, ср.стоимость, клиентов
    (101280, "Крепкий алкоголь — купон 100 ₽ на 7 дней", "крепкий алкоголь", 482, 500_000),
    (101281, "Вина — купон 100 ₽ на 7 дней",            "вино",            523, 500_000),
    (101282, "Пиво — купон 100 ₽ на 7 дней",            "пиво",            102, 1_250_000),
]

rows = []
sum_to = sum_disc = sum_pl = sum_comm = 0
for num, name, cat, avg, cl in promos:
    dop_to = (RESP - CANN) * CHECK * cl * COEF           # =(I-J)*1000*H*54%
    disc   = RESP * cl * COUPON / VAT                    # купон 100 ₽: =отклик*H*100/1,15
    pl     = dop_to * MARGIN / VAT - disc                # =ТО*30%/1,15 - скидка
    comm   = 10/100 * 0.82 * COEF * cl                   # =10/100*0,82*54%*H
    disc_pct = round(COUPON / avg * 100)
    rows.append((num, name, cat, avg, cl, disc_pct, dop_to, disc, pl, comm))
    sum_to += dop_to; sum_disc += disc; sum_pl += pl; sum_comm += comm

doc = Document()
st = doc.styles["Normal"].font; st.name = "Calibri"; st.size = Pt(11)

def p(text, bold=False, size=11):
    par = doc.add_paragraph()
    r = par.add_run(text); r.bold = bold; r.font.size = Pt(size)
    return par

p("Акции с использованием канала: слип-чек. 01.07–31.07.2026", bold=True, size=13)
p("Целевая аудитория: офлайн-клиенты ДИКСИ из сегментов «Активные» и «Новые», "
  "для которых слип-чек используется как основной канал персональной коммуникации после покупки.")
p("Цели:", bold=True)
for g in ["промотировать категории алкоголя (крепкий алкоголь, вино, пиво) среди активных и новых клиентов",
          "поднять частоту покупок и средний чек в категории",
          "вернуть купонный трафик в течение 7 дней после выдачи слип-чека"]:
    doc.add_paragraph(g, style="List Bullet")
p("Даты проведения акций: 01.07–31.07.2026")
p("Механика: за период акции после покупки клиенту выдаётся слип-чек с купоном на 100 ₽ "
  "на соответствующую категорию; купон действует 7 дней (применяется при предъявлении купона и карты, "
  "не суммируется с жёлтыми ценниками).")
p(f"Планируемый эффект:  +{rub(sum_to)} р. (Доп ТО); PL +{rub(sum_pl)} р.", bold=True)
p(f"Затраты на коммуникацию на чеках:  {rub(sum_comm)} р.", bold=True)
doc.add_paragraph()

headers = ["НОМЕР", "Название промо", "Старт", "Финиш", "Сегмент МС", "Категория",
           "Клиентов", "отклик, %", "Каннибал., %", "Скидка (купон)",
           "Доп ТО (план), р.", "Скидка в категории, р.", "PL, р.", "Расход бумаги, р."]
t = doc.add_table(rows=1, cols=len(headers)); t.style = "Light Grid Accent 1"
for i, h in enumerate(headers):
    c = t.rows[0].cells[i].paragraphs[0].add_run(h); c.bold = True; c.font.size = Pt(8)

for (num, name, cat, avg, cl, dpct, dop_to, disc, pl, comm) in rows:
    cells = t.add_row().cells
    vals = [str(num), name, "01.07.", "31.07.", "Активные, Новые", cat,
            rub(cl), "3%", "1%", f"100 ₽ (≈{dpct}%)",
            rub(dop_to), rub(disc), rub(pl), rub(comm)]
    for i, v in enumerate(vals):
        run = cells[i].paragraphs[0].add_run(v); run.font.size = Pt(8)

# строка ИТОГО
cells = t.add_row().cells
tot = ["", "ИТОГО", "", "", "", "", rub(sum(r[4] for r in rows)), "", "", "",
       rub(sum_to), rub(sum_disc), rub(sum_pl), rub(sum_comm)]
for i, v in enumerate(tot):
    run = cells[i].paragraphs[0].add_run(v); run.bold = True; run.font.size = Pt(8)

doc.add_paragraph()
note = doc.add_paragraph()
r = note.add_run("Параметры расчёта (по аналогии со скрином и вкладками «купоны»): "
    "средний чек 1000 ₽; доля инкремента 54%; фронт-маржа 30%; НДС-коэф. 1,15; "
    "отклик 3% и каннибализация 1% — по аналогии с акцией «Скидка на напитки» со скрина; "
    "номинал купона 100 ₽. Средняя стоимость закупки по категории "
    "(крепкий алкоголь 482 ₽, вино 523 ₽, пиво 102 ₽) — взвешенная по чекам из ассортимент.xlsx, "
    "использована для оценки глубины скидки в %.")
r.italic = True; r.font.size = Pt(8); r.font.color.rgb = RGBColor(0x60,0x60,0x60)

out = "output/Купоны на slip — июль (алкоголь) 01.07-31.07.docx"
doc.save(out)
print("SAVED:", out)
print()
print("=== ПРОВЕРКА РАСЧЁТА ===")
for (num, name, cat, avg, cl, dpct, dop_to, disc, pl, comm) in rows:
    print(f"{num} {cat:18} клиентов {rub(cl):>10}  ДопТО {rub(dop_to):>11}  "
          f"скидка {rub(disc):>10}  PL {rub(pl):>9}  бумага {rub(comm):>7}  (купон 100₽≈{dpct}%)")
print(f"{'ИТОГО':>25}{'':17}  ДопТО {rub(sum_to):>11}  скидка {rub(sum_disc):>10}  "
      f"PL {rub(sum_pl):>9}  бумага {rub(sum_comm):>7}")
