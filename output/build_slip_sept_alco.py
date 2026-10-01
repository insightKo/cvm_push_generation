# -*- coding: utf-8 -*-
"""Купоны на slip — сентябрь (алкоголь) 03.09-30.09.2026. Формат — как июльский файл (build_slip_july_alco.py)."""
from docx import Document
from docx.shared import Pt, RGBColor

NBSP = " "
def rub(x): return f"{int(round(x)):,}".replace(",", NBSP)

CHECK=1000; COEF=0.54; MARGIN=0.30; VAT=1.15; COUPON=100; RESP=0.03; CANN=0.01
promos=[
    (101360, "Вина — купон 100 ₽ на 7 дней",             "вино",             519,   500_000),
    (101361, "Крепкий алкоголь — купон 100 ₽ на 7 дней", "крепкий алкоголь", 479,   500_000),
    (101362, "Пиво — купон 100 ₽ на 7 дней",             "пиво",             105, 1_250_000),
]
rows=[]; sum_to=sum_disc=sum_pl=sum_comm=0
for num,name,cat,avg,cl in promos:
    dop_to=(RESP-CANN)*CHECK*cl*COEF; disc=RESP*cl*COUPON/VAT; pl=dop_to*MARGIN/VAT-disc; comm=10/100*0.82*COEF*cl
    rows.append((num,name,cat,avg,cl,round(COUPON/avg*100),dop_to,disc,pl,comm))
    sum_to+=dop_to; sum_disc+=disc; sum_pl+=pl; sum_comm+=comm

doc=Document(); st=doc.styles["Normal"].font; st.name="Calibri"; st.size=Pt(11)
def p(text,bold=False,size=11):
    par=doc.add_paragraph(); r=par.add_run(text); r.bold=bold; r.font.size=Pt(size); return par
p("Акции с использованием канала: слип-чек. 03.09-30.09.2026", bold=True, size=14)
p("Целевая аудитория: офлайн-покупатели категорий алкоголя (Активные, Новые), для которых слип-чек — основной канал персональной коммуникации после покупки.")
p("Цели:")
for g in ["дополнительный трафик и частота по Активным и Новым покупателям категории",
          "возврат за повторной покупкой в течение 7 дней после выдачи слип-чека"]:
    doc.add_paragraph(g, style="List Bullet")
p("Даты проведения акций: 03.09–30.09.2026")
p("Механика: за период акции после покупки категории клиенту выдаётся слип-чек с купоном на 100 ₽ "
  "на соответствующую категорию; купон действует 7 дней (применяется при предъявлении купона и карты, "
  "не суммируется с жёлтыми ценниками). Выдача купонов прекращается 23.09.2026.")
p(f"Планируемый эффект:  +{rub(sum_to)} р. (Доп ТО); PL +{rub(sum_pl)} р.", bold=True)
p(f"Затраты на коммуникацию на чеках:  {rub(sum_comm)} р.", bold=True)
doc.add_paragraph()
headers=["НОМЕР","Название промо","Старт","Финиш","Сегмент МС","Категория","Клиентов","отклик, %","Каннибал., %","Скидка (купон)","Доп ТО (план), р.","Скидка в категории, р.","PL, р.","Расход бумаги, р."]
t=doc.add_table(rows=1,cols=len(headers)); t.style="Light Grid Accent 1"
for i,h in enumerate(headers):
    c=t.rows[0].cells[i].paragraphs[0].add_run(h); c.bold=True; c.font.size=Pt(8)
for (num,name,cat,avg,cl,dpct,dop_to,disc,pl,comm) in rows:
    cells=t.add_row().cells
    vals=[str(num),name,"03.09.","30.09.","Активные, Новые",cat,rub(cl),"3%","1%",f"100 ₽ (≈{dpct}%)",rub(dop_to),rub(disc),rub(pl),rub(comm)]
    for i,v in enumerate(vals):
        run=cells[i].paragraphs[0].add_run(v); run.font.size=Pt(8)
cells=t.add_row().cells
tot=["","ИТОГО","","","","",rub(sum(r[4] for r in rows)),"","","",rub(sum_to),rub(sum_disc),rub(sum_pl),rub(sum_comm)]
for i,v in enumerate(tot):
    run=cells[i].paragraphs[0].add_run(v); run.bold=True; run.font.size=Pt(8)
out="output/Купоны на slip — сентябрь (алкоголь) 03.09-30.09.docx"
doc.save(out); print("SAVED:",out)
for (num,name,cat,avg,cl,dpct,dop_to,disc,pl,comm) in rows:
    print(f"{num} {cat:18} клиентов {rub(cl):>10}  ДопТО {rub(dop_to):>11}  скидка {rub(disc):>10}  PL {rub(pl):>9}  бумага {rub(comm):>7}  (купон 100₽≈{dpct}%)")
print(f"ИТОГО  ДопТО {rub(sum_to)}  скидка {rub(sum_disc)}  PL {rub(sum_pl)}  бумага {rub(sum_comm)}")
