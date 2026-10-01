# -*- coding: utf-8 -*-
import math, openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.properties import PageSetupProperties

PATH='Оценка_трудоёмкости_внедрение_ДИКСИ_Лена.xlsx'
SHEET="'Трудоёмкость (детально)'"
RATE=4350
MONTHS=["июль.26","авг.26","сент.26","окт.26","ноя.26","дек.26"]
SM={1:[0,1],2:[2,3,4],3:[5]}
SL={1:"ЭТАП 1 — Персонализация ДЦО (июль–август)",
    2:"ЭТАП 2 — Полноценный CVM: 100 сегментов (сентябрь–ноябрь)",
    3:"ЭТАП 3 — Масштаб + простая оптимизация (декабрь)"}
WBS=[
 (1,"1.1","Развёртывание сервиса на нашем сервере: окружение, домены, доступы, безопасность, бэкапы","Технический","Data engineer",56),
 (1,"1.2","Интеграция источников данных ДЦО (клиенты, история покупок, текущие ДЦО-акции)","Технический","Data engineer",30),
 (1,"1.3","Интеграция готового модуля сегментации MCI в проект + интерфейс выбора сегментов","Технический","Data engineer + Backend",70),
 (1,"1.4","Настройка автоматических контрольных групп (на базе MCI)","Технический","CVM-аналитик",40),
 (1,"1.5","Движок персонализации текстов ДЦО (адаптация генератора под сегмент)","Технический","AI-инженер",110),
 (1,"1.6","Методология персонализации ДЦО (карта сегмент → оффер)","Операционный","Руководитель проекта / CVM-методолог",40),
 (1,"1.7","Перевод текущих ДЦО-акций в персональный формат (контент)","Операционный","AI-инженер + маркетолог",100),
 (1,"1.8","Создание базы знаний: шаблоны, скиллы, гайдлайны, tone of voice","Операционный","AI-инженер + маркетолог",60),
 (1,"1.9","Согласование и выстраивание бизнес-процесса планирования и заведения ДЦО","Операционный","Руководитель проекта + аналитик",60),
 (1,"1.10","Пилотный запуск + аналитика постэффектов vs контрольная группа","Операционный","CVM-аналитик",60),
 (1,"1.11","Управление этапом: статусы, демо, приёмка","Операционный","Руководитель проекта / CVM-методолог",64),
 (1,"1.12","Аренда и настройка отдельных серверов под агентов: изолированные среды, оркестрация, мониторинг","Технический","Data engineer",40),
 (1,"1.13","Безопасность: шифрование и дешифровка данных для агентов (доступ к ПДн, маскирование, ключи)","Технический","Data engineer / безопасность",40),
 (2,"2.1","Интеграция и настройка 100 сегментов MCI в проекте (конфиг, обновление, выгрузки)","Технический","Data engineer",90),
 (2,"2.2","Конвейер массовой генерации контента (push / купон / баннер / e-mail)","Технический","AI-инженер + маркетолог",130),
 (2,"2.3","Операционный интерфейс заведения акций (бриф → сегмент → механика → контент → расписание → выгрузка)","Технический","Backend-разработчик",110),
 (2,"2.4","Интеграция деплинков, SKU и механик в конвейер","Технический","Data engineer",60),
 (2,"2.15","Разработка агента-конструктора акций: сборка механики, условий, SKU/деплинков, расчёт выгоды и сроков","Технический","AI-инженер",90),
 (2,"2.16","Разработка агента-оператора заведения акций: автозаполнение полей, расписание, выгрузка, валидация, ввод в систему","Технический","AI-инженер + Backend",80),
 (2,"2.5","Простой оптимизатор по акциям (подбор оффера под сегмент, правила/скоринг)","Технический","AI-инженер",100),
 (2,"2.6","Дашборд эффективности и аналитика постэффектов сегментных акций / контрольных групп","Технический","CVM-аналитик",90),
 (2,"2.7","Формирование контента под 100 сегментов (тексты, проверка ToV, скиллы)","Операционный","AI-инженер + маркетолог",130),
 (2,"2.8","Операционная часть заведения акций (настройка, расписание, QA)","Операционный","Маркетолог + аналитик",90),
 (2,"2.9","Выстраивание бизнес-процесса планирования и согласования сетки CVM-акций","Операционный","Руководитель проекта + аналитик",70),
 (2,"2.10","Методология 100 сегментов (карта сегмент → категория → механика)","Операционный","Руководитель проекта / CVM-методолог",50),
 (2,"2.11","Обучение и онбординг команды заказчика","Операционный","Руководитель проекта / CVM-методолог",35),
 (2,"2.12","Управление этапом: статусы, демо, приёмка","Операционный","Руководитель проекта / CVM-методолог",80),
 (2,"2.13","Обучение и вывод агентов в продакшн (копирайтер, редактор-гуманизатор, дизайнер баннеров, оптимизатор)","Технический","AI-инженер",50),
 (2,"2.14","Безопасность: дешифровка и подготовка данных для аналитики (постэффекты, контрольные группы, дашборды)","Технический","CVM-аналитик + Data engineer",30),
 (3,"3.1","Допиливание и стабилизация интерфейса заведения акций","Технический","Backend-разработчик",60),
 (3,"3.2","Формирование 200 акций (массовое заведение через конвейер)","Операционный","AI-инженер + маркетолог",90),
 (3,"3.3","Простая оптимизация: автоподбор оффера и частоты, антидубли, приоритизация","Технический","AI-инженер",70),
 (3,"3.4","Аналитика постэффектов по 200 акциям, финальный отчёт","Операционный","CVM-аналитик",45),
 (3,"3.5","Подготовка к передаче сервиса заказчику (документация, runbook, доступы)","Операционный","Data engineer",45),
 (3,"3.6","Управление этапом, приёмка, передача","Операционный","Руководитель проекта / CVM-методолог",35),
]
EXTRA=[  # без строки НДС
 ("Модуль сегментации MCI","Включено","0 ₽","Бесплатно на период проекта (6 мес); далее — по условиям MCI"),
 ("Серверная инфраструктура — аренда защищённого сервера (мощности, хранилище, при моделях GPU)","Ежемесячно","~45 000 ₽/мес","Аренда со всеми защитами и контурами безопасности"),
 ("Аренда отдельных серверов под агентов (изолированные среды исполнения)","Ежемесячно","~45 000 ₽/мес за среду","Число сред — по нагрузке/числу агентов"),
 ("Подписка Claude Max — максимальный тариф (20x)","Ежемесячно","$200/мес","Генерация текстов; оплата по курсу"),
 ("Подписка Nano Banana / Google AI Ultra — максимальный тариф","Ежемесячно","$200/мес","Генерация изображений/баннеров; оплата по курсу"),
 ("Доп. токены API сверх лимитов подписок","По факту","по объёму","При превышении лимитов подписок"),
 ("Интеграции на стороне заказчика (CRM/рассылки, выгрузки, API заведения)","Разовое","вне сметы","Трудозатраты ИТ заказчика"),
 ("Контур персональных данных (152-ФЗ)","Разовое + ежемес.","по факту","Защищённое хранение, обезличивание, согласование ИБ"),
 ("Резерв на изменение объёма и доработки","Разовое","~10–15%","К смете работ"),
]
# стили
TITLE=Font(name="Inter",size=16,bold=True,color="3A2A6B"); H=Font(name="Inter",size=11,bold=True,color="FFFFFF")
B=Font(name="Inter",size=11,bold=True); ST=Font(name="Inter",size=12,bold=True,color="FFFFFF")
N=Font(name="Inter",size=11); SMALL=Font(name="Inter",size=10,color="666666")
HEAD=PatternFill("solid",fgColor="6B4FBB"); ST_FILL=PatternFill("solid",fgColor="4A3A8C")
ORANGE_L=PatternFill("solid",fgColor="FDE8D6"); GREEN_L=PatternFill("solid",fgColor="E3F2E1")
GREY_L=PatternFill("solid",fgColor="EEEEEE")
med=Side(style="thin",color="BBBBBB"); BORD=Border(left=med,right=med,top=med,bottom=med)
CEN=Alignment(horizontal="center",vertical="center",wrap_text=True)
LEFT=Alignment(horizontal="left",vertical="center",wrap_text=True)
RIGHT=Alignment(horizontal="right",vertical="center")
NUM="# ##0"; MONEY="# ##0 ₽"
W2=[(0.6,0.4),(0.4,0.6),(0.65,0.35),(0.45,0.55),(0.55,0.45),(0.35,0.65),(0.7,0.3),(0.5,0.5),(0.45,0.55),(0.6,0.4),(0.35,0.65),(0.55,0.45),(0.65,0.35)]
W3=[(0.45,0.35,0.2),(0.2,0.35,0.45),(0.4,0.3,0.3),(0.3,0.3,0.4),(0.5,0.3,0.2),(0.25,0.4,0.35),(0.35,0.4,0.25),(0.3,0.45,0.25),(0.2,0.4,0.4),(0.45,0.25,0.3),(0.3,0.35,0.35),(0.4,0.35,0.25),(0.25,0.35,0.4),(0.35,0.3,0.35),(0.3,0.4,0.3),(0.4,0.3,0.3)]
def split(st,hrs,idx):
    ms=SM[st]; n=len(ms)
    if n==1: return {ms[0]:hrs}
    w=(W2 if n==2 else W3)[idx%len(W2 if n==2 else W3)]
    a=[round(hrs*x) for x in w]; a[-1]+=hrs-sum(a)
    return {ms[i]:a[i] for i in range(n)}

wb=openpyxl.load_workbook(PATH)

# =========================== ДЕТАЛЬНЫЙ ЛИСТ (формулы) ===========================
ws=wb["Трудоёмкость (детально)"]
for mc in list(ws.merged_cells.ranges): ws.unmerge_cells(str(mc))
ws.delete_rows(1, ws.max_row+10)
NC=12; WID=[6,9,50,16,30,9,9,9,9,9,9,10]
for i,w in enumerate(WID,1): ws.column_dimensions[get_column_letter(i)].width=w
ws.merge_cells(start_row=1,start_column=1,end_row=1,end_column=NC)
ws.cell(1,1,"Детальный план работ — помесячно   (ставка 4 350 ₽/час, без НДС)").font=TITLE
ws.row_dimensions[1].height=28
r=3
for i,h in enumerate(["№","Этап","Задача","Блок","Роль"]+MONTHS+["Часы"],1):
    c=ws.cell(r,i,h); c.font=H; c.fill=HEAD; c.alignment=CEN; c.border=BORD
ws.row_dimensions[r].height=34
r=4; idx=0; first=4
for st in (1,2,3):
    ws.merge_cells(start_row=r,start_column=1,end_row=r,end_column=NC)
    cc=ws.cell(r,1,SL[st]); cc.font=ST; cc.fill=ST_FILL; cc.alignment=LEFT; ws.row_dimensions[r].height=26
    for c in range(1,NC+1): ws.cell(r,c).border=BORD
    r+=1
    for (s,num,task,bl,role,hrs) in WBS:
        if s!=st: continue
        mm=split(st,hrs,idx); idx+=1
        ws.cell(r,1,num).font=B; ws.cell(r,2,f"Этап {st}").font=N
        ws.cell(r,3,task).font=N; ws.cell(r,4,bl).font=B; ws.cell(r,5,role).font=N
        for mi in range(6):
            v=mm.get(mi); x=ws.cell(r,6+mi, v if v else None); x.font=N; x.number_format=NUM; x.alignment=CEN
        ws.cell(r,12, f"=SUM(F{r}:K{r})").font=B; ws.cell(r,12).number_format=NUM   # ФОРМУЛА часов
        for c in range(1,NC+1):
            cell=ws.cell(r,c); cell.border=BORD; cell.alignment=LEFT if c in (3,5) else CEN
            if c==12: cell.alignment=RIGHT
            if c==4: cell.fill=ORANGE_L if bl=="Технический" else GREEN_L
        lines=max(math.ceil(len(task)/46), math.ceil(len(role)/24))
        ws.row_dimensions[r].height=max(30,18*lines+10)
        r+=1
last=r-1
# ИТОГО ПО МЕСЯЦАМ, часы (формулы-суммы)
ws.cell(r,3,"ИТОГО ПО МЕСЯЦАМ, часы").font=B
for mi in range(6):
    L=get_column_letter(6+mi); x=ws.cell(r,6+mi, f"=SUM({L}{first}:{L}{last})")
    x.font=B; x.number_format=NUM; x.alignment=CEN; x.fill=GREY_L
x=ws.cell(r,12, f"=SUM(L{first}:L{last})"); x.font=Font(name="Inter",size=12,bold=True,color="F47A20"); x.number_format=NUM; x.alignment=RIGHT
for c in range(1,NC+1): ws.cell(r,c).border=BORD; ws.cell(r,c).fill=GREY_L
ws.row_dimensions[r].height=24; hrow=r; r+=1
# ИТОГО ПО МЕСЯЦАМ, ₽ (= часы*ставка)
ws.cell(r,3,"ИТОГО ПО МЕСЯЦАМ, ₽").font=B
for mi in range(6):
    L=get_column_letter(6+mi); x=ws.cell(r,6+mi, f"={L}{hrow}*{RATE}")
    x.font=B; x.number_format=MONEY; x.alignment=RIGHT; x.fill=GREY_L
x=ws.cell(r,12, f"=L{hrow}*{RATE}"); x.font=Font(name="Inter",size=11,bold=True,color="F47A20"); x.number_format=MONEY; x.alignment=RIGHT; x.fill=GREY_L
for c in range(1,NC+1): ws.cell(r,c).border=BORD
ws.row_dimensions[r].height=24; r+=2
# ДОП. РАСХОДЫ (без НДС)
ws.merge_cells(start_row=r,start_column=1,end_row=r,end_column=NC)
ws.cell(r,1,"ДОПОЛНИТЕЛЬНЫЕ РАСХОДЫ (вне трудозатрат по часам)").font=ST; ws.cell(r,1).fill=ST_FILL; ws.cell(r,1).alignment=LEFT
ws.row_dimensions[r].height=24
for c in range(1,NC+1): ws.cell(r,c).border=BORD
r+=1
ws.cell(r,1,"Статья").font=H; ws.merge_cells(start_row=r,start_column=1,end_row=r,end_column=5)
ws.cell(r,6,"Стоимость").font=H; ws.merge_cells(start_row=r,start_column=6,end_row=r,end_column=8)
ws.cell(r,9,"Комментарий").font=H; ws.merge_cells(start_row=r,start_column=9,end_row=r,end_column=NC)
for c in range(1,NC+1): ws.cell(r,c).fill=HEAD; ws.cell(r,c).border=BORD; ws.cell(r,c).alignment=CEN
ws.row_dimensions[r].height=24; r+=1
for (item,typ,cost,note) in EXTRA:
    ws.cell(r,1,item).font=N; ws.merge_cells(start_row=r,start_column=1,end_row=r,end_column=5); ws.cell(r,1).alignment=LEFT
    ws.cell(r,6,cost).font=B; ws.merge_cells(start_row=r,start_column=6,end_row=r,end_column=8); ws.cell(r,6).alignment=LEFT
    ws.cell(r,9,f"{typ}. {note}").font=N; ws.merge_cells(start_row=r,start_column=9,end_row=r,end_column=NC); ws.cell(r,9).alignment=LEFT
    for c in range(1,NC+1): ws.cell(r,c).border=BORD
    ws.row_dimensions[r].height=26; r+=1
ws.cell(r+1,1,"Доп. расходы не входят в смету работ и оплачиваются отдельно.").font=SMALL
ws.freeze_panes="A4"

# =========================== СТОИМОСТЬ (формулы-связки) ===========================
wc=wb["Стоимость проекта"]
def sif(stage): return f"=SUMIFS({SHEET}!$L:$L,{SHEET}!$B:$B,\"{stage}\")"
def sifb(stage,block): return f"=SUMIFS({SHEET}!$L:$L,{SHEET}!$B:$B,\"{stage}\",{SHEET}!$D:$D,\"{block}\")"
rows={4:("Этап 1",None),5:("Этап 1","Технический"),6:("Этап 1","Операционный"),
      7:("Этап 2",None),8:("Этап 2","Технический"),9:("Этап 2","Операционный"),
      10:("Этап 3",None),11:("Этап 3","Технический"),12:("Этап 3","Операционный")}
for rr,(stg,bl) in rows.items():
    wc.cell(rr,4, sif(stg) if bl is None else sifb(stg,bl)); wc.cell(rr,4).number_format=NUM
    wc.cell(rr,5, f"=D{rr}*{RATE}"); wc.cell(rr,5).number_format=MONEY
wc.cell(13,4,"=D4+D7+D10").number_format=NUM
wc.cell(13,5,"=E4+E7+E10").number_format=MONEY
wc.cell(14,5,"=E13/6").number_format=MONEY

# =========================== АГЕНТЫ (+2, без заливки) ===========================
wa=wb["Агенты"]
new=[
 ("8. Агент-конструктор акций","Собирает акцию: механика, условия, SKU/деплинки, расчёт выгоды, сроки","Этап 2 (2.4, 2.15)","окт–ноя","Claude Max + механики/правила CVM"),
 ("9. Агент-оператор заведения акций","Вводит акцию в систему: автозаполнение полей, расписание, выгрузка, валидация/QA","Этап 2 (2.3, 2.16)","окт–ноя","Claude Max + интеграции/API заведения"),
]
rr=12
for ag,func,train,launch,base in new:
    wa.cell(rr,1,ag).font=B; wa.cell(rr,2,func).font=N; wa.cell(rr,3,train).font=N; wa.cell(rr,4,launch).font=N; wa.cell(rr,5,base).font=N
    for c in range(1,6):
        cell=wa.cell(rr,c); cell.border=BORD; cell.alignment=CEN if c==4 else LEFT
    wa.row_dimensions[rr].height=42
    rr+=1

# =========================== ROADMAP (красиво, 1 страница) ===========================
rm=wb["Roadmap"]
# читаем её тексты/значения
title=rm["H1"].value; sub=rm["H2"].value
badges=[rm["I4"].value, rm["M4"].value, rm["Q4"].value]
ctitles=[rm["I5"].value, rm["M5"].value, rm["Q5"].value]
bodies=[rm["I6"].value, rm["M6"].value, rm["Q6"].value]
itog=rm["I8"].value
kpis=[(rm.cell(r,2).value, rm.cell(r,4).value) for r in range(2,9)]  # (label,value) B,D
net_idx=next((i for i,(l,_) in enumerate(kpis) if l and "Чистый эффект" in str(l)), None)
dop=[]
for r in range(13,20):
    dop.append((rm.cell(r,2).value, rm.cell(r,3).value, rm.cell(r,4).value, rm.cell(r,5).value))
dop_hdr=[rm.cell(12,c).value for c in range(2,6)]
# чистим лист
for mc in list(rm.merged_cells.ranges): rm.unmerge_cells(str(mc))
rm.delete_rows(1, rm.max_row+5)
# раскладка
COLW={'A':2,'B':16,'C':13,'D':14,'E':3,'F':14,'G':13,'H':13,'I':12,'J':14,'K':13,'L':14,'M':2}
for c,w in COLW.items(): rm.column_dimensions[c].width=w
# заголовок
rm.merge_cells("B1:L1"); c=rm["B1"]; c.value=title; c.font=Font(name="Inter",size=16,bold=True,color="FFFFFF"); c.fill=HEAD; c.alignment=Alignment(horizontal="left",vertical="center",indent=1); rm.row_dimensions[1].height=30
rm.merge_cells("B2:L2"); c=rm["B2"]; c.value=sub; c.font=SMALL; c.alignment=Alignment(horizontal="left",vertical="center",indent=1); rm.row_dimensions[2].height=18
# карточки
blocks=[(2,4),(6,8),(10,12)]; colors=["F47A20","6B4FBB","3FAE5F"]
for (c0,c1),color,bd,ct,body in zip(blocks,colors,badges,ctitles,bodies):
    rm.merge_cells(start_row=4,start_column=c0,end_row=4,end_column=c1)
    x=rm.cell(4,c0,bd); x.font=Font(name="Inter",size=11,bold=True,color="FFFFFF"); x.fill=PatternFill("solid",fgColor=color); x.alignment=CEN
    rm.merge_cells(start_row=5,start_column=c0,end_row=5,end_column=c1)
    x=rm.cell(5,c0,ct); x.font=Font(name="Inter",size=12,bold=True,color="3A2A6B"); x.fill=PatternFill("solid",fgColor="EFEAF8"); x.alignment=CEN
    rm.merge_cells(start_row=6,start_column=c0,end_row=6,end_column=c1)
    x=rm.cell(6,c0,body); x.font=N; x.fill=PatternFill("solid",fgColor="F7F5FC"); x.alignment=Alignment(horizontal="left",vertical="top",wrap_text=True)
    for rr_ in (4,5,6):
        for cc in range(c0,c1+1): rm.cell(rr_,cc).border=BORD
rm.row_dimensions[4].height=24; rm.row_dimensions[5].height=34; rm.row_dimensions[6].height=250
for ac in (5,9):
    x=rm.cell(6,ac,"→"); x.font=Font(name="Inter",size=20,bold=True,color="999999"); x.alignment=Alignment(horizontal="center",vertical="center")
# ИТОГ-плашка
rm.merge_cells("B8:L8"); x=rm["B8"]; x.value=itog; x.font=B; x.fill=GREEN_L; x.alignment=LEFT; rm.row_dimensions[8].height=42
for cc in range(2,13): rm.cell(8,cc).border=BORD
# KPI (слева) + Доп ТО (справа)
rm.merge_cells("B10:D10"); x=rm["B10"]; x.value="КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ"; x.font=H; x.fill=HEAD; x.alignment=CEN
for cc in range(2,5): rm.cell(10,cc).border=BORD
kr=11
for (lab,val) in kpis:
    if lab is None: continue
    rm.merge_cells(start_row=kr,start_column=2,end_row=kr,end_column=3)
    rm.cell(kr,2,lab).font=N; rm.cell(kr,2).alignment=LEFT
    v=val
    if net_idx is not None and lab and "Валовая" in str(lab):
        v=f"=30%*D{11+net_idx}"
    cc=rm.cell(kr,4,v); cc.font=B; cc.alignment=RIGHT
    for c in (2,3,4): rm.cell(kr,c).border=BORD
    rm.row_dimensions[kr].height=22; kr+=1
# Доп ТО таблица справа (F..I)
rm.merge_cells("F10:I10"); x=rm["F10"]; x.value="ДОП. ТОВАРООБОРОТ ПО МЕСЯЦАМ, ₽"; x.font=H; x.fill=HEAD; x.alignment=CEN
for cc in range(6,10): rm.cell(10,cc).border=BORD
hdrs=["Месяц","ДЦО","CVM","Итого"]
for j,h in enumerate(hdrs):
    c=rm.cell(11,6+j,h); c.font=B; c.fill=GREY_L; c.alignment=CEN; c.border=BORD
dr=12
for (m,d1,d2,tot) in dop:
    rm.cell(dr,6,m).font=N; rm.cell(dr,6).alignment=CEN
    for j,vv in enumerate([d1,d2,tot]):
        c=rm.cell(dr,7+j,vv); c.font=(B if str(m)=="ИТОГО" else N); c.number_format=MONEY; c.alignment=RIGHT
    for c in range(6,10):
        rm.cell(dr,c).border=BORD
        if str(m)=="ИТОГО": rm.cell(dr,c).fill=GREY_L
    rm.row_dimensions[dr].height=20; dr+=1
# печать на 1 страницу
rm.sheet_properties.pageSetUpPr=PageSetupProperties(fitToPage=True)
rm.page_setup.orientation="landscape"; rm.page_setup.fitToWidth=1; rm.page_setup.fitToHeight=1
rm.page_margins.left=rm.page_margins.right=0.3; rm.page_margins.top=rm.page_margins.bottom=0.3
lastrow=max(kr,dr)
rm.print_area=f"A1:M{lastrow}"

wb.save(PATH)
print("OK сохранён", PATH)
print("Листы:", wb.sheetnames)
print("Задач:", len(WBS), "часов всего:", sum(w[5] for w in WBS))
