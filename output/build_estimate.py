# -*- coding: utf-8 -*-
"""Оценка трудоёмкости и план внедрения проекта (ДИКСИ CVM/ДЦО).
Ставка 4350 руб/час. Листы: План-график, Трудоёмкость (детально),
Стоимость проекта, Помесячная разбивка."""
import os
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

RATE = 4350
HRS_PER_MONTH = 168

# --- Предпосылки P&L / ROI (можно менять, согласовать с заказчиком) ---
MARGIN = 0.20          # валовая маржа на доп. товарооборот (ASSUMPTION)
USD_RATE = 100         # курс ₽/$ для подписок (ASSUMPTION)
SERVER_MAIN = 45_000   # ₽/мес — защищённый сервер сервиса
AGENT_ENVS = 2         # число изолированных сред под агентов (ASSUMPTION)
SERVER_AGENT = 45_000  # ₽/мес за среду агентов
SUBS_USD = 400         # $/мес — Claude Max ($200) + Nano Banana / Google AI Ultra ($200)

TITLE = Font(name="Inter", size=14, bold=True, color="3A2A6B")
H = Font(name="Inter", size=10, bold=True, color="FFFFFF")
B = Font(name="Inter", size=10, bold=True)
N = Font(name="Inter", size=10)
SMALL = Font(name="Inter", size=9, color="666666")
HEAD_FILL = PatternFill("solid", fgColor="6B4FBB")
ORANGE_L = PatternFill("solid", fgColor="FDE8D6")
GREEN_L = PatternFill("solid", fgColor="E3F2E1")
PURPLE_L = PatternFill("solid", fgColor="EFEAF8")
GREY_L = PatternFill("solid", fgColor="F4F4F4")
BLUE_L = PatternFill("solid", fgColor="E6F0FB")
ZEBRA = PatternFill("solid", fgColor="FAF8FF")
thin = Side(style="thin", color="DDDDDD")
BORD = Border(left=thin, right=thin, top=thin, bottom=thin)
CEN = Alignment(horizontal="center", vertical="center", wrap_text=True)
LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)
RIGHT = Alignment(horizontal="right", vertical="center")
MONEY = "# ##0 ₽"
NUM = "# ##0"

ROLES = {
    "PM": "Руководитель проекта / CVM-методолог",
    "DE": "Data engineer (инфраструктура, интеграции, сегменты MCI)",
    "AI": "AI-инженер (персонализация, генерация, оптимизатор)",
    "BE": "Backend-разработчик (интеграция, интерфейс заведения акций)",
    "AN": "CVM-аналитик (контрольные группы, постэффекты)",
    "MK": "Маркетолог / редактор (контент, тексты)",
}

# WBS: (этап, №, задача, блок, роль_отображение, [роли], часы)
WBS = [
    (1, "1.1", "Развёртывание сервиса на нашем сервере: окружение, домены, доступы, безопасность, бэкапы", "Технический", "Data engineer", ["DE"], 56),
    (1, "1.2", "Интеграция источников данных ДЦО (клиенты, история покупок, текущие ДЦО-акции)", "Технический", "Data engineer", ["DE"], 30),
    (1, "1.3", "Интеграция готового модуля сегментации MCI в проект + интерфейс выбора сегментов", "Технический", "Data engineer + Backend", ["DE", "BE"], 70),
    (1, "1.4", "Настройка автоматических контрольных групп (на базе MCI)", "Технический", "CVM-аналитик", ["AN"], 40),
    (1, "1.5", "Движок персонализации текстов ДЦО (адаптация генератора под сегмент)", "Технический", "AI-инженер", ["AI"], 110),
    (1, "1.6", "Методология персонализации ДЦО (карта сегмент -> оффер)", "Операционный", "Руководитель проекта / CVM-методолог", ["PM"], 40),
    (1, "1.7", "Перевод текущих ДЦО-акций в персональный формат (контент)", "Операционный", "AI-инженер + маркетолог", ["AI", "MK"], 100),
    (1, "1.8", "Создание базы знаний: шаблоны, скиллы, гайдлайны, tone of voice", "Операционный", "AI-инженер + маркетолог", ["AI", "MK"], 60),
    (1, "1.9", "Согласование и выстраивание бизнес-процесса планирования и заведения ДЦО", "Операционный", "Руководитель проекта + аналитик", ["PM", "AN"], 60),
    (1, "1.10", "Пилотный запуск + аналитика постэффектов vs контрольная группа", "Операционный", "CVM-аналитик", ["AN"], 60),
    (1, "1.11", "Управление этапом: статусы, демо, приёмка", "Операционный", "Руководитель проекта / CVM-методолог", ["PM"], 64),
    (1, "1.12", "Аренда и настройка отдельных серверов под агентов: изолированные среды, оркестрация, мониторинг", "Технический", "Data engineer", ["DE"], 40),
    (2, "2.1", "Интеграция и настройка 100 сегментов MCI в проекте (конфиг, обновление, выгрузки)", "Технический", "Data engineer", ["DE"], 90),
    (2, "2.2", "Конвейер массовой генерации контента (push / купон / баннер / e-mail)", "Технический", "AI-инженер + маркетолог", ["AI", "MK"], 130),
    (2, "2.3", "Операционный интерфейс заведения акций (бриф -> сегмент -> механика -> контент -> расписание -> выгрузка)", "Технический", "Backend-разработчик", ["BE"], 110),
    (2, "2.4", "Интеграция деплинков, SKU и механик в конвейер", "Технический", "Data engineer", ["DE"], 60),
    (2, "2.5", "Простой оптимизатор по акциям (подбор оффера под сегмент, правила/скоринг)", "Технический", "AI-инженер", ["AI"], 100),
    (2, "2.6", "Дашборд эффективности и аналитика постэффектов сегментных акций / контрольных групп", "Технический", "CVM-аналитик", ["AN"], 90),
    (2, "2.7", "Формирование контента под 100 сегментов (тексты, проверка ToV, скиллы)", "Операционный", "AI-инженер + маркетолог", ["AI", "MK"], 130),
    (2, "2.8", "Операционная часть заведения акций (настройка, расписание, QA)", "Операционный", "Маркетолог + аналитик", ["MK", "AN"], 90),
    (2, "2.9", "Выстраивание бизнес-процесса планирования и согласования сетки CVM-акций", "Операционный", "Руководитель проекта + аналитик", ["PM", "AN"], 70),
    (2, "2.10", "Методология 100 сегментов (карта сегмент -> категория -> механика)", "Операционный", "Руководитель проекта / CVM-методолог", ["PM"], 50),
    (2, "2.11", "Обучение и онбординг команды заказчика", "Операционный", "Руководитель проекта / CVM-методолог", ["PM"], 35),
    (2, "2.12", "Управление этапом: статусы, демо, приёмка", "Операционный", "Руководитель проекта / CVM-методолог", ["PM"], 80),
    (2, "2.13", "Обучение и вывод агентов в продакшн (копирайтер, редактор-гуманизатор, дизайнер баннеров, оптимизатор)", "Технический", "AI-инженер", ["AI"], 50),
    (3, "3.1", "Допиливание и стабилизация интерфейса заведения акций", "Технический", "Backend-разработчик", ["BE"], 60),
    (3, "3.2", "Формирование 200 акций (массовое заведение через конвейер)", "Операционный", "AI-инженер + маркетолог", ["AI", "MK"], 90),
    (3, "3.3", "Простая оптимизация: автоподбор оффера и частоты, антидубли, приоритизация", "Технический", "AI-инженер", ["AI"], 70),
    (3, "3.4", "Аналитика постэффектов по 200 акциям, финальный отчёт", "Операционный", "CVM-аналитик", ["AN"], 45),
    (3, "3.5", "Подготовка к передаче сервиса заказчику (документация, runbook, доступы)", "Операционный", "Data engineer", ["DE"], 45),
    (3, "3.6", "Управление этапом, приёмка, передача", "Операционный", "Руководитель проекта / CVM-методолог", ["PM"], 35),
]

STAGE_NAME = {
    1: "Этап 1. Персонализация ДЦО (июль-август 2026)",
    2: "Этап 2. Полноценный CVM: 100 сегментов (сентябрь-ноябрь 2026)",
    3: "Этап 3. Масштаб + простая оптимизация (декабрь 2026)",
}
MONTHS = ["июль.26", "авг.26", "сент.26", "окт.26", "ноя.26", "дек.26"]
STAGE_MONTHS = {1: [0, 1], 2: [2, 3, 4], 3: [5]}
STAGE_OF_MONTH = [1, 1, 2, 2, 2, 3]

TO_DCO = [27_000_000, 27_000_000, 27_000_000, 54_000_000, 54_000_000, 54_000_000]
TO_CVM = [0, 0, 208_000_000, 208_000_000, 208_000_000, 416_000_000]

# Доп. расходы (вне трудозатрат по часам) — внутри детального плана работ
EXTRA = [
    ("Модуль сегментации MCI", "Включено", "0 ₽", "Предоставляется бесплатно на период проекта (6 мес); далее — по условиям MCI"),
    ("Серверная инфраструктура — аренда защищённого сервера (мощности, хранилище, при моделях GPU)", "Ежемесячно", "~45 000 ₽/мес (~270 000 ₽ за 6 мес)", "Аренда со всеми защитами и контурами безопасности"),
    ("Аренда отдельных серверов под агентов (изолированные среды исполнения и оркестрации)", "Ежемесячно", "~45 000 ₽/мес за среду x число агентов/сред", "Изоляция, мониторинг, масштабирование; число сред — по нагрузке"),
    ("Подписка Claude Max — максимальный тариф (20x)", "Ежемесячно", "$200/мес (~$1 200 за 6 мес)", "Генерация текстов; оплата по курсу на дату"),
    ("Подписка Nano Banana / Google AI Ultra — максимальный тариф", "Ежемесячно", "$200/мес (~$1 200 за 6 мес)", "Генерация изображений/баннеров; оплата по курсу"),
    ("Доп. токены API сверх лимитов подписок", "По факту", "по объёму", "При превышении лимитов подписок Claude Max / Google AI Ultra"),
    ("Интеграции на стороне заказчика (CRM/рассылки, выгрузки клиентов, API заведения акций)", "Разовое", "вне сметы", "Трудозатраты ИТ заказчика — вне наших часов"),
    ("Контур персональных данных (152-ФЗ)", "Разовое + ежемес.", "по факту", "Защищённое хранение, обезличивание, согласование ИБ"),
    ("Резерв на изменение объёма и непредвиденные доработки", "Разовое", "~10-15%", "К смете работ 9,0 млн ₽"),
    ("НДС (если применимо)", "—", "поверх сумм", "Поверх всех сумм без НДС"),
]


# Агенты, которых обучаем и запускаем в рамках процесса
# (агент, функция, обучаем/настраиваем (этап, задачи), запуск, модель/основа)
AGENTS = [
    ("1. Агент-сегментатор", "Выбор аудитории и сегментов под акцию, контрольные группы", "Этап 1 (1.3, 1.4) → масштаб Этап 2 (2.1)", "июль", "Модуль MCI + интеграция"),
    ("2. Агент-персонализатор", "Подбор оффера под сегмент (сегмент → оффер)", "Этап 1 (1.5)", "июль-авг", "Claude Max + правила CVM"),
    ("3. Агент-копирайтер", "Генерация текстов: push / купон / баннер / e-mail", "Этап 1 (1.7) → масштаб Этап 2 (2.2, 2.7)", "авг-сент", "Claude Max + скиллы (hook-generator, frameworks)"),
    ("4. Агент-редактор / гуманизатор", "Проверка качества, tone of voice, антишаблон", "Этап 1 (1.8)", "авг", "Claude Max + скиллы (humanizer, beautiful-prose, voice-builder)"),
    ("5. Агент-дизайнер баннеров", "Генерация изображений и баннеров", "Этап 2 (2.2, 2.13)", "сент-окт", "Nano Banana / Google AI Ultra"),
    ("6. Агент-оптимизатор", "Подбор оффера/частоты, антидубли, приоритизация акций", "Этап 2 (2.5) → Этап 3 (3.3)", "ноя-дек", "Claude Max + скоринг/правила"),
    ("7. Агент-аналитик постэффектов", "Расчёт эффекта vs контрольные группы, отчёты", "Этап 1 (1.10) → Этап 2 (2.6) → Этап 3 (3.4)", "июль-дек", "Внутренние модели + дашборд"),
]


def stage_hours(st):
    return sum(t[6] for t in WBS if t[0] == st)


def stage_block_hours(st, block):
    return sum(t[6] for t in WBS if t[0] == st and t[3] == block)


def style_header(ws, row, cols):
    for c in range(1, cols + 1):
        cell = ws.cell(row=row, column=c)
        cell.font = H; cell.fill = HEAD_FILL; cell.alignment = CEN; cell.border = BORD


# ---- расчёты помесячно ----
role_month = {rk: [0.0] * 6 for rk in ROLES}
block_month = {"Технический": [0.0] * 6, "Операционный": [0.0] * 6}
for (st, _, _, block, _, keys, hrs) in WBS:
    ms = STAGE_MONTHS[st]
    for m in ms:
        block_month[block][m] += hrs / len(ms)
    per_role = hrs / len(keys)
    for k in keys:
        for m in ms:
            role_month[k][m] += per_role / len(ms)
month_hrs = [sum(role_month[rk][m] for rk in ROLES) for m in range(6)]
month_cost = [round(month_hrs[m]) * RATE for m in range(6)]
tot_hrs = sum(t[6] for t in WBS)
tot_cost = tot_hrs * RATE

wb = openpyxl.Workbook()

# ============================================================
# ЛИСТ 1. План-график (с затратами)
ws = wb.active
ws.title = "План-график"
NCOL = 7
ws.merge_cells("A1:G1")
ws["A1"] = "План внедрения: усиление ДЦО  ->  полноценный CVM  ->  оптимизация"
ws["A1"].font = Font(name="Inter", size=15, bold=True, color="FFFFFF")
ws["A1"].fill = HEAD_FILL
ws["A1"].alignment = Alignment(horizontal="left", vertical="center", indent=1)
ws.row_dimensions[1].height = 30
ws.merge_cells("A2:G2")
ws["A2"] = "Ставка 4 350 ₽/час  •  команда 6 специалистов  •  суммы без НДС  •  июль–декабрь 2026"
ws["A2"].font = SMALL
ws["A2"].alignment = Alignment(horizontal="left", vertical="center", indent=1)
ws.row_dimensions[2].height = 18

hdr = ["Месяц", "Содержание работ", "Команда, чел.", "Затраты работ, ₽/мес", "Доп ТО ДЦО, ₽", "Доп ТО CVM, ₽", "Доп ТО итого, ₽"]
r = 4
for i, h in enumerate(hdr, 1):
    ws.cell(row=r, column=i, value=h)
style_header(ws, r, NCOL)
ws.row_dimensions[r].height = 32
plan_rows = [
    ("июль.26", "Перевод ДЦО-акций на персональные рассылки; интеграция модуля сегментации MCI; автоматические контрольные группы"),
    ("авг.26", "Перевод ДЦО-акций на персональные рассылки; создание базы знаний; бизнес-процесс заведения ДЦО"),
    ("сент.26", "Увеличение количества сегментов до 100: формирование контента и операционная часть заведения акций"),
    ("окт.26", "Увеличение количества сегментов до 100: контент, операционная часть, интерфейс заведения акций"),
    ("ноя.26", "Формирование простого оптимизатора по акциям; аналитика постэффектов"),
    ("дек.26", "Формирование 200 акций, допиливание интерфейса, передача сервиса"),
]
r = 5
for i, (m, desc) in enumerate(plan_rows):
    zebra = (i % 2 == 1)
    ws.cell(row=r, column=1, value=m).font = B
    ws.cell(row=r, column=2, value=desc).font = N
    ws.cell(row=r, column=3, value=6).font = N
    c4 = ws.cell(row=r, column=4, value=month_cost[i]); c4.font = B; c4.number_format = MONEY; c4.fill = PURPLE_L
    c5 = ws.cell(row=r, column=5, value=TO_DCO[i]); c5.font = N; c5.number_format = MONEY; c5.fill = ORANGE_L
    c6 = ws.cell(row=r, column=6, value=TO_CVM[i] if TO_CVM[i] else None); c6.font = N; c6.number_format = MONEY; c6.fill = GREEN_L
    c7 = ws.cell(row=r, column=7, value=TO_DCO[i] + TO_CVM[i]); c7.font = B; c7.number_format = MONEY
    for c in range(1, NCOL + 1):
        cell = ws.cell(row=r, column=c); cell.border = BORD
        cell.alignment = LEFT if c == 2 else (CEN if c <= 3 else RIGHT)
        if zebra and c in (1, 2, 3):
            cell.fill = ZEBRA
    ws.row_dimensions[r].height = 42
    r += 1
ws.cell(row=r, column=1, value="ИТОГО").font = B
ws.cell(row=r, column=2, value="за период (6 мес)").font = B
ws.cell(row=r, column=4, value=sum(month_cost)).font = B
for col, vals in ((5, TO_DCO), (6, TO_CVM), (7, [a + b for a, b in zip(TO_DCO, TO_CVM)])):
    ws.cell(row=r, column=col, value=sum(vals)).font = B
for c in range(1, NCOL + 1):
    cell = ws.cell(row=r, column=c); cell.border = BORD; cell.fill = GREY_L
    if c >= 4:
        cell.number_format = MONEY; cell.alignment = RIGHT
    else:
        cell.alignment = LEFT if c == 2 else CEN
ws.row_dimensions[r].height = 24
r += 2
ws.cell(row=r, column=2, value="Доп ТО — оценка эффекта из плана заказчика (товарооборот, без НДС). Доп. расходы (сервер, подписки, MCI) — см. лист «Трудоёмкость (детально)».").font = SMALL
ws.freeze_panes = "A5"
for i, w in enumerate([11, 54, 11, 19, 17, 17, 17], 1):
    ws.column_dimensions[get_column_letter(i)].width = w

# ============================================================
# ЛИСТ 2. Трудоёмкость (детально) + доп. расходы
ws2 = wb.create_sheet("Трудоёмкость (детально)")
ws2["A1"] = "Детальный план работ и трудоёмкость (ставка 4 350 ₽/час, без НДС)"
ws2["A1"].font = TITLE; ws2.merge_cells("A1:G1")
hdr2 = ["№", "Задача", "Блок", "Роль", "Часы", "Ставка, ₽", "Стоимость, ₽"]
r = 3
for i, h in enumerate(hdr2, 1):
    ws2.cell(row=r, column=i, value=h)
style_header(ws2, r, len(hdr2))
r = 4
stage_tot = {1: 0, 2: 0, 3: 0}
for st in (1, 2, 3):
    ws2.cell(row=r, column=1, value=STAGE_NAME[st]).font = B
    ws2.merge_cells(start_row=r, start_column=1, end_row=r, end_column=7)
    ws2.cell(row=r, column=1).fill = PURPLE_L
    for c in range(1, 8):
        ws2.cell(row=r, column=c).border = BORD
    r += 1
    for (s, num, task, block, role_disp, _keys, hrs) in WBS:
        if s != st:
            continue
        cost = hrs * RATE; stage_tot[st] += cost
        ws2.cell(row=r, column=1, value=num).font = N
        ws2.cell(row=r, column=2, value=task).font = N
        ws2.cell(row=r, column=3, value=block).font = N
        ws2.cell(row=r, column=4, value=role_disp).font = N
        ws2.cell(row=r, column=5, value=hrs).font = N
        ws2.cell(row=r, column=6, value=RATE).font = N
        ws2.cell(row=r, column=7, value=cost).font = N
        for c in range(1, 8):
            cell = ws2.cell(row=r, column=c); cell.border = BORD
            cell.alignment = LEFT if c in (2, 4) else CEN
            if c in (5, 6, 7):
                cell.number_format = NUM if c == 5 else MONEY; cell.alignment = RIGHT
            if c == 3:
                cell.fill = ORANGE_L if block == "Технический" else GREEN_L
        r += 1
    hrs_st = stage_hours(st)
    ws2.cell(row=r, column=2, value=f"Итого по этапу {st}").font = B
    ws2.cell(row=r, column=5, value=hrs_st).font = B; ws2.cell(row=r, column=5).number_format = NUM; ws2.cell(row=r, column=5).alignment = RIGHT
    ws2.cell(row=r, column=7, value=stage_tot[st]).font = B; ws2.cell(row=r, column=7).number_format = MONEY; ws2.cell(row=r, column=7).alignment = RIGHT
    for c in range(1, 8):
        cell = ws2.cell(row=r, column=c); cell.border = BORD; cell.fill = GREY_L
    r += 1
ws2.cell(row=r + 1, column=2, value="ВСЕГО ПО ПРОЕКТУ (работы)").font = B
ws2.cell(row=r + 1, column=5, value=tot_hrs).font = B; ws2.cell(row=r + 1, column=5).number_format = NUM; ws2.cell(row=r + 1, column=5).alignment = RIGHT
c = ws2.cell(row=r + 1, column=7, value=tot_cost); c.font = Font(name="Inter", size=11, bold=True, color="F47A20"); c.number_format = MONEY; c.alignment = RIGHT
for cc in range(1, 8):
    ws2.cell(row=r + 1, column=cc).border = BORD; ws2.cell(row=r + 1, column=cc).fill = PURPLE_L
for i, w in enumerate([7, 64, 14, 34, 8, 11, 16], 1):
    ws2.column_dimensions[get_column_letter(i)].width = w

# --- Доп. расходы (вне трудозатрат) внутри детального плана ---
rr = r + 4
ws2.cell(row=rr, column=1, value="ДОПОЛНИТЕЛЬНЫЕ РАСХОДЫ (вне трудозатрат по часам)").font = B
ws2.merge_cells(start_row=rr, start_column=1, end_row=rr, end_column=7)
ws2.cell(row=rr, column=1).fill = BLUE_L
for c in range(1, 8):
    ws2.cell(row=rr, column=c).border = BORD
rr += 1
ws2.cell(row=rr, column=2, value="Статья").font = H
ws2.cell(row=rr, column=4, value="Стоимость").font = H
ws2.cell(row=rr, column=5, value="Комментарий").font = H
ws2.merge_cells(start_row=rr, start_column=2, end_row=rr, end_column=3)
ws2.merge_cells(start_row=rr, start_column=5, end_row=rr, end_column=7)
for c in range(1, 8):
    cell = ws2.cell(row=rr, column=c); cell.fill = HEAD_FILL; cell.border = BORD; cell.alignment = CEN
ws2.cell(row=rr, column=1, value="").fill = HEAD_FILL
rr += 1
for (item, typ, cost, note) in EXTRA:
    ws2.cell(row=rr, column=1, value="•").alignment = CEN
    ws2.cell(row=rr, column=2, value=item).font = N
    ws2.merge_cells(start_row=rr, start_column=2, end_row=rr, end_column=3)
    ws2.cell(row=rr, column=2).alignment = LEFT
    ws2.cell(row=rr, column=4, value=cost).font = B; ws2.cell(row=rr, column=4).alignment = LEFT
    ws2.cell(row=rr, column=5, value=note).font = N
    ws2.merge_cells(start_row=rr, start_column=5, end_row=rr, end_column=7)
    ws2.cell(row=rr, column=5).alignment = LEFT
    for c in range(1, 8):
        ws2.cell(row=rr, column=c).border = BORD
    rr += 1
ws2.cell(row=rr + 1, column=2, value=f"Доп. расходы не входят в смету работ ({tot_cost:,} ₽".replace(",", " ") + ") и оплачиваются отдельно.").font = SMALL

# ============================================================
# ЛИСТ 3. Стоимость проекта (свод по этапам)
ws4 = wb.create_sheet("Стоимость проекта")
ws4["A1"] = "Стоимость проекта (без НДС). Ставка 4 350 ₽/час"
ws4["A1"].font = TITLE; ws4.merge_cells("A1:E1")
ws4.merge_cells("A2:E2")
ws4["A2"] = ("Расчёт является ПРЕДВАРИТЕЛЬНЫМ (оценочным): объём часов — экспертная оценка и может быть "
             "уточнён по итогам обследования и согласования объёма работ с заказчиком.")
ws4["A2"].font = Font(name="Inter", size=9, bold=True, color="C0392B")
ws4["A2"].alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
ws4["A2"].fill = ORANGE_L
ws4.row_dimensions[2].height = 28
hdr4 = ["№", "Этап / услуга", "Ед.", "Кол-во часов", "Стоимость, ₽"]
r = 3
for i, h in enumerate(hdr4, 1):
    ws4.cell(row=r, column=i, value=h)
style_header(ws4, r, len(hdr4))
r = 4
for st in (1, 2, 3):
    sh = stage_hours(st)
    ws4.cell(row=r, column=1, value=f"{st}.").font = B
    ws4.cell(row=r, column=2, value=STAGE_NAME[st]).font = B
    ws4.cell(row=r, column=4, value=sh).font = B; ws4.cell(row=r, column=4).number_format = NUM; ws4.cell(row=r, column=4).alignment = RIGHT
    ws4.cell(row=r, column=5, value=sh * RATE).font = B; ws4.cell(row=r, column=5).number_format = MONEY; ws4.cell(row=r, column=5).alignment = RIGHT
    for c in range(1, 6):
        ws4.cell(row=r, column=c).border = BORD; ws4.cell(row=r, column=c).fill = PURPLE_L
    r += 1
    for bl in ("Технический", "Операционный"):
        bh = stage_block_hours(st, bl)
        ws4.cell(row=r, column=2, value=f"   • {bl} блок").font = N
        ws4.cell(row=r, column=3, value="чел.-час").font = N; ws4.cell(row=r, column=3).alignment = CEN
        ws4.cell(row=r, column=4, value=bh).font = N; ws4.cell(row=r, column=4).number_format = NUM; ws4.cell(row=r, column=4).alignment = RIGHT
        ws4.cell(row=r, column=5, value=bh * RATE).font = N; ws4.cell(row=r, column=5).number_format = MONEY; ws4.cell(row=r, column=5).alignment = RIGHT
        for c in range(1, 6):
            ws4.cell(row=r, column=c).border = BORD
        ws4.cell(row=r, column=2).alignment = LEFT
        r += 1
ws4.cell(row=r, column=2, value="ИТОГО работы по проекту").font = B
ws4.cell(row=r, column=4, value=tot_hrs).font = B; ws4.cell(row=r, column=4).number_format = NUM; ws4.cell(row=r, column=4).alignment = RIGHT
ws4.cell(row=r, column=5, value=tot_cost).font = B; ws4.cell(row=r, column=5).number_format = MONEY; ws4.cell(row=r, column=5).alignment = RIGHT
for c in range(1, 6):
    ws4.cell(row=r, column=c).border = BORD; ws4.cell(row=r, column=c).fill = GREY_L
r += 1
ws4.cell(row=r, column=2, value="в т.ч. в среднем в месяц (6 мес)").font = N
ws4.cell(row=r, column=5, value=round(tot_cost / 6)).font = N; ws4.cell(row=r, column=5).number_format = MONEY; ws4.cell(row=r, column=5).alignment = RIGHT
for c in range(1, 6):
    ws4.cell(row=r, column=c).border = BORD
r += 2
ws4.cell(row=r, column=2, value="Модель: сервис разворачивается на нашем сервере с возможностью последующей передачи заказчику (задача 3.5).").font = SMALL
ws4.cell(row=r + 1, column=2, value="Доп. расходы вне трудозатрат — см. лист «Трудоёмкость (детально)».").font = SMALL
for i, w in enumerate([6, 58, 12, 14, 18], 1):
    ws4.column_dimensions[get_column_letter(i)].width = w

# ============================================================
# ЛИСТ 4. Помесячная разбивка сумм
ws7 = wb.create_sheet("Помесячная разбивка")
ws7["A1"] = "Помесячная разбивка сумм (без НДС). Ставка 4 350 ₽/час"
ws7["A1"].font = TITLE; ws7.merge_cells("A1:I1")
hA = ["Месяц", "Этап", "Часы", "Технический, ₽", "Операционный, ₽", "Итого работы, ₽", "Доп ТО ДЦО, ₽", "Доп ТО CVM, ₽", "Доп ТО итого, ₽"]
r = 3
for i, h in enumerate(hA, 1):
    ws7.cell(row=r, column=i, value=h)
style_header(ws7, r, len(hA))
ws7.row_dimensions[r].height = 30
r = 4
for i, m in enumerate(MONTHS):
    th = round(block_month["Технический"][i]) * RATE
    oh = round(block_month["Операционный"][i]) * RATE
    ws7.cell(row=r, column=1, value=m).font = B
    ws7.cell(row=r, column=2, value=f"Этап {STAGE_OF_MONTH[i]}").font = N
    ws7.cell(row=r, column=3, value=round(month_hrs[i])).font = N
    c4 = ws7.cell(row=r, column=4, value=th); c4.fill = ORANGE_L
    c5 = ws7.cell(row=r, column=5, value=oh); c5.fill = GREEN_L
    c6 = ws7.cell(row=r, column=6, value=month_cost[i]); c6.font = B
    c7 = ws7.cell(row=r, column=7, value=TO_DCO[i]); c7.fill = ORANGE_L
    c8 = ws7.cell(row=r, column=8, value=TO_CVM[i] if TO_CVM[i] else None); c8.fill = GREEN_L
    c9 = ws7.cell(row=r, column=9, value=TO_DCO[i] + TO_CVM[i]); c9.font = B
    for c in range(1, 10):
        cell = ws7.cell(row=r, column=c); cell.border = BORD
        if c >= 4:
            cell.number_format = MONEY; cell.alignment = RIGHT
            if not cell.font.bold:
                cell.font = N
        else:
            cell.alignment = CEN
    ws7.cell(row=r, column=6).font = B
    r += 1
ws7.cell(row=r, column=1, value="ИТОГО").font = B
ws7.cell(row=r, column=3, value=tot_hrs).font = B
ws7.cell(row=r, column=4, value=round(sum(block_month["Технический"])) * RATE).font = B
ws7.cell(row=r, column=5, value=round(sum(block_month["Операционный"])) * RATE).font = B
ws7.cell(row=r, column=6, value=tot_cost).font = B
ws7.cell(row=r, column=7, value=sum(TO_DCO)).font = B
ws7.cell(row=r, column=8, value=sum(TO_CVM)).font = B
ws7.cell(row=r, column=9, value=sum(TO_DCO) + sum(TO_CVM)).font = B
for c in range(1, 10):
    cell = ws7.cell(row=r, column=c); cell.border = BORD; cell.fill = GREY_L
    if c >= 4:
        cell.number_format = MONEY; cell.alignment = RIGHT
    elif c == 3:
        cell.alignment = CEN
r += 3

ws7.cell(row=r, column=1, value="Суммы по ролям, ₽/мес").font = TITLE
ws7.merge_cells(start_row=r, start_column=1, end_row=r, end_column=9)
r += 1
hB = ["Роль"] + MONTHS + ["", "Итого, ₽"]
for i, h in enumerate(hB, 1):
    ws7.cell(row=r, column=i, value=h)
style_header(ws7, r, 9)
r += 1
for rk in ROLES:
    ws7.cell(row=r, column=1, value=ROLES[rk].split(" (")[0]).font = N
    ws7.cell(row=r, column=1).alignment = LEFT
    for i, hv in enumerate(role_month[rk]):
        val = round(hv) * RATE
        cell = ws7.cell(row=r, column=2 + i, value=val if val else None)
        cell.font = N; cell.number_format = MONEY; cell.alignment = RIGHT
    ws7.cell(row=r, column=9, value=round(sum(role_month[rk])) * RATE).font = B
    ws7.cell(row=r, column=9).number_format = MONEY; ws7.cell(row=r, column=9).alignment = RIGHT
    for c in range(1, 10):
        ws7.cell(row=r, column=c).border = BORD
    r += 1
ws7.cell(row=r, column=1, value="Итого / мес").font = B
for i in range(6):
    cell = ws7.cell(row=r, column=2 + i, value=month_cost[i]); cell.font = B; cell.number_format = MONEY; cell.alignment = RIGHT; cell.fill = GREY_L
ws7.cell(row=r, column=9, value=tot_cost).font = B; ws7.cell(row=r, column=9).number_format = MONEY; ws7.cell(row=r, column=9).alignment = RIGHT; ws7.cell(row=r, column=9).fill = GREY_L
ws7.cell(row=r, column=1).fill = GREY_L
for c in range(1, 10):
    ws7.cell(row=r, column=c).border = BORD
r += 2
ws7.cell(row=r, column=1, value="Часы комбинированных задач (напр. AI-инженер + маркетолог) делятся между ролями поровну.").font = SMALL
for i, w in enumerate([34, 13, 13, 13, 13, 13, 13, 4, 16], 1):
    ws7.column_dimensions[get_column_letter(i)].width = w

# ============================================================
# ЛИСТ 5. Агенты (которых обучаем и запускаем)
ws8 = wb.create_sheet("Агенты")
ws8["A1"] = "Агенты, которых обучаем и запускаем в рамках процесса"
ws8["A1"].font = TITLE; ws8.merge_cells("A1:E1")
ws8["A2"] = "Каждый агент исполняется в изолированной арендованной среде (см. задачу 1.12 и доп. расходы)."
ws8["A2"].font = SMALL; ws8.merge_cells("A2:E2")
hA8 = ["Агент", "Функция", "Обучаем / настраиваем (этап, задачи)", "Запуск", "Модель / основа"]
r = 4
for i, h in enumerate(hA8, 1):
    ws8.cell(row=r, column=i, value=h)
style_header(ws8, r, len(hA8))
ws8.row_dimensions[r].height = 30
r = 5
for i, (ag, func, train, launch, base) in enumerate(AGENTS):
    zebra = (i % 2 == 1)
    ws8.cell(row=r, column=1, value=ag).font = B
    ws8.cell(row=r, column=2, value=func).font = N
    ws8.cell(row=r, column=3, value=train).font = N
    ws8.cell(row=r, column=4, value=launch).font = N
    ws8.cell(row=r, column=5, value=base).font = N
    for c in range(1, 6):
        cell = ws8.cell(row=r, column=c); cell.border = BORD
        cell.alignment = CEN if c == 4 else LEFT
        if zebra:
            cell.fill = ZEBRA
    ws8.row_dimensions[r].height = 42
    r += 1
for i, w in enumerate([26, 40, 34, 12, 40], 1):
    ws8.column_dimensions[get_column_letter(i)].width = w

# ============================================================
# ЛИСТ. P&L / ROI
wsp = wb.create_sheet("P&L и ROI")
wsp["A1"] = "P&L и ROI проекта (предварительный расчёт, без НДС)"
wsp["A1"].font = TITLE; wsp.merge_cells("A1:I1")
# блок предпосылок
wsp.merge_cells("A2:I2")
wsp["A2"] = (f"ПРЕДПОСЫЛКИ (согласовать): валовая маржа на доп. ТО = {int(MARGIN*100)}%  •  курс {USD_RATE} ₽/$  •  "
             f"защищённый сервер {SERVER_MAIN:,} ₽/мес  •  {AGENT_ENVS} среды агентов x {SERVER_AGENT:,} ₽/мес  •  "
             f"подписки Claude Max + Nano Banana = ${SUBS_USD}/мес").replace(",", " ")
wsp["A2"].font = Font(name="Inter", size=9, bold=True, color="C0392B")
wsp["A2"].fill = ORANGE_L
wsp["A2"].alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
wsp.row_dimensions[2].height = 30

infra_m = SERVER_MAIN + AGENT_ENVS * SERVER_AGENT
subs_m = SUBS_USD * USD_RATE
hP = ["Месяц", "Доп ТО, ₽", f"Валовая прибыль ({int(MARGIN*100)}%), ₽", "Затраты работ, ₽", "Инфраструктура, ₽", "Подписки, ₽", "Итого затраты, ₽", "Чистый эффект, ₽", "Накопл. эффект, ₽"]
r = 4
for i, h in enumerate(hP, 1):
    wsp.cell(row=r, column=i, value=h)
style_header(wsp, r, len(hP))
wsp.row_dimensions[r].height = 32
r = 5
cum_net = 0
sum_to = sum_gp = sum_work = sum_cost = sum_net = 0
payback = None
for i, m in enumerate(MONTHS):
    to = TO_DCO[i] + TO_CVM[i]
    gp = round(to * MARGIN)
    work = month_cost[i]
    cost_m = work + infra_m + subs_m
    net = gp - cost_m
    cum_net += net
    if payback is None and cum_net > 0:
        payback = m
    sum_to += to; sum_gp += gp; sum_work += work; sum_cost += cost_m; sum_net += net
    vals = [m, to, gp, work, infra_m, subs_m, cost_m, net, cum_net]
    for c, v in enumerate(vals, 1):
        cell = wsp.cell(row=r, column=c, value=v)
        if c == 1:
            cell.font = B; cell.alignment = CEN
        else:
            cell.font = B if c in (8, 9) else N; cell.number_format = MONEY; cell.alignment = RIGHT
        cell.border = BORD
        if c == 3:
            cell.fill = GREEN_L
        if c == 7:
            cell.fill = ORANGE_L
        if c == 8:
            cell.fill = PURPLE_L
    r += 1
# итого
totals = ["ИТОГО", sum_to, sum_gp, sum_work, infra_m * 6, subs_m * 6, sum_cost, sum_net, ""]
for c, v in enumerate(totals, 1):
    cell = wsp.cell(row=r, column=c, value=v if v != "" else None)
    cell.font = B; cell.fill = GREY_L; cell.border = BORD
    if c == 1:
        cell.alignment = CEN
    else:
        cell.number_format = MONEY; cell.alignment = RIGHT
r += 2
# метрики
roi = sum_net / sum_cost * 100
metrics = [
    ("ROI за период", f"{roi:,.0f} %".replace(",", " ")),
    ("Чистый эффект за 6 мес", f"{sum_net:,} ₽".replace(",", " ")),
    ("Валовая прибыль за 6 мес", f"{sum_gp:,} ₽".replace(",", " ")),
    ("Итого затраты за 6 мес (работы + инфра + подписки)", f"{sum_cost:,} ₽".replace(",", " ")),
    ("Доп ТО на 1 ₽ затрат", f"{sum_to / sum_cost:,.0f} ₽".replace(",", " ")),
    ("Доля затрат в доп. товарообороте", f"{sum_cost / sum_to * 100:.2f} %"),
    ("Окупаемость (накопл. эффект > 0)", payback or "—"),
]
wsp.cell(row=r, column=2, value="КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ").font = B
wsp.cell(row=r, column=2).fill = BLUE_L
wsp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=4)
for c in range(2, 5):
    wsp.cell(row=r, column=c).border = BORD
r += 1
for name, val in metrics:
    wsp.cell(row=r, column=2, value=name).font = N
    wsp.merge_cells(start_row=r, start_column=2, end_row=r, end_column=3)
    wsp.cell(row=r, column=2).alignment = LEFT
    c4 = wsp.cell(row=r, column=4, value=val); c4.font = B; c4.alignment = RIGHT
    for c in (2, 3, 4):
        wsp.cell(row=r, column=c).border = BORD
    r += 1
wsp.cell(row=r + 1, column=2, value="Доп ТО — оценка эффекта из плана заказчика. Маржа и курс — предпосылки для согласования; меняются на листе.").font = SMALL
for i, w in enumerate([10, 17, 18, 16, 16, 13, 17, 17, 18], 1):
    wsp.column_dimensions[get_column_letter(i)].width = w

# ============================================================
# ЛИСТ. Roadmap (3 этапа визуально)
wsr = wb.create_sheet("Roadmap")
wsr["A1"] = "Roadmap проекта: 3 этапа за 6 месяцев"
wsr["A1"].font = TITLE; wsr.merge_cells("A1:L1")
wsr["A2"] = "От усиления ДЦО через персонализацию — к полноценному CVM — к промышленному масштабу и оптимизации"
wsr["A2"].font = SMALL; wsr.merge_cells("A2:L2")

cards = [
    ("F47A20", "ЭТАП 1  ·  июль–август 2026", "Персонализация ДЦО",
     ("ЧТО ДЕЛАЕМ:\n"
      "•  Перевод ДЦО-акций на персональные рассылки\n"
      "•  Интеграция готового модуля сегментации MCI\n"
      "•  Автоматические контрольные группы\n"
      "•  Запуск агентов: сегментатор, персонализатор,\n    копирайтер, редактор-гуманизатор\n"
      "•  База знаний + бизнес-процесс заведения ДЦО\n\n"
      "ЧТО ЭТО ДАЁТ:\n"
      "•  Рост пенетрации в проект, отдача ДЦО выше\n"
      "•  +27 млн ₽/мес доп. товарооборота ДЦО\n"
      "•  Фундамент для масштабирования CVM")),
    ("6B4FBB", "ЭТАП 2  ·  сентябрь–ноябрь 2026", "Полноценный CVM: 100 сегментов",
     ("ЧТО ДЕЛАЕМ:\n"
      "•  Масштаб до 100 сегментов (на базе MCI)\n"
      "•  Конвейер массовой генерации контента\n"
      "•  Операционный интерфейс заведения акций\n"
      "•  Запуск агентов: дизайнер баннеров, оптимизатор\n"
      "•  Дашборды эффективности и постэффекты\n\n"
      "ЧТО ЭТО ДАЁТ:\n"
      "•  Полноценный персонализированный CVM-движок\n"
      "•  +208 млн ₽/мес доп. товарооборота CVM\n"
      "•  Скорость и охват кампаний кратно выше")),
    ("3Fae5f", "ЭТАП 3  ·  декабрь 2026", "Масштаб + простая оптимизация",
     ("ЧТО ДЕЛАЕМ:\n"
      "•  Простой оптимизатор: автоподбор оффера/частоты,\n    антидубли, приоритизация\n"
      "•  Формирование 200 акций\n"
      "•  Допиливание интерфейса\n"
      "•  Передача сервиса заказчику\n\n"
      "ЧТО ЭТО ДАЁТ:\n"
      "•  Промышленный масштаб — 200 акций\n"
      "•  Доп ТО CVM до 416 млн ₽/мес\n"
      "•  Автономность и готовность к передаче")),
]
card_cols = [(2, 4), (6, 8), (10, 12)]   # B:D, F:H, J:L
arrow_cols = [5, 9]
for (c0, c1), (color, badge, title, body) in zip(card_cols, cards):
    fill = PatternFill("solid", fgColor=color)
    light = PatternFill("solid", fgColor="F7F5FC")
    # бейдж
    wsr.merge_cells(start_row=4, start_column=c0, end_row=4, end_column=c1)
    cell = wsr.cell(row=4, column=c0, value=badge)
    cell.font = Font(name="Inter", size=11, bold=True, color="FFFFFF"); cell.fill = fill
    cell.alignment = CEN
    wsr.row_dimensions[4].height = 26
    # заголовок этапа
    wsr.merge_cells(start_row=5, start_column=c0, end_row=5, end_column=c1)
    cell = wsr.cell(row=5, column=c0, value=title)
    cell.font = Font(name="Inter", size=12, bold=True, color="3A2A6B"); cell.fill = PURPLE_L
    cell.alignment = CEN
    wsr.row_dimensions[5].height = 36
    # тело
    wsr.merge_cells(start_row=6, start_column=c0, end_row=6, end_column=c1)
    cell = wsr.cell(row=6, column=c0, value=body)
    cell.font = N; cell.fill = light
    cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
    for rr_ in (4, 5, 6):
        for cc in range(c0, c1 + 1):
            wsr.cell(row=rr_, column=cc).border = BORD
wsr.row_dimensions[6].height = 260
for ac in arrow_cols:
    cell = wsr.cell(row=6, column=ac, value="→")
    cell.font = Font(name="Inter", size=20, bold=True, color="999999")
    cell.alignment = Alignment(horizontal="center", vertical="center")
# нижняя плашка-итог
wsr.merge_cells(start_row=8, start_column=2, end_row=8, end_column=12)
cell = wsr.cell(row=8, column=2, value=("ИТОГ: совокупный доп. товарооборот за 6 мес ~1,28 млрд ₽ (ДЦО 243 млн + CVM 1 040 млн).  "
                                        "100 сегментов, 200 акций/мес, 7 обученных агентов.  Сервис на нашем сервере с передачей заказчику."))
cell.font = B; cell.fill = GREEN_L
cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
wsr.row_dimensions[8].height = 40
for cc in range(2, 13):
    wsr.cell(row=8, column=cc).border = BORD
widths_r = {1: 2, 2: 13, 3: 13, 4: 14, 5: 4, 6: 13, 7: 13, 8: 14, 9: 4, 10: 13, 11: 13, 12: 14}
for c, w in widths_r.items():
    wsr.column_dimensions[get_column_letter(c)].width = w

# ---- порядок листов ----
order = ["План-график", "Roadmap", "Трудоёмкость (детально)", "Стоимость проекта",
         "P&L и ROI", "Помесячная разбивка", "Агенты"]
wb._sheets.sort(key=lambda s: order.index(s.title) if s.title in order else 99)

out = os.path.join(os.path.dirname(__file__), "Оценка_трудоёмкости_внедрение_ДИКСИ.xlsx")
wb.save(out)
print("SAVED:", out)
print("Листы:", wb.sheetnames)
print("Этапы (ч):", {st: stage_hours(st) for st in (1, 2, 3)}, "| Работы ₽:", tot_cost)
print("ROI:", round(sum_net / sum_cost * 100), "% | Чистый эффект ₽:", sum_net, "| Окупаемость:", payback)
print("Листы:", wb.sheetnames)
print("Всего часов:", tot_hrs, "| Стоимость работ, ₽:", tot_cost)
print("Стоимость по месяцам:", month_cost)
