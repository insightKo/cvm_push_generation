# -*- coding: utf-8 -*-
"""Роадмэп и статус проекта внедрения ИИ в ДИКСИ на 08.10.2026 (PPTX).
Источник плана и трудоёмкости: data/Оценка_трудоёмкости_внедрение_ДИКСИ_06.08.2026 (1).xlsx
(лист «Трудоёмкость (детально)» — помесячная раскладка задач; лист «Roadmap» — эффект).
Стиль: bonnie-slide + mckinsey-deck, тело >=14pt, 10-11pt только в плотной диаграмме Ганта."""
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

INK    = RGBColor(0x1E, 0x1E, 0x2F)
ORANGE = RGBColor(0xEE, 0x72, 0x03)
PURPLE = RGBColor(0x6C, 0x4A, 0xB6)
GREEN  = RGBColor(0x2E, 0x9E, 0x5B)
GREY   = RGBColor(0x5B, 0x61, 0x6E)
LGREY  = RGBColor(0xF4, 0xF5, 0xF7)
BORD   = RGBColor(0xD9, 0xDC, 0xE1)
WHITE  = RGBColor(0xFF, 0xFF, 0xFF)
WARM   = RGBColor(0xFD, 0xF8, 0xF2)
MUTE   = RGBColor(0xC9, 0xCD, 0xD6)
PLAN   = RGBColor(0xB9, 0xBF, 0xCA)
GREENBG = RGBColor(0xEF, 0xF6, 0xF0)
ORNGBG = RGBColor(0xFD, 0xF1, 0xE4)
RED    = RGBColor(0xD1, 0x43, 0x43)
REDBG  = RGBColor(0xFB, 0xEC, 0xEC)
FONT   = "Arial"
SW, SH = 13.333, 7.5

import calendar
import datetime as _dt
STATUS = _dt.date(2026, 10, 8)
STATUS_STR = STATUS.strftime("%d.%m.%Y")
PILOT = (_dt.date(2026, 9, 20), _dt.date(2026, 9, 30))   # план пилота 20–30 сентября (Елена, 16.09); 08.10 — застряли на загрузке списков в Campaign

# ── реальный перенос строк по метрикам Arial (а не по «примерно N символов»)
from PIL import ImageFont as _IF
_ARIAL = "/System/Library/Fonts/Supplemental/Arial.ttf"
_fcache = {}


def _font(pt, bold=False):
    key = (round(pt * 4), bold)
    if key not in _fcache:
        path = _ARIAL.replace("Arial.ttf", "Arial Bold.ttf") if bold else _ARIAL
        _fcache[key] = _IF.truetype(path, int(round(pt * 4)))  # 4x для точности
    return _fcache[key]


def text_w(t, pt, bold=False):
    """Ширина строки в дюймах при кегле pt."""
    return _font(pt, bold).getlength(t) / 4.0 / 72.0


def wrap_lines(t, w_in, pt, bold=False):
    """Число строк при переносе по словам в колонке шириной w_in дюймов."""
    words, lines, cur = t.split(), 0, ""
    for w in words:
        trial = (cur + " " + w).strip()
        if text_w(trial, pt, bold) <= w_in or not cur:
            cur = trial
        else:
            lines += 1; cur = w
    return lines + (1 if cur else 0)


DONE, WIP, PLND = "done", "wip", "plan"
ST_COL = {DONE: GREEN, WIP: ORANGE, PLND: PLAN}
ST_TXT = {DONE: "Сделано", WIP: "В работе", PLND: "План"}


def new_prs():
    p = Presentation(); p.slide_width = Inches(SW); p.slide_height = Inches(SH)
    return p


def add_slide(prs):
    return prs.slides.add_slide(prs.slide_layouts[6])


def box(s, l, t, w, h, fill=WHITE, line=BORD, lw=0.75, rounded=True):
    shp = s.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE if rounded else MSO_SHAPE.RECTANGLE,
        Inches(l), Inches(t), Inches(w), Inches(h))
    shp.fill.solid(); shp.fill.fore_color.rgb = fill
    if line is None:
        shp.line.fill.background()
    else:
        shp.line.color.rgb = line; shp.line.width = Pt(lw)
    shp.shadow.inherit = False
    if rounded:
        try: shp.adjustments[0] = 0.045
        except Exception: pass
    return shp


def txt(s, l, t, w, h, runs, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, space_after=2, ls=1.0):
    tb = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
    tf = tb.text_frame; tf.word_wrap = True; tf.vertical_anchor = anchor
    for m in ("left", "right"): setattr(tf, f"margin_{m}", Inches(0.05))
    for m in ("top", "bottom"): setattr(tf, f"margin_{m}", Inches(0.02))
    for i, para in enumerate(runs):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align; p.space_after = Pt(space_after); p.space_before = Pt(0); p.line_spacing = ls
        for (t_, sz, col, bold) in para:
            r = p.add_run(); r.text = t_
            r.font.size = Pt(sz); r.font.color.rgb = col; r.font.bold = bold; r.font.name = FONT
    return tb


def header_block(s, kicker, title, page):
    box(s, 0.5, 0.4, 0.1, 0.82, fill=ORANGE, line=None, rounded=False)
    txt(s, 0.74, 0.38, 12.1, 0.32, [[(kicker.upper(), 12.5, ORANGE, True)]])
    txt(s, 0.74, 0.66, 12.2, 0.72, [[(title, 21, INK, True)]])
    print(f"  слайд {page} · заголовок: {wrap_lines(title, 12.1, 21, bold=True)} строки")
    txt(s, 0.5, 7.04, 9.5, 0.32,
        [[(f"Внедрение ИИ в CVM · ДИКСИ · статус на {STATUS_STR}", 10, GREY, False)]])
    txt(s, 12.0, 7.04, 0.85, 0.32, [[(str(page), 10, GREY, True)]], align=PP_ALIGN.RIGHT)


def card(s, l, t, w, h, lines, fill=WHITE, line=BORD, accent=None, ls=1.0):
    box(s, l, t, w, h, fill=fill, line=line, lw=0.75)
    if accent:
        box(s, l, t, 0.07, h, fill=accent, line=None, rounded=False)
    txt(s, l + 0.18, t + 0.03, w - 0.26, h - 0.06, lines, anchor=MSO_ANCHOR.MIDDLE, ls=ls, space_after=2)


def legend(s, l, t):
    x = l
    for st in (DONE, WIP, PLND):
        box(s, x, t + 0.045, 0.26, 0.16, fill=ST_COL[st], line=None, rounded=False)
        txt(s, x + 0.34, t, 1.15, 0.26, [[(ST_TXT[st], 11.5, GREY, False)]])
        x += 1.42
    return x


# ── месячная ось: авг.26 … мар.27 (как в листе «Трудоёмкость (детально)»)
MONTHS = ["авг.26", "сент.26", "окт.26", "ноя.26", "дек.26", "янв.27", "фев.27", "мар.27"]


def date_idx(d):
    """Позиция даты на оси MONTHS: индекс месяца (авг.26 = 0) + доля прошедших дней."""
    m = (d.year - 2026) * 12 + d.month - 8
    return m + (d.day - 1) / calendar.monthrange(d.year, d.month)[1]


def today_idx():
    return date_idx(STATUS)


def gantt(s, rows, gx, gy, gw, row_h, name_w, milestone=None, name_sz=11.5):
    """rows: (label, sub, m_from, m_to, status) — m_from/m_to индексы месяцев (вкл.)."""
    cw = (gw - name_w) / len(MONTHS)
    # шапка месяцев
    for i, m in enumerate(MONTHS):
        x = gx + name_w + i * cw
        box(s, x, gy, cw, 0.34, fill=INK if i < 3 else GREY, line=WHITE, lw=1.0, rounded=False)
        txt(s, x, gy, cw, 0.34, [[(m, 11, WHITE, True)]], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    n = len(rows)
    ty = gy + 0.34
    # фон строк + вертикальные линии месяцев
    for j in range(n):
        box(s, gx, ty + j * row_h, gw, row_h, fill=WHITE if j % 2 else LGREY, line=None, rounded=False)
    for i in range(len(MONTHS) + 1):
        x = gx + name_w + i * cw
        ln = box(s, x, ty, 0.008, n * row_h, fill=BORD, line=None, rounded=False)
    # линия «сегодня»
    tx = gx + name_w + today_idx() * cw
    box(s, tx, gy, 0.022, 0.34 + n * row_h, fill=ORANGE, line=None, rounded=False)
    txt(s, tx - 0.72, gy - 0.28, 1.44, 0.26, [[(f"{STATUS:%d.%m} · {STATUS.isocalendar()[1]} неделя", 10, ORANGE, True)]], align=PP_ALIGN.CENTER)
    if milestone:
        ma, mb, mlabel = milestone          # веха-диапазон: индексы начала и конца на оси
        mx, mx2 = gx + name_w + ma * cw, gx + name_w + mb * cw
        box(s, mx, ty + n * row_h + 0.07, mx2 - mx, 0.15, fill=PURPLE, line=None, rounded=True)
        txt(s, mx2 + 0.1, ty + n * row_h + 0.02, 4.0, 0.26, [[(mlabel, 11, PURPLE, True)]])
    # строки; st может быть списком отрезков [(from, to, status), ...]
    for j, (label, sub, a, b, st) in enumerate(rows):
        y = ty + j * row_h
        if label.startswith("§"):   # заголовок этапа
            box(s, gx, y, gw, row_h, fill=WARM, line=None, rounded=False)
            txt(s, gx + 0.1, y, gw - 0.2, row_h, [[(label[1:], 12, ORANGE, True)]], anchor=MSO_ANCHOR.MIDDLE)
            continue
        runs = [[(label, name_sz, INK, False)]]
        if sub:
            runs[0].append(("  " + sub, name_sz - 1.5, GREY, False))
        txt(s, gx + 0.1, y, name_w - 0.16, row_h, runs, anchor=MSO_ANCHOR.MIDDLE, ls=0.95)
        for (a_, b_, st_) in (st if isinstance(st, list) else [(a, b, st)]):
            bx = gx + name_w + a_ * cw + 0.05
            bw = (b_ - a_ + 1) * cw - 0.10
            box(s, bx, y + row_h * 0.22, bw, row_h * 0.56, fill=ST_COL[st_], line=None, rounded=True)
            txt(s, bx, y + row_h * 0.22, bw, row_h * 0.56, [[(ST_TXT[st_], 9.5, WHITE, True)]],
                align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    frame = box(s, gx, gy, gw, 0.34 + n * row_h, fill=WHITE, line=BORD, rounded=False)
    frame.fill.background()


# ════════════════════════════ Слайд 1 — общий статус
def build_overview(prs):
    s = add_slide(prs)
    header_block(s, f"Роадмэп · статус на {STATUS_STR}",
                 "Пилот застрял на загрузке маркетинговых списков в Campaign — реализуем обходной путь: отправка через портал в Firebase и агент-загрузчик списков", 1)
    # KPI-плашки
    tiles = [
        ("ЭТАП 1 · ДЦО", "6 из 13", "задач этапа закрыто", GREEN),
        ("ЭТАП 2 · CVM", "В работе", "по пожеланиям клиента\nускорились по CVM", PURPLE),
        ("ПИЛОТ", "Застряли", "на загрузке маркетинговых\nсписков в Campaign", RED),
        ("ОБХОДНОЙ ПУТЬ", "В работе", "1 · портал → Firebase\n2 · агент сам загружает списки", ORANGE),
    ]
    tw, gap, tl, tt, th = 2.87, 0.22, 0.6, 1.42, 1.30      # 4 плашки ровно в поле 0.6–12.74
    for i, (k, big, sub, col) in enumerate(tiles):
        x = tl + i * (tw + gap)
        box(s, x, tt, tw, th, fill=WHITE, line=BORD)
        box(s, x, tt, tw, 0.09, fill=col, line=None, rounded=False)
        txt(s, x + 0.18, tt + 0.16, tw - 0.3, 0.26, [[(k, 10.5, col, True)]])
        txt(s, x + 0.18, tt + 0.44, tw - 0.3, 0.40, [[(big, 21, INK, True)]])
        txt(s, x + 0.18, tt + 0.85, tw - 0.3, 0.42,
            [[(l, 12, GREY, False)] for l in sub.split("\n")], ls=1.0)

    # Полоса этапов
    txt(s, 0.6, 3.00, 12.2, 0.3, [[("ЧЕТЫРЕ ЭТАПА · АВГУСТ 2026 — МАРТ 2027", 12, ORANGE, True)]])
    stages = [
        ("ЭТАП 1 · авг–окт 26", "Персонализация ДЦО", "670 ч", DONE,
         "Сервис, данные, MCI, КГ,\nдвижок текстов, контент ДЦО"),
        ("ЭТАП 2 · ноя 26 – янв 27", "CVM: 100 сегментов", "1 202 ч", WIP,
         "100 сегментов, конвейер контента,\nинтерфейс, агенты, дашборды"),
        ("ЭТАП 3 · фев 27", "Масштаб 200 акций", "310 ч", PLND,
         "Оптимизатор, 200 акций,\nпередача сервиса"),
        ("ЭТАП 4 · мар 27", "Дообучение на откликах", "299 ч", PLND,
         "Витрина откликов, дообучение,\nстоп-листы, итоги пилота"),
    ]
    sw_, sg, sl, st_, sh_ = 2.855, 0.24, 0.6, 3.34, 1.60   # 4 этапа ровно в поле 0.6–12.74
    for i, (per, name, hrs, status, det) in enumerate(stages):
        x = sl + i * (sw_ + sg)
        fill = GREENBG if status == DONE else WHITE
        box(s, x, st_, sw_, sh_, fill=fill, line=BORD)
        box(s, x, st_, sw_, 0.07, fill=ST_COL[status], line=None, rounded=False)
        txt(s, x + 0.18, st_ + 0.14, sw_ - 0.32, 0.26, [[(per, 10.5, GREY, True)]])
        txt(s, x + 0.16, st_ + 0.40, sw_ - 0.28, 0.34, [[(name, 14, INK, True)]])
        txt(s, x + 0.16, st_ + 0.78, sw_ - 0.28, 0.60,
            [[(l, 11, GREY, False)] for l in det.split("\n")], ls=1.05)
        txt(s, x + 0.18, st_ + sh_ - 0.34, sw_ - 0.32, 0.26,
            [[(hrs + " · ", 11.5, GREY, True), (ST_TXT[status] if status != DONE else "Этап в работе", 11.5, ST_COL[status], True)]])
        if i < 3:
            a = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW, Inches(x + sw_ + 0.045), Inches(st_ + 0.66),
                                   Inches(0.15), Inches(0.30))
            a.fill.solid(); a.fill.fore_color.rgb = ORANGE; a.line.fill.background(); a.shadow.inherit = False

    # Нижняя лента — эффект (ДЦО + CVM, стопка)
    box(s, 0.6, 5.20, 12.14, 1.66, fill=WARM, line=BORD)
    txt(s, 0.8, 5.28, 4.2, 0.3, [[("ДОП. ТОВАРООБОРОТ, млн ₽/мес", 12, ORANGE, True)]])
    # легенда
    lx = 5.25
    for lbl, col in (("ДЦО", ORANGE), ("CVM", PURPLE)):
        box(s, lx, 5.34, 0.22, 0.14, fill=col, line=None, rounded=False)
        txt(s, lx + 0.28, 5.26, 0.75, 0.26, [[(lbl, 11.5, GREY, False)]])
        lx += 1.02
    # сент/окт: в файле оценки CVM не было; с 16.09 — +20 и +50 млн ₽ (ускорились по CVM)
    # сент. ДЦО: пилот не запускался — прогноз 43 млн ₽ снят (Елена, 08.10)
    eff = [("сент.26", 0, 20), ("окт.26", 54, 50), ("ноя.26", 54, 208),
           ("дек.26", 34, 208), ("янв.27", 54, 100), ("фев.27", 54, 150)]
    bx0, byb, bwd, maxh, mx = 0.85, 6.52, 0.80, 0.70, 262.0
    for i, (m, dts, cvm) in enumerate(eff):
        x = bx0 + i * 1.02
        hd = maxh * dts / mx
        if dts:
            box(s, x, byb - hd, bwd, hd, fill=ORANGE, line=None, rounded=False)
        top = byb - hd
        if cvm:
            hc = maxh * cvm / mx
            box(s, x, top - hc, bwd, hc, fill=PURPLE, line=None, rounded=False)
            if hc >= 0.14:
                # в стопке подписываем каждую часть внутри своего сегмента
                if dts:
                    txt(s, x, byb - hd, bwd, hd, [[(str(dts), 10, WHITE, True)]],
                        align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
                txt(s, x, top - hc, bwd, hc, [[(str(cvm), 11, WHITE, True)]],
                    align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
                top -= hc
            else:
                # низкий сегмент — разбивка цветом над столбиком
                top -= hc
                if dts:
                    txt(s, x - 0.14, top - 0.23, bwd + 0.28, 0.22,
                        [[(str(dts), 10.5, ORANGE, True), (" + ", 10.5, GREY, False), (str(cvm), 10.5, PURPLE, True)]],
                        align=PP_ALIGN.CENTER)
                    top -= 0.20
        total = dts + (cvm or 0)
        txt(s, x - 0.14, top - 0.25, bwd + 0.28, 0.24,
            [[(str(total), 12, INK, True)]], align=PP_ALIGN.CENTER)
        txt(s, x - 0.14, byb + 0.03, bwd + 0.28, 0.24, [[(m, 10.5, GREY, False)]], align=PP_ALIGN.CENTER)
    # итоги файла оценки (ДЦО 243, CVM 1 040) + CVM сентябрь 20 и октябрь 50
    txt(s, 7.15, 5.36, 5.55, 1.45,
        [[("ДЦО за период — 243 млн ₽,  CVM — 1 110 млн ₽", 13.5, INK, True)],
         [("Совокупно за 6 месяцев ~1,35 млрд ₽", 13.5, INK, True)],
         [("По пожеланиям клиента оптимизировали и ускорились по CVM", 12, PURPLE, True)],
         [("CVM: +20 млн ₽ в сентябре, +50 млн ₽ в октябре", 12, PURPLE, False)]], ls=1.1)


# ════════════════════════════ Слайд 2 — пилот: загрузка списков в Campaign и обходной путь
def chip(s, l, t, w, h, main, sub=None, fill=WHITE, line=BORD, col=INK, lw=0.75):
    box(s, l, t, w, h, fill=fill, line=line, lw=lw)
    runs = [[(main, 14, col, True)]]
    if sub:
        runs.append([(sub, 11, GREY, False)])
    txt(s, l + 0.06, t, w - 0.12, h, runs, align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE, space_after=0)


def arrow(s, l, t, col):
    a = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW, Inches(l), Inches(t), Inches(0.24), Inches(0.24))
    a.fill.solid(); a.fill.fore_color.rgb = col; a.line.fill.background(); a.shadow.inherit = False


def tag(s, l, t, label, col):
    w = text_w(label, 11, True) + 0.26
    box(s, l, t, w, 0.26, fill=col, line=None)
    txt(s, l, t, w, 0.26, [[(label, 11, WHITE, True)]], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)


def build_blocker(prs):
    s = add_slide(prs)
    header_block(s, "Пилот · загрузка списков в Campaign",
                 "Обходной путь в двух направлениях: прямая отправка push через портал в Firebase и агент, который сам перекладывает и загружает списки в Campaign", 2)
    # три дорожки: штатный путь (застряли) и два обходных; 4 слота под чипы
    cx0, cw, ag = 3.05, 2.12, 0.40
    slot = lambda i: cx0 + i * (cw + ag)
    rh, rg, ry = 0.66, 0.16, 1.66
    mci = ("MCI", "маркетинговые списки")
    push = ("Push клиентам", "приложение ДИКСИ")
    lanes = [
        ("Штатный путь", "Застряли", RED,
         [(0, 1, *mci, "mute"), (1, 2, "Загрузка списков в Campaign", None, "block"), (3, 1, *push, "mute")]),
        ("Обходной путь 1", "В работе", ORANGE,
         [(0, 1, *mci, ""), (1, 1, "Портал CVM", "списки, тексты, отправка", "new"),
          (2, 1, "Firebase", "Cloud Messaging", "new"), (3, 1, *push, "")]),
        ("Обходной путь 2", "В работе", ORANGE,
         [(0, 1, *mci, ""), (1, 1, "Агент", "перекладывает и загружает", "new"),
          (2, 1, "Campaign", "загрузка списков", ""), (3, 1, *push, "")]),
    ]
    for j, (lab, st, stc, chips) in enumerate(lanes):
        y = ry + j * (rh + rg)
        txt(s, 0.6, y + 0.02, 2.35, 0.32, [[(lab, 14, INK, True)]])
        tag(s, 0.62, y + 0.36, st, stc)
        for k, (i, span, main, sub, kind) in enumerate(chips):
            x, w = slot(i), cw * span + ag * (span - 1)
            if kind == "mute":
                chip(s, x, y, w, rh, main, sub, fill=LGREY, line=BORD, col=GREY)
            elif kind == "block":
                chip(s, x, y, w, rh, main, sub, fill=REDBG, line=RED, col=RED, lw=1.5)
            elif kind == "new":
                chip(s, x, y, w, rh, main, sub, fill=WHITE, line=PURPLE, col=PURPLE, lw=1.5)
            else:
                chip(s, x, y, w, rh, main, sub)
            if k:
                arrow(s, x - ag / 2 - 0.12, y + rh / 2 - 0.12, MUTE if j == 0 else ORANGE)
        if j < len(lanes) - 1:
            box(s, 0.6, y + rh + rg / 2, 12.14, 0.008, fill=BORD, line=None, rounded=False)

    # карточки обходного пути
    cy = ry + 3 * rh + 2 * rg + 0.24
    chh = 6.86 - cy
    cards = [
        ("1 · Портал CVM → Firebase", [
            "Портал CVM отправляет маркетинговые push в приложение напрямую через Firebase",
            "Списки, тексты, отправка и аналитика доставки — на нашей стороне",
            "Транзакционные и сервисные push приложения канал не затрагивает",
            "От команды приложения нужны: доступ к отправке, токены устройств в MCI, статусы доставки, тестовые устройства",
        ]),
        ("2 · Агент-загрузчик списков", [
            "Доучиваем агента, который сам перекладывает маркетинговые списки и загружает их в Campaign",
            "Путь до клиента прежний: MCI → Campaign → push; меняется только загрузка списков",
        ]),
    ]
    cg, x = 0.22, 0.6
    for i, ((title_, items), cw2) in enumerate(zip(cards, (7.10, 4.82))):
        if i:
            x += 7.10 + cg
        box(s, x, cy, cw2, chh, fill=WHITE, line=BORD)
        box(s, x, cy, 0.07, chh, fill=PURPLE, line=None, rounded=False)
        txt(s, x + 0.24, cy + 0.12, cw2 - 1.8, 0.34, [[(title_, 15, INK, True)]])
        tag(s, x + cw2 - 0.20 - (text_w("В работе", 11, True) + 0.26), cy + 0.15, "В работе", ORANGE)
        iy, tw = cy + 0.58, cw2 - 0.66
        for it in items:
            n_lines = wrap_lines(it, tw - 0.12, 14)   # с запасом на поля текстбокса
            h = 0.215 * n_lines
            box(s, x + 0.26, iy + 0.075, 0.11, 0.11, fill=PURPLE, line=None, rounded=False)
            txt(s, x + 0.48, iy - 0.03, tw, h + 0.1, [[(it, 14, INK, False)]], ls=1.02)
            iy += h + 0.10
        print(f"  слайд 2 · {title_}: конец списка {iy:.2f} / дно карточки {cy + chh:.2f}")


# ════════════════════════════ Слайд 3 — что сделано на дату статуса
def build_status(prs):
    s = add_slide(prs)
    header_block(s, "Статус · 8 октября 2026",
                 "Сделано и в работе на 8 октября: пилот застрял на загрузке маркетинговых списков в Campaign, реализуем обходной путь", 3)
    gap, ty = 0.22, 1.44
    widths = (3.85, 4.55, 3.30)
    ch = 5.40
    cols = [
        ("СДЕЛАНО", GREEN, GREENBG, [
            "Подготовка пилота: списки оптимизированы (2.8)",
            "Подготовка пилота: выделены основные сегменты для запуска",
            "Сервис развёрнут: доступы, безопасность, бэкапы",
            "Подключены данные ДЦО: клиенты, покупки, текущие акции",
            "Встроен модуль сегментации MCI + интерфейс выбора сегментов",
            "Настроены автоматические контрольные группы на базе MCI",
            "Описана методология «сегмент → оффер»",
            "ДЦО-акции переведены в персональный формат",
        ]),
        ("В РАБОТЕ", ORANGE, ORNGBG, [
            "!Пилот: застряли на загрузке маркетинговых списков в Campaign",
            "Обходной путь 1: портал → Firebase",
            "Обходной путь 2: доучиваем агента, который сам загружает списки",
            "Акции ДЦО на согласовании в КД",
            "Движок персонализации текстов ДЦО под сегмент",
            "Агенты в обучении: копирайтер, редактор-гуманизатор",
            "По пожеланиям клиента ускорились по CVM: работы начаты",
            "База знаний: шаблоны, скиллы, гайдлайны, tone of voice",
            "Бизнес-процесс планирования и заведения ДЦО",
            "Отдельные среды под агентов, шифрование данных",
        ]),
        ("СЛЕДУЮЩИЕ ШАГИ", PURPLE, LGREY, [
            "Пилот: запуск через обходной путь и замер vs контрольные группы",
            "Постэффекты пилота: отклик, доп. ТО, маржа по сегментам",
            "Приёмка этапа 1, старт этапа 2 — 100 сегментов с ноября",
            "Конвейер контента и интерфейс заведения акций",
            "Агенты: конструктор акций, оператор заведения, дизайнер",
        ]),
    ]
    for i, ((title_, col, bg, items), col_w) in enumerate(zip(cols, widths)):
        x = 0.6 + sum(widths[:i]) + i * gap
        box(s, x, ty, col_w, ch, fill=bg, line=BORD)
        box(s, x, ty, col_w, 0.42, fill=col, line=None, rounded=False)
        txt(s, x + 0.18, ty, col_w - 0.3, 0.42, [[(title_, 13, WHITE, True)]], anchor=MSO_ANCHOR.MIDDLE)
        iy = ty + 0.58
        tw = col_w - 0.62
        for it in items:
            alert = it.startswith("!")          # блокер — красным
            it = it.lstrip("!")
            n_lines = wrap_lines(it, tw - 0.04, 14, bold=alert)
            h = 0.215 * n_lines
            box(s, x + 0.20, iy + 0.075, 0.12, 0.12, fill=RED if alert else col, line=None, rounded=False)
            txt(s, x + 0.44, iy - 0.03, tw, h + 0.1, [[(it, 14, RED if alert else INK, alert)]], ls=1.02)
            iy += h + 0.09
        print(f"  слайд 3 · {title_}: конец колонки {iy:.2f} / дно карточки {ty + ch:.2f}")


# ════════════════════════════ Слайд 3 — Гант этапа 1
def build_gantt_stage1(prs):
    s = add_slide(prs)
    header_block(s, "Роадмэп · этап 1",
                 "Персонализация ДЦО: данные, сегментация и контент готовы; пилот застрял на загрузке списков в Campaign — реализуем обходной путь", 4)
    rows = [
        ("Развёртывание сервиса: доступы, безопасность, бэкапы", "1.1", 0, 1, DONE),
        ("Интеграция источников данных ДЦО", "1.2", 0, 1, DONE),
        ("Модуль сегментации MCI + интерфейс сегментов", "1.3", 0, 1, DONE),
        ("Автоматические контрольные группы на базе MCI", "1.4", 0, 1, DONE),
        ("Движок персонализации текстов ДЦО", "1.5", 0, 2, WIP),
        ("Методология персонализации: сегмент → оффер", "1.6", 0, 1, DONE),
        ("Перевод ДЦО-акций в персональный формат", "1.7", 0, 1, DONE),
        ("База знаний: шаблоны, скиллы, tone of voice", "1.8", 1, 2, WIP),
        ("Бизнес-процесс планирования и заведения ДЦО", "1.9", 1, 2, WIP),
        ("Пилот + постэффекты vs контрольная группа", "1.10", 1, 2, WIP),
        ("Серверы под агентов: изолированные среды", "1.12", 1, 2, WIP),
        ("Безопасность: шифрование данных для агентов", "1.13", 1, 2, WIP),
        ("Управление этапом: статусы, демо, приёмка", "1.11", 0, 2, WIP),
    ]
    gantt(s, rows, gx=0.6, gy=1.70, gw=12.14, row_h=0.31, name_w=5.55,
          milestone=(date_idx(PILOT[0]), date_idx(PILOT[1]), "Пилот — план был 20–30 сентября"))
    legend(s, 0.62, 1.34)
    txt(s, 5.6, 1.30, 7.2, 0.3,
        [[("Этап 1 · 13 задач: 6 закрыто, 7 в работе", 12.5, GREY, False)]],
        align=PP_ALIGN.RIGHT)
    txt(s, 0.6, 6.42, 12.2, 0.56,
        [[("Пилот 1.10 застрял на загрузке маркетинговых списков в Campaign; обходной путь — портал → Firebase и агент-загрузчик списков.", 12.5, GREY, False)],
         [("Акции ДЦО по задаче 1.7 переданы на согласование в коммерческую дирекцию.", 12.5, GREY, False)]])


# ════════════════════════════ Слайд 4 — Гант этапов 2–4
def build_gantt_next(prs):
    s = add_slide(prs)
    header_block(s, "Роадмэп · этапы 2–4",
                 "С ноября — масштаб до 100 сегментов, затем 200 акций в месяц и дообучение агентов на откликах", 5)
    rows = [
        ("§ЭТАП 2 · ноябрь 2026 — январь 2027 · Полноценный CVM: 100 сегментов · 1 202 ч", "", 0, 0, PLND),
        ("100 сегментов MCI в проекте: конфиг, обновление, выгрузки", "2.1", 3, 5, PLND),
        ("Конвейер генерации контента + контент под 100 сегментов", "2.2 · 2.7", 3, 5, PLND),
        ("Операционный интерфейс заведения акций", "2.3", 3, 5, PLND),
        ("Деплинки, SKU и механики в конвейере", "2.4", 3, 5, PLND),
        ("Агенты: конструктор, оператор, дизайнер, оптимизатор", "2.5 · 2.13 · 2.15–2.16", 3, 5, PLND),
        ("Дашборд эффективности и постэффекты сегментных акций", "2.6 · 2.14", 3, 5, PLND),
        ("Операционная часть заведения акций: настройка, расписание, QA", "2.8", 0, 0,
         [(1, 2, WIP), (3, 5, PLND)]),
        ("Методология 100 сегментов, процесс, обучение команды заказчика", "2.9–2.12", 3, 5, PLND),
        ("§ЭТАП 3 · февраль 2027 · Масштаб + простая оптимизация · 310 ч", "", 0, 0, PLND),
        ("200 акций через конвейер + автоподбор оффера и сегмента", "3.2 · 3.3", 6, 6, PLND),
        ("Стабилизация интерфейса, отчёт по эффекту, передача сервиса", "3.1 · 3.4–3.6", 6, 6, PLND),
        ("§ЭТАП 4 · март 2027 · Дообучение на откликах и ограничения товаров · 299 ч", "", 0, 0, PLND),
        ("Витрина откликов клиентов, дообучение и калибровка агентов", "4.1–4.4", 7, 7, PLND),
        ("Стоп-листы товаров и ограничения в конструкторе акций", "4.5 · 4.6", 7, 7, PLND),
        ("Коррекция методологии по итогам пилота, итоговая аналитика", "4.7–4.10", 7, 7, PLND),
    ]
    gantt(s, rows, gx=0.6, gy=1.70, gw=12.14, row_h=0.30, name_w=6.05, name_sz=11)
    legend(s, 0.62, 1.34)


def main():
    prs = new_prs()
    build_overview(prs)
    build_blocker(prs)
    build_status(prs)
    build_gantt_stage1(prs)
    build_gantt_next(prs)
    out = f"output/Роадмэп_статус_внедрение_ИИ_ДИКСИ_{STATUS_STR}.pptx"
    prs.save(out)
    print("saved:", out)


if __name__ == "__main__":
    main()
