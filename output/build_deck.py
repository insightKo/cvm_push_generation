# -*- coding: utf-8 -*-
"""Презентация: ИИ-конвейер запуска акций CVM в ДИКСИ (PPTX).
Стиль: bonnie-slide + mckinsey-deck. Крупная типографика (тело ≥14pt; 10-11pt
только в таблицах/плотных диаграммах). Цифры эффекта — из слайда
«Эффективность сегментных акций» (CVM, окт.2025–май.2026)."""
import sys
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

INK    = RGBColor(0x1E, 0x1E, 0x2F)
ORANGE = RGBColor(0xEE, 0x72, 0x03)
PURPLE = RGBColor(0x6C, 0x4A, 0xB6)
GREEN  = RGBColor(0x2E, 0x9E, 0x5B)
RED    = RGBColor(0xC6, 0x28, 0x28)
GREY   = RGBColor(0x5B, 0x61, 0x6E)
LGREY  = RGBColor(0xF4, 0xF5, 0xF7)
BORD   = RGBColor(0xD9, 0xDC, 0xE1)
WHITE  = RGBColor(0xFF, 0xFF, 0xFF)
WARM   = RGBColor(0xFD, 0xF8, 0xF2)
MUTE   = RGBColor(0xC9, 0xCD, 0xD6)
GREENBG = RGBColor(0xEF, 0xF6, 0xF0)
FONT   = "Arial"
SW, SH = 13.333, 7.5


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
    txt(s, 0.5, 7.04, 9, 0.32, [[("CVM · ИИ-конвейер запуска акций · ДИКСИ", 10, GREY, False)]])
    txt(s, 12.0, 7.04, 0.85, 0.32, [[(str(page), 10, GREY, True)]], align=PP_ALIGN.RIGHT)


def arrow(s, l, t, w=0.2, h=0.44, col=ORANGE):
    a = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW, Inches(l), Inches(t), Inches(w), Inches(h))
    a.fill.solid(); a.fill.fore_color.rgb = col; a.line.fill.background(); a.shadow.inherit = False


def darrow(s, l, t, w=0.32, h=0.18, col=PURPLE):
    a = s.shapes.add_shape(MSO_SHAPE.DOWN_ARROW, Inches(l), Inches(t), Inches(w), Inches(h))
    a.fill.solid(); a.fill.fore_color.rgb = col; a.line.fill.background(); a.shadow.inherit = False


def col_header(s, l, t, w, text, col, h=0.42, sz=13):
    box(s, l, t, w, h, fill=col, line=None, rounded=True)
    txt(s, l, t, w, h, [[(text, sz, WHITE, True)]], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)


def card(s, l, t, w, h, lines, fill=WHITE, line=BORD, accent=None, ls=1.0):
    box(s, l, t, w, h, fill=fill, line=line, lw=0.75)
    if accent:
        box(s, l, t, 0.07, h, fill=accent, line=None, rounded=False)
    txt(s, l + 0.16, t + 0.03, w - 0.22, h - 0.06, lines, anchor=MSO_ANCHOR.MIDDLE, ls=ls, space_after=1)


# ════════════════════════════════════════════════════════════════
def build_cover(prs):
    s = add_slide(prs)
    box(s, 0, 0, SW, SH, fill=INK, line=None, rounded=False)
    box(s, 0, 5.5, SW, 0.13, fill=ORANGE, line=None, rounded=False)
    txt(s, 0.9, 1.35, 11.5, 0.4, [[("ЗАПУСК АКЦИЙ · ЗАКРЫТЫЙ КОНТУР", 15, ORANGE, True)]])
    txt(s, 0.9, 1.9, 11.6, 2.2,
        [[("ИИ-конвейер запуска акций", 44, WHITE, True)],
         [("от идеи акции до постанализа", 26, WHITE, False)],
         [("внутри периметра ДИКСИ", 26, MUTE, False)]], ls=1.05)
    txt(s, 0.9, 4.8, 11.6, 0.5,
        [[("Бизнес-кейс · Архитектура · Жизненный цикл · Агенты · Обучение · Модерация · План", 15, MUTE, False)]])
    txt(s, 0.9, 6.45, 11.6, 0.5,
        [[("Сегментные акции: +20 ₽/клиента vs +3 ₽  →  потенциал до +1,5 млрд ₽/год", 14, GREY, False)]])


def build_case(prs):
    s = add_slide(prs)
    header_block(s, "Слайд 1 · Бизнес-кейс",
                 "Сегментные акции дают +20 ₽/клиента против +3 ₽ — потенциал до +1,5 млрд ₽/год", 1)
    # ── left: comparison table
    lx = 0.6
    txt(s, lx, 1.45, 6.0, 0.32, [[("Широкая рассылка vs сегментные акции", 14, INK, True)]])
    rows = [
        ("Метрика", "Широкая", "Сегментная", True),
        ("Доп. ТО на клиента", "+3 ₽", "+20 ₽", False),
        ("Доля акций «в плюсе»", "58%", "68%", False),
        ("Переход по ссылке", "~4%", "8–12%", False),
    ]
    rt, rh = 1.85, 0.52
    cwm, cwa, cwb = 3.05, 1.4, 1.55
    for i, (m, a, b, hdr) in enumerate(rows):
        t = rt + i * rh
        if hdr:
            box(s, lx, t, cwm, rh, fill=INK, line=None, rounded=False)
            box(s, lx + cwm, t, cwa, rh, fill=GREY, line=None, rounded=False)
            box(s, lx + cwm + cwa, t, cwb, rh, fill=ORANGE, line=None, rounded=False)
            txt(s, lx + 0.12, t, cwm - 0.12, rh, [[(m, 12.5, WHITE, True)]], anchor=MSO_ANCHOR.MIDDLE)
            txt(s, lx + cwm, t, cwa, rh, [[(a, 12.5, WHITE, True)]], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
            txt(s, lx + cwm + cwa, t, cwb, rh, [[(b, 12.5, WHITE, True)]], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
        else:
            box(s, lx, t, cwm + cwa + cwb, rh, fill=WHITE if i % 2 else LGREY, line=BORD, rounded=False)
            txt(s, lx + 0.12, t, cwm - 0.12, rh, [[(m, 13, INK, False)]], anchor=MSO_ANCHOR.MIDDLE)
            txt(s, lx + cwm, t, cwa, rh, [[(a, 14, GREY, False)]], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
            txt(s, lx + cwm + cwa, t, cwb, rh, [[(b, 17, ORANGE, True)]], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    # ── right: mini bar chart
    rx, rw = 6.85, 5.88
    txt(s, rx, 1.45, rw, 0.32, [[("Переход растёт с точностью сегмента, %", 14, INK, True)]])
    bars = [("Широкий", 4, GREY), ("Мамы", 7, ORANGE), ("Готовая\nеда", 8, ORANGE),
            ("Быт.\nхимия", 9, ORANGE), ("Зоо", 12, GREEN)]
    base_y = 3.5; max_h = 1.3; maxv = 12
    slot = rw / len(bars); bw = 0.7
    for i, (lab, val, col) in enumerate(bars):
        cx = rx + i * slot + slot / 2
        bh = val / maxv * max_h
        box(s, cx - bw / 2, base_y - bh, bw, bh, fill=col, line=None, rounded=False)
        txt(s, cx - slot / 2, base_y - bh - 0.32, slot, 0.3, [[(f"{val}%", 15, col, True)]], align=PP_ALIGN.CENTER)
        txt(s, cx - slot / 2, base_y + 0.04, slot, 0.5, [[(lab, 11.5, INK, False)]], align=PP_ALIGN.CENTER, ls=0.92)
    # ── potential ladder
    txt(s, 0.6, 4.32, 12, 0.32, [[("Потенциал на активной push-базе", 14, INK, True)]])
    nodes = [
        ("2,6 млн", "активных клиентов\nдоступны по push", INK, LGREY),
        ("+600 млн ₽/год", "категорийная сегментация\nвсей базы: +20 ₽/клиента", ORANGE, WARM),
        ("до +1,5 млрд ₽/год", "глубокое деление аудиторий,\nдо +50 ₽/клиента", GREEN, GREENBG),
    ]
    ny, nh, nw = 4.72, 1.12, 3.72
    xs = [0.6, 4.82, 9.04]
    for i, (big, sub, col, bg) in enumerate(nodes):
        l = xs[i]
        box(s, l, ny, nw, nh, fill=bg, line=col, lw=1.5)
        txt(s, l + 0.18, ny + 0.12, nw - 0.36, 0.5, [[(big, 18, col, True)]])
        txt(s, l + 0.18, ny + 0.58, nw - 0.36, 0.5, [[(sub, 11.5, GREY, False)]], ls=1.0)
        if i < 2:
            arrow(s, l + nw + 0.04, ny + nh / 2 - 0.18, 0.42, 0.36, col)
    box(s, 0.6, 5.98, 12.13, 0.46, fill=INK, line=None)
    txt(s, 0.85, 5.98, 11.7, 0.46,
        [[("ЧТОБЫ ПОКРЫТЬ ВСЕ ПОДСЕГМЕНТЫ:  ", 11.5, ORANGE, True),
          ("~20-25 → ~200 акций/мес (×8-10)", 11.5, WHITE, False)]], anchor=MSO_ANCHOR.MIDDLE)
    txt(s, 0.6, 6.55, 12, 0.3,
        [[("Источник: слайд «Эффективность сегментных акций», CVM, окт.2025–май.2026; "
           "сегмент «Активные», медианы, эффект vs контрольная группа.", 9.5, GREY, False)]])


def build_overview(prs):
    s = add_slide(prs)
    header_block(s, "Слайд 2 · Верхнеуровневая архитектура",
                 "Решение строится снизу вверх: инфраструктура → база знаний → агенты → модератор", 2)
    stack_l, stack_w = 0.6, 9.35
    panel_l, panel_w = 10.15, 2.58

    def plank(t, h, num, title, desc, contents, col):
        box(s, stack_l, t, stack_w, h, fill=WHITE, line=BORD)
        box(s, stack_l, t, 0.11, h, fill=col, line=None, rounded=False)
        o = s.shapes.add_shape(MSO_SHAPE.OVAL, Inches(stack_l + 0.26), Inches(t + h / 2 - 0.28),
                               Inches(0.56), Inches(0.56))
        o.fill.solid(); o.fill.fore_color.rgb = col; o.line.fill.background(); o.shadow.inherit = False
        txt(s, stack_l + 0.26, t + h / 2 - 0.30, 0.56, 0.56, [[(num, 19, WHITE, True)]],
            align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
        txt(s, stack_l + 1.02, t + 0.1, 3.0, h - 0.2,
            [[(title, 15, col, True)], [(desc, 11, GREY, False)]], anchor=MSO_ANCHOR.MIDDLE, ls=1.05, space_after=2)
        txt(s, stack_l + 4.15, t + 0.08, stack_w - 4.3, h - 0.16,
            [[(contents, 11, INK, False)]], anchor=MSO_ANCHOR.MIDDLE, ls=1.12)

    box(s, stack_l, 1.5, stack_w, 0.4, fill=GREEN, line=None)
    txt(s, stack_l, 1.5, stack_w, 0.4,
        [[("↑  ВЫПУСК: действие в Manzana и каналах (push · slip)", 12, WHITE, True)]],
        align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    planks = [
        (1.98, "4", "Модератор", "проверяет каждого агента до действия",
         "безопасность · экономика акции · голос бренда · комплаенс (ФЗ-38/152) · анти-дубли · ревью человеком", RED),
        (3.18, "3", "Агенты", "команда на весь цикл акции",
         "идея · прогноз · выбор предложения · механика · сегмент · план · заведение · тексты · оптимизация", ORANGE),
        (4.38, "2", "База знаний", "на ней обучаются агенты ↑",
         "голос бренда: «ты», лексика, запреты · правила коммуникаций: длины и структура текстов · "
         "механики акций и расчёт выгоды для клиента · соответствие сегментов и категорий · удачные примеры", PURPLE),
        (5.58, "1", "Инфраструктура", "фундамент — нужна первой",
         "on-premise / частный контур · LLM внутри периметра · вычисления · коннекторы к данным · ИБ и маскирование", GREY),
    ]
    ph = 1.06
    for t, num, title, desc, contents, col in planks:
        plank(t, ph, num, title, desc, contents, col)
    for ty in (5.50, 4.30, 3.10):
        a = s.shapes.add_shape(MSO_SHAPE.UP_ARROW, Inches(stack_l + stack_w / 2 - 0.14),
                               Inches(ty), Inches(0.28), Inches(0.14))
        a.fill.solid(); a.fill.fore_color.rgb = GREY; a.line.fill.background(); a.shadow.inherit = False
    box(s, panel_l, 1.5, panel_w, 6.14, fill=INK, line=None)
    txt(s, panel_l + 0.2, 1.66, panel_w - 0.36, 0.8,
        [[("ИНТЕРФЕЙС", 14, ORANGE, True)], [("УПРАВЛЕНИЯ", 14, ORANGE, True)]], ls=1.0)
    txt(s, panel_l + 0.2, 2.5, panel_w - 0.36, 0.5,
        [[("Показать и управлять агентами внутри", 11, MUTE, False)]], ls=1.1)
    items = ["Дашборд всех агентов", "Запуск / пауза / настройка",
             "Просмотр результатов", "Логи и аудит решений", "Ручное вмешательство"]
    txt(s, panel_l + 0.2, 3.35, panel_w - 0.36, 4.0,
        [[("•  " + it, 12.5, WHITE, False)] for it in items], ls=1.1, space_after=10)


def build_arch(prs):
    s = add_slide(prs)
    header_block(s, "Слайд 3 · Архитектура",
                 "Промо-фабрика целиком внутри закрытого контура ДИКСИ, без выхода наружу", 3)
    box(s, 0.4, 1.5, 12.55, 5.22, fill=WARM, line=ORANGE, lw=1.75, rounded=True)
    txt(s, 0.58, 1.55, 9.5, 0.3,
        [[("КОНТУР ДИКСИ — внешний egress запрещён · LLM внутри периметра", 11, ORANGE, True)]])
    top = 2.04
    c1l, c1w = 0.6, 2.5
    c2l, c2w = 3.36, 3.5
    c3l, c3w = 7.18, 2.92
    c4l, c4w = 10.42, 2.36
    for ax in (3.08, 6.92, 10.18):
        arrow(s, ax, 3.95, 0.2, 0.46)
    # Col1 — данные
    col_header(s, c1l, top, c1w, "ДАННЫЕ", GREY, sz=12)
    txt(s, c1l + 0.04, top + 0.5, c1w, 0.24, [[("ВНУТРЕННИЕ", 10, GREY, True)]])
    card(s, c1l, top + 0.76, c1w, 0.62,
         [[("DWH", 13, INK, True), ("  — транзакции, чеки", 10.5, GREY, False)]], accent=GREY)
    card(s, c1l, top + 1.44, c1w, 0.66,
         [[("MCI", 13, INK, True)], [("сегментация клиентов, аналитика", 10.5, GREY, False)]], accent=GREY)
    txt(s, c1l + 0.04, top + 2.18, c1w, 0.24, [[("ДЛЯ ОБУЧЕНИЯ КОНТЕНТА", 10, GREY, True)]])
    card(s, c1l, top + 2.44, c1w, 0.56, [[("Голос бренда и правила текстов", 11.5, INK, False)]], accent=GREY, ls=1.0)
    card(s, c1l, top + 3.06, c1w, 0.56, [[("Механики акций и выгода клиента", 11.5, INK, False)]], accent=GREY, ls=1.0)
    # Col2 — промо-фабрика
    col_header(s, c2l, top, c2w, "ПРОМО-ФАБРИКА · ON-PREMISE", ORANGE, sz=12)
    y = top + 0.54
    card(s, c2l, y, c2w, 0.6, [[("LLM внутри периметра", 13, INK, True)]], accent=ORANGE); y += 0.68
    card(s, c2l, y, c2w, 0.66,
         [[("Стратегия акций", 13, INK, True)], [("идеи · прогноз · выбор · механика · сегмент · план", 10.5, GREY, False)]],
         accent=ORANGE, ls=1.0); y += 0.74
    card(s, c2l, y, c2w, 0.52, [[("Коммуникации: тексты пуш · купон", 12, INK, False)]], accent=ORANGE); y += 0.6
    card(s, c2l, y, c2w, 0.52, [[("Оптимизатор эффективности", 13, INK, True)]], accent=ORANGE); y += 0.6
    card(s, c2l, y, c2w, 0.52, [[("Оркестратор кампаний", 13, INK, True)]], accent=ORANGE)
    # Col3 — заведение Manzana
    col_header(s, c3l, top, c3w, "ЗАВЕДЕНИЕ · MANZANA", PURPLE, sz=12)
    y = top + 0.54
    card(s, c3l, y, c3w, 0.58, [[("Агент завода акций", 12.5, INK, True)]], accent=PURPLE); y += 0.64
    darrow(s, c3l + c3w / 2 - 0.16, y, 0.32, 0.18, PURPLE); y += 0.24
    card(s, c3l, y, c3w, 0.58,
         [[("Manzana Processing", 12, PURPLE, True)], [("акции", 10.5, GREY, False)]], accent=PURPLE, ls=0.95); y += 0.72
    card(s, c3l, y, c3w, 0.58, [[("Агент завода коммуникаций", 12.5, INK, True)]], accent=PURPLE, ls=0.95); y += 0.64
    darrow(s, c3l + c3w / 2 - 0.16, y, 0.32, 0.18, PURPLE); y += 0.24
    card(s, c3l, y, c3w, 0.58,
         [[("Manzana Campaign", 12, PURPLE, True)], [("сообщения + slip-чеки", 10.5, GREY, False)]], accent=PURPLE, ls=0.95)
    # Col4 — запуск
    col_header(s, c4l, top, c4w, "ЗАПУСК", GREEN, sz=12)
    y = top + 0.54
    card(s, c4l, y, c4w, 0.56, [[("Модератор", 12.5, INK, True)], [("авто-проверка", 10.5, GREY, False)]], accent=GREEN, ls=0.95); y += 0.64
    darrow(s, c4l + c4w / 2 - 0.14, y, 0.28, 0.16, GREEN); y += 0.22
    card(s, c4l, y, c4w, 0.52, [[("Ревью человеком", 12.5, INK, True)]], accent=GREEN); y += 0.6
    darrow(s, c4l + c4w / 2 - 0.14, y, 0.28, 0.16, GREEN); y += 0.22
    card(s, c4l, y, c4w, 0.52, [[("Старт акции", 13, GREEN, True)]], accent=GREEN); y += 0.6
    card(s, c4l, y, c4w, 0.5, [[("Каналы: PUSH · slip → Клиент", 11, INK, False)]], accent=GREEN)
    box(s, 3.36, 6.2, 9.16, 0.36, fill=GREENBG, line=GREEN, lw=1)
    txt(s, 3.36, 6.2, 9.16, 0.36,
        [[("Обратная связь: факт отклика  →  Оптимизатор пересчитывает эффективность акций", 11, GREEN, True)]],
        align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)


def build_lifecycle(prs):
    s = add_slide(prs)
    header_block(s, "Слайд 4 · Жизненный цикл акции",
                 "Жизненный цикл акции: 9 этапов — от идеи до постанализа", 4)
    stages = [
        ("1", "Идея акции", "генерация акций-кандидатов", ORANGE),
        ("2", "Прогноз", "uplift и отклик по кандидатам", ORANGE),
        ("3", "Выбор предложения", "отбор лучших акций по прогнозу", ORANGE),
        ("4", "Механика + экономика", "механика, юнит-экономика, маржа", PURPLE),
        ("5", "Сегмент", "узкий подсегмент под акцию", PURPLE),
        ("6", "План / сетка", "календарь, частоты, анти-конфликт", PURPLE),
        ("7", "Заведение", "Manzana Processing + Campaign", GREEN),
        ("8", "Коммуникации и запуск", "тексты · ревью · старт акции", GREEN),
        ("9", "Постанализ", "факт vs прогноз → будущие прогнозы", INK),
    ]

    def stage_card(l, t, w, h, num, title, line, col):
        card(s, l, t, w, h,
             [[(num + "   ", 19, col, True), (title, 13, INK, True)], [(line, 11, GREY, False)]],
             accent=col, ls=1.05)

    cw, gap, ch = 2.2, 0.29, 1.75
    y1 = 2.0
    for i in range(5):
        l = 0.6 + i * (cw + gap)
        n, ti, ln, col = stages[i]
        stage_card(l, y1, cw, ch, n, ti, ln, col)
        if i < 4:
            arrow(s, l + cw + 0.02, y1 + ch / 2 - 0.2, 0.25, 0.4, GREY)
    arrow(s, 0.6 + 4 * (cw + gap) + cw / 2 - 0.2, y1 + ch + 0.05, 0.4, 0.32, GREY)
    total2 = 4 * cw + 3 * gap
    x0 = (SW - total2) / 2
    y2 = 4.35
    for j in range(4):
        i = 5 + j
        l = x0 + j * (cw + gap)
        n, ti, ln, col = stages[i]
        stage_card(l, y2, cw, ch, n, ti, ln, col)
        if j < 3:
            arrow(s, l + cw + 0.02, y2 + ch / 2 - 0.2, 0.25, 0.4, GREY)


def build_agents(prs):
    s = add_slide(prs)
    header_block(s, "Слайд 5 · Агенты",
                 "Команда агентов на весь цикл акции — от идеи до постанализа", 5)
    gx, gap = 0.6, 0.24
    cw = (12.13 - 2 * gap) / 3
    cols = [gx + i * (cw + gap) for i in range(3)]
    hy = 1.55
    col_header(s, cols[0], hy, cw, "СТРАТЕГИЯ И ПЛАНИРОВАНИЕ", ORANGE, h=0.5, sz=12)
    A = [
        ("Генератор идей акций", "оффер, повод, категория"),
        ("Архитектор механики", "механика, юнит-экономика, маржа"),
        ("Сегментатор", "узкие подсегменты под акцию"),
        ("Прогнозист", "uplift, отклик, прирост ТО"),
        ("Планировщик сетки", "календарь, анти-конфликт"),
    ]
    y = hy + 0.64
    for name, role in A:
        card(s, cols[0], y, cw, 0.84, [[(name, 14, INK, True)], [(role, 11, GREY, False)]], accent=ORANGE)
        y += 0.92
    col_header(s, cols[1], hy, cw, "ЗАВЕДЕНИЕ И КОММУНИКАЦИИ", PURPLE, h=0.5, sz=12)
    B = [
        ("Агент завода акций", "акции → Manzana Processing"),
        ("Агент завода коммуникаций", "сообщения + slip-чеки → Campaign"),
        ("Копирайт-агенты", "заголовок · текст · голос бренда"),
        ("Форматы и привязка", "deeplink · купон · e-mail"),
    ]
    y = hy + 0.64
    for name, role in B:
        card(s, cols[1], y, cw, 0.84, [[(name, 13.5, INK, True)], [(role, 11, GREY, False)]], accent=PURPLE, ls=1.0)
        y += 0.92
    col_header(s, cols[2], hy, cw, "УПРАВЛЕНИЕ И ОПТИМИЗАЦИЯ", GREEN, h=0.5, sz=12)
    C = [
        ("Оркестратор кампаний", "дирижирует всем конвейером"),
        ("Оптимизатор эффективности", "факт → переранжирование, дообучение"),
    ]
    y = hy + 0.64
    for name, role in C:
        card(s, cols[2], y, cw, 0.84, [[(name, 13.5, INK, True)], [(role, 11, GREY, False)]], accent=GREEN, ls=1.0)
        y += 0.92


def _cell(cell, text, sz, col, bold, fill=None, align=PP_ALIGN.LEFT):
    cell.fill.solid(); cell.fill.fore_color.rgb = fill if fill is not None else WHITE
    cell.margin_left = Inches(0.09); cell.margin_right = Inches(0.06)
    cell.margin_top = Inches(0.02); cell.margin_bottom = Inches(0.02)
    cell.vertical_anchor = MSO_ANCHOR.MIDDLE
    tf = cell.text_frame; tf.word_wrap = True
    p = tf.paragraphs[0]; p.alignment = align
    r = p.add_run(); r.text = text
    r.font.size = Pt(sz); r.font.color.rgb = col; r.font.bold = bold; r.font.name = FONT


def build_training(prs):
    s = add_slide(prs)
    header_block(s, "Слайд 6 · Агенты: приоритеты, обучение, контекст",
                 "P0 — конвейер для пилота; P1 — масштаб и оптимизация; P2 — расширение", 6)
    headers = ["Агент", "Приор.", "Как обучаем", "Контекст / данные"]
    data = [
        ("Генератор идей акций", "P0", "Промпт + примеры удачных акций", "Голос бренда, поводы, категории", ORANGE),
        ("Архитектор механики", "P0", "Правила + ML на марже и чеках", "Юнит-экономика, ассортимент, бюджеты", ORANGE),
        ("Сегментатор", "P0", "ML-кластеризация + правила", "MCI, чеки, история покупок", ORANGE),
        ("Прогнозист (uplift)", "P0", "ML на истории акций", "Историч. акции, отклик, продажи", ORANGE),
        ("Копирайт (заголовок, текст, голос)", "P0", "Промпт + примеры + правила бренда", "Голос бренда, лучшие тексты", ORANGE),
        ("Агент завода акций → Processing", "P0", "Промпт + схемы Manzana", "Manzana Processing, правила завода", ORANGE),
        ("Планировщик сетки", "P1", "Правила + оптимизация календаря", "Календарь, частоты, анти-конфликт", PURPLE),
        ("Агент завода коммуникаций → Campaign", "P1", "Промпт + схемы Manzana", "Manzana Campaign, шаблоны slip-чеков", PURPLE),
        ("Оптимизатор эффективности", "P1", "Переобучение по факту отклика", "Факт vs прогноз, A/B, продажи", PURPLE),
        ("Модерация / комплаенс", "P1", "Правила + классификаторы", "ФЗ-38/152, табу, лимиты скидки", PURPLE),
        ("Форматы (купон, e-mail, deeplink)", "P2", "Промпт + примеры", "Шаблоны форматов", GREY),
    ]
    nrows = len(data) + 1
    tl, tt, tw, th = 0.6, 1.55, 12.13, 4.7
    tbl = s.shapes.add_table(nrows, 4, Inches(tl), Inches(tt), Inches(tw), Inches(th)).table
    tbl.first_row = False; tbl.horz_banding = False
    tbl.columns[0].width = Inches(3.95)
    tbl.columns[1].width = Inches(0.98)
    tbl.columns[2].width = Inches(3.5)
    tbl.columns[3].width = Inches(3.7)
    for j, h in enumerate(headers):
        _cell(tbl.cell(0, j), h, 12, WHITE, True, fill=INK, align=PP_ALIGN.CENTER if j == 1 else PP_ALIGN.LEFT)
    pcol = {"P0": ORANGE, "P1": PURPLE, "P2": GREY}
    for i, (ag, pr, tr, ctx, col) in enumerate(data):
        r = i + 1
        bg = WHITE if i % 2 == 0 else LGREY
        _cell(tbl.cell(r, 0), ag, 11, INK, True, fill=bg)
        _cell(tbl.cell(r, 1), pr, 11, WHITE, True, fill=pcol[pr], align=PP_ALIGN.CENTER)
        _cell(tbl.cell(r, 2), tr, 10.5, GREY, False, fill=bg)
        _cell(tbl.cell(r, 3), ctx, 10.5, GREY, False, fill=bg)
    for r in range(nrows):
        tbl.rows[r].height = Inches(th / nrows)
    box(s, 0.6, 6.4, 12.13, 0.5, fill=GREENBG, line=GREEN, lw=1)
    txt(s, 0.85, 6.4, 11.7, 0.5,
        [[("Контекст:  ", 11, GREEN, True),
          ("база знаний (голос бренда · правила коммуникаций · механики · ассортимент) + данные сегментов "
           "и чеков; факт отклика дообучает прогнозиста и оптимизатор.", 11, INK, False)]],
        anchor=MSO_ANCHOR.MIDDLE, ls=1.0)


def build_moderation(prs):
    s = add_slide(prs)
    header_block(s, "Слайд 7 · Модерация и проверки",
                 "Каждая акция и текст проходят 7 гейтов — от безопасности до ревью человеком", 7)
    gates = [
        ("1   Гейт безопасности (до LLM)", GREY,
         ["Маскирование персональных данных", "Наружу — только обезличенные метаданные"]),
        ("2   Экономика акции", ORANGE,
         ["Юнит-экономика и маржа", "Бюджет и потолок скидки по категории", "Прогноз отсекает слабые акции"]),
        ("3   Анти-галлюцинации", ORANGE,
         ["Запрет выдуманных %, сроков, SKU, цен", "Ссылки — только реальные", "Нет данных → вопрос, не фантазия"]),
        ("4   Бренд и голос", ORANGE,
         ["Обращение «ты», лексика и запреты", "«минус N%», без слова «скидка»", "Заголовок не дублирует тело"]),
        ("5   Логика сетки", PURPLE,
         ["Антидубль механики в одну неделю", "Монеты — 1-я декада; частотные — Активные",
          "Сегмент = реальная аудитория категории"]),
        ("6   Комплаенс и право", PURPLE,
         ["ФЗ-38 «О рекламе», 152-ФЗ о перс. данных", "Фильтр токсичности", "Длина и санитайз полей"]),
        ("7   Ревью человеком + аудит", GREEN,
         ["Обязательное финальное ревью человеком", "Аудит-лог: кто, когда, что утвердил",
          "Версионирование правил"]),
    ]
    colx = [0.6, 6.97]; cwd = 5.76
    ys = [1.6, 1.6]
    for i, (title, col, items) in enumerate(gates):
        c = 0 if i < 4 else 1
        h = 0.44 + 0.3 * len(items) + 0.12
        l = colx[c]; t = ys[c]
        box(s, l, t, cwd, h, fill=WHITE, line=BORD)
        box(s, l, t, 0.08, h, fill=col, line=None, rounded=False)
        txt(s, l + 0.24, t + 0.08, cwd - 0.32, 0.32, [[(title, 13, INK, True)]])
        para = [[("•  " + it, 11.5, GREY, False)] for it in items]
        txt(s, l + 0.24, t + 0.46, cwd - 0.36, h - 0.5, para, ls=1.0, space_after=2)
        ys[c] += h + 0.16


def build_plan(prs):
    s = add_slide(prs)
    header_block(s, "Слайд 8 · План по результату",
                 "От закрытого пилота до +1,5 млрд ₽/год — через сегментацию всей базы", 8)
    phases = [
        ("ЭТАП 1", "Закрытый пилот\nна одном сегменте", ORANGE,
         "Claude на сервере ДИКСИ, без выгрузки наружу: коннекторы генерации и скрипты к базе",
         ["«Активные» → категорийные подсегменты", "Первые акции: +20 ₽/клиента vs +3 ₽",
          "Потенциал +600 млн ₽/год"]),
        ("ЭТАП 2", "Вся база +\nполная архитектура", PURPLE,
         "Разворачиваем мультиагентную промо-фабрику и интеграцию с Manzana",
         ["Сегментация всей push-базы (2,6 млн)", "Поток акций на каждый подсегмент",
          "Результат: +600 млн ₽/год"]),
        ("ЭТАП 3", "Оптимизация и\nотбор лучших акций", GREEN,
         "Оптимизатор считает эффективность по факту и оставляет сильнейшие",
         ["Доля «в плюсе» 58% → 68% → выше", "Бюджет — в эффективные акции",
          "Результат: рост маржи и отдачи"]),
        ("ЭТАП 4", "Глубокое деление\nи масштаб", INK,
         "Работа с каждой узкой аудиторией, автономный конвейер",
         ["До +50 ₽/клиента", "~200 акций/мес, самообучение",
          "Результат: до +1,5 млрд ₽/год"]),
    ]
    n = len(phases); gap = 0.24
    pw = (12.13 - (n - 1) * gap) / n
    y = 1.7
    box(s, 0.6, y, 12.13, 0.07, fill=ORANGE, line=None, rounded=False)
    for i, (ph, name, col, how, items) in enumerate(phases):
        l = 0.6 + i * (pw + gap)
        dot = s.shapes.add_shape(MSO_SHAPE.OVAL, Inches(l + pw / 2 - 0.09), Inches(y - 0.08), Inches(0.22), Inches(0.22))
        dot.fill.solid(); dot.fill.fore_color.rgb = col
        dot.line.color.rgb = WHITE; dot.line.width = Pt(1.5); dot.shadow.inherit = False
        ct = y + 0.34
        box(s, l, ct, pw, 4.5, fill=WHITE, line=BORD)
        box(s, l, ct, pw, 0.92, fill=col, line=None)
        txt(s, l + 0.05, ct + 0.08, pw - 0.1, 0.32, [[(ph, 13, WHITE, True)]], align=PP_ALIGN.CENTER)
        txt(s, l + 0.05, ct + 0.38, pw - 0.1, 0.52, [[(name, 12, WHITE, True)]], align=PP_ALIGN.CENTER, ls=0.95)
        box(s, l + 0.12, ct + 1.04, pw - 0.24, 1.06, fill=LGREY, line=None)
        txt(s, l + 0.24, ct + 1.1, pw - 0.46, 0.96, [[(how, 11, INK, False)]], ls=1.05)
        txt(s, l + 0.22, ct + 2.22, pw - 0.36, 0.3, [[("РЕЗУЛЬТАТ", 11, col, True)]])
        para = [[("•  " + it, 11.5, GREY, False)] for it in items]
        txt(s, l + 0.22, ct + 2.54, pw - 0.36, 1.8, para, ls=1.05, space_after=5)
    txt(s, 0.6, 6.95, 12, 0.3,
        [[("Цифры эффекта — слайд «Эффективность сегментных акций», CVM окт.2025–май.2026.", 9.5, GREY, False)]])


BUILDERS = [build_cover, build_case, build_overview, build_arch, build_lifecycle, build_agents,
            build_training, build_moderation, build_plan]


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "preview":
        import os, subprocess
        os.makedirs("/tmp/deckprev", exist_ok=True)
        for i, b in enumerate(BUILDERS):
            p = new_prs(); b(p)
            fp = f"/tmp/deckprev/s{i}.pptx"; p.save(fp)
            subprocess.run(["qlmanage", "-t", "-s", "1700", "-o", "/tmp/deckprev", fp], capture_output=True)
        print("preview done")
        return
    prs = new_prs()
    for b in BUILDERS:
        b(prs)
    out = ("/Users/elenakoryakova/new projects/cvm_push_generation/output/"
           "CVM_Push_Generator_внедрение_ДИКСИ.pptx")
    prs.save(out); print("saved:", out)


if __name__ == "__main__":
    main()
