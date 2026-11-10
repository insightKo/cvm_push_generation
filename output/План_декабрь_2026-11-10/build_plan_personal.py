"""Персональный план CVM на декабрь 2026 по дням → План_CVM_декабрь_2026_персональный.xlsx.

Цель Елены — 600 млн Доп ТО в мере ГКГ («1б», 10.11.2026). Персонально: группы кластеров (формат 01.11), серии подарков монет
по сегментам ЖЦ, сообщение каждому клиенту каждый день, SEO-пики декабря 2025, психолог.

Входы (все в этой папке):
  треки_декабрь_кластеры.csv, сообщения_по_дням.csv, разбор_маркетолога_v2.txt — маркетолог и психолог (10.11);
  коэффициенты_v2.csv, разбор_аналитика_v2.txt — аналитик (факты окт-25 – окт-26, мост к ГКГ);
  аудитории_кластеры_результат.jsonl — численности 10.11 (запрос аудитории_кластеры_запрос.sql, ревью sql-senior), кластеры на 01.11;
  гкг/cl_score.jsonl — I_CLUSTER_CATEGORY_SCORE (окно 21.09–01.11); гкг/cat_price2.jsonl — типичная цена категории CVM
  (снимок «Товары» 08.11, PRICE2W, медиана по чекам); гкг/seo_декабрь2025_по_темам.json, гкг/seo_психолог_comments.json;
  гкг/gcg_read2.jsonl — I_GCG_REPORT.
Мост к ГКГ (аналитик): ГКГ_дек = 82,8 (± 30,5) + Σ (Доп ТО итого − скидка ЦГ) акций; ошибка месяца ± 112 млн.
"""
import collections
import csv
import datetime as dt
import json
import math
import re
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

D = Path(__file__).resolve().parent
OUT = D / 'План_CVM_декабрь_2026_персональный.xlsx'
BASE, BASE_SE, MONTH_SE = 82.8, 30.5, 112.0
WD = ['пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс']
DAYS = [dt.date(2026, 12, d) for d in range(1, 32)]
WEEKS = [('01–06.12 (вт–вс)', dt.date(2026, 12, 1), dt.date(2026, 12, 6)), ('08–13.12 (вт–вс)', dt.date(2026, 12, 8), dt.date(2026, 12, 13)),
         ('15–20.12 (вт–вс)', dt.date(2026, 12, 15), dt.date(2026, 12, 20)), ('22–27.12 (вт–вс)', dt.date(2026, 12, 22), dt.date(2026, 12, 27)),
         ('28–30.12 (пн–ср)', dt.date(2026, 12, 28), dt.date(2026, 12, 30))]
CL_NAMES = {1: 'Перекусы на ходу', 2: 'Редкие', 3: 'Частые с большой корзиной', 4: 'Регулярные за продуктами', 5: 'Большая закупка продуктов',
            6: 'Много нон-фуда', 7: 'Большая закупка с нон-фудом', 8: 'За хлебом и водой', 9: 'Ультра-частые', 10: 'Вино, крепкое, кофе'}
SEG = {1: 'Новые', 2: 'Активные', 3: 'Активные LFL', 4: 'Случайные', 5: 'Отток', 6: 'Спящие'}
KG = {1: .05, 2: .05, 3: .05, 4: .10, 5: .10, 6: .10}


def rcsv(name):
    return list(csv.DictReader(open(D / name, encoding='utf-8-sig'), delimiter=';'))


def num(s):
    try:
        return float(str(s).replace(',', '.'))
    except ValueError:
        return None


# ---------- аудитории 10.11 ----------
AUD = [json.loads(x) for x in open(D / 'аудитории_кластеры_результат.jsonl', encoding='utf-8')]
CL_DATE = AUD[0]['CL_DATE']


def aud(seg=None, cl=None, push=None, rare=None):
    n = 0
    for r in AUD:
        if r['GCG'] == 1 or r['FRAUD'] == 1 or r['ID_SALE_CHANNEL'] not in (1, 2):
            continue
        if seg is not None and r['SEGMENT'] not in seg:
            continue
        if cl is not None and r['CL'] not in cl:
            continue
        if push is not None and r['PUSH'] != push:
            continue
        if rare is not None and r['RARE'] != rare:
            continue
        n += r['CLIENTS']
    return n


# ---------- коэффициенты аналитика ----------
COEF = rcsv('коэффициенты_v2.csv')


def coef(fam, seg, cell):
    for r in COEF:
        if r['семейство'].startswith(fam) and r['сегмент / список'] == seg and r['ячейка'].startswith(cell):
            return num(r['₽ на клиента (прогнозный, после сжатия)']), num(r['скидка ₽ на клиента']), r['ID аналогов'], r['годится в прогноз (да/нет/осторожно)']
    raise SystemExit(f'нет коэффициента: {fam} / {seg} / {cell}')


C_SERIES = coef('а)', 'Случайные (все Случайные офлайна)', 'серия на месяц')                       # 101426: 121,39 / 21,91
C_ACT_WAVE = coef('а)', 'Активные', 'Активные | с «Активируй')                                    # 3,40 / 0,82 за волну
C_OUT_WAVE = coef('а)', 'Отток+Спящие (список)', 'Отток+Спящие (список) | с «Активируй/Акцептны')  # 5,32 / 1,71 за волну
C_CAT = {k: coef('б)', 'кластеры (101445–101474) и Акт', k) if False else None for k in ()}
CAT = {}
for r in COEF:
    if r['семейство'].startswith('б)') and r['ячейка'] in ('сладкое', 'напитки', 'нон-фуд', 'еда', 'все типы категорий'):
        CAT[r['ячейка']] = (num(r['₽ на клиента (прогнозный, после сжатия)']), num(r['скидка ₽ на клиента']), r['ID аналогов'])
C_THR = None
for r in COEF:
    if r['семейство'].startswith('г)') and r['сегмент / список'] == 'Активные' and r['ячейка'].startswith('N р. с порогом чека'):
        C_THR = (num(r['₽ на клиента (прогнозный, после сжатия)']), num(r['скидка ₽ на клиента']), r['ID аналогов'])
SLEEP_MONTH = 2.4    # аналитик v2: Спящие — около 2,4 ₽ на клиента за месяц (101062: 0,6 ± 1,5 за волну)
NEW_WAVE = 4.4       # аналитик v1/v2: Новые — слой в списках Активных, 4,4 ₽ за волну

# ---------- цены категорий ----------
PRICE = {}
for x in open(D / 'гкг' / 'cat_price2.jsonl', encoding='utf-8'):
    j = json.loads(x)
    if 'name' in j and j['name']:
        PRICE[j['name'].lower()] = (j['median_price'], j['checks_week'], j['name'])


def typical_price(text, by_checks=False):
    """Категории CVM, названные в тексте; цена самой покупаемой из них."""
    t = text.lower()
    # название категории CVM целыми словами; при нескольких — самое длинное название, потом самое покупаемое
    hits = [(len(k), v) for k, v in PRICE.items() if re.search(r'(?<![а-яё])' + re.escape(k) + r'(?![а-яё])', t)]
    if not hits:   # совпадение по основам слов (первые 5 букв каждого слова длиной от 5): больше совпавших основ — выше
        stems = {w[:5] for w in re.findall(r'[а-яё]+', t) if len(w) >= 5}
        for k, v in PRICE.items():
            ks = [w[:5] for w in re.findall(r'[а-яё]+', k) if len(w) >= 5]
            if ks and all(s in stems for s in ks):
                hits.append((len(ks), v))
    if not hits:
        return None, ''
    best = max(hits, key=lambda h: (h[1][1],) if by_checks else (h[0], h[1][1]))[1]
    return best[0], best[2]


SWEET = ('шокол', 'конфет', 'печен', 'торт', 'батонч', 'пряник', 'вафл', 'мармел', 'сладк', 'мороже', 'десерт', 'зефир', 'пастил', 'кекс', 'рулет')
DRINK = ('газиров', 'кола', 'лимонад', 'вода', 'сок', 'кофе', 'чай', 'пиво', 'вино', 'игрист', 'шампан', 'водк', 'коньяк', 'напит', 'крепк')
NONFOOD = ('бумаг', 'гигиен', 'стирк', 'уборк', 'корм', 'кошк', 'шампун', 'мыл', 'зубн', 'подгуз', 'салфет', 'посуд', 'наполн', 'дезодор', 'бритв', 'прокладк')


def cat_type(text):
    t = text.lower()
    for name, keys in (('нон-фуд', NONFOOD), ('сладкое', SWEET), ('напитки', DRINK)):
        if any(k in t for k in keys):
            return name
    return 'еда'


# ---------- группы кластеров: размер — доля группы в кластере по числу покупателей её категорий ----------
TRK = rcsv('треки_декабрь_кластеры.csv')
grp_buyers = {}
for r in TRK:
    k = (r['кластер'], r['группа'])
    if k in grp_buyers:
        continue
    b = [int(x.replace('\xa0', '').replace(' ', '')) for x in re.findall(r'([\d  \xa0]+) покупател', r['как найдена (категории, индекс, CONTACTS)'])]
    grp_buyers[k] = max(b) if b else 0
cl_push = {cl: aud(seg=(2, 3), cl=(cl,), push=1) for cl in CL_NAMES}
grp_size = {}
for (cl_s, g), b in grp_buyers.items():
    cl = int(cl_s.split()[0])
    tot = sum(v for (c2, _), v in grp_buyers.items() if c2 == cl_s)
    grp_size[(cl_s, g)] = round(cl_push[cl] * b / tot) if tot else 0

# средний чек Активных и LFL кластера (6 недель) — для порога «чек от X»
avg_chk = {}
for cl in CL_NAMES:
    n = s = 0
    for r in AUD:
        if r['CL'] == cl and r['SEGMENT'] in (2, 3) and r['GCG'] == 0 and r['SUM_AVG_CHECK']:
            n += r['CLIENTS']; s += r['SUM_AVG_CHECK']
    avg_chk[cl] = s / n if n else None

# ---------- строки треков с монетами и прогнозом ----------
track_rows = []
for r in TRK:
    cl = int(r['кластер'].split()[0])
    mech = r['механика']
    what = r['категория / что именно']
    coins_note = r['монет']
    price, pcat = typical_price(what + ' ' + (r['как найдена (категории, индекс, CONTACTS)'] if 'Каскад' in mech else ''), by_checks='Каскад' in mech)
    n_cg = round(grp_size[(r['кластер'], r['группа'])] * (1 - KG[2]))
    if mech.startswith('«Коммуникация»'):
        coins, c_eff, c_disc, src, ctype = '—', 0.0, 0.0, 'без начислений — эффект не считается', ''
    elif mech.startswith('«Кешбэк X монет за покупку»'):
        x = math.ceil(avg_chk[cl] * 1.5 / 100) * 100
        coins = f'100 монет с чека от {x} р.'
        c_eff, c_disc, src = C_THR[0], C_THR[1], f'«N р. с порогом чека», Активные: {C_THR[2]}'
        ctype = f'порог = 1,5 среднего чека Активных кластера ({avg_chk[cl]:.0f} ₽), как 101145 (дек-25: 100 монет с чека от 1200 р.)'
    else:
        explicit = re.match(r'^\s*(50|100)(?=\s*\(|\s*$)', coins_note)
        if explicit:
            coins = int(explicit.group(1))
            why = coins_note
        elif price is not None:
            coins = 100 if price >= 100 else 50
            why = f'типичная цена «{pcat}» {price:.0f} ₽ (PRICE2W, медиана по чекам) → {coins}'
            if coins == 50 and any(k in pcat.lower() for k in ('шоколад', 'батонч')):
                coins = 100
                why += '; шоколад и батончики — 100 (решение Елены 01.11, как 101445)'
        else:
            coins = 100
            why = 'категория не найдена в справочнике CVM по названию — 100 по ноябрьскому аналогу; проверить при сборке'
        ctype = cat_type(what)
        if mech.startswith('«Каскад'):
            c_eff, c_disc, src = CAT['все типы категорий'][0], CAT['все типы категорий'][1], f'семейство «Вернём N р. за категорию» (каскада в фактах нет): {CAT["все типы категорий"][2][:60]}'
            coins = f'{coins} за вторую покупку за неделю'
        else:
            c_eff, c_disc, src = CAT[ctype][0], CAT[ctype][1], f'«Вернём N р.», {ctype}: {CAT[ctype][2]}'
        ctype = f'{ctype}; {why}'
    track_rows.append(dict(r, coins=coins, ctype=ctype, n_cg=n_cg, c_eff=c_eff, c_disc=c_disc, src=src,
                           dop=n_cg * c_eff / 1e6, disc=n_cg * c_disc / 1e6))

# ---------- серии подарков по сегментам ЖЦ ----------
n_rand = aud(seg=(4,)); n_rare = aud(seg=(2, 3), rare=1); n_out = aud(seg=(5,)); n_sleep = aud(seg=(6,)); n_new = aud(seg=(1,))
C_OUT_GIFT = coef('а)', 'Отток+Спящие (список)', 'Отток+Спящие (список) | без «Активируй» в наз')   # первая такая строка: 4,68 / 3,07
for r in COEF:   # «Дарим 100 монет» Отток без активации, 4 дня: 101266, 101060 — 3,15 / 1,27 за волну
    if r['семейство'].startswith('а)') and r['ID аналогов'] == '101266, 101060':
        C_OUT_GIFT = (num(r['₽ на клиента (прогнозный, после сжатия)']), num(r['скидка ₽ на клиента']), r['ID аналогов'], r['годится в прогноз (да/нет/осторожно)'])
C_ACT_GIFT = coef('а)', 'Активные', 'Активные | без «Активируй» в названии | 50 мо')   # 101131: 4,45 / 9,97 за волну
W = C_SERIES[0] / 4, C_SERIES[1] / 4    # 101426 на одну неделю (линейно по неделям — допущение)
# редкие Активные: 01–13.12 половина — формат 101426, половина — «Активируй»; с 14.12 всем — победитель
rare_a = (0.5 * 2 * C_ACT_GIFT[0] + 0.5 * 2 * C_ACT_WAVE[0] + 2 * C_ACT_WAVE[0], 0.5 * 2 * C_ACT_GIFT[1] + 0.5 * 2 * C_ACT_WAVE[1] + 2 * C_ACT_WAVE[1])
rare_b = (0.5 * 2 * W[0] + 0.5 * 2 * C_ACT_WAVE[0] + 2 * W[0], 0.5 * 2 * W[1] + 0.5 * 2 * C_ACT_WAVE[1] + 2 * W[1])
SERIES = [
    dict(seg='Случайные (все, с пушем и без)', n=n_rand, kg=.10,
         mech='«Дарим 100 монет на неделю» без активации: зачисление вт 01.12, пн 07, 14, 21.12 (монеты недели сгорают в вс), пн 28.12 — сгорают 30.12',
         a=(C_SERIES[0], C_SERIES[1], 'повтор 101426 (окт-26, Случайные, +217 млн, очень сильно значимо)'),
         b=(C_SERIES[0], C_SERIES[1], 'то же')),
    dict(seg='Активные и LFL «редкие» (≤ 4 визитов за 6 нед. или 21 день без покупок)', n=n_rare, kg=.05,
         mech='01–13.12: половина случайно — формат 101426 (100 монет на неделю без активации), половина — «Активируй 100 монет» на неделю; 11.12 — сравнение по Доп ТО − скидка; с 14.12 всем — победитель',
         a=(rare_a[0], rare_a[1], f'101426 не повторился: половина 01–13.12 — подарок без активации как 101131 ({C_ACT_GIFT[0]:.2f} ₽, скидка {C_ACT_GIFT[1]:.2f} за волну), остальное — «Активируй» ({C_ACT_WAVE[0]:.2f} / {C_ACT_WAVE[1]:.2f}: {C_ACT_WAVE[2]})'),
         b=(rare_b[0], rare_b[1], '101426 повторился: формат 101426 — половина 01–13.12 и все с 14.12 (по ¼ месячного эффекта 101426 на неделю — допущение)')),
    dict(seg='Отток', n=n_out, kg=.10,
         mech='«Дарим 100 монет на неделю» без активации (формат 101426): вт 01.12, пн 07, 14, 21.12; 11.12 — проверка Доп ТО − скидка; если ≤ 0 — с 14.12 «Активируй 100 монет»',
         a=(4 * C_OUT_GIFT[0], 4 * C_OUT_GIFT[1], f'101426 не повторился: 4 волны «Дарим 100 монет», Отток ({C_OUT_GIFT[2]})'),
         b=(C_SERIES[0], C_SERIES[1], '101426 повторился на Оттоке')),
    dict(seg='Спящие', n=n_sleep, kg=.10,
         mech='«Дарим 200 монет на неделю» — продолжение 101556 ноября: вт 01.12, пн 07, 14, 21.12',
         a=(SLEEP_MONTH, 4 * 0.4, 'аналитик: Спящие ≈ 2,4 ₽ за месяц (101062: 0,6 ± 1,5 за волну)'),
         b=(SLEEP_MONTH, 4 * 0.4, 'то же — переносить 101426 на Спящих нечем')),
    dict(seg='Новые', n=n_new, kg=.05,
         mech='«Активируй 50 монет» на неделю: вт 01.12, пн 07, 14, 21.12 (+ трек своей группы кластера)',
         a=(4 * NEW_WAVE, 4 * C_ACT_WAVE[1], 'Новые — слой в списках Активных: 4,4 ₽ за волну (аналитик)'),
         b=(4 * NEW_WAVE, 4 * C_ACT_WAVE[1], 'то же')),
]
for s in SERIES:
    s['n_cg'] = round(s['n'] * (1 - s['kg']))
    for k in ('a', 'b'):
        e, dsc, _ = s[k]
        s[k + '_dop'] = s['n_cg'] * e / 1e6
        s[k + '_disc'] = s['n_cg'] * dsc / 1e6

# ---------- SEO: темы-пики по неделям (контент за 7–10 дней до пика) ----------
SEO = json.load(open(D / 'гкг' / 'seo_декабрь2025_по_темам.json', encoding='utf-8'))
SKIP = ('энергетик', 'похмел', 'незамерз', 'едадил', 'экономия', 'сравнить', 'первой цены', 'по акции', 'акции в', 'скидка по', 'что купить',
        'аксессуар', 'атрибут', 'упаковк', 'ватные', 'косметик', 'новый год', 'рецепты', 'салат', 'лёд', 'личи', 'авокадо', 'фольга')
seo_food = [x for x in SEO if x['dec_peak_index'] >= 1.6 and x['avg_week_2025'] >= 3000 and not any(k in x['theme'] for k in SKIP)]


def seo_for(week_start):
    """Темы с пиком через 6–13 дней после старта недели (окно «за 7–10 дней до пика»)."""
    out = []
    for x in seo_food:
        pk = dt.date.fromisoformat(x['dec_peak_week'])
        if 6 <= (pk - week_start).days <= (20 if week_start.day == 8 else 13) or (week_start == dt.date(2025, 12, 1) and x['dec_peak_week'] == '2025-12-01'):
            out.append(x)
    out.sort(key=lambda x: -x['dec_peak_index'])
    return out[:6]


SEO_WEEK = {}
for wname, ws, we in WEEKS:
    d25 = dt.date(2025, 12, ws.day)
    mon25 = d25 - dt.timedelta(days=d25.weekday())          # неделя декабря 2025 с понедельника на ту же дату
    if ws.day == 28:                                          # 28–30.12: пик 29.12 идёт в эти дни
        SEO_WEEK[wname] = sorted([x for x in seo_food if x['dec_peak_week'] == '2025-12-29'], key=lambda x: -x['dec_peak_index'])[:6]
    else:
        SEO_WEEK[wname] = seo_for(mon25)


def seo_txt(wname):
    return '; '.join(f"{x['theme']} ×{x['dec_peak_index']:.1f} (пик {x['dec_peak_week'][8:10]}.12)" for x in SEO_WEEK[wname]) or '—'


def week_of(d):
    for wname, ws, we in WEEKS:
        if ws <= d <= we:
            return wname
    if d == dt.date(2026, 12, 7):
        return WEEKS[1][0]
    if d in (dt.date(2026, 12, 14), dt.date(2026, 12, 21)):
        return WEEKS[2][0] if d.day == 14 else WEEKS[3][0]
    return None


# ---------- сообщения по дням ----------
MSG = rcsv('сообщения_по_дням.csv')
msg_by = collections.defaultdict(dict)
for r in MSG:
    msg_by[r['дата']][r['тип группы'].split(' (')[0]] = r


def series_msg(seg_kind, d):
    """Ежедневное сообщение серии подарков (одно в день, 11:00 или 18:00)."""
    w = WD[d.weekday()]
    wn = week_of(d)
    if d.day == 31:
        return '11:00', 'Поздравление', 'С Новым годом — без предложения (праздник)'
    if d.day in (29, 30):
        return '11:00' if d.day == 29 else '18:00', 'Сгорание монет', 'Монеты сгорят 30.12 в 23:59 — потратьте к праздничному столу'
    topics = SEO_WEEK.get(wn) or []
    topic = topics[(d.day) % len(topics)]['theme'] if topics else 'праздничный стол'
    if w == 'пн' or d.day == 1:
        return '11:00', 'Зачисление', f'{seg_kind}: монеты недели уже на карте — до вс'
    if w in ('вт', 'чт'):
        return '18:00', 'Рецепт и повод (SEO)', f'«{topic}» — что купить к нему, монетами недели'
    if w == 'ср':
        return '11:00', 'Напоминание', 'Монеты ждут — на любые покупки до вс'
    if w == 'пт':
        return '18:00', 'Выходные (SEO)', f'К выходным: «{topic}» — монеты недели в счёт покупки'
    if w == 'сб':
        return '11:00', 'Баланс', 'Сколько монет осталось — сгорят завтра в 23:59'
    return '11:00', 'Последний день', 'Сегодня последний день монет этой недели; завтра — новые'


daily = []
for d in DAYS:
    wn = week_of(d)
    key = d.strftime('%d.%m.%Y')
    mrow = msg_by.get(key, {})
    cg = mrow.get('Кластерная группа')
    for (cl_s, g), size in grp_size.items():
        tr = next((t for t in track_rows if t['кластер'] == cl_s and t['группа'] == g and t['неделя'] == wn), None)
        offer = f"{tr['механика'].split('»')[0]}» · {tr['категория / что именно']} · {tr['coins']}" if tr else '—'
        m1 = m2 = ''
        if cg:
            th1 = cg['утро — тема']
            if tr and WD[d.weekday()] == 'вт' and tr['пуш вт (тема)']:
                th1 = tr['пуш вт (тема)']
            if tr and WD[d.weekday()] == 'чт' and tr['пуш чт (тема)']:
                th1 = tr['пуш чт (тема)']
            m1 = f"{cg['утро — время']} {cg['утро — сообщение']}: {th1}" if cg['утро — сообщение'] else ''
            m2 = f"{cg['вечер — время']} {cg['вечер — сообщение']}: {cg['вечер — тема']}" if cg['вечер — сообщение'] else ''
        daily.append([key, WD[d.weekday()], f'{cl_s} · {g}', 'Активные и LFL кластера, с пушем', size, offer, m1, m2])
    for sname, kind in (('Случайные', 'Случайные'), ('Активные и LFL «редкие»', 'редкие'), ('Отток', 'Отток'), ('Спящие', 'Спящие'), ('Новые', 'Новые')):
        s = next(x for x in SERIES if x['seg'].startswith(sname))
        t, typ, theme = series_msg(sname, d)
        extra = ''
        if sname == 'Новые' and WD[d.weekday()] == 'ср':
            extra = '11:00 ДЦО коммерции (общий пуш Активным и Новым)'
        note = 'монеты серии + трек своей группы кластера (≤ 2 пушей в день)' if sname in ('Активные и LFL «редкие»', 'Новые') else 'серия подарков'
        daily.append([key, WD[d.weekday()], f'Серия: {s["seg"]}', note, s['n'], s['mech'], f'{t} {typ}: {theme}', extra])

# ---------- итоги ----------
tr_dop = sum(t['dop'] for t in track_rows); tr_disc = sum(t['disc'] for t in track_rows)
A_dop = sum(s['a_dop'] for s in SERIES) + tr_dop; A_disc = sum(s['a_disc'] for s in SERIES) + tr_disc
B_dop = sum(s['b_dop'] for s in SERIES) + tr_dop; B_disc = sum(s['b_disc'] for s in SERIES) + tr_disc
A_gcg = BASE + A_dop - A_disc; B_gcg = BASE + B_dop - B_disc


def pnorm(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


pA = 1 - pnorm((600 - A_gcg) / MONTH_SE); pB = 1 - pnorm((600 - B_gcg) / MONTH_SE)

# ---------- книга ----------
wb = openpyxl.Workbook()
H = Font(bold=True); WRAP = Alignment(wrap_text=True, vertical='top'); FILL = PatternFill('solid', fgColor='FFF2E5')


def sheet(title, head, rows, widths):
    ws = wb.create_sheet(title)
    ws.append(head)
    for c in ws[1]:
        c.font = H; c.alignment = WRAP; c.fill = FILL
    for r in rows:
        ws.append(r)
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.alignment = WRAP
    ws.freeze_panes = 'A2'
    return ws


ws = wb.active; ws.title = 'Итог'
rows = [
    ['Цель', '600 млн Доп ТО в мере ГКГ (общий эффект CVM к глобальной контрольной группе, Активные + Отток)'],
    ['Как считается ГКГ декабря', f'База программы {BASE} млн (± {BASE_SE}) + Σ по акциям (Доп ТО − скидка) — аналитик: июль–октябрь, одна ГКГ (нарезка 01.07); ошибка месяца ± {MONTH_SE:.0f} млн, из них ± 108 — ошибка самой ГКГ'],
    ['ГКГ по месяцам, млн', 'мар 212 · апр 255 · май 163 · июн 296 · июл 77 · авг 130 · сен 212 · окт 143 · ноя по 06.11 — 58 за 6 дней'],
    [],
    ['', 'Сценарий А: факты как есть', 'Сценарий Б: эффект 101426 повторяется на редких Активных и Оттоке'],
    ['Серии подарков: Доп ТО, млн', round(sum(s['a_dop'] for s in SERIES), 1), round(sum(s['b_dop'] for s in SERIES), 1)],
    ['Серии подарков: скидка, млн', round(sum(s['a_disc'] for s in SERIES), 1), round(sum(s['b_disc'] for s in SERIES), 1)],
    ['Треки 30 групп кластеров: Доп ТО, млн', round(tr_dop, 1), round(tr_dop, 1)],
    ['Треки 30 групп кластеров: скидка, млн', round(tr_disc, 1), round(tr_disc, 1)],
    ['Σ (Доп ТО − скидка), млн', round(A_dop - A_disc, 1), round(B_dop - B_disc, 1)],
    ['PL, млн: (Доп ТО × 30 % − скидка) / 1,15', round((A_dop * .3 - A_disc) / 1.15, 1), round((B_dop * .3 - B_disc) / 1.15, 1)],
    ['ГКГ декабря, млн', round(A_gcg, 0), round(B_gcg, 0)],
    ['Интервал 95 %, млн', f'{A_gcg - 1.96 * MONTH_SE:.0f} … {A_gcg + 1.96 * MONTH_SE:.0f}', f'{B_gcg - 1.96 * MONTH_SE:.0f} … {B_gcg + 1.96 * MONTH_SE:.0f}'],
    ['Вероятность ≥ 600', f'{pA * 100:.0f} %', f'{pB * 100:.0f} %'],
    [],
    ['Точки решения', ''],
    ['03–04.12', 'Факт ноября: 101550 (редкие Активные, 100 р. на неделю) и 101556 (Спящие, 200 на неделю) — предварительный эффект после финиша 29.11'],
    ['11.12', 'Предварительный эффект (Доп ТО − скидка): редкие Активные — формат 101426 против «Активируй» по половинам; Отток — серия 101426 к своей КГ'],
    ['14.12', 'Редким Активным — формат-победитель; Оттоку — если серия ≤ 0, переход на «Активируй 100 монет»; Случайным — без изменений (факт 101426)'],
    [],
    ['Что в плане по дням', 'лист «Каждый день»: 31 день × 30 групп кластеров + 5 серий; у каждой группы на каждый день — действующая акция и 1–2 сообщения'],
    ['Не входит в прогноз', 'ДЦО по средам (скидки коммерции), онлайн; сообщения без выгоды отдельно не оцениваются — у тематических рассылок без выгоды факт от −39 до +12 млн (101397–101403)'],
]
for r in rows:
    ws.append(r)
ws.column_dimensions['A'].width = 42; ws.column_dimensions['B'].width = 70; ws.column_dimensions['C'].width = 62
for row in ws.iter_rows():
    for c in row:
        c.alignment = WRAP
for r in (1, 5, 15):
    for c in ws[r]:
        c.font = H

sheet('Каждый день', ['Дата', 'День', 'Кому (группа / серия)', 'Аудитория', 'Клиентов (с пушем у групп; серии — все без ГКГ)',
                      'Что действует у клиента', 'Сообщение 1', 'Сообщение 2'], daily, [11, 5, 46, 26, 14, 60, 70, 46])

sheet('Треки групп кластеров',
      ['Кластер', 'Группа', 'Как найдена', 'Неделя', 'Шаг трека', 'Механика', 'Что именно', 'Монет', 'Тип категории; почему такая сумма',
       'Почему (факт / SEO / психолог)', 'Пуш вт', 'Пуш чт', 'Клиентов ЦГ (оценка)', '₽ на клиента', 'Скидка ₽ на клиента', 'Аналоги',
       'Доп ТО, млн', 'Скидка, млн'],
      [[t['кластер'], t['группа'], t['как найдена (категории, индекс, CONTACTS)'], t['неделя'], t['шаг трека'], t['механика'], t['категория / что именно'],
        t['coins'], t['ctype'], t['почему (факт прошлой акции / SEO-пик / психолог)'], t['пуш вт (тема)'], t['пуш чт (тема)'], t['n_cg'],
        t['c_eff'], t['c_disc'], t['src'], round(t['dop'], 2), round(t['disc'], 2)] for t in track_rows],
      [20, 22, 40, 14, 16, 30, 30, 18, 34, 46, 34, 34, 12, 9, 9, 34, 9, 9])

sheet('Серии подарков', ['Сегмент', 'Клиентов (без ГКГ и фрода)', 'КГ', 'Клиентов ЦГ', 'Механика и даты', 'А: ₽ на клиента за месяц', 'А: скидка ₽', 'А: источник',
                         'А: Доп ТО, млн', 'А: скидка, млн', 'Б: ₽ на клиента', 'Б: скидка ₽', 'Б: условие', 'Б: Доп ТО, млн', 'Б: скидка, млн'],
      [[s['seg'], s['n'], f"{s['kg'] * 100:.0f} %", s['n_cg'], s['mech'], round(s['a'][0], 2), round(s['a'][1], 2), s['a'][2], round(s['a_dop'], 1),
        round(s['a_disc'], 1), round(s['b'][0], 2), round(s['b'][1], 2), s['b'][2], round(s['b_dop'], 1), round(s['b_disc'], 1)] for s in SERIES],
      [36, 12, 6, 12, 60, 10, 9, 40, 9, 9, 10, 9, 40, 9, 9])

sheet('SEO декабрь', ['Неделя акции', 'Темы с пиком через 7–13 дней (Вордстат, декабрь 2025: индекс пика к средней неделе года)'],
      [[w, seo_txt(w)] for w, _, _ in WEEKS], [20, 140])

arows = []
for cl in CL_NAMES:
    arows.append([f'{cl} {CL_NAMES[cl]}'] + [aud(seg=(s,), cl=(cl,)) for s in range(1, 7)] + [aud(seg=(2, 3), cl=(cl,), push=1)])
arows.append(['без кластера'] + [aud(seg=(s,), cl=(0,)) for s in range(1, 7)] + [aud(seg=(2, 3), cl=(0,), push=1)])
sheet('Аудитории', ['Кластер (присвоение ' + CL_DATE + ')'] + [SEG[s] for s in range(1, 7)] + ['Активные и LFL с пушем'], arows, [34] + [12] * 7)

sheet('Коэффициенты', list(COEF[0].keys()), [list(r.values()) for r in COEF], [24, 26, 40, 12, 8, 20, 10, 8, 8, 8, 8, 8, 8, 10, 6, 40, 30, 12, 12, 60])

wb.save(OUT)
print(f'групп {len(grp_size)}, строк треков {len(track_rows)}, строк по дням {len(daily)}')
print(f'серии А {sum(s["a_dop"] for s in SERIES):.1f}/{sum(s["a_disc"] for s in SERIES):.1f}  Б {sum(s["b_dop"] for s in SERIES):.1f}/{sum(s["b_disc"] for s in SERIES):.1f}')
print(f'треки {tr_dop:.1f}/{tr_disc:.1f}')
print(f'ГКГ А {A_gcg:.0f} (p600 {pA:.2f})  Б {B_gcg:.0f} (p600 {pB:.2f})')
for s in SERIES:
    print(s['seg'][:30], s['n'], s['n_cg'], round(s['a_dop'], 1), round(s['a_disc'], 1), round(s['b_dop'], 1), round(s['b_disc'], 1))
for w, _, _ in WEEKS:
    print(w, seo_txt(w)[:200])
