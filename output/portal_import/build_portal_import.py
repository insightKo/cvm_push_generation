#!/usr/bin/env python3
"""
Сборка JSON-файла импорта реальных данных ДИКСИ в портал CVM.

Источники (только чтение):
  output/cvm_offline_dump_2026-09-07.json   — вкладки «CVM offline», «PUSH» Google-таблицы
  output/ДЦО_волна_трафик_4.xlsx            — ДЦО волна «Трафик» №4 (100 SKU + прогноз)
  data/ассортимент.xlsx                     — каталог SKU
  data/cat5_catalog.json                    — иерархия cat4 → cat5
  data/I_PRODUCT_SCORE_NEW.xlsx (выгрузка)  — названия cat5 (I_CATEGORY_5 по ID_CATEGORY_5_ext)
  ../dixy-dashboard/sql/справочник_ext_названия.sql — ручные названия cat5 (блок MANUAL)
  data/deeplink.xlsx                        — категория → deeplink

Результат: output/portal_import/portal_import_2026-09-07.json
Запуск:    python3 output/portal_import/build_portal_import.py
"""
import datetime as dt
import json
import re
import sys
from collections import Counter, OrderedDict, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / 'output' / 'portal_import'
DASH = ROOT.parent / 'dixy-dashboard'

SRC_DUMP = ROOT / 'output' / 'cvm_offline_dump_2026-09-07.json'
SRC_DCO = ROOT / 'output' / 'ДЦО_волна_трафик_4.xlsx'
SRC_ASSORT = ROOT / 'data' / 'ассортимент.xlsx'
SRC_CAT5 = ROOT / 'data' / 'cat5_catalog.json'
SRC_SCORE = ROOT / 'data' / 'I_PRODUCT_SCORE_NEW.xlsx'
SRC_SQL_NAMES = DASH / 'sql' / 'справочник_ext_названия.sql'
SRC_DEEPLINK = ROOT / 'data' / 'deeplink.xlsx'
CONFIG = OUT_DIR / 'import_config.json'
OUT = OUT_DIR / 'portal_import_2026-09-07.json'

TODAY = dt.date(2026, 9, 7)
OWNER = 'u_elena'
YEARS = {2025, 2026}
APPROVE_COMMENT = 'Согласовано до внедрения портала (импорт из CVM offline)'
EVENT_COMMENT = 'Импорт из CVM offline: согласовано до внедрения портала'

# ---------------------------------------------------------------- справочники портала (src/api/mock/seed/base.ts)
SEGMENTS = {
    1: 'Новые', 2: 'Отток', 3: 'Спящие', 4: 'Активные', 5: 'Активные (курильщики)', 6: 'Активные (дети)',
    7: 'Активные, высокий ЦИ', 8: 'Активные, низкий ЦИ', 9: 'Активные (животные)', 10: 'Активные (алкоголь)',
    11: 'Активные (готовая еда)', 12: 'Случайные', 13: 'Аффинитивные', 14: 'ВСЕ онлайн', 15: 'Активные онлайн',
    16: 'Новые онлайн', 17: 'Новые. Только онлайн', 18: 'Новые омни', 19: 'Активные. Омни', 20: 'Активные. Только онлайн',
    21: 'Спящие онлайн', 22: 'Спящие. Только онлайн', 23: 'Спящие. Омни', 24: 'Отток онлайн', 25: 'Отток. Только онлайн',
    26: 'Отток. Омни', 27: 'Дарксторы', 28: 'ВСЕ оффлайн', 29: 'Активные (тонус)', 30: 'Активные (вино)',
    31: 'Активные (полуфабрикаты)', 32: 'Активные (Правильное питание)',
    101: 'пп Новый', 102: 'пп Низкочастотный', 103: 'пп Среднечастотный', 104: 'пп Высокочастотный', 105: 'пп Склонные к оттоку', 106: 'пп Отток',
}
CHANNELS = {1: 'PUSH', 2: 'SMS', 3: 'EMAIL', 4: 'SLIP', 5: 'DIGITAL', 6: 'WALLET', 7: 'APP'}
MECHANICS = {  # id: (name, benefit, activation)
    1: ('Кэшбек', 'cashback_pct', False), 2: ('Целевые бонусы', 'bonus', False), 3: ('Скидка', 'discount_pct', False),
    4: ('Активируемая скидка', 'discount_pct', True), 5: ('Купон', 'discount_rub', False), 6: ('Купон активируемый', 'discount_rub', True),
    7: ('Коммуникация', 'communication', False), 8: ('Кэшбек X баллов', 'cashback_rub', False), 9: ('Кэшбек Х баллов за чек', 'cashback_rub', False),
    10: ('Кратный кэшбек', 'cashback_pct', False), 11: ('Промокод', 'promocode', False), 12: ('Отложенная скидка', 'discount_pct', False),
    13: ('Скидка x% от чека', 'discount_pct', False), 14: ('Скидка Xр. от чека', 'discount_rub', False), 15: ('Предначисленные бонусы', 'bonus', False),
    16: ('Кэшбек активируемый', 'cashback_pct', True),
}
COMM_CHANNEL_BY_TYPE = {'push': 1, 'sms': 2, 'email': 3, 'slip': 4, 'banner': 7}
ROUTES = {  # src/api/mock/seed/approvals.ts DEFAULT_ROUTES
    'route_promo_list': [
        (1, ['cvm_manager'], 'any', 2, 'CVM-менеджер'),
        (2, ['commercial'], 'any', 2, 'Коммерческая служба'),
        (3, ['marketing_director'], 'any', 2, 'Директор по маркетингу'),
    ],
    'route_communications': [
        (1, ['cvm_manager', 'content_manager'], 'all', 2, 'CVM и контент-менеджер'),
        (2, ['marketing_director'], 'any', 2, 'Директор по маркетингу'),
    ],
    'route_dco': [
        (1, ['analyst'], 'any', 2, 'Аналитик'),
        (2, ['commercial'], 'any', 2, 'Коммерческая служба'),
        (3, ['marketing_director'], 'any', 2, 'Директор по маркетингу'),
    ],
}

# Метки колонки «Сегмент» → id сегмента портала (сверено с SEGMENTS и data/cvm_dictionaries.md)
SEGMENT_LABELS = {
    'новые': 1, 'отток': 2, 'спящие': 3, 'активные': 4, 'случайные': 12, 'все оффлайн': 28,
    'активные. зоо': 9, 'активные. мамы': 6, 'активные. пп': 32, 'активные. перекус': 11,
    'активные. пиво и п/ф': 31, 'активные. п/ф': 31, 'активные. вино': 30, 'активные. просекко': 30,
    'активные. тонус': 29, 'активные. курильщики': 5,
}
# Точные значения колонки «Механика» → id механики портала
MECHANIC_LABELS = {
    'активируемая скидка': 4, 'кэшбек активируемый': 16, 'кешбэк x% баллами': 1, 'кэшбек': 1, 'кешбэк': 1,
    'целевой кешбэк': 2, 'купон на скидку': 5, 'предначисленные бонусы': 15, 'предначисление бонусов с активацией': 15,
    'коммуникация': 7, 'сервисный': 7, 'отложенная скидка': 12, 'купон x р. на категорию': 5,
    'купон x р. от суммы чека': 5, 'спеццена купон': 5, 'кэшбек x баллов': 8,
}
MECHANIC_APPROX = {'частотная: 2 чека от 1000р. за неделю → 300 монет': 9}
MECHANIC_INFER = {'', 'активация', 'автоматическая'}  # механика выводится по колонкам «Скидка»/«Бонусы»
CHANNEL_LABELS = {'push': 1, 'sms': 2, 'slip': 4}

NOTES: list[str] = []
UNMAPPED: dict = {
    'segments': {}, 'mechanics': {}, 'mechanicApprox': {}, 'mechanicInferred': {}, 'benefitUnitAdjusted': [],
    'valueMissing': [], 'promoCategoryCodesNotInCatalog': [], 'pushScreenTexts': {}, 'commBranchFallback': [],
}
SKIPPED = {'promos': Counter(), 'communications': Counter()}


# ---------------------------------------------------------------- утилиты
def clean(s) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ''
    return str(s).replace('\xa0', ' ').replace('\r\n', '\n').replace('\r', '\n').strip()


def to_int(s):
    t = clean(s).replace(' ', '').replace(' ', '')
    return int(t) if re.fullmatch(r'-?\d+', t) else None


def parse_value(s):
    """«20%» → (20,'pct'); «0,1» → (10,'pct'); «100» / «100р.» → (100,'rub'); прочее → (None, None)."""
    t = clean(s).replace(' ', '').lower()
    m = re.fullmatch(r'(\d+(?:[.,]\d+)?)%', t)
    if m:
        return float(m.group(1).replace(',', '.')), 'pct'
    m = re.fullmatch(r'(\d+(?:[.,]\d+)?)(?:р\.?|руб\.?|₽)?', t)
    if m:
        v = float(m.group(1).replace(',', '.'))
        if 0 < v < 1:  # доля 0,1 = 10%
            return round(v * 100, 2), 'pct'
        return v, 'rub'
    return None, None


def num_out(v):
    return int(v) if v is not None and float(v).is_integer() else v


def parse_date(s, year: int | None, month_hint: int | None = None):
    """Форматы: dd.mm. | dd.mm | dd.mm.yyyy | d.m.yyyy | yyyy-mm-dd (+ хвост времени игнорируется)."""
    t = clean(s)
    if not t:
        return None
    m = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})', t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        m = re.match(r'^(\d{1,2})\.(\d{1,2})\.?(\d{4})?', t)
        if not m:
            return None
        d, mo = int(m.group(1)), int(m.group(2))
        if m.group(3):
            y = int(m.group(3))
        elif year is None:
            return None
        else:
            y = year
            if month_hint == 1 and mo == 12:
                y -= 1
            elif month_hint == 12 and mo == 1:
                y += 1
    try:
        return dt.date(y, mo, d)
    except ValueError:
        return None


def iso_dt(d: dt.date, time='09:00:00') -> str:
    return f'{d.isoformat()}T{time}.000Z'


def norm_label(s: str) -> str:
    return re.sub(r'\s+', ' ', re.sub(r'\s*\.\s*', '. ', clean(s).lower())).strip(' .')


DEEPLINK_RE = re.compile(r'(dixyapp://[^\n]*)')


def split_deeplink(text: str):
    """Возвращает (текст без ссылки, ссылка)."""
    t = clean(text)
    m = DEEPLINK_RE.search(t)
    link = m.group(1).strip() if m else ''
    rest = t.replace(link, '') if link else t
    return re.sub(r'\s+', ' ', rest).strip(' "\'/'), link


def button_text(text: str) -> str:
    rest, _ = split_deeplink(text)
    rest = re.sub(r'(нужен\s+)?диплинк(\s+(на|в)\s+(категорию|приложении))?', '', rest, flags=re.I)
    return re.sub(r'\s+', ' ', rest).strip(' "\'')


def steps_done(route_id: str, at: str):
    return [{
        'order': o, 'mode': mode, 'title': title, 'status': 'done', 'overdue': False,
        'visas': [{'role': r, 'decision': 'approved', 'actorId': OWNER, 'at': at, 'comment': APPROVE_COMMENT} for r in roles],
    } for o, roles, mode, sla, title in ROUTES[route_id]]


THRESHOLD_RE = re.compile(r'(?:\bот|с\s+чека|чек\w*\s+(?:на|от))\s*(\d[\d ]{0,8}?)\s*(?:р\b|р\.|₽|руб)', re.I)


def check_threshold(*texts):
    for t in texts:
        m = THRESHOLD_RE.search(clean(t))
        if m:
            v = to_int(m.group(1))
            if v:
                return v
    return None


# ---------------------------------------------------------------- users
def build_users():
    return [{
        'id': OWNER, 'name': 'Елена Корякова', 'email': 'elena.koryakova@gmail.com',
        'roles': ['admin', 'analyst', 'cvm_manager'], 'position': 'Аналитик MCI', 'active': True, 'color': '#0f766e',
    }]


# ---------------------------------------------------------------- сегменты / механика
def map_segments(label: str, promo_no: str):
    ids, unknown = [], []
    for part in clean(label).split(','):
        p = norm_label(part)
        if not p:
            continue
        sid = SEGMENT_LABELS.get(p)
        if sid is None:
            unknown.append(part.strip())
        elif sid not in ids:
            ids.append(sid)
    for u in unknown:
        e = UNMAPPED['segments'].setdefault(u, {'count': 0, 'promos': []})
        e['count'] += 1
        if promo_no not in e['promos']:
            e['promos'].append(promo_no)
    return ids, unknown


def map_mechanic(row: dict, disc_unit, bonus_unit):
    text = clean(row['Механика'])
    t = norm_label(text)
    title = clean(row['Название промо']).lower()
    act_word = 'актив' in t
    if t in MECHANIC_LABELS:
        return MECHANIC_LABELS[t], 'exact', act_word
    if t in MECHANIC_APPROX:
        UNMAPPED['mechanicApprox'][text] = MECHANIC_APPROX[t]
        return MECHANIC_APPROX[t], 'approx', act_word
    if t in MECHANIC_INFER:
        if bonus_unit == 'pct':
            mid = 16 if act_word else 1
        elif bonus_unit == 'rub':
            mid = 15 if act_word else 2
        elif disc_unit == 'pct':
            mid = 4 if act_word else 3
        elif disc_unit == 'rub':
            mid = 6 if act_word else 5
        elif t == 'сервисный' or re.match(r'^(коммуникация|тематическая рассылка|подборка|доп инфо|баланс)', title):
            mid = 7
        else:
            UNMAPPED['mechanics'].setdefault(text or '(пусто)', []).append(row['НОМЕР'])
            return 3, 'fallback', act_word
        UNMAPPED['mechanicInferred'].setdefault(str(mid), []).append(row['НОМЕР'])
        return mid, 'inferred', act_word
    if re.match(r'^(коммуникация|тематическая рассылка)', t):
        return 7, 'exact', act_word
    UNMAPPED['mechanics'].setdefault(text, []).append(row['НОМЕР'])
    return 3, 'fallback', act_word


# ---------------------------------------------------------------- promos
def build_promos(rows: list[dict], categories_codes: set[str]):
    by_no: OrderedDict[str, dict] = OrderedDict()
    for r in rows:
        no = clean(r['НОМЕР'])
        year = to_int(r['Год'])
        if not no.isdigit():
            SKIPPED['promos']['нет НОМЕР'] += 1
            continue
        if year not in YEARS:
            SKIPPED['promos']['Год вне 2025/2026'] += 1
            continue
        if no in by_no:
            SKIPPED['promos']['дубль НОМЕР'] += 1
            keep_new = bool(clean(r['Название МС'])) and not clean(by_no[no]['Название МС'])
            NOTES.append(f'НОМЕР {no} встречается дважды («{clean(by_no[no]["Название промо"])}» и «{clean(r["Название промо"])}»); '
                         f'оставлена строка «{clean(r["Название промо"]) if keep_new else clean(by_no[no]["Название промо"])}» (с заполненным «Название МС»).')
            if keep_new:
                by_no[no] = r
            continue
        by_no[no] = r

    promos, mech_stats, seg_stats, cat_text_only = [], Counter(), Counter(), 0
    unit_adjusted = 0
    for no, r in by_no.items():
        year = to_int(r['Год'])
        month = to_int(r['Месяц'])
        start = parse_date(r['Старт акции'], year, month)
        end = parse_date(r['Окончание акции'], year, month)
        if not start or not end:
            SKIPPED['promos']['не разобрана дата'] += 1
            continue
        if end < start:
            end = dt.date(end.year + 1, end.month, end.day)
        title = clean(r['Название промо'])
        mc_name = clean(r['Название МС']) or f'{no}_{title}'
        created = iso_dt(start - dt.timedelta(days=14))

        # каналы
        channel_ids = []
        for part in re.split(r'[,/;]+', clean(r['Каналы коммуникации'])):
            cid = CHANNEL_LABELS.get(part.strip().lower())
            if cid and cid not in channel_ids:
                channel_ids.append(cid)

        # механика и выгода
        disc_v, disc_u = parse_value(r['Скидка'])
        bonus_v, bonus_u = parse_value(r['Бонусы'])
        mid, msrc, act_word = map_mechanic(r, disc_u, bonus_u)
        mech_stats[(clean(r['Механика']) or '(пусто)', mid, msrc)] += 1
        mname, benefit, activation = MECHANICS[mid]
        activation = activation or act_word
        if benefit in ('bonus', 'cashback_pct', 'cashback_rub'):
            value, unit = (bonus_v, bonus_u) if bonus_v is not None else (disc_v, disc_u)
        else:
            value, unit = (disc_v, disc_u) if disc_v is not None else (bonus_v, bonus_u)
        if value is None:
            value = 0
            if benefit != 'communication':
                UNMAPPED['valueMissing'].append(no)
        elif unit == 'pct' and benefit in ('discount_rub', 'cashback_rub', 'bonus'):
            new_b = {'discount_rub': 'discount_pct', 'cashback_rub': 'cashback_pct', 'bonus': 'cashback_pct'}[benefit]
            UNMAPPED['benefitUnitAdjusted'].append({'promo': no, 'from': benefit, 'to': new_b, 'raw': clean(r['Скидка']) or clean(r['Бонусы'])})
            benefit, unit_adjusted = new_b, unit_adjusted + 1
        elif unit == 'rub' and benefit in ('discount_pct', 'cashback_pct'):
            new_b = {'discount_pct': 'discount_rub', 'cashback_pct': 'cashback_rub'}[benefit]
            UNMAPPED['benefitUnitAdjusted'].append({'promo': no, 'from': benefit, 'to': new_b, 'raw': clean(r['Скидка']) or clean(r['Бонусы'])})
            benefit, unit_adjusted = new_b, unit_adjusted + 1

        # категории
        cat_raw = clean(r['Категория'])
        codes = []
        for c in re.findall(r'\d{8}', cat_raw):
            if c not in codes:
                codes.append(c)
        if cat_raw and not codes:
            cat_text_only += 1
        for c in codes:
            if c not in categories_codes and c not in UNMAPPED['promoCategoryCodesNotInCatalog']:
                UNMAPPED['promoCategoryCodesNotInCatalog'].append(c)

        expiry = parse_date(r['Срок сгорания бонусов'], end.year)
        expiry_days = (expiry - end).days if expiry and expiry >= end else None

        mechanic = {
            'mechanicId': mid, 'benefit': benefit, 'value': num_out(value), 'activation': bool(activation),
            'online': True, 'offline': True, 'scope': 'categories' if codes else 'all',
            'categoryCodes': codes, 'skus': [], 'excludeSkus': [], 'onlineText': clean(r['Механика для Manzana Online']),
        }
        thr = check_threshold(r['Описание акции'], r['Механика'], r['Название промо'])
        if thr:
            mechanic['checkThreshold'] = thr
        if expiry_days is not None:
            mechanic['bonusExpiryDays'] = expiry_days

        btn_text = button_text(r['Кнопка'])
        _, btn_link = split_deeplink(r['Кнопка'])
        coupon = {
            'title': clean(r['Название информационного купона для МП']), 'slipText': clean(r['Текст на информационном купоне / слип-чеке']),
            'buttonText': btn_text, 'deeplink': btn_link, 'screen': '',
        }
        resp, resp_u = parse_value(r['отклик'])
        forecast = {
            'clients': to_int(r['Примерное количество клиентов']) or 0,
            'responsePct': num_out(resp) if resp is not None else 0,
            'upliftRub': to_int(r['Доп ТО (план), р.']) or 0,
            'discountRub': to_int(r['Скидка_1']) or to_int(r['Скидка на категорию, р.']) or 0,
            'plRub': to_int(r['PL']) or 0,
            'source': 'manual',
        }

        seg_ids, _ = map_segments(r['Сегмент'], no)
        if not seg_ids:
            seg_ids = [4]
        for s in seg_ids:
            seg_stats[s] += 1
        pid = f'promo_{no}'
        branches = [{
            'id': f'br_{no}_{sid}', 'promoId': pid, 'segmentId': sid,
            'clientsPlan': forecast['clients'] if len(seg_ids) == 1 else 0,
            'audienceSettings': {'minusFraud': True, 'controlGroupPct': 0},
        } for sid in seg_ids]

        status = 'executed' if end < TODAY else 'approved'
        promo = {
            'id': pid, 'number': int(no), 'title': title, 'mcName': mc_name, 'type': 'cvm',
            'start': start.isoformat(), 'end': end.isoformat(), 'categoryMonth': '',
            'description': clean(r['Описание акции']), 'goal': '', 'restrictions': clean(r['Ограничения и комментарии']),
            'active': clean(r['Настройка']).lower() == 'да', 'channelIds': channel_ids,
            'mechanic': mechanic, 'coupon': coupon, 'forecast': forecast, 'branches': branches,
            'ownerId': OWNER, 'stage': 3, 'packageIds': [], 'attachments': [],
            'createdAt': created, 'updatedAt': created,
            'status': status, 'versionNo': 1, 'steps': steps_done('route_promo_list', created), 'routeId': 'route_promo_list',
            'effectiveVersionNo': 1, 'revision': 1,
        }
        promo['versions'] = [{'no': 1, 'snapshot': {k: v for k, v in promo.items() if k != 'versions'}, 'createdBy': OWNER, 'createdAt': created}]
        promo['_src'] = {'segmentLabel': clean(r['Сегмент']), 'layout': clean(r['ССЫЛКА на макет купона (https://)'])}
        promos.append(promo)

    NOTES.append(f'Категория задана текстом без кодов (scope=all) у {cat_text_only} акций.')
    NOTES.append(f'Единица выгоды (%/₽) в источнике не совпала с benefit механики у {unit_adjusted} акций — benefit заменён на парный тип по данным источника (список в unmapped.benefitUnitAdjusted).')
    return promos, mech_stats, seg_stats


# ---------------------------------------------------------------- communications
def build_communications(rows: list[dict], promos: dict[str, dict], deeplinks: dict[str, str]):
    comms, ordinal = [], Counter()
    trigger_time = 0
    for r in rows:
        year = to_int(r['Год'])
        if year not in YEARS:
            SKIPPED['communications']['Год вне 2025/2026'] += 1
            continue
        no = clean(r['Номер промо'])
        if not no.isdigit():
            SKIPPED['communications']['Номер промо не число / несколько номеров'] += 1
            continue
        promo = promos.get(f'promo_{no}')
        if not promo:
            SKIPPED['communications']['промо не найдено'] += 1
            continue
        chan = clean(r['Канал']).upper()
        if chan not in ('', 'PUSH', 'SMS'):
            SKIPPED['communications'][f'Канал {chan}'] += 1
            continue
        ctype = 'sms' if chan == 'SMS' else 'push'
        date = parse_date(r['Дата'], year, to_int(r['Месяц']))
        if not date:
            SKIPPED['communications']['не разобрана дата'] += 1
            continue
        time = clean(r['Время'])
        m = re.fullmatch(r'(\d{1,2}):(\d{2})', time)
        if m:
            time = f'{int(m.group(1)):02d}:{m.group(2)}'
        else:
            if time:
                trigger_time += 1
            time = '11:00'
        msg = to_int(r['Номер msg']) or 1
        ordinal[(no, msg)] += 1
        cid = f'comm_{no}_{msg}_{ordinal[(no, msg)]}'

        seg_ids, _ = map_segments(r['Сегмент'], no)
        branch = next((b for b in promo['branches'] if b['segmentId'] in seg_ids), None)
        if branch is None:
            branch = promo['branches'][0]
            UNMAPPED['commBranchFallback'].append({'comm': cid, 'segmentLabel': clean(r['Сегмент'])})

        title = clean(r['PUSH заголовок'])
        body = clean(r['текст PUSH'])
        title = '' if title == '.' else title
        body = '' if body == '.' else body
        screen_raw = clean(r['Экран - ссылка'])
        screen_text, screen_link = split_deeplink(screen_raw)
        btn_txt = button_text(r['Кнопка'])
        _, btn_link = split_deeplink(r['Кнопка'])
        deeplink = screen_link or btn_link
        if not deeplink and screen_text:
            deeplink = deeplinks.get(screen_text.lower(), '')
        if screen_text:
            UNMAPPED['pushScreenTexts'][screen_text] = UNMAPPED['pushScreenTexts'].get(screen_text, 0) + 1
        if ctype == 'push':
            payload = {'title': title, 'body': body, 'screen': screen_text, 'deeplink': deeplink,
                       'button': btn_txt, 'couponText': clean(r['Текст купона'])}
        else:
            payload = {'text': body or title}

        created = iso_dt(date - dt.timedelta(days=7))
        clients = to_int(r['Клиентов'])
        comm = {
            'id': cid, 'promoId': promo['id'], 'branchId': branch['id'], 'segmentId': branch['segmentId'],
            'promoNumber': promo['number'], 'promoTitle': promo['title'],
            'type': ctype, 'channelId': COMM_CHANNEL_BY_TYPE[ctype], 'date': date.isoformat(), 'time': time, 'messageNo': msg,
        }
        if clients is not None:
            comm['clients'] = clients
        comm.update({
            'payload': payload, 'createdAt': created, 'updatedAt': created,
            'status': 'executed' if date < TODAY else 'approved', 'versionNo': 1,
            'steps': steps_done('route_communications', created), 'routeId': 'route_communications',
        })
        comms.append(comm)
    if trigger_time:
        NOTES.append(f'Время отправки не в формате ЧЧ:ММ («триггер») у {trigger_time} коммуникаций — поставлено 11:00.')
    return comms


# ---------------------------------------------------------------- content / events
def build_content(promos: list[dict]):
    items = []
    for p in promos:
        st = p['status']
        base = {'promoId': p['id'], 'versionNo': 1, 'status': st, 'createdAt': p['createdAt'], 'updatedAt': p['createdAt']}
        if p['coupon']['slipText']:
            ctype = 'slip_text' if 4 in p['channelIds'] and 1 not in p['channelIds'] else 'coupon_text'
            items.append({'id': f'cnt_{p["number"]}_coupon_text', 'type': ctype, 'title': p['coupon']['title'] or p['title'],
                          'text': p['coupon']['slipText'], **base})
        url = p['_src']['layout']
        if url:
            items.append({'id': f'cnt_{p["number"]}_layout', 'type': 'file', 'title': f'Макет купона · {p["coupon"]["title"] or p["title"]}',
                          'url': url, 'fileName': url.rstrip('/').split('/')[-1], **base})
    return items


def build_events(promos: list[dict], comms: list[dict]):
    ev = []
    for p in promos:
        ev.append({'id': f'ev_{p["id"]}', 'objectType': 'promo', 'objectId': p['id'], 'versionNo': 1, 'actorId': OWNER,
                   'action': 'approve', 'comment': EVENT_COMMENT, 'at': p['createdAt']})
    for c in comms:
        ev.append({'id': f'ev_{c["id"]}', 'objectType': 'communication', 'objectId': c['id'], 'versionNo': 1, 'actorId': OWNER,
                   'action': 'approve', 'comment': EVENT_COMMENT, 'at': c['createdAt']})
    return ev


# ---------------------------------------------------------------- categories / catalog
def load_cat5_names(cat5_codes: set[str], dco_names: dict[str, str]):
    names, src = {}, Counter()
    manual = {}
    if SRC_SQL_NAMES.exists():
        sql = SRC_SQL_NAMES.read_text(encoding='utf-8')
        manual = dict(re.findall(r"\(N'(\d+)',\s*N'((?:[^']|'')*)',", sql))
    auto = {}
    if SRC_SCORE.exists():
        v = pd.read_excel(SRC_SCORE, sheet_name='выгрузка', usecols=['ID_CATEGORY_5_ext', 'I_CATEGORY_5'])
        v['c'] = v['ID_CATEGORY_5_ext'].astype(str)
        g = v.dropna(subset=['I_CATEGORY_5']).groupby('c')['I_CATEGORY_5'].agg(lambda s: sorted(set(map(str, s))))
        auto = {c: n[0] for c, n in g.items() if len(n) == 1}
    for c in cat5_codes:
        if c in manual:
            names[c], src['manual_sql'] = manual[c].replace("''", "'"), src['manual_sql'] + 1
        elif c in auto:
            names[c], src['auto_unique'] = auto[c], src['auto_unique'] + 1
        elif c in dco_names:
            names[c], src['dco_file'] = dco_names[c], src['dco_file'] + 1
        else:
            names[c], src['code'] = c, src['code'] + 1
    return names, src


def build_categories(dco_names: dict[str, str]):
    cat = json.load(open(SRC_CAT5, encoding='utf-8'))
    cat5_codes = {c5 for v in cat['cat4'].values() for c5 in v['cat5']} | set(cat['cat5_names'])
    names5, src = load_cat5_names(cat5_codes, dco_names)
    out = []
    for c4 in sorted(cat['cat4']):
        out.append({'id': c4, 'code': c4, 'name': c4, 'level': 4})
        for c5 in sorted(cat['cat4'][c4]['cat5']):
            out.append({'id': c5, 'code': c5, 'name': names5.get(c5, c5), 'level': 5, 'parentCode': c4})
    NOTES.append(f'Названия категорий уровня 5: {src["manual_sql"]} из ручного блока справочник_ext_названия.sql, {src["auto_unique"]} — единственное название кода в I_PRODUCT_SCORE_NEW.xlsx (лист «выгрузка»), '
                 f'{src["dco_file"]} — из файла ДЦО, {src["code"]} — источника нет, name = код.')
    NOTES.append('Названий категорий уровня 4 (cat4) по ext-кодам нет ни в одном локальном источнике — name = код.')
    return out


BAG_RE = re.compile(r'^ПАКЕТ[\s-]*МАЙКА|^ПАКЕТ (БУМАЖНЫЙ|ВУР) ДИКСИ|^ПАКЕТ ДИКСИ|^ПАКЕТ ИЗ СПАНБОНДА')
LOYALTY_RE = re.compile(r'\bАКЦИ|\bКУПОН|(?<!BONUS/)\bБОНУС')


def build_catalog():
    a = pd.read_excel(SRC_ASSORT, sheet_name='Лист1',
                      usecols=['PRODUCT_NAME', 'ID_PRODUCT_EXTERNAL', 'ID_CATEGORY_5_ext', 'ID_CATEGORY_4_ext', 'COUNT_CHECK', 'AVG_PRICE'])
    a['sku'] = a['ID_PRODUCT_EXTERNAL'].astype(str).str.replace(r'_0$', '', regex=True)
    a['name'] = a['PRODUCT_NAME'].astype(str)
    excl = a['name'].str.contains(BAG_RE) | a['name'].str.contains(LOYALTY_RE)
    excluded = a[excl].drop_duplicates('sku')
    a = a[~excl]
    a['w'] = a['COUNT_CHECK'].fillna(0).clip(lower=0)
    a['pw'] = a['AVG_PRICE'].fillna(0) * a['w']
    g = a.groupby('sku', sort=True).agg(name=('name', 'first'), cat4=('ID_CATEGORY_4_ext', 'first'), cat5=('ID_CATEGORY_5_ext', 'first'),
                                        w=('w', 'sum'), pw=('pw', 'sum'), p_mean=('AVG_PRICE', 'mean'))
    out = []
    for sku, r in g.iterrows():
        price = r['pw'] / r['w'] if r['w'] > 0 else (r['p_mean'] if pd.notna(r['p_mean']) else 0)
        out.append({'id': sku, 'sku': sku, 'name': r['name'], 'cat4': str(int(r['cat4'])), 'cat5': str(int(r['cat5'])),
                    'price': round(float(price), 2), 'inStock': True, 'supplierDco': False})
    NOTES.append(f'Каталог: исключены {len(excluded)} SKU кассовых пакетов ДИКСИ ({"; ".join(excluded["name"].tolist())}). '
                 'Подстрочные ключи ПАКЕТ/ФАСОВ/БОНУС не применялись как есть: они задели бы реальные товары '
                 '(творог/кетчуп «пакет», мусорные и подарочные пакеты, «САХАР-ПЕСОК ФАСОВАННЫЙ» из ДЦО, бренд BONUS/БОНУС); '
                 'ключи АКЦИ/КУПОН в ассортименте не встречаются.')
    return out


# ---------------------------------------------------------------- dco
def build_dco(cfg: dict):
    rows_df = pd.read_excel(SRC_DCO, sheet_name='Промо (оффлайн)')
    fc = pd.read_excel(SRC_DCO, sheet_name='Прогноз (оффлайн)')
    fc_map = {clean(k): v for k, v in zip(fc.iloc[:, 0], fc.iloc[:, 1])}

    def fc_val(label):
        for k, v in fc_map.items():
            if k.lower().startswith(label.lower()):
                return float(v)
        raise KeyError(label)

    rows = []
    for _, r in rows_df.iterrows():
        rows.append({
            'id': f'dcorow_{int(r["№"])}', 'sku': str(r['SKU']).strip(), 'categoryCode': str(int(r['Категория'])), 'name': clean(r['Товар']),
            'segmentId': 28, 'basePrice': 0, 'discountPct': num_out(float(r['Скидка, %'])), 'clients': 0, 'responsePct': 0,
            'upliftRub': num_out(float(r['Доп. ТО, ₽'])), 'discountCostRub': num_out(float(r['Скидка, ₽'])), 'plRub': num_out(float(r['PL, ₽'])),
            'visits': num_out(float(r['Доп. трафик (визиты)'])),
        })
    discounts = [x['discountPct'] for x in rows]
    budget = num_out(fc_val('Скидка волны'))
    pl = num_out(fc_val('PL'))
    uplift = num_out(fc_val('Общий доп. ТО'))
    sku_count = int(fc_val('SKU в наборе'))
    params = {'discountMin': min(discounts), 'discountMax': max(discounts), 'budgetRub': budget, 'coverageTarget': 0,
              'targetPlRub': pl, 'segmentIds': [28], 'excludeSupplierSkus': False}
    metrics = {'clients': 0, 'upliftRub': uplift, 'discountCostRub': budget, 'plRub': pl,
               'avgDiscountPct': round(sum(discounts) / len(discounts), 2), 'skuCount': sku_count}
    created = '2026-09-02T14:41:00.000Z'
    snap_id = 'model_2026-08-24'
    due = (dt.datetime(2026, 9, 2, 14, 41) + dt.timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:00.000Z')
    r1, r2, r3 = ROUTES['route_dco']
    steps = [
        {'order': 1, 'mode': 'any', 'title': r1[4], 'status': 'done', 'overdue': False,
         'visas': [{'role': 'analyst', 'decision': 'approved', 'actorId': OWNER, 'at': created, 'comment': APPROVE_COMMENT}]},
        {'order': 2, 'mode': 'any', 'title': r2[4], 'status': 'active', 'dueAt': due, 'overdue': False,
         'visas': [{'role': 'commercial', 'decision': 'pending'}]},
        {'order': 3, 'mode': 'any', 'title': r3[4], 'status': 'pending', 'overdue': False,
         'visas': [{'role': 'marketing_director', 'decision': 'pending'}]},
    ]
    dco = {
        'id': 'dco_wave_traffic_4', 'title': 'ДЦО волна «Трафик» №4', 'source': 'own',
        'period': {'start': cfg['dcoPeriod']['start'], 'end': cfg['dcoPeriod']['end']}, 'segmentIds': [28],
        'goal': 'Трафик: волна №4 по модели promo_product_model', 'rows': rows, 'params': params, 'metrics': metrics,
        'sourceSnapshotId': snap_id, 'sourceSnapshotAt': '2026-08-24T00:00:00.000Z', 'proposals': [], 'ownerId': OWNER,
        'status': 'in_review', 'versionNo': 1, 'currentStep': 2, 'steps': steps, 'routeId': 'route_dco',
        'createdAt': created, 'updatedAt': created,
        'versions': [{'no': 1, 'snapshot': {'rows': rows, 'params': params, 'metrics': metrics, 'sourceSnapshotId': snap_id},
                      'createdBy': OWNER, 'createdAt': created, 'sourceSnapshotId': snap_id}],
        'revision': 1,
    }
    NOTES.append(f'ДЦО: период {cfg["dcoPeriod"]["start"]}–{cfg["dcoPeriod"]["end"]} взят из import_config.json '
                 f'(dcoPeriodConfirmed={cfg.get("dcoPeriodConfirmed")}); в файле ДЦО периода нет — уточнить.')
    NOTES.append('ДЦО: basePrice, clients, responsePct строк = 0 — в файле ДЦО их нет; поле visits (доп. трафик) — расширение строки. '
                 f'Общий доп. трафик волны по листу «Прогноз»: {int(fc_val("Общий доп. трафик"))} визитов (в DcoMetrics поля нет).')
    dco_names = {}
    for _, r in rows_df.groupby(rows_df['Категория'].astype(str))['Название категории'].agg(lambda s: sorted(set(s))).items():
        pass
    g = rows_df.groupby(rows_df['Категория'].astype(str))['Название категории'].agg(lambda s: sorted(set(map(clean, s))))
    dco_names = {c: n[0] for c, n in g.items() if len(n) == 1}
    return dco, dco_names


# ---------------------------------------------------------------- проверки целостности
def integrity(col: dict):
    errs = []
    for name, items in col.items():
        ids = [x['id'] for x in items]
        dup = [k for k, v in Counter(ids).items() if v > 1]
        if dup:
            errs.append(f'{name}: дубли id {dup[:10]}')
    promo_ids = {p['id'] for p in col['promos']}
    branch_ids = {b['id'] for p in col['promos'] for b in p['branches']}
    cat_ids = {c['id'] for c in col['categories']}
    for p in col['promos']:
        if p['start'] > p['end']:
            errs.append(f'{p["id"]}: start > end')
        if p['mechanic']['mechanicId'] not in MECHANICS:
            errs.append(f'{p["id"]}: mechanicId')
        for b in p['branches']:
            if b['segmentId'] not in SEGMENTS:
                errs.append(f'{b["id"]}: segmentId')
        for c in p['channelIds']:
            if c not in CHANNELS:
                errs.append(f'{p["id"]}: channelId {c}')
    for c in col['communications']:
        if c['promoId'] not in promo_ids:
            errs.append(f'{c["id"]}: promoId')
        if c['branchId'] not in branch_ids:
            errs.append(f'{c["id"]}: branchId')
        dt.date.fromisoformat(c['date'])
    for c in col['content']:
        if c['promoId'] not in promo_ids:
            errs.append(f'{c["id"]}: promoId')
    obj_ids = promo_ids | {c['id'] for c in col['communications']}
    for e in col['events']:
        if e['objectId'] not in obj_ids:
            errs.append(f'{e["id"]}: objectId')
    miss_cat = sum(1 for i in col['catalog'] if i['cat5'] not in cat_ids or i['cat4'] not in cat_ids)
    if miss_cat:
        errs.append(f'catalog: {miss_cat} SKU с категорией вне categories')
    for d in col['dcos']:
        if d['period']['start'] > d['period']['end']:
            errs.append(f'{d["id"]}: period')
    return errs


# ---------------------------------------------------------------- main
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG.exists():
        CONFIG.write_text(json.dumps({'dcoPeriod': {'start': '2026-09-14', 'end': '2026-09-27'}, 'dcoPeriodConfirmed': False},
                                     ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    cfg = json.load(open(CONFIG, encoding='utf-8'))
    dump = json.load(open(SRC_DUMP, encoding='utf-8'))

    dl = pd.read_excel(SRC_DEEPLINK)
    dl.columns = [clean(c) for c in dl.columns]
    deeplinks = {clean(k).lower(): clean(v) for k, v in zip(dl['КАТЕГОРИЯ'], dl['deeplink']) if clean(v)}

    dco, dco_names = build_dco(cfg)
    categories = build_categories(dco_names)
    cat_codes = {c['code'] for c in categories}
    promos, mech_stats, seg_stats = build_promos(dump['CVM offline'], cat_codes)
    promo_by_id = {p['id']: p for p in promos}
    comms = build_communications(dump['PUSH'], promo_by_id, deeplinks)
    content = build_content(promos)
    events = build_events(promos, comms)
    catalog = build_catalog()
    for p in promos:
        p.pop('_src', None)

    multi = sum(1 for p in promos if len(p['branches']) > 1)
    NOTES.insert(0, 'Статусы: акции и коммуникации с датой окончания/отправки раньше 07.09.2026 — «executed», остальные — «approved»; '
                    'и то и другое означает «согласовано вне портала», маршруты пройдены полностью визами u_elena.')
    NOTES.append(f'Распределения клиентов по сегментам в источнике нет: у {multi} акций с несколькими ветками clientsPlan = 0 у всех веток, '
                 'общее число клиентов — в promo.forecast.clients.')
    NOTES.append('Механика: для 184 строк с пустой «Механикой» и меток «активация»/«автоматическая» id выведен по колонкам «Скидка»/«Бонусы» '
                 '(% в Бонусах → кешбэк 1/16, ₽ в Бонусах → бонусы 2/15, % в Скидке → скидка 3/4, ₽ в Скидке → купон 5/6; '
                 'суффикс «актив» → активируемый вариант). Номера акций — в unmapped.mechanicInferred.')
    NOTES.append('Значение выгоды берётся из «Скидка», при отсутствии — из «Бонусы» (и наоборот для бонусных механик); '
                 f'ни там ни там числа нет у {len(UNMAPPED["valueMissing"])} акций (value = 0, список в unmapped.valueMissing).')
    NOTES.append('checkThreshold проставлен только при явном «от N р./с чека N р.» в описании/механике/названии.')
    NOTES.append('bonusExpiryDays: в источнике дата сгорания, не число дней — записана разница (дата сгорания − окончание акции) в днях; при отсутствии/некорректной дате поле не задано.')
    NOTES.append('Ветка коммуникации: по метке «Сегмент» строки PUSH берётся первая ветка акции из покрываемых сегментов; '
                 f'если ни одна не совпала — первая ветка акции ({len(UNMAPPED["commBranchFallback"])} случаев, unmapped.commBranchFallback).')
    NOTES.append('Экран пуша: текстовые значения «Экран - ссылка» (купон, каталог, подборка…) записаны в payload.screen как есть; '
                 'в data/deeplink.xlsx совпадений по этим текстам нет, deeplink взят только из явных ссылок dixyapp:// (экран или кнопка).')
    NOTES.append('Визы шага 1 маршрута коммуникаций (cvm_manager + content_manager) обе проставлены u_elena — других пользователей в импорте нет.')
    NOTES.append(f'Коды категорий акций, отсутствующие в текущем ассортименте (cat5_catalog): {len(UNMAPPED["promoCategoryCodesNotInCatalog"])} — оставлены в categoryCodes как есть.')
    NOTES.append('Форматы дат источника: «дд.мм.» + Год (вкладка CVM offline), в PUSH также «дд.мм.гггг», «д.м.гггг», «гггг-мм-дд»; при переходе через год конец акции = следующий год.')

    collections = OrderedDict([
        ('users', build_users()), ('categories', categories), ('catalog', catalog), ('promos', promos),
        ('communications', comms), ('content', content), ('dcos', [dco]), ('events', events),
    ])
    errs = integrity(collections)
    counts = {k: len(v) for k, v in collections.items()}
    counts['branches'] = sum(len(p['branches']) for p in promos)
    meta = OrderedDict([
        ('builtAt', dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')),
        ('sources', [str(p.relative_to(ROOT.parent)) for p in (SRC_DUMP, SRC_DCO, SRC_ASSORT, SRC_CAT5, SRC_SCORE, SRC_SQL_NAMES, SRC_DEEPLINK, CONFIG)]),
        ('counts', counts),
        ('skipped', {k: dict(v) for k, v in SKIPPED.items()}),
        ('mechanicMapping', [{'source': s, 'mechanicId': m, 'mechanic': MECHANICS[m][0], 'how': how, 'promos': n} for (s, m, how), n in sorted(mech_stats.items(), key=lambda x: -x[1])]),
        ('segmentUsage', {str(k): {'name': SEGMENTS[k], 'promos': v} for k, v in sorted(seg_stats.items())}),
        ('notes', NOTES),
        ('unmapped', UNMAPPED),
        ('integrityErrors', errs),
    ])
    OUT.write_text(json.dumps({'meta': meta, 'collections': collections}, ensure_ascii=False, indent=1), encoding='utf-8')

    # ---- сводка
    print(f'Файл: {OUT} ({OUT.stat().st_size / 1e6:.1f} МБ)')
    print('Counts:', json.dumps(counts, ensure_ascii=False))
    print('Пропуски:', json.dumps(meta['skipped'], ensure_ascii=False))
    print('\nМеханика (источник → id):')
    for m in meta['mechanicMapping']:
        print(f'  {m["promos"]:4d} | {m["source"]!r:45} → {m["mechanicId"]:2d} {m["mechanic"]} [{m["how"]}]')
    print('\nСегменты (id: акций):', {k: v['promos'] for k, v in meta['segmentUsage'].items()})
    print('Unmapped segments:', json.dumps(UNMAPPED['segments'], ensure_ascii=False))
    print('Unmapped mechanics:', json.dumps(UNMAPPED['mechanics'], ensure_ascii=False), '| approx:', UNMAPPED['mechanicApprox'])
    print('Inferred mechanics by id:', {k: len(v) for k, v in UNMAPPED['mechanicInferred'].items()})
    print('Benefit unit adjusted:', len(UNMAPPED['benefitUnitAdjusted']), '| value missing:', len(UNMAPPED['valueMissing']))
    print('Comm branch fallback:', len(UNMAPPED['commBranchFallback']), '| screen texts:', UNMAPPED['pushScreenTexts'])
    print('\nЦелостность:', 'OK' if not errs else errs)
    print('\nNotes:')
    for n in NOTES:
        print(' -', n)
    return 0 if not errs else 1


if __name__ == '__main__':
    sys.exit(main())
