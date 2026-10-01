"""Подборки октября 2026 — сборка из исходников по образцу output/build_podborki_sept.py.
Формат августа: один лист «SKU по акциям октябрь», колонки ID акции, Название акции, Дата, Эпизод, Блок-категория, SKU название, SKU ID. Без цен.
Источники: data/ассортимент.xlsx (SKU, коды cat5, представленность), data/I_PRODUCT_SCORE_NEW.xlsx (имена категорий),
коды акций — колонка «Категория» вкладки CVM offline (дамп output/cvm_offline_dump_2026-09-29.json).
Фильтр представленности: SKU в >= 500 магазинах (сумма COUNT_SHOPS по городам).
Версия 1 (29.09.2026): неделя 40 — акции 101404, 101406, 101428_1…_3 и эпизоды 1 серий пиво/вино (пт 02.10) под черновики пушей 29.09.
Версия 2 (29.09.2026): эпизоды 1 серий пересобраны по замечаниям Елены — алкоголь нельзя выбрать любимым товаром, -20% на сыр/закуски;
вино — пары «вино + сыр» из реального ассортимента, пиво — колбаски к пиву.
Версия 3 (29.09.2026): 101406 расширена решением Елены до всех средств для уборки (cat4 630105 без посуды и ПММ);
коды записаны в «Категорию» CVM offline 29.09, дамп обновлён.
"""
import json, pandas as pd

MIN_SHOPS = 500
VERSION = '3'   # версия сборки — файл пишется с суффиксом _1, _2…, прежние версии не перезаписываются (схема Елены, 14.09.2026)
a = pd.read_excel('data/ассортимент.xlsx')
a['SKU'] = a['ID_PRODUCT_EXTERNAL'].astype(str).str.replace(r'_0$', '', regex=True)
g = (a.groupby('SKU').agg(name=('PRODUCT_NAME', 'first'), c5=('ID_CATEGORY_5_ext', 'first'),
                          shops=('COUNT_SHOPS', 'sum'), checks=('COUNT_CHECK', 'sum')).reset_index())
g['c5'] = g['c5'].astype(int).astype(str)
g = g[g.shops >= MIN_SHOPS].sort_values('checks', ascending=False)

s = pd.read_excel('data/I_PRODUCT_SCORE_NEW.xlsx')
s['c5'] = s['ID_CATEGORY_5_ext'].astype(str)
cat_name = s.groupby('c5')['I_CATEGORY_5'].agg(lambda x: x.value_counts().index[0]).to_dict()

# человеческие имена блоков там, где имя cat5 служебное («прочие», «(П)», «темный»)
BLOCK_NAME = {
    '62050102': 'Конфеты в коробках', '62060202': 'Шоколад горький и тёмный', '62110402': 'Шоколад мини-плитки',
    '63010503': 'Средства для мытья полов', '63010511': 'Универсальные чистящие средства', '63010506': 'Средства для сантехники',
    '63010507': 'Средства для стёкол и зеркал', '63010513': 'Средства от засоров', '63010515': 'Средства для плиты и микроволновки',
    '63010501': 'Средства для ковров', '63010508': 'Средства для мебели',
}
def block_for(code):
    if code in BLOCK_NAME:
        return BLOCK_NAME[code]
    return cat_name.get(code, f'Категория {code}').strip().capitalize()

rows = []
def add(pid, promo, date, episode, block, df):
    for _, r in df.iterrows():
        rows.append(dict(**{'ID акции': pid, 'Название акции': promo, 'Дата': date, 'Эпизод': episode,
                            'Блок-категория': block, 'SKU название': r['name'], 'SKU ID': r['SKU']}))

def by_codes(codes, name_re=None, excl_re=None):
    df = g[g.c5.isin(codes)]
    if name_re: df = df[df.name.str.contains(name_re, regex=True)]
    if excl_re: df = df[~df.name.str.contains(excl_re, regex=True)]
    return df

BEER = [
    ('Пиво светлое', by_codes(['61060504', '61060505', '61060104'])),
    ('Пиво тёмное', by_codes(['61060604', '61060605', '61060804'])),
    ('Пиво нефильтрованное', by_codes(['61060204', '61060205', '61060803'])),
    ('Пиво крафтовое и крепкое', by_codes(['61060801', '61060802', '61060702', '61060703'])),
    ('Пиво ароматизированное', by_codes(['61060304', '61060305'])),
    ('Пиво безалкогольное', by_codes(['61060404', '61060405'])),
]
PROSECCO = ('Просекко и игристое', by_codes(['61010605', '61010601', '61010602', '61010607', '61010701']))

# ---------------- СЕРИИ НЕДЕЛИ 40 (пт 02.10, черновики пушей 29.09.2026, версия 2) ----------------
SWEET = r'П/СЛ|ПЛ/СЛ|ПОЛУСЛ|\.СЛ\b|\bСЛ\.|СЛАД'
WINE_RED = by_codes([c for c in cat_name if c.startswith('610108')])
WINE_WHITE = by_codes([c for c in cat_name if c.startswith('610109')])
WINE_ROSE = by_codes([c for c in cat_name if c.startswith('610110')])
SPARK = by_codes(['61010605', '61010601', '61010602', '61010607', '61010701'])
def sweet(df): return df[df.name.str.contains(SWEET, regex=True)]
def dry(df): return df[~df.name.str.contains(SWEET, regex=True)]
S = [
 (101401, 'Тематическая рассылка вино и просекко', '02.10.2026', 'Эп.1 — Пары вина и сыра', [
    ('Пара 1 · Вино полусладкое: красное, белое, игристое', pd.concat([sweet(WINE_RED), sweet(WINE_WHITE), sweet(SPARK)])),
    ('Пара 1 · Сыр с голубой плесенью', by_codes(['66050105'])),
    ('Пара 2 · Вино белое сухое и полусухое', dry(WINE_WHITE)),
    ('Пара 2 · Сыр с белой плесенью: бри и камамбер', by_codes(['66050104'])),
    ('Пара 3 · Вино красное сухое и полусухое', dry(WINE_RED)),
    ('Пара 3 · Сыр твёрдый выдержанный: пармезан, чеддер, грана', by_codes(['66050403'])),
    ('Пара 4 · Просекко, брют и розовое вино', pd.concat([dry(SPARK), WINE_ROSE])),
    ('Пара 4 · Моцарелла', by_codes(['66050302'], excl_re='ПИЦЦ')),
 ]),
 (101402, 'Тематическая рассылка пиво', '02.10.2026', 'Эп.1 — Колбаски к пиву', [
    ('Купаты и колбаски для жарки', by_codes(['65120703', '65150503'], name_re='КУПАТ|КОЛБАСК', excl_re='КАРТОФ')),
    ('Колбаски копчёные и охотничьи', by_codes(['65020201', '65020208'], name_re='КОЛБАСК')),
    *BEER,
 ]),
]
for pid, promo, date, ep, blocks in S:
    seen = set()
    for blk, df in blocks:
        df = df[~df.SKU.isin(seen)]
        seen |= set(df.SKU)
        add(pid, promo, date, ep, blk, df)

# ---------------- АКЦИИ МЕСЯЦА ПО КОДАМ ----------------
d = json.load(open('output/cvm_offline_dump_2026-09-29.json'))
cvm = {r['НОМЕР']: r for r in d['CVM offline']}
OFFERS = ['101404', '101428_1', '101428_2', '101428_3', '101406']
for pid in OFFERS:
    r = cvm[pid]
    promo = r['Название промо']
    dd, mm = r['Старт акции'].strip('.').split('.')[:2]
    date = f'{dd}.{mm}.2026'
    codes = [c.strip() for c in str(r.get('Категория', '')).replace(',', '\n').split('\n') if c.strip().isdigit()]
    df = g[g.c5.isin(codes)].copy()
    df['block'] = df.c5.map(block_for)
    for blk, part in df.groupby('block', sort=False):
        add(pid, promo, date, None, blk, part)

out = pd.DataFrame(rows, columns=['ID акции', 'Название акции', 'Дата', 'Эпизод', 'Блок-категория', 'SKU название', 'SKU ID'])
dup = out.duplicated(['ID акции', 'Эпизод', 'SKU ID']).sum()
assert dup == 0, f'дубли: {dup}'
out.to_excel(f'output/подборки_октябрь_{VERSION}.xlsx', index=False, sheet_name='SKU по акциям октябрь')   # один файл месяца: акции + серии
print('строк всего:', len(out), '| акции:', out['Эпизод'].isna().sum(), '| серии:', out['Эпизод'].notna().sum())
summ = out.groupby(['ID акции', 'Название акции', 'Дата', 'Эпизод', 'Блок-категория'], dropna=False, sort=False).size()
print(summ.to_string())
