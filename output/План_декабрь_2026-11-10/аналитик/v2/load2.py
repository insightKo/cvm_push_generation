# v2: одна таблица акций (CHANNEL_ID = 0) из двух источников:
#   выгрузка I_CVM_EFFECT 10.11 (старты 03.10.2025–30.07.2026) и снимок авг–окт 2026 (посчитан 25.10–05.11, до перезаливки 07.11).
# В снимке нет ДИ: ошибка восстановлена моделью ошибки по выгрузке (ln SE ~ ln N + ln(КГ/ЦГ) + ln окна + ln доли купивших),
# доля КГ — по соотношению исключённых правилом «больше 5 чеков в день» (на выгрузке совпадает с фактом: медиана откл. 0, sd 0,10 в ln),
# затем ошибка зажата в полосу метки значимости итога из снимка.
import json, re
import numpy as np, pandas as pd

D = r"C:/Projects/cvm_push_generation/output/План_декабрь_2026-11-10/"
OLD = D + "I_CVM_EFFECT_выгрузка_2026-11-10.jsonl"
NEW = D + "гкг/факт_эффекта_авг-окт2026_снимок_2026-11-06.tsv"
BANDS = {'Незначимо': (0, 1.645), 'Слабо значимо': (1.645, 1.96), 'Значимо': (1.96, 2.576),
         'Сильно значимо': (2.576, 3.291), 'Очень сильно значимо': (3.291, np.inf)}


def band(lbl):
    lbl = str(lbl)
    for k in ['Очень сильно значимо', 'Сильно значимо', 'Слабо значимо', 'Незначимо', 'Значимо']:
        if lbl.startswith(k):
            return BANDS[k]
    return (np.nan, np.nan)


def load_old():
    df = pd.DataFrame([json.loads(l) for l in open(OLD, encoding='utf-8')])
    a = df[df.CHANNEL_ID == 0].copy()
    o = pd.DataFrame({
        'ID': a.ID.astype(int), 'name': a['Название'], 'start': pd.to_datetime(a['Дата старта']), 'fin': pd.to_datetime(a['Дата окончания']),
        'hdr': a['Сегмент'].astype(str), 'mech': a['Механика'].astype(str), 'send': a['Канал'].fillna(''),
        'N': a['Клиентов ЦГ'].astype(float), 'Ne': a['В оценке ЦГ'].astype(float),
        'add_in': a['Доп ТО в акцию, ₽'], 'add': a['Доп ТО итого, ₽'],
        'se': (a['Доп ТО итого ДИ до, ₽'] - a['Доп ТО итого ДИ от, ₽']) / 3.92,
        'se_in': (a['Доп ТО в акцию ДИ до, ₽'] - a['Доп ТО в акцию ДИ от, ₽']) / 3.92,
        'disc': a['Скидка ЦГ, ₽'].fillna(0), 'disc_all': a['Скидка всего, ₽'].fillna(0),
        'sig': a['Значимость итого'], 'kgn': a['КГ несопоставима'], 'nokg': a['Нет КГ'].fillna(False).astype(bool),
        'prelim': a['Предварительная'].fillna(False).astype(bool), 'nocamp': a['Кампании не найдены'].fillna(False).astype(bool),
        'after_no': a['После не считается'].fillna(False).astype(bool),
        'r': a['Клиентов КГ'] / a['Клиентов ЦГ'], 'buy': a['Купили в акцию ЦГ %'].astype(float),
        'calc': a['Дата расчета'], 'note': a['Пометки'].fillna('')})
    o['src'] = 'выгрузка 10.11'
    o['se_src'] = 'ДИ витрины'
    return o


def load_new():
    d = pd.read_csv(NEW, sep='\t')
    d = d[d.CHANNEL_ID == 0]
    ex = d['Пометки'].fillna('').str.extract(r'исключено ЦГ (\d+), КГ (\d+)').astype(float)
    o = pd.DataFrame({
        'ID': d.ID.astype(int), 'name': d['Название'], 'start': pd.to_datetime(d['Дата старта']), 'fin': pd.to_datetime(d['Дата окончания']),
        'hdr': d['Сегмент'].astype(str), 'mech': d['Механика'].astype(str), 'send': '',
        'N': d['Клиентов ЦГ'].astype(float), 'Ne': d['В оценке ЦГ'].astype(float),
        'add_in': d['Доп ТО в акцию, ₽'], 'add': d['Доп ТО итого, ₽'], 'se': np.nan, 'se_in': np.nan,
        'disc': d['Скидка ЦГ, ₽'].fillna(0), 'disc_all': d['Скидка ЦГ, ₽'].fillna(0),
        'sig': d['Значимость итого'], 'sig_in': d['Значимость в акцию'], 'kgn': d['КГ несопоставима'], 'nokg': False,
        'prelim': d['Предварительная'].astype(bool), 'nocamp': False,
        'after_no': d['Пометки'].fillna('').str.contains('«после» не считается'),
        'ex_cg': ex[0].values, 'ex_kg': ex[1].values, 'buy': d['Купили в акцию ЦГ %'].astype(float),
        'calc': d['Дата расчета'], 'note': d['Пометки'].fillna('')})
    o['src'] = 'снимок 06.11'
    return o


def se_model(old):
    o = old[(old.ID < 110000) & old.se.notna() & (old.se > 0) & (old.buy > 0) & (old.kgn == False) & (old.Ne / old.N >= 0.5)].copy()
    o['win'] = (o.fin - o.start).dt.days + 1 + np.where(o.after_no, 0, 4)
    X = np.column_stack([np.ones(len(o)), np.log(o.N), np.log(o.r), np.log(o.win), np.log(o.buy)])
    y = np.log(o.se / o.N)
    b = np.linalg.lstsq(X, y, rcond=None)[0]
    e = y - X @ b
    return b, float(e.std()), len(o), float(1 - e.var() / y.var())


def kg_ratio_rule(hdr):
    h = hdr.lower()
    if any(k in h for k in ('отток', 'спящ', 'случайн')):
        return 1 / 9
    return 5 / 95


def fill_new_se(new, b):
    n = new.copy()
    n['win'] = (n.fin - n.start).dt.days + 1 + np.where(n.after_no, 0, 4)
    n['r_rule'] = n.hdr.map(kg_ratio_rule)
    n['r_ex'] = n.ex_kg / n.ex_cg
    use_ex = (n.ex_cg >= 100) & (n.ex_kg >= 10)
    n['r'] = np.where(use_ex, n.r_ex, n.r_rule)
    n['r_src'] = np.where(use_ex, 'по исключённым правилом 5 чеков', 'по правилу доли КГ (5 % / 10 %)')
    X = np.column_stack([np.ones(len(n)), np.log(n.N), np.log(n.r), np.log(n.win), np.log(n.buy.clip(lower=0.1))])
    n['se_model'] = np.exp(X @ b) * n.N
    lo_z = n.sig.map(lambda s: band(s)[0]); hi_z = n.sig.map(lambda s: band(s)[1])
    a = n['add'].abs()
    se_min = np.where(np.isfinite(hi_z), a / hi_z, 0)              # |z| ≤ верх полосы → SE ≥ |Доп ТО| / верх
    se_max = np.where(lo_z > 0, a / lo_z, np.inf)                  # |z| ≥ низ полосы → SE ≤ |Доп ТО| / низ
    n['se'] = np.clip(n.se_model, se_min, se_max)
    n['se_src'] = np.where(np.isclose(n.se, n.se_model), 'модель ошибки (в полосе метки)',
                           'модель ошибки, зажата в полосу метки «' + n.sig.astype(str).str.slice(0, 22) + '»')
    return n


def load_all():
    old = load_old()
    b, rsd, nfit, r2 = se_model(old)
    new = fill_new_se(load_new(), b)
    cols = ['ID', 'name', 'start', 'fin', 'hdr', 'mech', 'send', 'N', 'Ne', 'add_in', 'add', 'se', 'disc', 'disc_all', 'sig', 'kgn', 'nokg', 'prelim',
            'nocamp', 'after_no', 'r', 'buy', 'calc', 'note', 'src', 'se_src']
    A = pd.concat([old[cols], new[cols + ['se_model', 'r_src', 'r_rule']]], ignore_index=True)
    assert A.ID.is_unique
    A['dur'] = (A.fin - A.start).dt.days + 1
    A['y'] = A['add'] / A.N          # ₽ Доп ТО итого на клиента СПИСКА ЦГ
    A['s'] = A.se / A.N
    A['d'] = A.disc / A.N            # скидка ЦГ на клиента списка
    A['ch'] = np.where(A.ID >= 110000, 'онлайн', 'офлайн')
    # исключения из коэффициентов — как в v1
    def reason(r):
        if r.nokg or pd.isna(r['add']):
            return 'нет КГ в оценке'
        if r.kgn is True or r.kgn == 'True':
            return 'КГ несопоставима'
        if r.kgn is None or (isinstance(r.kgn, float) and np.isnan(r.kgn)):
            return 'проверка «до старта» не проведена'
        if r.Ne / r.N < 0.5:
            return f'в оценке {r.Ne / r.N:.1%} ЦГ'
        return ''
    A['excl'] = A.apply(reason, axis=1)
    A['ok'] = A.excl == ''
    return A, dict(b=b.round(3).tolist(), resid_sd=round(rsd, 3), n=nfit, r2=round(r2, 3))


if __name__ == '__main__':
    A, m = load_all()
    print(m)
    A.to_pickle('A.pkl')
    pd.set_option('display.width', 250); pd.set_option('display.max_rows', 200)
    n = A[A.src == 'снимок 06.11']
    print(n[['ID', 'hdr', 'N', 'r', 'buy', 'y', 's', 'se_model', 'se', 'sig', 'se_src']].assign(se_model=lambda x: x.se_model / x.N).round(3).to_string())
    print(A.excl.value_counts())
