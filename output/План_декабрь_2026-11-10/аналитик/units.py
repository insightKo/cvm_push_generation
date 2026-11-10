# Единицы для коэффициентов: акция × сегмент. ₽ на клиента в оценке; покрытие (в оценке / список ЦГ) — отдельно.
import numpy as np, pandas as pd

a = pd.read_pickle('promos.pkl')
s = pd.read_pickle('segsplit.pkl')          # офлайн-акции: Доп ТО по сегменту давности на старте (база офлайн + онлайн того же сегмента)
SEGN = {1: 'Новые', 2: 'Активные', 5: 'Отток', 6: 'Спящие'}
HDR_SPLIT = {'Активные': {1: 'Новые', 2: 'Активные (вкл. ВЦ/LFL)'},
             'Новые': {1: 'Новые', 2: 'Новые'},
             'Отток+Спящие (список 2026)': {1: 'Отток', 2: 'Отток', 5: 'Отток', 6: 'Спящие'},
             'Отток (алкоголь)': {1: 'Отток', 2: 'Отток', 5: 'Отток', 6: 'Спящие'},
             'Спящие': {6: 'Спящие'},
             'Случайные/Отток/Спящие (смешанный)': {5: 'Отток', 6: 'Спящие'},
             'ВСЕ офлайн (широкий список)': {1: 'Новые', 2: 'Активные (вкл. ВЦ/LFL)', 5: 'Отток', 6: 'Спящие'}}


def build_units():
    ok = a[a.ok]
    U = []
    # офлайн: сегменты давности из слоёв
    for (pid, hdr), g in s.groupby(['ID_PROMO', 'seg']):
        if hdr not in HDR_SPLIT:
            continue
        mp = HDR_SPLIT[hdr]
        if hdr in ('Отток+Спящие (список 2026)', 'Отток (алкоголь)'):
            # вернувшиеся до старта (Новые/Активные на старте) — их Доп ТО раскладываем поровну на клиента списка Оттока и Спящих
            ret = g[g.SEGMENT_ID.isin([1, 2])]; base = g[g.SEGMENT_ID.isin([5, 6])]
            Ntot = g.N.sum(); rpc = ret.ADDt.sum() / Ntot; rse = np.sqrt((ret.SEt ** 2).sum()) / Ntot
            for sid, t in ((5, 'Отток'), (6, 'Спящие')):
                h = base[base.SEGMENT_ID == sid]
                if len(h) and h.N.sum() > 0:
                    n = h.N.sum()
                    U.append(dict(ID=pid, tseg=t, src='слои', N=n, add=h.ADDt.sum() + rpc * n,
                                  se=np.sqrt((h.SEt ** 2).sum() + (rse * n) ** 2)))
            continue
        g = g.assign(tseg=g.SEGMENT_ID.map(mp)).dropna(subset=['tseg'])
        for t, h in g.groupby('tseg'):
            U.append(dict(ID=pid, tseg=t, src='слои', N=h.N.sum(), add=h.ADDt.sum(), se=np.sqrt((h.SEt ** 2).sum())))
    # офлайн по шапке (не сегмент давности) и весь онлайн — итог акции «Все каналы»
    hdr_rows = ok[(ok.ch == 'онлайн') | ~ok.seg.isin(list(HDR_SPLIT))]
    for _, r in hdr_rows.iterrows():
        U.append(dict(ID=r.ID, tseg=r.seg, src='итог акции', N=r['Клиентов ЦГ в акцию'], add=r.add3, se=r.se3))
    U = pd.DataFrame(U)
    U = U[U.N > 0]
    U = U.merge(ok[['ID', 'ch', 'seg', 'fam', 'dcl', 'ben', 'thr', 'dur', 'ym', 'start', 'fin', 'cov', 'disc', 'pl', 'N', 'nocamp', 'sig_n', 'Название']]
                .rename(columns={'N': 'Nlist', 'seg': 'hdr'}), on='ID')
    U['y'] = U['add'] / U.N
    U['s'] = U.se / U.N
    # скидка и PL на клиента — по акции целиком (по сегментам их нет)
    return U


if __name__ == '__main__':
    U = build_units()
    pd.set_option('display.width', 220); pd.set_option('display.max_rows', 200)
    print(U.groupby(['ch', 'tseg']).agg(n=('ID', 'nunique'), N=('N', 'sum')).to_string())
    # покрытие списка ЦГ клиентами в оценке по шапке
    ok = a[a.ok]
    cv = ok.groupby('seg').apply(lambda g: g.Ne.sum() / g.N.sum())
    print(cv.round(3).to_string())
    U.to_pickle('units.pkl')
