import pandas as pd, numpy as np
L=pd.read_pickle('layers.pkl')
FAM = {'Монеты, список Активных': (L.fam == 'Начисление N монет без порога') & (L.seg == 'Активные') & (L.dcl == '2–14 дн.'),
       'Монеты, список Отток+Спящие': (L.fam == 'Начисление N монет без порога') & (L.seg == 'Отток+Спящие (список 2026)'),
       'Скидка N р. на чек, Активные': (L.fam == 'Скидка N р. на чек от X') & (L.seg == 'Активные'),
       'Целевые монеты, Активные': (L.fam == 'Монеты N на чек от X (целевые)') & (L.seg == 'Активные'),
       'Вернём 300/1000, Отток+Спящие': (L.fam == 'Вернём N р. монетами с чека от X') & L.seg.isin(['Отток+Спящие (список 2026)']),
       'Вернём 300/1000 без 101075/101076': (L.fam == 'Вернём N р. монетами с чека от X') & L.seg.isin(['Отток+Спящие (список 2026)','Спящие']) & ~L.ID_PROMO.isin([101075,101076]),
       'Кешбэк 20% кат., Активные': (L.fam == 'Кешбэк X% на категорию') & (L.seg == 'Активные') & (L.ben == 20),
       'Кешбэк 50% кат., Отток+Спящие': (L.fam == 'Кешбэк X% на категорию') & (L.seg == 'Отток+Спящие (список 2026)')}
def f(h,tot):
    n=h.CLIENTS_TG.sum(); return f"{h.ADD_TO.sum()/n:6.2f}±{1.96*np.sqrt((h.ADD_TO_SE**2).sum())/n:5.2f} ({n/tot:4.0%})"
for nm,m in FAM.items():
    x=L[m]
    for sid,h in x.groupby('SEGMENT_ID'):
        tot=h.CLIENTS_TG.sum()
        if tot<50000: continue
        wk=h.WEEK_FROM
        if sid in (1,2): grp={'нед.1':wk==1,'нед.2–3':wk.between(2,3),'нед.4–6':wk.between(4,6)}
        elif sid==5: grp={'нед.7–9':wk.between(7,9),'нед.10–12':wk.between(10,12),'нед.13–15':wk.between(13,15)}
        else: grp={'16–26 нед.':wk<=26,'27–52 нед.':wk.between(27,52),'53+ нед.':wk>=53}
        parts=' | '.join(f'{k} {f(h[v],tot)}' for k,v in grp.items() if v.any())
        terc=' | '.join(f'{t} {f(h[h.terc==t],tot)}' for t in ['T1 низ','T2 середина','T3 верх'])
        print(f'{nm} | сегм {sid} | всего {f(h,tot)} N={tot:,.0f} акций {h.ID_PROMO.nunique()}\n    давность: {parts}\n    сумма «до»: {terc}')
        # лучшая комбинация: верхняя терциль × поздние недели
