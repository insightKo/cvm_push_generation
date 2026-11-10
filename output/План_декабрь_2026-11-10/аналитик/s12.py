import pandas as pd, numpy as np
pd.set_option('display.width',250); pd.set_option('display.max_rows',400)
a=pd.read_pickle('promos.pkl'); cg=pd.read_pickle('cg.pkl')
# сегментный срез: офлайн-база (CHANNEL_ID 1) + онлайн-база (3) того же SEGMENT_ID; клиенты — по офлайн-базе для офлайн-акций
g=cg.groupby(['ID_PROMO','CHANNEL_ID','SEGMENT_ID']).agg(N=('CLIENTS_TG','sum'),ADD=('ADD_TO','sum'),SE2=('ADD_TO_SE',lambda s:(s**2).sum())).reset_index()
off=g[g.ID_PROMO<110000]
p1=off[off.CHANNEL_ID==1].set_index(['ID_PROMO','SEGMENT_ID'])
p3=off[off.CHANNEL_ID==3].set_index(['ID_PROMO','SEGMENT_ID'])
s=p1[['N','ADD','SE2']].join(p3[['ADD','SE2']],rsuffix='3',how='left').fillna(0).reset_index()
s['ADDt']=s.ADD+s.ADD3; s['SEt']=np.sqrt(s.SE2+s.SE23)
s=s.merge(a[['ID','seg','fam','dcl','ben','thr','ok','ym']],left_on='ID_PROMO',right_on='ID')
s=s[s.ok]
s['y1']=s.ADD/s.N; s['yt']=s.ADDt/s.N
s.to_pickle('segsplit.pkl')
segn={1:'Новые',2:'Активные',5:'Отток',6:'Спящие'}
s['sg']=s.SEGMENT_ID.map(segn)
fams=['Начисление N монет без порога','Вернём N р. монетами с чека от X','Скидка N р. на чек от X','Монеты N на чек от X (целевые)','Кешбэк X% на категорию','Купон N р. на покупку (слип/пуш)','Скидка X% на категорию','Коммуникация: баланс монет','Коммуникация: тематическая / подборка']
for f in fams:
    x=s[(s.fam==f)]
    print('====',f)
    t=x.groupby(['seg','dcl','sg']).agg(n=('ID','nunique'),N=('N','sum'),ADD=('ADDt','sum'),ADD1=('ADD','sum'),SE2=('SEt',lambda v:(v**2).sum())).reset_index()
    t['pc']=t.ADD/t.N; t['pc_off']=t.ADD1/t.N; t['se']=np.sqrt(t.SE2)/t.N; t['z']=t.pc/t.se
    print(t[['seg','dcl','sg','n','N','pc','pc_off','se','z']].round(2).to_string(index=False))
