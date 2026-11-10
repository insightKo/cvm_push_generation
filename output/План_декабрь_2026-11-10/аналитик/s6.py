exec(open('load.py', encoding='utf-8').read())
cg=pd.read_pickle('cg.pkl')
a = df[df.CHANNEL_ID==0].set_index('ID')
g=cg.groupby(['ID_PROMO','CHANNEL_ID','SEGMENT_ID']).agg(N=('CLIENTS_TG','sum'),ADD=('ADD_TO','sum'),SE2=('ADD_TO_SE',lambda s:(s**2).sum())).reset_index()
p=g.pivot_table(index='ID_PROMO',columns=['CHANNEL_ID','SEGMENT_ID'],values=['N','ADD'],aggfunc='sum').fillna(0)
# compare channel sums to I_CVM_EFFECT ADD3
s=g.groupby(['ID_PROMO','CHANNEL_ID']).ADD.sum().unstack().fillna(0)
s['tot']=s.sum(axis=1)
s['eff_add3']=a['Доп ТО итого, ₽']
s['eff_all3']=a['Доп ТО итого на всю ЦГ, ₽']
s['diff']=s.tot-s.eff_add3
print(s.describe())
print(s[abs(s['diff'])>1000].head(20))
off=s[s.index<110000]
print('offline promos: share of ch3 in |ADD|', (off[3].abs().sum()/ (off[1].abs().sum())))
print(off[[1,3]].sum())
# segment composition of offline promos by channel-1 segment
n1=g[(g.CHANNEL_ID==1)].pivot_table(index='ID_PROMO',columns='SEGMENT_ID',values='N',aggfunc='sum').fillna(0)
n1['tot']=n1.sum(axis=1)
comp=(n1[[1,2,5,6]].div(n1.tot,axis=0)*100).round(1)
comp['hdr']=a['Сегмент']; comp['N']=n1.tot
pd.set_option('display.max_rows',300)
print(comp.groupby('hdr')[[1,2,5,6,'N']].mean().round(1))
print(comp[comp.hdr.isin(['Отток','Случайные','Спящие','Новые','Случайные Отток Спящие','Отток Спящие','ВСЕ оффлайн','Все оффлайн','Активные Новые'])].sort_index().to_string())
