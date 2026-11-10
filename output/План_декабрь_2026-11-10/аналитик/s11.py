import pandas as pd, numpy as np
from eb import eb2
pd.set_option('display.width',250); pd.set_option('display.max_rows',300); pd.set_option('display.max_colwidth',90)
a=pd.read_pickle('promos.pkl')
u=a[a.ok].copy(); u['comm']=u.fam.str.startswith('Коммуникация')
for ch in ['офлайн','онлайн']:
  for comm in [False,True]:
    x=u[(u.ch==ch)&(u.comm==comm)]
    C,F,par,_=eb2(x,['seg','fam','dcl','ben','thr'],['ch','fam','dcl'])
    print('=====',ch,'comm' if comm else 'offers', round(par['tau_promo_all'],2), round(par['tau_cell'],2), round(par['Qc'],1), par['dfc'])
    print({k.replace(ch+' | ',''):v for k,v in par['tau_promo_by_fam'].items()})
    C=C.sort_values(['kf','post'],ascending=[True,False])
    print(C[['kc','n','N','r','post','lo','hi','B','rf']].assign(se0=np.sqrt(C.v0),tw=np.sqrt(C.tw)).round(2).to_string(index=False))
