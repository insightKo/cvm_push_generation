import pandas as pd, numpy as np
from eb import eb, sig_label
pd.set_option('display.width',250); pd.set_option('display.max_rows',300); pd.set_option('display.max_colwidth',85)
a=pd.read_pickle('promos.pkl')
u=a[a.ok].copy()
u['comm']=u.fam.str.startswith('Коммуникация')
for ch in ['офлайн','онлайн']:
  for comm in [False, True]:
    x=u[(u.ch==ch)&(u.comm==comm)]
    C,F,par,_=eb(x,['ch','seg','fam','dcl','ben','thr'],['ch','seg','fam','dcl'],['ch','fam'])
    print('=====',ch,'comm' if comm else 'offers', {k:(round(v,2) if not isinstance(v,dict) else v) for k,v in par.items()})
    C=C.sort_values('post',ascending=False)
    C['kc']=C.kc.str.replace(ch+' | ','')
    print(C[['kc','n','N','r','post','lo','hi','Bc','Bf','tw']].assign(se0=np.sqrt(C.v0),tw=np.sqrt(C.tw)).round(2).to_string(index=False))
