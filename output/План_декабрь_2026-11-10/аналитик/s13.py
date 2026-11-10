import pandas as pd, numpy as np
from eb import eb3 as eb
pd.set_option('display.width',250); pd.set_option('display.max_rows',400); pd.set_option('display.max_colwidth',80)
U=pd.read_pickle('units.pkl')
U['comm']=U.fam.str.startswith('Коммуникация')
out=[]
for ch in ['офлайн','онлайн']:
  for comm in [False,True]:
    x=U[(U.ch==ch)&(U.comm==comm)]
    C,F,par,_=eb(x,['ch','tseg','fam','dcl','ben','thr'],['ch','tseg','fam','dcl'],['ch','fam','dcl'])
    print('=====',ch,'comm' if comm else 'offers', 'tau_cell',round(par['tau_cell'],2),'tau_fam_all',round(par['tau_fam_all'],2),'tau_mech',round(par['tau_mech'],2),'mu',round(par['mu'],2))
    C=C.sort_values('post',ascending=False)
    C['kc']=C.kc.str.replace(ch+' | ','')
    print(C[['kc','n','N','r','rf','pf','post','lo','hi','Bc','Bf','tf']].assign(se0=np.sqrt(C.v0),tw=np.sqrt(C.tw),tf=np.sqrt(C.tf)).round(2).to_string(index=False))
