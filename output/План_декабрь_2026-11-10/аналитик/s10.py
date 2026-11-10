import pandas as pd, numpy as np
from eb import eb
pd.set_option('display.width',250); pd.set_option('display.max_rows',300); pd.set_option('display.max_colwidth',85)
a=pd.read_pickle('promos.pkl')
u=a[a.ok & (a.ch=='офлайн') & ~a.fam.str.startswith('Коммуникация')].copy()
C,F,par,uu=eb(u,['ch','seg','fam','dcl','ben','thr'],['ch','seg','fam','dcl'],['ch','fam'])
print(par)
M=F.drop_duplicates('km')[['km','nm','nfam','rm','vm','pm','vpm','Bm']]
print(M.round(3).to_string())
print(F[['kf','nf','ncell','rf','vf','pf','vpf','Bf']].round(3).sort_values('kf').to_string())
