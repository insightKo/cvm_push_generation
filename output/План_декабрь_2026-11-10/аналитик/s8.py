import pandas as pd
a=pd.read_pickle('promos.pkl')
a=a.sort_values(['ch','fam','seg','ID'])
for _,r in a.iterrows():
    print(f"{r.ID}|{r.seg[:22]}|{r.fam[:30]}|{r.ben}|{r.thr}|{r.dur}|{r.N:.0f}|{r.y:.2f}|{r.s:.2f}|{r.disc:.2f}|{r.excl[:20]}|{r['Название'][:40]}")
