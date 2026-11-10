import json, pandas as pd
rows=[json.loads(l) for l in open('cg_layers.jsonl',encoding='utf-8') if l.startswith('{"t": "cg"')]
cg=pd.DataFrame(rows); print(cg.shape); print(cg.isna().sum()); print(cg.X_BAND.value_counts().head(20)); print(cg.groupby(['CHANNEL_ID','SEGMENT_ID']).size())
cg.to_pickle('cg.pkl')
