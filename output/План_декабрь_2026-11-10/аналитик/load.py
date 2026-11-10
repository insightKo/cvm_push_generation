import json, pandas as pd, numpy as np
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 60); pd.set_option('display.max_rows', 600); pd.set_option('display.max_colwidth', 60)
P = r"C:/Projects/cvm_push_generation/output/План_декабрь_2026-11-10/I_CVM_EFFECT_выгрузка_2026-11-10.jsonl"
rows = [json.loads(l) for l in open(P, encoding='utf-8')]
df = pd.DataFrame(rows)
