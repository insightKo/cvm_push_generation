exec(open('load.py', encoding='utf-8').read())
a = df[df.CHANNEL_ID==0].copy()
for m,g in a.groupby('Механика', dropna=False):
    if m=='Коммуникация': continue
    print('==',m)
    for _,r in g.sort_values('ID').iterrows():
        print('  ',r.ID, r['Название'], '|', r['Сегмент'], '|', r['Дата старта'], r['Дата окончания'], '| кампании не найдены' if r['Кампании не найдены'] else '')
