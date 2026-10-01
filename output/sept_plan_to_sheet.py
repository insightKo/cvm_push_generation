"""Перезаписать заготовку сентября 2026 в Google Sheets («CVM offline» + PUSH) из output/план_сентябрь_2026_заготовка.json.
Удаляет прежние строки заготовки (НОМЕР из диапазона плана; PUSH-строки с этими номерами) и пишет заново. Запуск: python output/sept_plan_to_sheet.py"""
import json, datetime as dt, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent; sys.path.insert(0,str(ROOT))
import gspread, sheets_client as sc

plan=json.load(open(ROOT/"output/план_сентябрь_2026_заготовка.json",encoding="utf-8"))
def is_series(p): return p["channel"]=="slip" or p["mech"]=="Коммуникация"
order={"slip":0,"Коммуникация":1}
P=sorted(plan["promos"],key=lambda p:(not is_series(p),p["start"],p["end"],p["name"].startswith("50%"),p["name"]))
for i,p in enumerate(P): p["num"]=101360+i
plan["promos"]=P
json.dump(plan,open(ROOT/"output/план_сентябрь_2026_заготовка.json","w",encoding="utf-8"),ensure_ascii=False,indent=1)
NUMS={p["num"] for p in P}; OLD=set(range(101360,101360+60))

ss=sc.get_spreadsheet(); wo=ss.worksheet(sc.SHEET_CVM_OFFLINE); wp=ss.worksheet(sc.SHEET_PUSH)
# --- CVM offline: удалить старые строки заготовки ---
v=wo.get_all_values(); hdr=v[0]
old_rows=[i for i,r in enumerate(v[1:],start=2) if r[1].isdigit() and int(r[1]) in OLD]
def batch_delete(ws,rows):
    if not rows: return
    rows=sorted(rows,reverse=True); reqs=[]
    while rows:
        end=rows[0]; start=end
        while len(rows)>1 and rows[1]==start-1: rows.pop(0); start-=1
        rows.pop(0); reqs.append({"deleteDimension":{"range":{"sheetId":ws.id,"dimension":"ROWS","startIndex":start-1,"endIndex":end}}})
    ss.batch_update({"requests":reqs})
batch_delete(wo,old_rows)
print("CVM offline: удалено",len(old_rows))
DOW=["пн","вт","ср","чт","пт","сб","вс"]
d=dt.date.fromisoformat; ddm=lambda s:f"{d(s).day:02d}.{d(s).month:02d}."
cnt={"Активные. Мамы":"1 070 000","Активные. Зоо":"192 000","Активные. Перекус":"485 000","Активные. ПП":"143 000","Активные. Пиво и П/ф":"750 000","Активные. Вино, Активные. Просекко":"150 000","Активные, Новые, Спящие, Отток":"3 986 000","Активные, Новые":"2 770 000","Отток, Спящие":"1 345 000","Случайные":"1 345 000"}
slipcnt={"spirits":"208 000","wine":"106 000","beer":"1 079 000"}
rows=[]
for p in P:
    s,e=d(p["start"]),d(p["end"]); num=p["num"]
    r={h:"" for h in hdr}
    comm=p["mech"]=="Коммуникация"
    r.update({"Настройка":"нет" if (comm or p["name"]=="Баланс баллов") else "да","НОМЕР":num,"Название промо":p["name"],"Сегмент":p["segment"],"Год":2026,"Месяц":9,
      "Неделя":s.isocalendar()[1],"День недели":DOW[s.weekday()],"Старт акции":ddm(p["start"]),"Окончание акции":ddm(p["end"]),"Каналы коммуникации":p["channel"],
      "Название МС":f"{num}_{p['name']}","Примерное количество клиентов":slipcnt.get(p.get("cat_key"),"") if p["channel"]=="slip" else cnt.get(p["segment"],""),
      "Механика":p["mech"],"Категория":p.get("codes","") if not comm and p["channel"]=="PUSH" else p["cat"],
      "Ограничения и комментарии":"ЗАГОТОВКА: "+p["why"]+((" · "+p["cat"]) if p["cat"] and not comm else "")})
    if "монет" in p["name"]: r["Бонусы"]="50" if "50 монет" in p["name"] else "100"; r["Срок сгорания бонусов"]="14.09.2026 23:59:00"
    if "кешбэка" in p["name"]: r["Бонусы"]="50%" if p["name"].startswith("50%") else "20%"; r["Срок сгорания бонусов"]=(e+dt.timedelta(days=7)).strftime("%d.%m.%Y 23:59:00")
    if p["name"].startswith("Купи 2 раза"): r["Бонусы"]="300"
    if p["channel"]=="PUSH" and not comm and "Баланс" not in p["name"]: r["Купон"]="да"
    rows.append([r[h] for h in hdr])
wo.append_rows(rows,value_input_option="USER_ENTERED"); print("CVM offline: добавлено",len(rows))
# --- PUSH ---
pv=wp.get_all_values(value_render_option="FORMULA")
old_push=[i for i,r in enumerate(pv[1:],start=2) if len(r)>11 and str(r[11]).isdigit() and int(r[11]) in OLD]
batch_delete(wp,old_push)
print("PUSH: удалено",len(old_push))
pv=wp.get_all_values(value_render_option="FORMULA")
start=next((i for i,r in enumerate(pv[1:],start=2) if not any(str(c).strip() for c in (r[7:8]+r[11:13]))),len(pv)+1)
pushes=sorted([(d(m["date"]),p["num"],int(m["msg"])) for p in P for m in p["push"]])
def frow(i,date,num,msg):
    ser=(date-dt.date(1899,12,30)).days
    return [f"=VLOOKUP(L{i};'CVM offline'!B:LO;3;FALSE)",f"=VLOOKUP(L{i};'CVM offline'!B:LO;10;FALSE)",f"=VLOOKUP(L{i};'CVM offline'!B:LO;2;FALSE)",f"=YEAR(H{i})",f"=MONTH(H{i})",f"=WEEKNUM(H{i};2)",f"=VLOOKUP(WEEKDAY(H{i};2);'данные'!A:B;2;FALSE)",ser,0.4583333333333333,f"=VLOOKUP(L{i};'CVM offline'!B:N;13;false)","минус фрод",num,msg,"",f"=LEN(N{i})","",f"=LEN(P{i})","","",""]
vals=[frow(start+k,*x) for k,x in enumerate(pushes)]; end=start+len(vals)-1
if end>wp.row_count: wp.add_rows(end-wp.row_count)
wp.update(range_name=f"A{start}:T{end}",values=vals,value_input_option="USER_ENTERED")
ss.batch_update({"requests":[{"copyPaste":{"source":{"sheetId":wp.id,"startRowIndex":467,"endRowIndex":468,"startColumnIndex":0,"endColumnIndex":20},"destination":{"sheetId":wp.id,"startRowIndex":start-1,"endRowIndex":end,"startColumnIndex":0,"endColumnIndex":20},"pasteType":"PASTE_FORMAT"}}]})
print(f"PUSH: строки {start}-{end} = {len(vals)} сообщений")
# --- проверка дублей ---
v=wo.get_all_values(); nums=[r[1] for r in v[1:] if r[1].isdigit()]
import collections
dup=[n for n,c in collections.Counter(nums).items() if c>1]; print("дубли НОМЕР в CVM offline:",dup or "нет")
sept=[r for r in v[1:] if r[1].isdigit() and int(r[1]) in NUMS]; print("строк сентября в CVM offline:",len(sept),"ожидалось",len(P))
pv=wp.get_all_values(); sp=[r for r in pv[1:] if len(r)>11 and r[11].isdigit() and int(r[11]) in NUMS]
c=collections.Counter((r[11],r[12]) for r in sp); print("дубли (промо,msg) в PUSH:",[k for k,x in c.items() if x>1] or "нет"); print("push-строк сентября:",len(sp),"ожидалось",len(pushes))
