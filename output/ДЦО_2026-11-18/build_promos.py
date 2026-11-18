# ДЦО 18.11: 65 акций из подбора v3 (final.json сессии кластеров) → promos18.json (номер, имя, имя ML, заголовок, GOLD) и slots18.tsv для раздачи.
import json, sys, io, re, collections
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
F = json.load(open("final_b33.json", encoding="utf-8"))
# номер по порядку: короткое имя для I_PROMO и Google, хвост заголовка пуша
N = [
 ("Ролл Take it", "второй ролл Take it"), ("Батончик Big Picnic", "батончик Big Picnic"),
 ("Булочка с сыром ХБИ", "вторую булочку с сыром"), ("Чипсы Русская картошка", "вторую Русскую картошку"),
 ("Напиток Псыж", "напиток Псыж"), ("Хурма", "хурму"), ("Печенье Яшкино", "печенье Яшкино"),
 ("Шоколад Kinder", "шоколад Kinder"), ("Семечки OPNGO", "вторые семечки OPNGO"), ("Семечки Джинн", "семечки Джинн"),
 ("Конфеты Рот Фронт", "конфеты Рот Фронт"), ("Мармелад Французский сад", "мармелад Французский сад"),
 ("Сыр тёртый Стародуб", "второй сыр Стародуб"), ("Маслины Iberica", "вторые маслины Iberica"),
 ("Свёкла варёная", "варёную свёклу"), ("Креветки Мореслав", "креветки Мореслав"), ("Огурцы Дядя Ваня", "огурцы Дядя Ваня"),
 ("Огурцы Круглое лето", "огурцы Круглое лето"), ("Горошек Дядя Ваня", "горошек Дядя Ваня"),
 ("Полотенца бумажные Papia", "полотенца Papia"), ("Губки Big City", "губки Big City"), ("Салфетки Pero", "салфетки Pero"),
 ("Synergetic для посуды", "Synergetic для посуды"), ("Гель для стирки Persil", "гель Persil"),
 ("Domestos для унитаза", "Domestos для унитаза"), ("Свёкла", "свёклу"), ("Помело", "помело"),
 ("Шпикачки Бородина", "шпикачки Бородина"), ("Мороженое 48 копеек", "мороженое 48 копеек"),
 ("Творожная масса", "вторую творожную массу"), ("Салат Айсберг", "салат Айсберг"),
 ("Колбаса Брауншвейгская", "брауншвейгскую колбасу"), ("Крылышки копчёные", "копчёные крылышки"),
 ("Студень Мясолюбов", "студень Мясолюбов"), ("Грудинка Папа может", "грудинку Папа может"),
 ("Колбаса Филейская Вязанка", "Филейскую Вязанку"), ("Кофе MacCoffee 3в1", "MacCoffee 3в1"),
 ("Нектар детский", "детский нектар"), ("Булочки для бургера", "булочки для бургера"), ("Сдоба Венская Дарница", "сдобу Дарница"),
 ("Корм Brit для собак", "корм Brit для собак"), ("Порошок Бимакс", "порошок Бимакс"), ("Прокладки Bella", "прокладки Bella"),
 ("Domestos универсальный", "Domestos универсальный"), ("Мыло Absolut", "мыло Absolut"), ("Мыло Absolut", "мыло Absolut"),
 ("Пюре фруктовое детское", "второе детское пюре"), ("Kinder Сюрприз", "второй Kinder Сюрприз"), ("Макароны Barilla", "макароны Barilla"),
 ("Чай Greenfield", "чай Greenfield"), ("Подушечки Любятово", "подушечки Любятово"), ("Зелень ассорти", "зелень"),
 ("Минеральная вода Рычал-Су", "Рычал-Су"), ("Попкорн Ouch", "второй попкорн Ouch!"), ("Томатная паста Круглое лето", "вторую томатную пасту"),
 ("Вареники Сибирская коллекция", "вторые вареники"), ("Пюре Доширак", "пюре Доширак"), ("Пюре Доширак", "пюре Доширак"),
 ("Палочки кукурузные Кузя", "палочки Кузя"), ("Корнишоны Круглое лето", "корнишоны Круглое лето"),
 ("Лакомство Dreamies", "лакомство Dreamies"), ("Торт Невские берега", "торт Невские берега"), ("Торт Фили Бейкер", "торт Фили Бейкер"),
 ("Мегачипсы", "вторые Мегачипсы"), ("Конфеты Отломи", "конфеты Отломи"),
]
ML_OVR = {101582: "Русская картошка", 101625: "Пюре детское", 101633: "Томатная паста", 101634: "Вареники"}
pk = lambda v: (v["sku"], v["mech"], v["disc"])
groups = collections.OrderedDict()
for v in F:
    groups.setdefault(pk(v), []).append(v)
assert len(groups) == len(N) == 65, (len(groups), len(N))
P, key2num = [], {}
for i, (k, vs) in enumerate(groups.items()):
    num = 101579 + i; v = vs[0]; short, tail = N[i]
    ds = sorted({x["d"] for x in vs}); only = ds[0] if len(ds) == 1 else ""
    sec = v["typ"] == "C"
    x = int(re.search(r"(\d+)\s*$", v["mech"]).group(1)) if sec else v["disc"]
    kind = "ДЦО на второй" if sec else ("ДЦО скидка" if v["mech"] == "СКИДКА" else "ДЦО по карте")
    name = f"ДЦО {short}{' ' + only if only else ''} -{x}%{' на второй' if sec else ''}"
    ml_core = re.sub(r"[^0-9A-Za-zА-Яа-яЁё ]", " ", f"{ML_OVR.get(num, short)}{' ' + only if only else ''}{' второй' if sec else ''} {x}")
    ml = re.sub(r"\s+", " ", f"{num}_ДЦО {ml_core}").strip()
    title = f"18.11. 🫰-{x}% на {tail}"
    P.append(dict(num=num, name=name, ml=ml, title=title, gold=v["sku"], kind=kind, x=x, cat=v["k"], catname=v["catname"],
                  product=v["name"], until=v["e"], districts=ds, clusters=sorted({x["cl"] for x in vs}), nf=v["nf"]))
    key2num[k] = num
long = [(p["ml"], len(p["ml"])) for p in P if len(p["ml"]) > 40]
print("ML > 40:", long)
print("title > 40:", [(p["title"], len(p["title"])) for p in P if len(p["title"]) > 40])
# слоты кластера: порядок как в подборе (еда по месту, потом нон-фуд), слот по умолчанию — еда, место 1
S = []
for c in range(1, 11):
    seen = collections.OrderedDict()
    for v in F:
        if v["cl"] == c:
            seen.setdefault((v["nf"], v["k"], v["rank"]), {})[v["d"]] = key2num[pk(v)]
    for j, ((nf, k, rank), dd) in enumerate(seen.items()):
        S.append((c, j, k, dd.get("ЦФО", 0), dd.get("СЗФО", 0), int(nf), int((not nf) and rank == 1)))
assert all(sum(1 for s in S if s[0] == c and s[6]) == 1 for c in range(1, 11))
with open("slots18.tsv", "w", encoding="utf-8", newline="\n") as f:
    f.write("# cl\tslot\tcat\tpromo_cfo\tpromo_szfo\tnf\tdefault\n")
    for s in S: f.write("\t".join(map(str, s)) + "\n")
json.dump(P, open("promos18.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print("slots", len(S), "max per cluster", max(collections.Counter(s[0] for s in S).values()))
for p in P: print(p["num"], p["name"], "|", p["ml"], len(p["ml"]), "|", p["title"], len(p["title"]))
