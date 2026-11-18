"""ДЦО 18.11 по кластерам: какие действующие ДЦО подходят каждому кластеру офлайна (только товарные данные, без клиентов).
Мера «подходит» — категория CVM, которую клиенты кластера покупают чаще базы: индекс доли покупателей
(доля покупателей категории в кластере ÷ доля в базе, I_CLUSTER_CATEGORY_SCORE, окно 05.10–15.11, присвоение 15.11).
Товар в категории — тот, который чаще всего покупают клиенты этого кластера (I_CLUSTER_PRODUCT_SCORE)."""
import csv, io, json, re, sys, collections

D = "C:/Users/836D~1/AppData/Local/Temp/2/claude/28286f9351f8c920/b33b8b02-868a-4ead-a84a-03d659b6907f/scratchpad/"
rows = list(csv.DictReader(open(D + "cand18e.tsv", encoding="utf-8"), delimiter="\t"))

names, cat = {}, collections.defaultdict(dict)
for l in open(D + "cl_score.jsonl", encoding="utf-8"):
    d = json.loads(l)
    if d["t"] == "dict" and d["ID_ORGANIZATION"] == 1 and d["LEVEL_NO"] == 2:
        names[d["CLUSTER_ID"]] = d["CLUSTER_NAME"]
    if d["t"] == "score":
        cat[d["CLUSTER"]][d["ID_CVM_CATEGORY"]] = d
prod = collections.defaultdict(dict)
for l in open(D + "cps.tsv", encoding="utf-8"):
    c, p, k, n, ch, call, fi, filo = l.rstrip("\n").split("\t")
    prod[int(c)][int(p)] = (int(n or 0), int(call or 0))

BREADTH = {c: sum(x["CONTACTS"] for x in v.values()) / next(iter(v.values()))["C_ALL"] for c, v in cat.items()}


def share(c, k):
    x = cat[c].get(k)
    return (x["CONTACTS"] / x["C_ALL"]) if x and x["C_ALL"] else 0.0

# --- исключения (действующие правила) ---
PREV_1111 = {1001, 1002, 1004, 1005, 1006, 1007, 1008, 1015, 1042, 1052, 1064, 1178, 1211, 1213, 1214, 1219, 1284, 1318, 1321,
             1334, 1335, 1346, 1352, 1387, 1412, 1576}                     # категории ДЦО 11.11 (товар и категорию не повторяем)
PEREKUS_WEEK = {1047, 1213, 1321, 1331, 1324}                               # 101544–101546 «Перекусы на ходу» 17–22.11
PEREKUS_CL1 = {1484}                                                         # готовая паста 101544 — только кластеру 1
SUNDAY_2211 = {1418, 1420, 1427}                                             # 101562 вс 22.11 дезодоранты и гели для душа
GROUPS_OFF = {3, 7, 20}                                                      # алкоголь, табак, сервис/лояльность
ENERGY = {1316}
SAUCE = {1073, 1042, 1051, 1052, 1057, 1063, 1068, 1081, 1092, 1104, 1662}         # соусы и специи — не трафик
CHEAP_SMALL = {1289, 1295, 1299, 1300}                                       # жвачка, карамель — дешёвая мелочь, не трафик
NONFOOD_GRP = {13, 14, 17, 18, 19}                                          # нон-фуд
BABY_NONFOOD = {1511, 1513, 1514, 1516, 1517, 1522, 1523, 1524, 1525, 1526, 1527, 1528, 1529, 1530, 1531, 1533, 1535, 1536, 1537,
                1544, 1545, 1546, 1550, 1552}                                  # детское не еда
MIN_DISC, MIN_SHOPS, MIN_BUYWK = 8, 500, 1500
MIN_IDX = float(sys.argv[2]) if len(sys.argv) > 2 else 1.15
MIN_REACH = float(sys.argv[3]) if len(sys.argv) > 3 else 0.05
ORDER = sys.argv[4] if len(sys.argv) > 4 else "idx"
ENTRY = re.compile(r"(ЦФО|СЗФО):(.+?) (\S*) (\d+|\?)% (\d\d\.\d\d)-(\d\d\.\d\d) ([\d.]*)>([\d.]*) \[([^\]]*)\]")


def entries(det):
    out = []
    for part in det.split(" | "):
        m = ENTRY.search(part)
        if m:
            d, mech, n1, disc, s, e, p1, p2, src = m.groups()
            out.append(dict(d=d, mech=mech.strip(), n1=n1, disc=None if disc == "?" else int(disc), s=s, e=e, p1=p1, p2=p2))
    return out


cands = []
for r in rows:
    if not r["cat"] or not r["id_product"]:
        continue
    k, g = int(r["cat"]), int(r["grp"])
    if g in GROUPS_OFF or k in ENERGY | PREV_1111 | PEREKUS_WEEK | SUNDAY_2211:
        continue
    if int(r["shops"] or 0) < MIN_SHOPS or int(r["buy_wk"] or 0) < MIN_BUYWK or r["in_sale"] != "True":
        continue
    es = entries(r["det"])
    typ = r["typ"]
    if typ == "T":
        es = [x for x in es if x["disc"] is not None and x["disc"] >= MIN_DISC]
        if not es or k in SAUCE | CHEAP_SMALL:
            continue
        nonfood = g in NONFOOD_GRP or k in BABY_NONFOOD
        if nonfood:
            continue                                                         # трафик — еда (правило 01.11)
    if not es:
        continue
    cands.append(dict(typ=typ, sku=r["sku"], name=r["name"], k=k, g=g, catname=r["catname"], p=int(r["id_product"]),
                      price=r["price2w"], buy_wk=int(r["buy_wk"] or 0), es=es))

res = {}
for c in range(1, 11):
    best = {}
    for x in cands:
        if c == 1 and x["k"] in PEREKUS_CL1:
            continue
        s0, sc = share(0, x["k"]), share(c, x["k"])
        if not s0 or not sc:
            continue
        idx = (sc / s0) / (BREADTH[c] / BREADTH[0])     # × к среднему: доля категории в покупках кластера ÷ в базе
        pn, pall = prod[c].get(x["p"], (0, 0))
        key = (x["typ"], x["k"])
        cur = best.get(key)
        cand = dict(x, idx=idx, sc=sc, pbuy=pn, pshare=(pn / pall if pall else 0))
        if cur is None or cand["pbuy"] > cur["pbuy"]:
            best[key] = cand
    res[c] = best

json.dump({str(c): [dict(v, es=v["es"]) for v in b.values()] for c, b in res.items()},
          open(D + "match_all.json", "w", encoding="utf-8"), ensure_ascii=False)

def fmt(v):
    ds = "+".join(sorted({e["d"] for e in v["es"]}))
    e = v["es"][0]
    return (f"{v['typ']} {v['k']} {v['catname'][:24]:24} ×{v['idx']:.2f} охв {v['sc']*100:4.1f}% | {v['name'][:42]:42} "
            f"{ds:8} {e['mech'][:18]:18} -{e['disc']}% до {e['e']} {e['p1']}>{e['p2']} | кл.покуп {v['pbuy']}")

PICK = {}
for c in range(1, 11):
    b = res[c]
    print(f"\n=== {c} {names[c]}  C_ALL {cat[c][1001]['C_ALL'] if 1001 in cat[c] else ''}")
    one = {}
    for (t, k), v in b.items():                                              # одна акция на категорию: «по карте/скидка», иначе «на второй»
        if v["idx"] >= MIN_IDX and v["sc"] >= MIN_REACH and (k not in one or (t == "T" and one[k]["typ"] == "C")):
            one[k] = v
    vs = sorted(one.values(), key=(lambda v: -v["sc"]) if ORDER == "reach" else (lambda v: -v["idx"]))
    PICK[c] = vs[:int(sys.argv[1]) if len(sys.argv) > 1 else 12]
    for v in PICK[c]:
        print(fmt(v))
json.dump({str(c): v for c, v in PICK.items()}, open(D + "pick.json", "w", encoding="utf-8"), ensure_ascii=False)
