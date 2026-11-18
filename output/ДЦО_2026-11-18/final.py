"""ДЦО 18.11 по кластерам — итог подбора (только товарные данные и счётчики аудитории).
Подходит кластеру: категория CVM, которую кластер берёт чаще среднего — «× к среднему» = доля категории в покупках кластера
÷ доля в базе (по числу покупателей, I_CLUSTER_CATEGORY_SCORE 05.10–15.11, присвоение 15.11); её покупают ≥ 3 % клиентов кластера.
Товар — действующий 18.11 ДЦО этой категории, который чаще всего покупают клиенты кластера (I_CLUSTER_PRODUCT_SCORE), отдельно по округу;
«по карте / скидка» (≥ 8 %) раньше «на второй». Еда и нон-фуд — отдельными списками (трафик — еда, правило 01.11)."""
import csv, json, re, sys, collections

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
    prod[int(c)][int(p)] = int(n or 0)
aud = collections.defaultdict(lambda: collections.Counter())
for l in open(D + "cl_contacts.out", encoding="utf-8"):
    x = l.rstrip("\n").split("\t")
    if len(x) == 4 and x[0].isdigit():
        aud[int(x[0])][x[1]] += int(x[3])
BREADTH = {c: sum(x["CONTACTS"] for x in v.values()) / next(iter(v.values()))["C_ALL"] for c, v in cat.items()}


def share(c, k):
    x = cat[c].get(k)
    return (x["CONTACTS"] / x["C_ALL"]) if x and x["C_ALL"] else 0.0


PREV_1111 = {1001, 1002, 1004, 1005, 1006, 1007, 1008, 1015, 1042, 1052, 1064, 1178, 1211, 1213, 1214, 1219, 1284, 1318, 1321,
             1334, 1335, 1346, 1352, 1387, 1412, 1576}                     # категории ДЦО 11.11
PEREKUS_WEEK = {1047, 1213, 1321, 1331, 1324}                               # 101544–101546 «Перекусы на ходу» 17–22.11
PEREKUS_CL1 = {1484}                                                         # готовая паста 101544 — кластеру 1
SUNDAYS = {1418, 1420, 1427, 1414, 1413}                                     # вс 22.11 дезодоранты и гели; вс 29.11 шампуни; вс 15.11 полость рта
GROUPS_OFF = {3, 7, 20}
ENERGY = {1316}
SAUCE = {1073, 1042, 1051, 1052, 1057, 1063, 1068, 1081, 1092, 1104, 1662}
CHEAP_SMALL = {1289, 1295, 1299, 1300}
NONFOOD_GRP = {13, 14, 17, 18, 19}
BABY_NONFOOD = {1511, 1513, 1514, 1516, 1517, 1522, 1523, 1524, 1525, 1526, 1527, 1528, 1529, 1530, 1531, 1533, 1535, 1536, 1537,
                1544, 1545, 1546, 1550, 1552}
MIN_DISC, MIN_SHOPS, MIN_BUYWK, MIN_IDX, MIN_REACH, TOP = 8, 500, 1500, 1.0, 0.03, 6
RECIPES_3 = {1003, 1013, 1214, 1188, 1002, 1221,            # фриттата 19.11
             1345, 1054, 1015, 1005,                        # хачапури по-аджарски 21.11
             1004, 1006, 1046, 1218,                        # творожная запеканка с мандаринами 24.11
             1178, 1050, 1233, 1222, 1243, 1045, 1057}      # плов с курицей 26.11
CL_OWN = {2: {1337, 1343, 1041, 1212, 1281},                                   # булочки с сыром, чипсы, бананы, батончики
          3: RECIPES_3,
          4: {1487, 1238, 1490, 1022, 1484, 1485, 1489, 1337},                 # комбо: сэндвич+киви, сырники+ряженка, горячее+салат, суп+выпечка
          5: {1037, 1191},                                                     # сыр с плесенью, бекон
          6: {1573, 1416, 1401, 1402, 1405, 1406},                             # гели для стирки, полотенца; кошек нет (Елена 17.11)
          7: {1505, 1011, 1573},                                               # творожки, гели для стирки
          8: {1488, 1020, 1016},                                               # кофе с собой, молочные коктейли и питьевые йогурты
          9: {1575, 1416, 1579, 1415, 1422}}                                   # порошок, полотенца, освежители; прокладок нет (Елена 17.11)
FOOD_ONLY = {7}                                                                # ДЦО 18.11 кластеру 7 — только еда (Елена 17.11, «5 б»)
ENTRY = re.compile(r"(ЦФО|СЗФО):(.+?) (\S*) (\d+|\?)% (\d\d\.\d\d)-(\d\d\.\d\d) ([\d.]*)>([\d.]*) \[([^\]]*)\]")

cands = []
for r in rows:
    if not r["cat"] or not r["id_product"]:
        continue
    k, g = int(r["cat"]), int(r["grp"])
    if g in GROUPS_OFF or k in ENERGY | PREV_1111 | PEREKUS_WEEK | SUNDAYS:
        continue
    if int(r["shops"] or 0) < MIN_SHOPS or int(r["buy_wk"] or 0) < MIN_BUYWK or r["in_sale"] != "True":
        continue
    nonfood = g in NONFOOD_GRP or k in BABY_NONFOOD
    for part in r["det"].split(" | "):
        m = ENTRY.search(part)
        if not m:
            continue
        d, mech, n1, disc, s, e, p1, p2, src = m.groups()
        disc = None if disc == "?" else int(disc)
        if e == "18.11":
            continue                                                         # акция кончается в день пуша
        if r["typ"] == "T" and (disc is None or disc < MIN_DISC or (not nonfood and k in SAUCE | CHEAP_SMALL)):
            continue
        cands.append(dict(typ=r["typ"], sku=r["sku"], name=r["name"], k=k, g=g, catname=r["catname"], p=int(r["id_product"]),
                          nf=nonfood, d=d, mech=mech.strip(), n1=n1.strip(), disc=disc, s=s, e=e, p1=p1, p2=p2))

out = []
for c in range(1, 11):
    best = {}
    for x in cands:
        if c == 1 and x["k"] in PEREKUS_CL1:
            continue
        if x["k"] in CL_OWN.get(c, set()):                                   # категории своих кешбэков 19–27.11 (кл. 2) и рецептов (кл. 3)
            continue
        s0, sc = share(0, x["k"]), share(c, x["k"])
        if not s0 or not sc:
            continue
        idx = (sc / s0) / (BREADTH[c] / BREADTH[0])
        if idx < MIN_IDX or sc < MIN_REACH:
            continue
        key = (x["nf"], x["k"], x["d"])
        v = dict(x, idx=idx, sc=sc, pbuy=prod[c].get(x["p"], 0))
        cur = best.get(key)
        if cur is None or (v["typ"] != "T", -v["pbuy"]) < (cur["typ"] != "T", -cur["pbuy"]):     # «T» раньше «C», затем больше покупателей кластера
            best[key] = v
    for nf in ((False,) if c in FOOD_ONLY else (False, True)):
        ks = sorted({(v["idx"], v["sc"], k) for (f, k, d), v in best.items() if f == nf}, reverse=True)[:TOP]
        for rank, (idx, sc, k) in enumerate(ks, 1):
            for d in ("ЦФО", "СЗФО"):
                v = best.get((nf, k, d))
                if v:
                    out.append(dict(v, cl=c, clname=names[c], rank=rank))

json.dump(out, open(D + "final.json", "w", encoding="utf-8"), ensure_ascii=False)
TYP = {"T": "по карте/скидка", "C": "на второй"}
for c in range(1, 11):
    a = aud[c]
    sp = lambda n: f"{n:,}".replace(",", " ")
    print(f"\n### {c}. {names[c]} — {sp(a['ЦФО'] + a['СЗФО'])} (ЦФО {sp(a['ЦФО'])} / СЗФО {sp(a['СЗФО'])})")
    for nf in (False, True):
        grp = [v for v in out if v["cl"] == c and v["nf"] == nf]
        if not grp:
            continue
        if nf:
            print("  нон-фуд:")
        seen = collections.OrderedDict()
        for v in grp:
            seen.setdefault(v["k"], []).append(v)
        for k, vs in seen.items():
            same = len(vs) == 2 and vs[0]["sku"] == vs[1]["sku"] and vs[0]["disc"] == vs[1]["disc"]
            items = [vs[0]] if same or len(vs) == 1 else vs
            desc = " / ".join(
                f"{('' if same else v['d'] + ': ')}{v['name']} — {v['mech'].lower()}{(' ' + v['n1']) if v['n1'] and v['typ'] == 'C' else ''} −{v['disc']}% до {v['e']}"
                for v in items)
            only = "" if same or len(vs) == 2 else f" (только {vs[0]['d']})"
            print(f"  {vs[0]['catname']} ×{vs[0]['idx']:.2f}, берут {vs[0]['sc'] * 100:.0f}% | {desc}{only}")
