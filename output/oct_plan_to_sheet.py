"""Заготовка октября 2026 → Google Sheets «CVM offline» (+ PUSH для пуш-акций) из output/план_октябрь_2026_заготовка.json.

Пишет только выбранные промо (по НОМЕР или по каналу), идемпотентно: строки с этими НОМЕР удаляются и пишутся заново.
Запуск:  python output/oct_plan_to_sheet.py --nums 101393 101394 101395 101396      (слипы)
         python output/oct_plan_to_sheet.py --channel PUSH --push-grid                 (пуш-акции + сетка PUSH)
"""
import argparse, collections, datetime as dt, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402

PLAN = ROOT / "output/план_октябрь_2026_заготовка.json"
COND = ROOT / "output/условия_октябрь_2026.json"
DOW = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
d = dt.date.fromisoformat
ddm = lambda s: f"{d(s).day:02d}.{d(s).month:02d}."
STOP = "24.10.2026"
WHITE = {"red": 1, "green": 1, "blue": 1}
STREAM_NAME = {101426: "Дарим 100 монет на неделю", 101428: "Отток 50% кешбэка по дням"}  # выдача слипов прекращается за 7 дней до конца месяца

SMALL_ALCO = ("*Действует при предъявлении купона и вашей карты на 1 покупку на сумму от 1000р. в течение 7 дней с даты выдачи. "
              "Скидка 100р. распределяется по всем товарам чека. Не суммируется с другими акциями и не применяется к промотоварам по желтым ценникам.")
MANZANA_ALCO = ("Купон выдается после покупки товаров из списка категорий в течение срока акции. Выдача купонов прекращается {stop}, купон действует 7 дней. "
                "Скидка 100р. предоставляется при сумме чека от 1000р. и распределяется по всем товарам чека (к категории не привязана). "
                "Не суммируется с другими акциями, не применяется к промотоварам по желтым ценникам.")

# Условия слип-купонов (решение Елены 29.09.2026: клиенту — скидка на алкоголь; в настройке — 100р. на чек от 1000р. с распределением по чеку)
SLIP = {
    101393: {"desc": f"Купон 50р. на любую покупку в течение 7 дней. Выдача купонов прекращается {STOP}", "disc": "50",
             "manzana": f"Купон выдается после любой покупки в течение срока акции. Выдача купонов прекращается {STOP}, купон действует 7 дней. "
                        "Скидка по купону не применяется на социально значимые товары, алкоголь, табак, лотерейные билеты, а также к промотоварам по желтым ценникам. Скидка применяется к любому чеку",
             "title": "Купон 50р. на любую покупку",
             "text": "Купон* 50р. на любую покупку\n\n(QR-код)\n\n*Купон действует при предъявлении купона и карты на 1 покупку в течение 7 дней с даты выдачи. "
                     "Скидка по купону не суммируется с другими акциями, применяется наиболее выгодная скидка",
             "limits": "не более 2 применений в сутки; аудитория: Активные, Новые без PUSH/APP, без покупок алкоголя за 8 недель (покупатели алкоголя — купоны 101394–101396)"},
    101394: {"desc": f"Купон 100р. на чек от 1000р. после покупки вина и игристого, действует 7 дней. Выдача купонов прекращается {STOP}", "disc": "100",
             "manzana": MANZANA_ALCO.format(stop=STOP), "title": "100р. на вино и игристое",
             "text": "100р.\n\nна покупку вина и игристого\n\n(QR-код)\n\n" + SMALL_ALCO,
             "limits": "как 101360; аудитория — покупатели вина и игристого за 8 недель, Активные, Новые без PUSH/APP; клиенту — скидка на алкоголь, в настройке — 100р. на чек от 1000р. с распределением по чеку"},
    101395: {"desc": f"Купон 100р. на чек от 1000р. после покупки крепкого алкоголя, действует 7 дней. Выдача купонов прекращается {STOP}", "disc": "100",
             "manzana": MANZANA_ALCO.format(stop=STOP), "title": "100р. на крепкий алкоголь",
             "text": "100р.\n\nна покупку виски, коньяка, рома, водки и другого крепкого алкоголя\n\n(QR-код)\n\n" + SMALL_ALCO,
             "limits": "как 101361; аудитория — покупатели крепкого алкоголя за 8 недель, Активные, Новые без PUSH/APP; клиенту — скидка на алкоголь, в настройке — 100р. на чек от 1000р. с распределением по чеку"},
    101396: {"desc": f"Купон 100р. на чек от 1000р. после покупки пива, действует 7 дней. Выдача купонов прекращается {STOP}", "disc": "100",
             "manzana": MANZANA_ALCO.format(stop=STOP), "title": "100р. на пиво",
             "text": "100р.\n\nна покупку светлого, тёмного, нефильтрованного и другого пива\n\n(QR-код)\n\n" + SMALL_ALCO,
             "limits": "как 101362; аудитория — покупатели пива за 8 недель, Активные, Новые без PUSH/APP; клиенту — скидка на алкоголь, в настройке — 100р. на чек от 1000р. с распределением по чеку"},
}


def batch_delete(ss, ws, rows):
    if not rows:
        return
    rows = sorted(rows, reverse=True); reqs = []
    while rows:
        end = rows[0]; start = end
        while len(rows) > 1 and rows[1] == start - 1:
            rows.pop(0); start -= 1
        rows.pop(0); reqs.append({"deleteDimension": {"range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": start - 1, "endIndex": end}}})
    ss.batch_update({"requests": reqs})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nums", nargs="*", default=[])
    ap.add_argument("--base", default=None, help="все дни потока: НОМЕР начинается с базы, напр. 101428")
    ap.add_argument("--channel", default=None)
    ap.add_argument("--push-grid", action="store_true")
    a = ap.parse_args()
    plan = json.load(open(PLAN, encoding="utf-8"))
    P = [p for p in plan["promos"] if (str(p["num"]) in a.nums) or (a.base and str(p["num"]).startswith(a.base)) or (a.channel and p["channel"] == a.channel)]
    if not P:
        sys.exit("нечего писать")
    nums = {str(p["num"]) for p in P}
    global PBYNUM; PBYNUM = {str(p["num"]): p for p in P}
    bases = {str(p["num"]).split("_")[0] for p in P if "_" in str(p["num"])}
    ss = sc.get_spreadsheet(); wo = ss.worksheet(sc.SHEET_CVM_OFFLINE)
    v = wo.get_all_values(); hdr = v[0]
    old = [i for i, r in enumerate(v[1:], start=2) if r[1].strip() in nums]
    batch_delete(ss, wo, old); print("CVM offline: удалено", len(old))
    rows = []
    for p in sorted(P, key=lambda p: (len(str(p["num"])), str(p["num"]))):
        s, e = d(p["start"]), d(p["end"]); num = p["num"]
        comm = p["mech"] == "Коммуникация"
        r = {h: "" for h in hdr}
        r.update({"Настройка": "нет" if (comm or p["name"] == "Баланс баллов") else "да", "НОМЕР": num, "Название промо": p["name"], "Сегмент": p["segment"],
                  "Год": 2026, "Месяц": 10, "Неделя": s.isocalendar()[1], "День недели": DOW[s.weekday()], "Старт акции": ddm(p["start"]), "Окончание акции": ddm(p["end"]),
                  "Каналы коммуникации": p["channel"], "Название МС": f"{num}_{p['name']}", "Механика": p["mech"],
                  "Категория": p.get("codes", "") if not comm else p["cat"],
                  "Ограничения и комментарии": "ЗАГОТОВКА: " + p["why"] + ((" · " + p["cat"]) if p["cat"] and not comm else "")})
        if isinstance(p.get("dop_to"), (int, float)): r["Доп ТО (план), р."] = int(p["dop_to"])
        if isinstance(p.get("pl"), (int, float)): r["PL"] = int(p["pl"])
        if "кешбэка" in p["name"]:
            r["Бонусы"] = "50%" if p["name"].startswith("50%") else "20%"; r["Срок сгорания бонусов"] = (e + dt.timedelta(days=7)).strftime("%d.%m.%Y 23:59:00")
        if p["channel"] == "PUSH" and not comm and "Баланс" not in p["name"]: r["Купон"] = "да"
        if p.get("stream"):  # поток: один сегмент на все дни — Название МС и Сегмент МС по базовому номеру (решение Елены 29.09)
            r["Название МС"] = f"{p['stream']}_{STREAM_NAME[p['stream']]}"; r["Сегмент МС"] = str(p["stream"])
        if p.get("burn"): r["Срок сгорания бонусов"] = p["burn"]
        if p.get("bonus"): r["Бонусы"] = p["bonus"]
        if p.get("discount"): r["Скидка"] = p["discount"]
        cond = json.loads(COND.read_text(encoding="utf-8")).get(str(num), {}) if COND.exists() else {}
        for k_, v_ in cond.items():
            if not k_.startswith("_") and k_ in r: r[k_] = v_
        if cond.get("_note"): r["Ограничения и комментарии"] += " · " + cond["_note"]
        if num in SLIP:
            c = SLIP[num]
            r.update({"Описание акции": c["desc"], "Скидка": c["disc"], "Механика для Manzana Online": c["manzana"], "Купон": "слип",
                      "Название информационного купона для МП": c["title"], "Текст на информационном купоне / слип-чеке": c["text"],
                      "Ограничения и комментарии": "ЗАГОТОВКА: " + c["limits"]})
        vals = []
        for h in hdr:
            vals.append(r[h])
        # столбец «Скидка» встречается дважды (16 и 20): номинал купона — в первый, второй оставляем пустым
        seen = 0
        for i, h in enumerate(hdr):
            if h == "Скидка":
                seen += 1
                if seen == 2: vals[i] = ""
        rows.append(vals)
    res = wo.append_rows(rows, value_input_option="USER_ENTERED"); print("CVM offline: добавлено", len(rows), sorted(nums))
    # новые строки — белые (цвет статусов ставит другая программа, Елена 29.09.2026)
    import re as _re
    m = _re.search(r"!A?(\d+):\D*(\d+)", res.get("updates", {}).get("updatedRange", ""))
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        ss.batch_update({"requests": [{"repeatCell": {"range": {"sheetId": wo.id, "startRowIndex": a - 1, "endRowIndex": b, "startColumnIndex": 0, "endColumnIndex": wo.col_count}, "cell": {"userEnteredFormat": {"backgroundColor": WHITE}}, "fields": "userEnteredFormat.backgroundColor"}}]})
    if a.push_grid:
        wp = ss.worksheet(sc.SHEET_PUSH)
        pv = wp.get_all_values(value_render_option="FORMULA")
        old_push = [i for i, r in enumerate(pv[1:], start=2) if len(r) > 11 and (str(r[11]).strip() in nums or str(r[11]).strip() in bases)]
        batch_delete(ss, wp, old_push); print("PUSH: удалено", len(old_push))
        wp = ss.worksheet(sc.SHEET_PUSH)  # заново: после удаления строк размер листа изменился
        pv = wp.get_all_values(value_render_option="FORMULA")
        start = next((i for i, r in enumerate(pv[1:], start=2) if not any(str(c).strip() for c in (r[7:8] + r[11:13]))), len(pv) + 1)
        pushes = sorted([(d(m["date"]), p["num"], int(m["msg"]), m.get("time")) for p in P for m in p.get("push", [])], key=lambda x: (x[0], str(x[1]), x[2]))
        def frow(i, date, num, msg, time):
            ser = (date - dt.date(1899, 12, 30)).days
            t = 0.4583333333333333 if not time else (int(time[:2]) * 60 + int(time[3:5])) / 1440
            dop = "минус фрод"
            if "_" in str(num):  # поток: в PUSH номер сегмента (база), msg сквозной; сегмент/канал/название — значениями (VLOOKUP по базе пуст)
                x = PBYNUM[str(num)]
                return [x["segment"], "PUSH", x["name"], f"=YEAR(H{i})", f"=MONTH(H{i})", f"=WEEKNUM(H{i};2)", f"=VLOOKUP(WEEKDAY(H{i};2);'данные'!A:B;2;FALSE)", ser, t, "", dop, int(str(num).split("_")[0]), msg, "", f"=LEN(N{i})", "", f"=LEN(P{i})", "", "", ""]
            return [f"=VLOOKUP(L{i};'CVM offline'!B:LO;3;FALSE)", f"=VLOOKUP(L{i};'CVM offline'!B:LO;10;FALSE)", f"=VLOOKUP(L{i};'CVM offline'!B:LO;2;FALSE)", f"=YEAR(H{i})", f"=MONTH(H{i})", f"=WEEKNUM(H{i};2)", f"=VLOOKUP(WEEKDAY(H{i};2);'данные'!A:B;2;FALSE)", ser, t, f"=VLOOKUP(L{i};'CVM offline'!B:N;13;false)", dop, num, msg, "", f"=LEN(N{i})", "", f"=LEN(P{i})", "", "", ""]
        vals = [frow(start + k, *x) for k, x in enumerate(pushes)]; end = start + len(vals) - 1
        if end > wp.row_count: wp.add_rows(end - wp.row_count)
        wp.update(range_name=f"A{start}:T{end}", values=vals, value_input_option="USER_ENTERED")
        ss.batch_update({"requests": [{"copyPaste": {"source": {"sheetId": wp.id, "startRowIndex": 467, "endRowIndex": 468, "startColumnIndex": 0, "endColumnIndex": 20}, "destination": {"sheetId": wp.id, "startRowIndex": start - 1, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": 20}, "pasteType": "PASTE_FORMAT"}}, {"repeatCell": {"range": {"sheetId": wp.id, "startRowIndex": start - 1, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": 20}, "cell": {"userEnteredFormat": {"backgroundColor": WHITE}}, "fields": "userEnteredFormat.backgroundColor"}}]})  # строки кладём белыми — цвет статусов ставит другая программа
        print(f"PUSH: строки {start}-{end} = {len(vals)} сообщений")
    v = wo.get_all_values()
    allnums = [r[1].strip() for r in v[1:] if r[1].strip()]
    dup = [n for n, c in collections.Counter(allnums).items() if c > 1]; print("дубли НОМЕР в CVM offline:", dup or "нет")


if __name__ == "__main__":
    main()
