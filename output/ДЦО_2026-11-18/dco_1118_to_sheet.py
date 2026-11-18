"""ДЦО ср 18.11.2026: 65 акций 101579–101643 → Google «CVM offline» (строки акций) и PUSH (сетка: 18.11, msg 1).

Как ДЦО 11.11 (dco_1111_to_sheet.py, строки 478–503 / PUSH 896–921): «Настройка» = нет, механика «Коммуникация»,
сегмент — «Активные, Новые» и кластеры, которым дана акция; количество клиентов — раздача (до вычета без пуша, глоб. КГ и КГ 5 %)
или на отправку из I_PROMO_OFFER, если передан файл счётчиков. PUSH — формулы как у соседних строк, «минус фрод», ссылка —
карточка товара по GOLD. Время отправки — из аргумента --time ЧЧ:ММ (без него ячейка пустая: время решает Елена).
Заголовок и текст PUSH не пишутся: тексты согласуются отдельно.
Перед записью — бэкап обоих листов; пишется только в пустые строки в конце, номера не должны встречаться в листах.
Запуск: .venv/Scripts/python.exe "output/ДЦО_2026-11-18/dco_1118_to_sheet.py" <promos18.json> <counts.tsv> <папка_бэкапа> [--time 15:00] [--dry]
"""
import datetime as dt, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]; sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402
sys.stdout.reconfigure(encoding="utf-8")

DAY = 46344            # 18.11.2026 (сериал Google: 30.10.2026 = 46325)
CL = {1: "Перекусы на ходу", 2: "Редкие", 3: "Частые с большой корзиной", 4: "Регулярные за продуктами",
      5: "Большая закупка продуктов", 6: "Много нон-фуда", 7: "Большая закупка с нон-фудом", 8: "За хлебом и водой",
      9: "Ультра-частые", 10: "Вино, крепкое, кофе"}


def first_empty(v):
    return max(i for i, r in enumerate(v, start=1) if any(str(x).strip() for x in r)) + 1


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    P = json.load(open(args[0], encoding="utf-8"))
    cnt = {int(a): int(b) for a, b in (l.split("\t") for l in open(args[1], encoding="utf-8") if l.strip())}
    bdir = Path(args[2]); bdir.mkdir(parents=True, exist_ok=True)
    dry = "--dry" in sys.argv
    tm = ""
    if "--time" in sys.argv:
        h, m = sys.argv[sys.argv.index("--time") + 1].split(":"); tm = (int(h) * 60 + int(m)) / 1440
    nums = [p["num"] for p in P]
    assert len(P) == 65 and nums == list(range(101579, 101644)) and set(cnt) == set(nums)
    ss = sc.get_spreadsheet()
    wo, wp = ss.worksheet(sc.SHEET_CVM_OFFLINE), ss.worksheet(sc.SHEET_PUSH)
    vo = wo.get_all_values(value_render_option="FORMULA")
    vp = wp.get_all_values(value_render_option="FORMULA")
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    (bdir / f"backup_cvm_offline_{stamp}.json").write_text(json.dumps(vo, ensure_ascii=False), encoding="utf-8")
    (bdir / f"backup_push_{stamp}.json").write_text(json.dumps(vp, ensure_ascii=False), encoding="utf-8")
    sn = {str(n) for n in nums}
    assert not [r for r in vo[1:] if str(r[1]).split("_")[0].strip() in sn], "номера уже есть в CVM offline"
    assert not [r for r in vp[1:] if len(r) > 11 and str(r[11]).strip() in sn], "номера уже есть в PUSH"
    assert vo[0][36] == "Ограничения и комментарии" and vo[0][13] == "Примерное количество клиентов"
    assert vp[0][11] == "Номер промо" and vp[0][17] == "Экран - ссылка" and vp[0][8] == "Время"

    o0 = first_empty(vo)
    rows_o = []
    for p in P:
        r = [""] * 37
        seg = "Активные, Новые; кластеры: " + ", ".join(CL[c] for c in p["clusters"])
        note = f"{p['kind']} до {p['until']}, пуш 18.11; раздача внутри кластера офлайна (присвоение 15.11), список в MCI 18.11, КГ 5%"
        r[0], r[1], r[2], r[3] = "нет", p["num"], p["name"], seg
        r[4], r[5], r[6], r[7], r[8], r[9], r[10] = 2026, 11, 47, "ср", DAY, DAY, "PUSH"
        r[12], r[13], r[25], r[36] = p["ml"], cnt[p["num"]], "Коммуникация", note
        rows_o.append(r)
    p0 = first_empty(vp)
    rows_p = []
    for k, p in enumerate(P):
        i = p0 + k
        rows_p.append([
            f"=VLOOKUP(L{i};'CVM offline'!B:LO;3;FALSE)", f"=VLOOKUP(L{i};'CVM offline'!B:LO;10;FALSE)",
            f"=VLOOKUP(L{i};'CVM offline'!B:LO;2;FALSE)", f"=YEAR(H{i})", f"=MONTH(H{i})", f"=WEEKNUM(H{i};2)",
            f"=VLOOKUP(WEEKDAY(H{i};2);'данные'!A:B;2;FALSE)", DAY, tm,
            f"=VLOOKUP(L{i};'CVM offline'!B:N;13;false)", "минус фрод", p["num"], 1,
            "", f"=LEN(N{i})", "", f"=LEN(P{i})", f"dixyapp://app/product?gold_erp_id={p['gold']}", "", ""])
    print(f"CVM offline: строки {o0}–{o0 + len(rows_o) - 1}; PUSH: строки {p0}–{p0 + len(rows_p) - 1}; время {tm or 'пусто'}")
    if dry:
        print(rows_o[0]); print(rows_p[0]); return
    wo.update(range_name=f"A{o0}:AK{o0 + len(rows_o) - 1}", values=rows_o, value_input_option="USER_ENTERED")
    wp.update(range_name=f"A{p0}:T{p0 + len(rows_p) - 1}", values=rows_p, value_input_option="USER_ENTERED")
    # формат новых строк = формат строки над ними (как fmt_fix 11.11): даты датами, время временем
    reqs = []
    for ws, first, n, ncol in ((wo, o0, len(rows_o), 37), (wp, p0, len(rows_p), 20)):
        reqs.append({"copyPaste": {
            "source": {"sheetId": ws.id, "startRowIndex": first - 2, "endRowIndex": first - 1, "startColumnIndex": 0, "endColumnIndex": ncol},
            "destination": {"sheetId": ws.id, "startRowIndex": first - 1, "endRowIndex": first - 1 + n, "startColumnIndex": 0, "endColumnIndex": ncol},
            "pasteType": "PASTE_FORMAT"}})
    ss.batch_update({"requests": reqs})
    chk_o = wo.get_values(f"B{o0}:N{o0 + len(rows_o) - 1}")
    chk_p = wp.get_values(f"A{p0}:R{p0 + len(rows_p) - 1}")
    print("бэкап", stamp)
    print("CVM offline:", chk_o[0][:3], "…", chk_o[-1][:3], "| клиентов",
          sum(int(str(r[12]).replace(" ", "").replace("\xa0", "") or 0) for r in chk_o))
    print("PUSH:", chk_p[0][:12], "…", chk_p[-1][11:13], chk_p[-1][17] if len(chk_p[-1]) > 17 else "")


if __name__ == "__main__":
    main()
