"""Нон-фуд воскресенья ноября 2026 (101560–101563) → Google «CVM offline» + PUSH. ОК Елены 01.11.2026.

Условия — по прошлым акциям тех же категорий (101374, 101357, 101389, 101354): коды, кнопка, формат инфокупона;
марки — по живому ассортименту (cvm.live_assort, сумма чеков марки за 4 недели).
Идемпотентно: строки с этими НОМЕР удаляются и пишутся заново. Перед записью — бэкап обоих листов в scratchpad.
Запуск: .venv/Scripts/python.exe output/nonfood_nov_to_sheet.py <папка_бэкапа>
"""
import datetime as dt, json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402

WHITE = {"red": 1, "green": 1, "blue": 1}
TAIL = "\n\nСкидка не суммируется с другими акциями — применяется наиболее выгодная. Не действует на товары по жёлтым ценникам."
MONTH = {11: "ноября"}

P = [
    dict(num=101560, date="2026-11-08", name="Скидка 20% на средства для мытья посуды", codes="63010504\n63010505",
         coupon_title="Только 08.11. - скидка 20% на средства для посуды",
         coupon="Гели и бальзамы для посуды Fairy, AOS, Synergetic, Sorti и Haus Herz, таблетки и средства для посудомоечных машин.",
         button="Выбрать dixyapp://app/products_list?id=729&name=Бытовая химия",
         title="🍽-20% на средства для посуды",
         body="Гели и бальзамы Fairy, AOS, Synergetic, Sorti и Haus Herz. Действует только сегодня, 8 ноября. Активируй скидку в приложении.",
         why="как 101374 (06.09: +2,43 ₽ на клиента)"),
    dict(num=101561, date="2026-11-15", name="Скидка 20% на средства для ухода за полостью рта",
         codes="63030801\n63030802\n63030803\n63030804\n63030805\n63030807",
         coupon_title="Только 15.11. - скидка 20% на средства для зубов",
         coupon="Зубные пасты и щётки Splat, Colgate, Лесной бальзам, Aquafresh и Biomed, ополаскиватели и нити.",
         button="Выбрать dixyapp://app/products_list?id=4029&name=Средства для ухода за полостью рта",
         title="🦷-20% на средства для зубов",
         body="Пасты, щётки и ополаскиватели Splat, Colgate, Лесной бальзам, Aquafresh и Biomed. Действует только сегодня, 15 ноября. Активируй скидку в приложении.",
         why="как 101357 (30.08: +5,04 ₽ на клиента, значимо)"),
    dict(num=101562, date="2026-11-22", name="Скидка 20% на дезодоранты и гели для душа",
         codes="63030602\n63030603\n63030901\n63030902",
         coupon_title="Только 22.11. - скидка 20% на дезодоранты и гели для душа",
         coupon="Дезодоранты Rexona, Nivea, Next Generation и Axe, гели для душа Palmolive, Nivea и Dove.",
         button="Выбрать dixyapp://app/products_list?id=730&name=Гигиена и косметика",
         title="🧴-20% на дезодоранты",
         body="Спреи и стики, а ещё гели для душа: Rexona, Palmolive, Nivea, Next Generation, Axe и Dove. Действует только сегодня, 22 ноября. Активируй скидку в приложении.",
         why="как 101389 (27.09: +1,68 ₽ на клиента)"),
    dict(num=101563, date="2026-11-29", name="Скидка 20% на шампуни и бальзамы для волос",
         codes="63030706\n63030701\n63030707",
         coupon_title="Только 29.11. - скидка 20% на шампуни и бальзамы",
         coupon="Шампуни, бальзамы и маски Elseve, Schauma, Garnier Fructis, Чистая линия, Head & Shoulders и Pantene.",
         button="Выбрать dixyapp://app/products_list?id=4027&name=Шампуни и уход за волосами",
         title="💆-20% на шампуни и бальзамы",
         body="Для всех типов волос и против перхоти: Elseve, Schauma, Garnier Fructis, Чистая линия, Head & Shoulders и Pantene. Действует только сегодня, 29 ноября. Активируй скидку в приложении.",
         why="как 101354 (23.08: +0,86 ₽ на клиента)"),
]
NUMS = {str(p["num"]) for p in P}


def batch_delete(ss, ws, rows):
    if not rows:
        return
    ss.batch_update({"requests": [{"deleteDimension": {"range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": r - 1, "endIndex": r}}}
                                  for r in sorted(rows, reverse=True)]})


def white(ss, ws, a, b, ncol):
    ss.batch_update({"requests": [{"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": a - 1, "endRowIndex": b, "startColumnIndex": 0, "endColumnIndex": ncol},
                                                  "cell": {"userEnteredFormat": {"backgroundColor": WHITE}}, "fields": "userEnteredFormat.backgroundColor"}}]})


def main():
    bdir = Path(sys.argv[1]); bdir.mkdir(parents=True, exist_ok=True)
    ss = sc.get_spreadsheet(); wo = ss.worksheet(sc.SHEET_CVM_OFFLINE); wp = ss.worksheet(sc.SHEET_PUSH)
    v = wo.get_all_values(value_render_option="FORMULA"); pv = wp.get_all_values(value_render_option="FORMULA")
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    (bdir / f"backup_cvm_offline_{stamp}.json").write_text(json.dumps(v, ensure_ascii=False), encoding="utf-8")
    (bdir / f"backup_push_{stamp}.json").write_text(json.dumps(pv, ensure_ascii=False), encoding="utf-8")
    print("бэкап:", bdir, stamp, "CVM offline", len(v), "PUSH", len(pv))

    # CVM offline
    hdr = v[0]
    batch_delete(ss, wo, [i for i, r in enumerate(v[1:], start=2) if str(r[1]).strip() in NUMS])
    rows = []
    for p in P:
        s = dt.date.fromisoformat(p["date"]); ddm = f"{s.day:02d}.{s.month:02d}."
        r = {h: "" for h in hdr}
        r.update({"Настройка": "да", "НОМЕР": p["num"], "Название промо": p["name"], "Сегмент": "Активные, Новые", "Год": 2026, "Месяц": s.month,
                  "Неделя": s.isocalendar()[1], "День недели": "вс", "Старт акции": ddm, "Окончание акции": ddm, "Каналы коммуникации": "PUSH",
                  "Название МС": f"{p['num']}_{p['name']}", "Описание акции": p["name"], "Механика": "Скидка X% на категорию", "Категория": p["codes"],
                  "Купон": "да", "Название информационного купона для МП": p["coupon_title"],
                  "Текст на информационном купоне / слип-чеке": f"{p['coupon']}\n\nАктивируй скидку. Действует только {s.day} {MONTH[s.month]}." + TAIL,
                  "Кнопка": p["button"],
                  "Ограничения и комментарии": f"non-food вс; условия {p['why']}; пуш {ddm[:-1]} в 11:00, КГ 5%"})
        vals, seen = [], 0
        for h in hdr:  # «Скидка» дважды: номинал — в первый столбец
            if h == "Скидка":
                seen += 1; vals.append("20%" if seen == 1 else "")
            else:
                vals.append(r[h])
        rows.append(vals)
    res = wo.append_rows(rows, value_input_option="USER_ENTERED")
    m = re.search(r"!A?(\d+):\D*(\d+)", res.get("updates", {}).get("updatedRange", ""))
    if m:
        white(ss, wo, int(m.group(1)), int(m.group(2)), wo.col_count)
    print("CVM offline: строки", res.get("updates", {}).get("updatedRange"))

    # PUSH: формулы как у строк сетки (VLOOKUP по номеру в CVM offline), дата — серийным числом, время 11:00
    pv = wp.get_all_values(value_render_option="FORMULA")
    batch_delete(ss, wp, [i for i, r in enumerate(pv[1:], start=2) if len(r) > 11 and str(r[11]).strip() in NUMS])
    wp = ss.worksheet(sc.SHEET_PUSH); pv = wp.get_all_values(value_render_option="FORMULA")
    fmt_src = next(i for i, r in enumerate(pv[1:], start=2) if len(r) > 11 and str(r[11]).strip() == "101419")
    start = next((i for i, r in enumerate(pv[1:], start=2) if not any(str(c).strip() for c in (r[7:8] + r[11:13]))), len(pv) + 1)
    out = []
    for k, p in enumerate(P):
        i = start + k; ser = (dt.date.fromisoformat(p["date"]) - dt.date(1899, 12, 30)).days
        out.append([f"=VLOOKUP(L{i};'CVM offline'!B:LO;3;FALSE)", f"=VLOOKUP(L{i};'CVM offline'!B:LO;10;FALSE)", f"=VLOOKUP(L{i};'CVM offline'!B:LO;2;FALSE)",
                    f"=YEAR(H{i})", f"=MONTH(H{i})", f"=WEEKNUM(H{i};2)", f"=VLOOKUP(WEEKDAY(H{i};2);'данные'!A:B;2;FALSE)", ser, 11 / 24,
                    f"=VLOOKUP(L{i};'CVM offline'!B:N;13;false)", "минус фрод", p["num"], 1, p["title"], f"=LEN(N{i})", p["body"], f"=LEN(P{i})", "купон", "", ""])
    end = start + len(out) - 1
    if end > wp.row_count:
        wp.add_rows(end - wp.row_count)
    wp.update(range_name=f"A{start}:T{end}", values=out, value_input_option="USER_ENTERED")
    ss.batch_update({"requests": [{"copyPaste": {"source": {"sheetId": wp.id, "startRowIndex": fmt_src - 1, "endRowIndex": fmt_src, "startColumnIndex": 0, "endColumnIndex": 20},
                                                 "destination": {"sheetId": wp.id, "startRowIndex": start - 1, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": 20},
                                                 "pasteType": "PASTE_FORMAT"}}]})
    white(ss, wp, start, end, 20)
    print(f"PUSH: строки {start}-{end}, формат с строки {fmt_src}")


if __name__ == "__main__":
    main()
