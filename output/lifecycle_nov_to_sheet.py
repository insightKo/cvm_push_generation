"""Общие акции ноября 2026 на сегменты жизненного цикла (101550–101557) → Google «CVM offline» + PUSH. ОК Елены 01.11.2026.

Источник — черновик output/пуши_ноябрь_ЖЦ_общие.json (акции, сегменты, сроки, пуши). Пишутся только согласованные поля:
строка акции (номер, название, сегмент, даты, механика, текст для Manzana, сгорание) и пуши. Тексты купонов и коды категорий
(101554) — отдельным шагом после согласования.
Серии с подномером (101550_1 …) — как 101426: «Сегмент МС» = номер серии, в PUSH сегмент и название значениями, «Номер промо» —
номер серии, msg по порядку. Одиночные (101552, 101557) — формулами VLOOKUP, как у остальных строк сетки.
Идемпотентно: строки с этими НОМЕР удаляются и пишутся заново. Перед записью — бэкап обоих листов.
Запуск: .venv/Scripts/python.exe output/lifecycle_nov_to_sheet.py <папка_бэкапа>
"""
import datetime as dt, json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402

WHITE = {"red": 1, "green": 1, "blue": 1}
DRAFT = ROOT / "output" / "пуши_ноябрь_ЖЦ_общие.json"
DOW = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
BASES = {"101550", "101551", "101552", "101553", "101554", "101555", "101556", "101557"}
SERIES_NAME = {
    "101550": "Дарим 100р. монетами на выходные", "101551": "Дарим 50р. монетами на выходные", "101553": "Дарим 50р. монетами на выходные",
    "101554": "50% кешбэк на категории по выходным", "101555": "Вернём 200р. монетами с покупки от 700р.", "101556": "Дарим 200р. монетами на неделю",
}


def d(s):  # «06.11.» → date 2026
    return dt.date(2026, int(s[3:5]), int(s[0:2]))


def ddmm(x):
    return f"{x.day:02d}.{x.month:02d}"


def sunday_after(x):  # П1: ближайшее воскресенье после последнего дня; акция кончилась в вс — следующее воскресенье
    return x + dt.timedelta(days=(6 - x.weekday()) or 7)


def fields(p):
    base, sub = p["num"].partition("_")[0], p["num"].partition("_")[2]
    s, e = d(p["start"]), d(p["end"])
    r = {"Бонусы": "", "Скидка": "", "Срок сгорания бонусов": "", "Механика для Manzana Online": ""}
    if base in ("101550", "101551", "101553"):
        n = 100 if base == "101550" else 50
        r.update({"Описание акции": f"Дарим {n}р. монетами на любые покупки с пятницы по воскресенье, монеты — после активации в приложении",
                  "Бонусы": n, "Механика": "Начисление монет",
                  "Механика для Manzana Online": f"Начисление {n} монет с активацией: клиент активирует купон в приложении, после активации ему начисляются {n} монет; монеты действуют до {ddmm(e)}.2026 23:59 включительно; на любые покупки. 1 раз за акцию. Повторяется каждые выходные ({base}_1–{base}_3), сегмент один на весь период",
                  "Срок сгорания бонусов": f"{ddmm(e)}.2026 23:59:00"})
    elif base == "101552":
        r.update({"Описание акции": "Вернём 150р. монетами с покупки от 1000р.", "Бонусы": 150, "Механика": "Кешбэк X монет за покупку",
                  "Механика для Manzana Online": "Вернём 150 монет за покупку от 1000р. (с учётом скидок) в период акции 03.11–26.11. Монеты начисляются на следующий день после покупки. Начисление не более 3 раз за период акции",
                  "Срок сгорания бонусов": "29.11.2026 23:59:00"})
    elif base == "101554":
        cats = p["name"].replace("50% кешбэк на ", "")
        r.update({"Описание акции": f"Кешбэк 50% на {cats}", "Бонусы": "50%", "Механика": "Кешбэк X% с покупки категории",
                  "Механика для Manzana Online": f"Акцептный кешбэк 50% монетами с покупок в категориях акции ({cats}) в период акции {ddmm(s)}–{ddmm(e)}",
                  "Срок сгорания бонусов": f"{ddmm(sunday_after(e))}.2026 23:59:00"})
    elif base == "101555":
        r.update({"Описание акции": "Вернём 200р. монетами с покупки от 700р. — кешбэк выходного дня", "Бонусы": 200, "Механика": "Кешбэк X монет за покупку",
                  "Механика для Manzana Online": f"Вернём 200 монет за покупку от 700р. (с учётом скидок) в период акции {ddmm(s)}–{ddmm(e)}. Монеты начисляются на следующий день после покупки. Начисление 1 раз за период акции",
                  "Срок сгорания бонусов": f"{ddmm(sunday_after(e))}.2026 23:59:00"})
    elif base == "101556":
        r.update({"Описание акции": "Дарим 200р. монетами на неделю на любые покупки в ДИКСИ", "Бонусы": 200, "Механика": "Начисление монет",
                  "Механика для Manzana Online": f"Начисление 200 монет на счёт клиентам сегмента в понедельник {ddmm(s)}.2026 без активации; монеты действуют до {ddmm(e)}.2026 23:59 включительно; на любые покупки. Повторяется каждую неделю (101556_1–101556_3), сегмент один на весь период",
                  "Срок сгорания бонусов": f"{ddmm(e)}.2026 23:59:00"})
    elif base == "101557":
        r.update({"Описание акции": "Активируй 250р. на чек от 1500р.", "Скидка": 250, "Механика": "Скидка X р. на чек",
                  "Механика для Manzana Online": "Акцептная скидка 250р. на покупку от 1500р. в период акции 27.11–30.11 на все товары кроме акционных. Применяется 1 раз за весь период акции"})
    r["Сегмент МС"] = int(base) if sub else ""
    r["Название МС"] = f"{base}_{SERIES_NAME[base]}" if sub else f"{base}_{p['name']}"
    r["Ограничения и комментарии"] = (f"общие акции ЖЦ ноября (файл Елены «ноябрь общие сегменты», ОК 01.11.2026); КГ {p['kg']}; "
                                      f"пуши: {', '.join(m['date'][8:10] + '.11' for m in p['pushes'])} в 11:00")
    return r


def batch_delete(ss, ws, rows):
    if rows:
        ss.batch_update({"requests": [{"deleteDimension": {"range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": r - 1, "endIndex": r}}}
                                      for r in sorted(rows, reverse=True)]})


def white(ss, ws, a, b, ncol):
    ss.batch_update({"requests": [{"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": a - 1, "endRowIndex": b, "startColumnIndex": 0, "endColumnIndex": ncol},
                                                  "cell": {"userEnteredFormat": {"backgroundColor": WHITE}}, "fields": "userEnteredFormat.backgroundColor"}}]})


def main():
    P = json.loads(DRAFT.read_text(encoding="utf-8"))
    nums = {p["num"] for p in P}
    assert {n.split("_")[0] for n in nums} == BASES, nums
    bdir = Path(sys.argv[1]); bdir.mkdir(parents=True, exist_ok=True)
    ss = sc.get_spreadsheet(); wo = ss.worksheet(sc.SHEET_CVM_OFFLINE); wp = ss.worksheet(sc.SHEET_PUSH)
    v = wo.get_all_values(value_render_option="FORMULA"); pv = wp.get_all_values(value_render_option="FORMULA")
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    (bdir / f"backup_cvm_offline_{stamp}.json").write_text(json.dumps(v, ensure_ascii=False), encoding="utf-8")
    (bdir / f"backup_push_{stamp}.json").write_text(json.dumps(pv, ensure_ascii=False), encoding="utf-8")
    print("бэкап:", bdir, stamp, "CVM offline", len(v), "PUSH", len(pv))
    # чужие строки с этими номерами — стоп (номера сверены с сессиями 01.11, но лист правят параллельно)
    foreign = [r[1] for r in v[1:] if str(r[1]).split("_")[0] in BASES and str(r[1]) not in nums]
    assert not foreign, f"в CVM offline чужие строки с моими номерами: {foreign}"

    # CVM offline
    hdr = v[0]
    batch_delete(ss, wo, [i for i, r in enumerate(v[1:], start=2) if str(r[1]).strip() in nums])
    rows = []
    for p in P:
        s = d(p["start"])
        r = {h: "" for h in hdr}
        r.update({"Настройка": "да", "НОМЕР": p["num"] if "_" in p["num"] else int(p["num"]), "Название промо": p["name"], "Сегмент": p["seg"],
                  "Год": 2026, "Месяц": s.month, "Неделя": s.isocalendar()[1], "День недели": DOW[s.weekday()],
                  "Старт акции": p["start"], "Окончание акции": p["end"], "Каналы коммуникации": "PUSH", "Купон": "да"})
        f = fields(p)
        vals, seen = [], 0
        for h in hdr:  # «Скидка» дважды: номинал — в первый столбец
            if h == "Скидка":
                seen += 1; vals.append(f["Скидка"] if seen == 1 else "")
            elif h in f:
                vals.append(f[h])
            else:
                vals.append(r[h])
        rows.append(vals)
    res = wo.append_rows(rows, value_input_option="USER_ENTERED")
    m = re.search(r"!A?(\d+):\D*(\d+)", res.get("updates", {}).get("updatedRange", ""))
    if m:
        white(ss, wo, int(m.group(1)), int(m.group(2)), wo.col_count)
    print("CVM offline: строки", res.get("updates", {}).get("updatedRange"))

    # PUSH
    pv = wp.get_all_values(value_render_option="FORMULA")
    batch_delete(ss, wp, [i for i, r in enumerate(pv[1:], start=2) if len(r) > 11 and str(r[11]).strip() in BASES])
    wp = ss.worksheet(sc.SHEET_PUSH); pv = wp.get_all_values(value_render_option="FORMULA")
    fmt_src = next(i for i, r in enumerate(pv[1:], start=2) if len(r) > 11 and str(r[11]).strip() == "101419")
    start = next((i for i, r in enumerate(pv[1:], start=2) if not any(str(c).strip() for c in (r[7:8] + r[11:13]))), len(pv) + 1)
    sends = sorted(((m["date"], p, m) for p in P for m in p["pushes"]), key=lambda t: (t[0], t[1]["num"]))
    out = []
    for k, (date, p, m) in enumerate(sends):
        i = start + k; ser = (dt.date.fromisoformat(date) - dt.date(1899, 12, 30)).days
        base = p["num"].split("_")[0]
        if "_" in p["num"]:
            a, b, c, j = p["seg"], "PUSH", SERIES_NAME[base], ""
        else:
            a, b, c = f"=VLOOKUP(L{i};'CVM offline'!B:LO;3;FALSE)", f"=VLOOKUP(L{i};'CVM offline'!B:LO;10;FALSE)", f"=VLOOKUP(L{i};'CVM offline'!B:LO;2;FALSE)"
            j = f"=VLOOKUP(L{i};'CVM offline'!B:N;13;false)"
        out.append([a, b, c, f"=YEAR(H{i})", f"=MONTH(H{i})", f"=WEEKNUM(H{i};2)", f"=VLOOKUP(WEEKDAY(H{i};2);'данные'!A:B;2;FALSE)", ser, 11 / 24,
                    j, p["k"], int(base), m["msg"], m["title"], f"=LEN(N{i})", m["body"], f"=LEN(P{i})", p["screen"], "", ""])
    end = start + len(out) - 1
    if end > wp.row_count:
        wp.add_rows(end - wp.row_count)
    wp.update(range_name=f"A{start}:T{end}", values=out, value_input_option="USER_ENTERED")
    ss.batch_update({"requests": [{"copyPaste": {"source": {"sheetId": wp.id, "startRowIndex": fmt_src - 1, "endRowIndex": fmt_src, "startColumnIndex": 0, "endColumnIndex": 20},
                                                 "destination": {"sheetId": wp.id, "startRowIndex": start - 1, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": 20},
                                                 "pasteType": "PASTE_FORMAT"}}]})
    white(ss, wp, start, end, 20)
    print(f"PUSH: строки {start}-{end} ({len(out)} отправок), формат с строки {fmt_src}")


if __name__ == "__main__":
    main()
