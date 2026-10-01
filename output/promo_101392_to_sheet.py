"""Акция 101392 «Активируй 100 монет» (26.09–30.09.2026) → Google-таблица «CVM offline».
Пишет одну строку в лист «CVM offline» и три отправки (сб 26.09, пн 28.09, ср 30.09) в лист PUSH с текстами.
По умолчанию только печатает, что будет записано. Запись — после ОК Елены: python output/promo_101392_to_sheet.py --write\n(--write --push-only — только три строки в PUSH, без строки в CVM offline)"""
import sys, datetime as dt
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent; sys.path.insert(0, str(ROOT))
import sheets_client as sc

NUM = 101392
NAME = "Активируй 100 монет"
SEG = "Активные, Новые"
START, END = dt.date(2026, 9, 26), dt.date(2026, 9, 30)
COUPON_TEXT = ("с картой клуба друзей ДИКСИ в магазине\n"
    "или онлайн-заказа в приложении ДИКСИ с доставкой (от 40 мин) или самовывозом\n"
    "с 26.09 по 30.09 включительно.\n\n"
    "Монеты придут на счёт на следующий день после активации.\n\n"
    "1 монета = 1 рубль.\n"
    "Использование монет осуществляется согласно Правилам программы лояльности.")
ROW = {
    "Настройка": "да", "НОМЕР": NUM, "Название промо": NAME, "Сегмент": SEG,
    "Год": 2026, "Месяц": 9, "Неделя": START.isocalendar()[1], "День недели": "сб",
    "Старт акции": "26.09.", "Окончание акции": "30.09.", "Каналы коммуникации": "PUSH",
    "Название МС": f"{NUM}_{NAME}", "Примерное количество клиентов": "2 770 000",
    "Описание акции": "Активируй 100 монет в ДИКСИ", "Бонусы": "100",
    "Механика": "Предначисление бонусов с активацией", "Срок сгорания бонусов": "30.09.2026 23:59:00",
    "Купон": "да", "Название информационного купона для МП": "Активируй 100 монет",
    "Текст на информационном купоне / слип-чеке": COUPON_TEXT, "Кнопка": "В КАТАЛОГ dixyapp://app/catalog",
    "Ограничения и комментарии": "КГ 5%; добавлено 25.09; минус список 101380",
}
PUSHES = [  # дата, msg, заголовок, текст
    (dt.date(2026, 9, 26), 1, '🎁Дарим 100р. монетами',
     'Трать в магазине или онлайн. Приходи в ДИКСИ — у нас ещё и цены приятные на все необходимые продукты, и ассортимент достойный. Активируй в приложении до 30.09.'),
    (dt.date(2026, 9, 28), 2, '💰Активируй 100р. монетами',
     'и трать до 30.09 в магазине или онлайн. Приходи в ДИКСИ — у нас ещё и цены приятные на все необходимые продукты, и ассортимент достойный.'),
    (dt.date(2026, 9, 30), 3, '⏰Потрать 100р. сегодня',
     'Монеты по акции действуют до конца дня. Трать в магазине или онлайн. Приходи в ДИКСИ — у нас ещё и цены приятные на все необходимые продукты, и ассортимент достойный.'),
]

def push_row(i, date, msg, title, body):
    ser = (date - dt.date(1899, 12, 30)).days
    return [f"=VLOOKUP(L{i};'CVM offline'!B:LO;3;FALSE)", f"=VLOOKUP(L{i};'CVM offline'!B:LO;10;FALSE)",
            f"=VLOOKUP(L{i};'CVM offline'!B:LO;2;FALSE)", f"=YEAR(H{i})", f"=MONTH(H{i})", f"=WEEKNUM(H{i};2)",
            f"=VLOOKUP(WEEKDAY(H{i};2);'данные'!A:B;2;FALSE)", ser, 0.4583333333333333,
            f"=VLOOKUP(L{i};'CVM offline'!B:N;13;false)", "минус фрод", NUM, msg, title, f"=LEN(N{i})", body, f"=LEN(P{i})",
            "купон", "", ""]

if "--write" not in sys.argv:
    print("=== CVM offline, новая строка ===")
    for k, v in ROW.items(): print(f"  [{k}] {v}")
    print("=== PUSH, 3 строки ===")
    for d, m, t, b in PUSHES: print(f"  {d} msg{m} | {t} ({len(t)}) | {b} ({len(b)}) | купон")
    sys.exit()

ss = sc.get_spreadsheet(); wo = ss.worksheet(sc.SHEET_CVM_OFFLINE); wp = ss.worksheet(sc.SHEET_PUSH)
if "--push-only" not in sys.argv:  # строка в CVM offline — только по отдельному ОК Елены
    v = wo.get_all_values(); hdr = v[0]
    assert not any(r[1].strip() == str(NUM) for r in v[1:]), f"{NUM} уже есть в CVM offline"
    wo.append_rows([[ROW.get(h, "") for h in hdr]], value_input_option="USER_ENTERED"); print("CVM offline: строка добавлена")
if "--offline-only" in sys.argv:
    sys.exit()
pv = wp.get_all_values(value_render_option="FORMULA")
assert not any(len(r) > 12 and str(r[11]) == str(NUM) for r in pv[1:]), f"{NUM} уже есть в PUSH"
start = next((i for i, r in enumerate(pv[1:], start=2) if not any(str(c).strip() for c in (r[7:8] + r[11:13]))), len(pv) + 1)
vals = [push_row(start + k, *x) for k, x in enumerate(PUSHES)]; end = start + len(vals) - 1
if end > wp.row_count: wp.add_rows(end - wp.row_count)
wp.update(range_name=f"A{start}:T{end}", values=vals, value_input_option="USER_ENTERED")
ss.batch_update({"requests": [{"copyPaste": {"source": {"sheetId": wp.id, "startRowIndex": 467, "endRowIndex": 468, "startColumnIndex": 0, "endColumnIndex": 20},
    "destination": {"sheetId": wp.id, "startRowIndex": start - 1, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": 20}, "pasteType": "PASTE_FORMAT"}}]})
print(f"PUSH: строки {start}-{end} записаны")
