"""Купоны и условия общих акций ЖЦ ноября 2026 (101550–101557) → Google «CVM offline», точечно по НОМЕР.

Пишутся только колонки: «Механика для Manzana Online» (101554), «Категория» (101554 — список SKU), «Купон»,
«Название информационного купона для МП», «Текст на информационном купоне / слип-чеке», «Кнопка». Остальные ячейки строк не трогаются.
Тексты — по образцам тех же механик: 101376 (начисление с активацией), 101426 (без активации), 101358 (кешбэк за покупку),
101428 (50% кешбэк на категорию), 101432 (скидка на чек). Инфокупон 101552 согласован Еленой 01.11.2026, остальные она
поручила записать самой («ты сама записываешь все промо»).
101554: в «Категорию» — все SKU живого ассортимента (I_CVM_LIVE_ASSORT, чеки за 4 недели) категорий CVM акции, как у 101445.
Перед записью — бэкап листа. Запуск: .venv/Scripts/python.exe output/lifecycle_nov_coupons.py <la2_out.tsv> <папка_бэкапа>
"""
import datetime as dt, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent; sys.path.insert(0, str(ROOT))
import sheets_client as sc  # noqa: E402
from gspread.utils import rowcol_to_a1  # noqa: E402

LEGAL = "1 монета = 1 рубль.\nИспользование монет осуществляется согласно Правилам программы лояльности."
WHERE = "с картой клуба друзей ДИКСИ в магазине\nили онлайн-заказа в приложении ДИКСИ с доставкой (от 40 мин) или самовывозом"
CATALOG = "В КАТАЛОГ dixyapp://app/catalog"
MON = "ноября"
WEEKENDS = [("06.11", "08.11", "15.11"), ("13.11", "15.11", "22.11"), ("20.11", "22.11", "29.11")]
CATS_101554 = {1: ([1279, 1281], "Шоколад и шоколадные батончики.", "шоколад и батончики", "В КАТАЛОГ dixyapp://app/products_list?id=717&name=Кондитерские изделия, торты"),
               2: ([1041, 1049, 1321], "Чипсы, сухарики и кола.", "чипсы, сухарики и колу", "В КАТАЛОГ dixyapp://app/products_list?id=1538&name=Чипсы, орехи и снеки"),
               3: ([1282, 1498, 1388], "Торты, пирожные и чёрный чай в пакетиках.", "торты, пирожные и чай", "В КАТАЛОГ dixyapp://app/products_list?id=717&name=Кондитерские изделия, торты")}


def day(ddmm):
    return int(ddmm[:2])


def coupons(skus):
    c = {}
    for k, (a, b, burn) in enumerate(WEEKENDS, start=1):
        for base, n, title in (("101550", 100, f"До {b}. - активируй 100 монет на выходные"),
                               ("101551", 50, f"До {b}. - активируй 50 монет на выходные"),
                               ("101553", 50, f"До {b}. - активируй 50р. монетами на выходные")):
            c[f"{base}_{k}"] = {"title": title, "button": CATALOG,
                                "text": f"для любых покупок\n{WHERE}\nс {a} по {b} включительно.\n\nАктивируй купон — монеты придут на счёт. Срок действия монет: {b}.2026 включительно.\n\n{LEGAL}"}
        cats, what, short, btn = CATS_101554[k]
        c[f"101554_{k}"] = {"title": f"До {b}. - 50% кешбэка на {short}", "button": btn,
                            "text": f"{what}\n\nАктивируй и покупай с {day(a)} по {day(b)} {MON}. Монеты придут на счёт на следующий день после покупки — потратить можно до {day(burn)} {MON} включительно.\n\n{LEGAL}",
                            "codes": "\n".join(skus[k]),
                            "manzana": f"Акцептный кешбэк 50% монетами с суммы покупок товаров из списка SKU в период акции {a}–{b}"}
        c[f"101555_{k}"] = {"title": f"До {b}. - 200 монет за покупку от 700р.", "button": CATALOG,
                            "text": f"за покупку от 700р. {WHERE}\nс {a} по {b} включительно.\n\nМонеты придут на счёт на следующий день после покупки — потратить можно до {day(burn)} {MON} включительно.\nБонус в размере 200 монет можно получить один раз за весь период акции.\n\n{LEGAL}"}
    for k, (a, b) in enumerate([("02.11", "08.11"), ("09.11", "15.11"), ("16.11", "22.11"), ("23.11", "29.11")], start=1):
        c[f"101556_{k}"] = {"title": f"До {b}. - 200 монет на неделю", "button": CATALOG,
                            "text": f"для любых покупок\n{WHERE}\nс {a} по {b} включительно.\n\nМонеты уже на счёте.\n\n{LEGAL}"}
    c["101552"] = {"title": "До 26.11. - 150 монет с покупки от 1000р.", "button": CATALOG,
                   "text": "Вернём 150 монет за покупку от 1000р. с картой клуба друзей ДИКСИ в магазине или при онлайн-заказе в приложении ДИКСИ — до 3 раз с 3 по 26 ноября.\n\nМонеты придут на счёт на следующий день после покупки — потратить можно до 29 ноября включительно.\n\n" + LEGAL}
    c["101557"] = {"title": "До 30.11. - активируй 250р. на чек от 1500р.", "button": CATALOG,
                   "text": "Скидка действует на 1 покупку.\n\nДля применения скидки просто используй карту клуба друзей ДИКСИ на кассе.\n\nПосле активации скидка действует до 30 ноября 2026 включительно.\n\nСкидка не применяется к категориям товаров: социально значимые товары («соцтовар»), алкоголь, табак, лотерейные билеты."}
    return c


def main():
    la, bdir = Path(sys.argv[1]), Path(sys.argv[2]); bdir.mkdir(parents=True, exist_ok=True)
    rows = [l.split("\t") for l in la.read_text(encoding="utf-8").splitlines() if l[:1].isdigit()]
    skus = {}
    for k, (cats, *_r) in CATS_101554.items():
        lst = [r[4].rsplit("_", 1)[0] for r in sorted(rows, key=lambda r: -int(r[3])) if int(r[0]) in cats]
        skus[k] = list(dict.fromkeys(lst))
        print(f"101554_{k}: SKU {len(skus[k])}")
    C = coupons(skus)
    ss = sc.get_spreadsheet(); wo = ss.worksheet(sc.SHEET_CVM_OFFLINE)
    v = wo.get_all_values(value_render_option="FORMULA")
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    (bdir / f"backup_cvm_offline_coupons_{stamp}.json").write_text(json.dumps(v, ensure_ascii=False), encoding="utf-8")
    h = v[0]
    col = {name: h.index(name) + 1 for name in ("Механика для Manzana Online", "Категория", "Купон", "Название информационного купона для МП",
                                                 "Текст на информационном купоне / слип-чеке", "Кнопка")}
    upd, found = [], set()
    for i, r in enumerate(v[1:], start=2):
        num = str(r[1]).strip()
        if num not in C:
            continue
        found.add(num); x = C[num]
        cells = {"Купон": "да", "Название информационного купона для МП": x["title"], "Текст на информационном купоне / слип-чеке": x["text"], "Кнопка": x["button"]}
        if "codes" in x:
            cells["Категория"] = x["codes"]; cells["Механика для Manzana Online"] = x["manzana"]
        for name, val in cells.items():
            upd.append({"range": rowcol_to_a1(i, col[name]), "values": [[val]]})
    missing = set(C) - found
    assert not missing, f"нет строк в CVM offline: {sorted(missing)}"
    wo.batch_update(upd, value_input_option="RAW")
    print("бэкап", stamp, "| строк", len(found), "| ячеек", len(upd))


if __name__ == "__main__":
    main()
