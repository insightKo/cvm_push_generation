# ДЦО 18.11: output/CVM_DCO_20261118.sql из скрипта 11.11 (ревью sql-senior, выполнен 10.11) — меняются только дата, номера,
# названия, эталон раздачи (#ref) и имя транспортной таблицы. Вход: promos18.json, refs.tsv (id_promo<TAB>n).
import json, re, sys, io
from pathlib import Path
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(r"c:\Projects\cvm_push_generation")
src = (ROOT / "output/CVM_DCO_20261111.sql").read_text(encoding="utf-8")
P = json.load(open("promos18.json", encoding="utf-8"))
ref = {int(a): int(b) for a, b in (l.split("\t") for l in open("refs.tsv", encoding="utf-8") if l.strip())}
assert sorted(ref) == [p["num"] for p in P] and len(P) == 65
lo, hi, n = P[0]["num"], P[-1]["num"], len(P)
total = sum(ref.values())

head = f"""-- CVM · ДЦО на день: пуш ср 18.11.2026 · {n} акций {lo}–{hi} · Активные, Новые · механика «Коммуникация»
-- Решения Елены 17.11.2026 (сессия ДЦО по кластерам): ДЦО на 18.11 разложены по кластерам офлайна (присвоение 15.11) — каждому
--   кластеру до 6 товаров еды и до 6 нон-фуда, которые кластер берёт чаще среднего (× к среднему с нормировкой на число категорий
--   на клиента), товар — самый покупаемый кластером в округе; у кластеров 2–9 без категорий их кешбэков 19–27.11; кластеру 6 — без
--   кошек, 9 — без прокладок, 7 — только еда. Подбор: output/ДЦО_2026-11-18/ДЦО_2026-11-18_по_кластерам.xlsx (версия 3).
-- Акция = товар со скидкой (одинаковый товар у нескольких кластеров — одна акция); разная скидка или товар в округах — две акции.
-- Пуш получают Активные и Новые (сегменты 1–3 с пушем и приложением, ЦФО и СЗФО) с кластером; каждому один пуш — раздача
--   внутри кластера по правилам 11.11: покупал за 6 недель категорию товара кластера — каскадом от редкой в кластере к частой;
--   нет — товар из группы справочника CVM, где больше всего купленных за 6 недель категорий; нет — категория за 52 недели;
--   остаток — товар кластера по умолчанию (еда, место 1). Без кластера (присвоение 15.11) — не входят.
-- Раздачу считал портал на российском сервере по снимку покупок на 16.11 и положил в базу списков портала
--   (lists.dco_20261118: id_promo, id_contact; {str(f"{total:,}").replace(",", " ")} строк, клиент один раз). Скрипт читает её через связанный сервер CVM_PORTAL.
-- Стандартная сборка: + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG; других пушей на 18.11 нет (кешбэки кластеров
--   101567–101578 — с 19.11), вычитать некого; КГ 5% (RN/20) по BUDGET — подбирается по каждой акции отдельно.
-- LOAD_TO_ML = 2 (PUSH-коммуникация, в Manzana Online не идёт), LOAD_TO_MC = 1 (PUSH) — как ДЦО 30.10 и 11.11.
--
-- Скрипт для задания CVM_API_nightly (обменник): раннер выполняет файл, пока он не пройдёт без ошибки, поэтому:
--   · после дня акции (позже 18.11.2026) — ничего не делается;
--   · шапки I_PROMO заводятся, только если их нет;
--   · акция, список которой уже есть в I_PROMO_OFFER, пропускается — повторный прогон её не задвоит;
--   · из базы списков прочитано не столько строк по акциям, сколько в раздаче → ошибка, ничего не пишется;
--   · у акции после сборки меньше 80% раздачи → ошибка, ничего не пишется;
--   · КГ акции не подобралась за 1 000 итераций → списки сошедшихся акций записываются, в конце ошибка —
--     через 10 минут повтор только по оставшимся.
-- Выгрузки CRM_GUID здесь нет: списки в Campaign грузит робот из базы списков после согловования в портале.
""".replace("согловования", "согласования")

body = src[src.index("SET QUOTED_IDENTIFIER ON"):]
body = body.replace("'2026-11-11'", "'2026-11-18'")
body = body.replace("-- старт и финиш (ср)", "-- старт и финиш (ср)")
body = body.replace("DCO 11.11", "DCO 18.11")
body = body.replace("101497-101522", f"{lo}-{hi}").replace("101497–101522", f"{lo}–{hi}")
body = body.replace("between 101497 and 101522", f"between {lo} and {hi}")
body = body.replace("< 26\n", f"< {n}\n").replace(") = 26\n", f") = {n}\n").replace("N' of ', 26 -", f"N' of ', {n} -")
body = body.replace("all 26 lists", f"all {n} lists").replace("by 26 lists", f"by {n} lists").replace("все 26 шапок", f"все {n} шапок")
body = body.replace("ПО ВСЕМ 26 АКЦИЯМ", f"ПО ВСЕМ {n} АКЦИЯМ").replace("сводка по всем 26 акциям", f"сводка по всем {n} акциям")
body = body.replace("lists.dco_20261111", "lists.dco_20261118")
body = body.replace("(2 953 971 строка, клиент один раз)", f"({str(f"{total:,}").replace(",", " ")} строк, клиент один раз)".replace(",", " "))
body = body.replace("-- раздача на 10.11 (ru: cvm-api:/tmp/dco1111/assign.log) — эталон для сверки чтения и для защиты 80%",
                    "-- раздача на 18.11 (ru: cvm-api:/tmp/dco1811/assign18.log) — эталон для сверки чтения и для защиты 80%")
# шапки
vals = []
for i, p in enumerate(P):
    sep = " " if i == 0 else ","
    nm = p["name"].replace("'", "''"); ml = p["ml"].replace("'", "''")
    vals.append(f"\t{sep} ({p['num']}, N'{nm}', N'{ml}')")
body = re.sub(r"from \(values\n.*?\n\t\) v \(ID_PROMO, PROMO_NAME, PROMO_NAME_ML\)",
              "from (values\n" + "\n".join(vals) + "\n\t) v (ID_PROMO, PROMO_NAME, PROMO_NAME_ML)", body, flags=re.S)
# эталон
items = [f"({k}, {v:>7})" for k, v in sorted(ref.items())]
lines = []
for i in range(0, len(items), 7):
    lines.append(("\t  " if i == 0 else "\t, ") + ", ".join(items[i:i + 7]))
body = re.sub(r"insert into #ref \(ID_PROMO, N_REF\) values\n.*?\n\n", "insert into #ref (ID_PROMO, N_REF) values\n" + "\n".join(lines) + "\n\n",
              body, flags=re.S)
body = body.replace("ЗАПИСЬ", "ЗАПИСЬ")
out = head + "\n" + body
# остатки старых номеров и даты не должны встречаться
left = re.findall(r"1014\d\d|10152\d|11\.11|20261111|2026-11-11", body)
assert not left, left[:10]
(ROOT / "output/CVM_DCO_20261118.sql").write_text(out, encoding="utf-8", newline="\n")
print("ok", n, "акций", total, "строк раздачи; файл", len(out), "знаков")
