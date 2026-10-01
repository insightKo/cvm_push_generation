#!/bin/bash
# Загрузка справочника ассортимента в Postgres портала на ru.
#
# Запускать с Mac после ОК: bash output/assortment/load_to_ru.sh
# Данные ДИКСИ идут потоком через kz прямо в psql на ru — на диск kz ничего не пишется.
# Загрузка одной транзакцией: при любой ошибке база остаётся в прежнем состоянии.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
REMOTE=/srv/cvm_portal/data/import/assortment

echo "→ база: DDL + данные одной транзакцией"
gzip -c "$DIR/assortment_load.sql" \
  | ssh kz "ssh ru 'gunzip -c | (cd /srv/cvm_portal/app/db && ./psql.sh -q -f -)'"

echo "→ исходники в $REMOTE (чтобы было видно, из чего собрано)"
ssh kz "ssh ru 'mkdir -p $REMOTE'"
for f in 004_assortment.sql category.csv product.csv product_city.csv ассортимент_полный.xlsx build_report.json; do
  gzip -c "$DIR/$f" | ssh kz "ssh ru 'gunzip -c > \"$REMOTE/$f\"'"
  echo "   $f"
done

echo "→ проверка"
ssh kz "ssh ru 'cd /srv/cvm_portal/app/db && ./psql.sh -c \"select level, count(*) as кодов, count(name) as с_именем from cvm.category group by level order by level\" -c \"select count(*) as товаров from cvm.product\" -c \"select city, count(*) from cvm.product_city group by city\"'"
