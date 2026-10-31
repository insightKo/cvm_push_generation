#!/bin/bash
# Выкладка в обменник MCI (D:\cvm_api\exchange) двух скриптов по порядку: сначала разметка единиц CVM_PRODUCT_UNIT_LOAD.sql,
# за ней CVM_PRICE_INDEX_MARTS.sql (процедуры с единицами, разовый сброс расчёта «за упаковку», задание пересчёта всей истории).
# Путь: эта машина → kz → ru → контейнер cvm-api → SFTP. Записи других скриптов в manifest.json сохраняются.
# Задание CVM_API_nightly выполняет их в ближайшие 10 минут окна 06:00–21:00. Витрины без разметки стоят на стопе
# (ошибка, обменник повторит через 10 минут) — порядок безопасен при любом раскладе.
# Файлы уходят в UTF-16 LE с BOM (sqlcmd -i без -f читает их верно); BOM UTF-8 исходника снимается.
# Запуск:  bash output/CVM_PRICE_INDEX_UNITS_upload.sh
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
NAMES="CVM_PRODUCT_UNIT_LOAD.sql CVM_PRICE_INDEX_MARTS.sql"
# Раннер обменника идёт по алфавиту имён и отрезает «^\d+_» для журнала — номера задают порядок
declare -A PUB=([CVM_PRODUCT_UNIT_LOAD.sql]=1_CVM_PRODUCT_UNIT_LOAD.sql [CVM_PRICE_INDEX_MARTS.sql]=2_CVM_PRICE_INDEX_MARTS.sql)
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run 'rm -rf /tmp/xcd_up; mkdir -p /tmp/xcd_up; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > /tmp/kh_mci; ssh-keygen -lf /tmp/kh_mci' </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
for NAME in $NAMES; do
  TMP16="$(mktemp)"
  printf '\xff\xfe' > "$TMP16"
  sed '1s/^\xEF\xBB\xBF//' "$DIR/$NAME" | iconv -f UTF-8 -t UTF-16LE >> "$TMP16"
  echo "${PUB[$NAME]}: $(sha256sum "$TMP16" | cut -c1-64)"
  run "cat > /tmp/xcd_up/${PUB[$NAME]}" < "$TMP16"
  rm -f "$TMP16"
done
run 'cat > /tmp/xcd_up/upload.sh' <<'UP'
set -e
cd /tmp/xcd_up
H=/tmp/kh_mci
K=/srv/cvm_api/.ssh/id_ed25519
T=cvm-portal@185.130.112.179
printf 'get manifest.json /tmp/xcd_up/manifest.json\n' > b1.txt
sftp -b b1.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 - <<'PY'
import json, datetime, hashlib
names = ['1_CVM_PRODUCT_UNIT_LOAD.sql', '2_CVM_PRICE_INDEX_MARTS.sql']   # порядок выполнения
old = {n.split('_', 1)[1] for n in names}                                # прежние записи без номера
m = json.load(open('/tmp/xcd_up/manifest.json', encoding='utf-8-sig'))   # манифест может быть с BOM
m['scripts'] = [s for s in m['scripts'] if s['name'] not in names and s['name'] not in old] + [
    {'name': n, 'sha256': hashlib.sha256(open('/tmp/xcd_up/' + n, 'rb').read()).hexdigest()} for n in names]
m['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
m['publishedBy'] = 'Елена (скрипт Claude): разметка единиц и пересчёт витрин ценового индекса'
json.dump(m, open('/tmp/xcd_up/manifest.new.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
PY
printf 'put 1_CVM_PRODUCT_UNIT_LOAD.sql 1_CVM_PRODUCT_UNIT_LOAD.sql\nput 2_CVM_PRICE_INDEX_MARTS.sql 2_CVM_PRICE_INDEX_MARTS.sql\nput manifest.new.json manifest.json\nget manifest.json /tmp/xcd_up/check.json\n' > b2.txt
sftp -b b2.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 -c "import json;print('В манифесте:', ', '.join(s['name'] for s in json.load(open('/tmp/xcd_up/check.json',encoding='utf-8-sig'))['scripts']))"
cd /tmp && rm -rf /tmp/xcd_up
UP
run 'sh /tmp/xcd_up/upload.sh' </dev/null
echo "Выложено: $NAMES. Проверка через 10–20 минут: CVM_API_JOB_LOG (оба ok), I_CVM_PRODUCT_UNIT (SOURCE 1 ≈ 242 тыс.), журнал I_CVM_PM_LOG (старые записи — old, идут новые недели)."
