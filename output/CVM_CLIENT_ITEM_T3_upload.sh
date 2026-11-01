#!/bin/bash
# Выкладка CVM_CLIENT_ITEM_T3_20261101.sql (шаг CLIENT_ITEM не переписывает неизменившиеся дни витрины покупок, бэклог ночного
# задания Т3) в обменник MCI (D:\cvm_api\exchange). Путь: эта машина → kz → ru → контейнер cvm-api → SFTP. Записи других
# скриптов в manifest.json сохраняются. Задание CVM_API_nightly выполняет скрипт каждые 10 минут; вне 05:30–09:00 по серверу (или задание ближе трёх шагов к CLIENT_ITEM)
# или пока замок витрины занят скрипт падает своей ошибкой без изменений и повторяется; применённый — выходит без ошибки,
# раннер уносит файл в done\.
# Файл уходит в UTF-16 LE с BOM (sqlcmd -i без -f читает его верно); BOM UTF-8 исходника снимается.
# Запуск:  bash output/CVM_CLIENT_ITEM_T3_upload.sh
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
NAME="CVM_CLIENT_ITEM_T3_20261101.sql"
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run 'rm -rf /tmp/xt3_up; mkdir -p /tmp/xt3_up; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > /tmp/kh_mci_t3; ssh-keygen -lf /tmp/kh_mci_t3' </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
TMP16="$(mktemp)"
printf '\xff\xfe' > "$TMP16"
sed '1s/^\xEF\xBB\xBF//' "$DIR/$NAME" | iconv -f UTF-8 -t UTF-16LE >> "$TMP16"
echo "$NAME: $(sha256sum "$TMP16" | cut -c1-64)"
run "cat > /tmp/xt3_up/$NAME" < "$TMP16"
rm -f "$TMP16"
run 'cat > /tmp/xt3_up/upload.sh' <<'UP'
set -e
cd /tmp/xt3_up
H=/tmp/kh_mci_t3
K=/srv/cvm_api/.ssh/id_ed25519
T=cvm-portal@185.130.112.179
printf 'get manifest.json /tmp/xt3_up/manifest.json\n' > b1.txt
sftp -b b1.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 - <<'PY'
import json, datetime, hashlib
name = 'CVM_CLIENT_ITEM_T3_20261101.sql'
m = json.load(open('/tmp/xt3_up/manifest.json', encoding='utf-8-sig'))   # манифест может быть с BOM
print('В манифесте до:', ', '.join(s['name'] for s in m['scripts']) or '(пусто)')
m['scripts'] = [s for s in m['scripts'] if s['name'] != name] + [
    {'name': name, 'sha256': hashlib.sha256(open('/tmp/xt3_up/' + name, 'rb').read()).hexdigest()}]
m['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
m['publishedBy'] = 'Елена (скрипт Claude): витрина покупок — не переписывать неизменившиеся дни (бэклог ночного задания Т3)'
json.dump(m, open('/tmp/xt3_up/manifest.new.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
PY
printf 'put CVM_CLIENT_ITEM_T3_20261101.sql CVM_CLIENT_ITEM_T3_20261101.sql\nput manifest.new.json manifest.json\nget manifest.json /tmp/xt3_up/check.json\n' > b2.txt
sftp -b b2.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 -c "import json;print('В манифесте после:', ', '.join(s['name'] for s in json.load(open('/tmp/xt3_up/check.json',encoding='utf-8-sig'))['scripts']))"
cd /tmp && rm -rf /tmp/xt3_up /tmp/kh_mci_t3
UP
run 'sh /tmp/xt3_up/upload.sh' </dev/null
echo "Выложено: $NAME. Применение — в первый проход обменника 05:30–09:00 по серверу при свободном замке витрины."
echo "Проверка: CVM_API_JOB_LOG; I_DISCOUNT_PROC_BACKUP (PROC_NAME client_item_load, client_item_keys_day); после ночи — CVM_CLIENT_ITEM_LOG.ERROR_MSG."
