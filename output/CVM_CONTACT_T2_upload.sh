#!/bin/bash
# Выкладка CVM_CONTACT_T2_20261101.sql (ускорение CVM_CONTACT, бэклог ночного задания Т2) в обменник MCI
# (D:\cvm_api\exchange). Путь: эта машина → kz → ru → контейнер cvm-api → SFTP. Записи других скриптов в manifest.json
# сохраняются. Задание CVM_API_nightly выполняет скрипт каждые 10 минут; до 17:00 по серверу и пока идёт «MCI Regular update»
# или загрузка MCI, скрипт падает своей ошибкой без изменений и повторяется; сверка и ALTER — один раз (строка
# 'CVM_CONTACT T2 сверка' в I_DISCOUNT_PROC_BACKUP), после неё раннер уносит файл в done\.
# Файл уходит в UTF-16 LE с BOM (sqlcmd -i без -f читает его верно); BOM UTF-8 исходника снимается.
# Запуск:  bash output/CVM_CONTACT_T2_upload.sh
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
NAME="CVM_CONTACT_T2_20261101.sql"
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run 'rm -rf /tmp/xt2_up; mkdir -p /tmp/xt2_up; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > /tmp/kh_mci_t2; ssh-keygen -lf /tmp/kh_mci_t2' </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
TMP16="$(mktemp)"
printf '\xff\xfe' > "$TMP16"
sed '1s/^\xEF\xBB\xBF//' "$DIR/$NAME" | iconv -f UTF-8 -t UTF-16LE >> "$TMP16"
echo "$NAME: $(sha256sum "$TMP16" | cut -c1-64)"
run "cat > /tmp/xt2_up/$NAME" < "$TMP16"
rm -f "$TMP16"
run 'cat > /tmp/xt2_up/upload.sh' <<'UP'
set -e
cd /tmp/xt2_up
H=/tmp/kh_mci_t2
K=/srv/cvm_api/.ssh/id_ed25519
T=cvm-portal@185.130.112.179
printf 'get manifest.json /tmp/xt2_up/manifest.json\n' > b1.txt
sftp -b b1.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 - <<'PY'
import json, datetime, hashlib
name = 'CVM_CONTACT_T2_20261101.sql'
m = json.load(open('/tmp/xt2_up/manifest.json', encoding='utf-8-sig'))   # манифест может быть с BOM
print('В манифесте до:', ', '.join(s['name'] for s in m['scripts']) or '(пусто)')
m['scripts'] = [s for s in m['scripts'] if s['name'] != name] + [
    {'name': name, 'sha256': hashlib.sha256(open('/tmp/xt2_up/' + name, 'rb').read()).hexdigest()}]
m['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
m['publishedBy'] = 'Елена (скрипт Claude): ускорение CVM_CONTACT со сверкой до применения (бэклог ночного задания Т2)'
json.dump(m, open('/tmp/xt2_up/manifest.new.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
PY
printf 'put CVM_CONTACT_T2_20261101.sql CVM_CONTACT_T2_20261101.sql\nput manifest.new.json manifest.json\nget manifest.json /tmp/xt2_up/check.json\n' > b2.txt
sftp -b b2.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 -c "import json;print('В манифесте после:', ', '.join(s['name'] for s in json.load(open('/tmp/xt2_up/check.json',encoding='utf-8-sig'))['scripts']))"
cd /tmp && rm -rf /tmp/xt2_up /tmp/kh_mci_t2
UP
run 'sh /tmp/xt2_up/upload.sh' </dev/null
echo "Выложено: $NAME. Сверка и применение — в первый проход обменника после 17:00 по серверу и окончания «MCI Regular update»."
echo "Проверка: SELECT DEFINITION FROM dbo.I_DISCOUNT_PROC_BACKUP (nolock) WHERE PROC_NAME = N'CVM_CONTACT T2 сверка'; CVM_API_JOB_LOG."
