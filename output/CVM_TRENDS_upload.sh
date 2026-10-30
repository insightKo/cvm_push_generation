#!/bin/bash
# Выкладка скрипта трендов в обменник MCI: эта машина → kz → ru → cvm-api → SFTP (D:\cvm_api\exchange).
# Аргумент — имя файла в output/: CVM_TRENDS_FIX_20261031.sql или CVM_TRENDS_HISTORY_20261031.sql.
# Скрипт встаёт в конец manifest.json, записи других скриптов сохраняются. Задание CVM_API_nightly выполнит его
# в ближайшие 10 минут окна 06:00–23:59; при «СТОП (подождать)» — повтор через 10 минут. После успеха раннер сам
# переносит файл в done\ и убирает из манифеста.
# Файл уходит в UTF-16 LE с BOM (sqlcmd -i без -f читает его верно, кириллица в N'…' сохраняется).
# Запуск:  bash output/CVM_TRENDS_upload.sh CVM_TRENDS_FIX_20261031.sql
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
NAME="$1"
case "$NAME" in CVM_TRENDS_FIX_20261031.sql|CVM_TRENDS_HISTORY_20261031.sql) ;; *) echo "неизвестный файл: $NAME"; exit 1;; esac
SQL="$DIR/$NAME"
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run 'rm -rf /tmp/trd_up; mkdir -p /tmp/trd_up; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > /tmp/kh_mci; ssh-keygen -lf /tmp/kh_mci' </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
# UTF-8 → UTF-16 LE с BOM, затем в контейнер
TMP16="$(mktemp).sql"
printf '\xff\xfe' > "$TMP16"
iconv -f UTF-8 -t UTF-16LE "$SQL" >> "$TMP16"
run "cat > /tmp/trd_up/$NAME" < "$TMP16"
rm -f "$TMP16"
run 'cat > /tmp/trd_up/upload.sh' <<UP
set -e
cd /tmp/trd_up
H=/tmp/kh_mci
K=/srv/cvm_api/.ssh/id_ed25519
T=cvm-portal@185.130.112.179
NAME=$NAME
printf 'get manifest.json /tmp/trd_up/manifest.json\n' > b1.txt
sftp -b b1.txt -i \$K -o UserKnownHostsFile=\$H \$T >/dev/null
SHA=\$(sha256sum \$NAME | cut -c1-64)
python3 - "\$SHA" "\$NAME" <<'PY'
import json, sys, datetime
sha, name = sys.argv[1], sys.argv[2]
m = json.load(open('/tmp/trd_up/manifest.json', encoding='utf-8-sig'))   # манифест может быть с BOM
m['scripts'] = [s for s in m['scripts'] if s['name'] != name] + [{'name': name, 'sha256': sha}]
m['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
m['publishedBy'] = 'Елена (скрипт Claude): тренды — исправление dbo.trends и пересчёт истории, 31.10.2026'
json.dump(m, open('/tmp/trd_up/manifest.new.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
PY
printf 'put %s %s\nput manifest.new.json manifest.json\nget manifest.json /tmp/trd_up/check.json\n' "\$NAME" "\$NAME" > b2.txt
sftp -b b2.txt -i \$K -o UserKnownHostsFile=\$H \$T >/dev/null
python3 -c "import json;print('В манифесте:', ', '.join(s['name'] for s in json.load(open('/tmp/trd_up/check.json',encoding='utf-8-sig'))['scripts']))"
cd /tmp && rm -rf /tmp/trd_up
UP
run 'sh /tmp/trd_up/upload.sh'
echo "Выложено: $NAME. Проверка: SELECT * FROM dbo.CVM_API_JOB_LOG (nolock) WHERE JOB LIKE N'CVM_TRENDS%'"
