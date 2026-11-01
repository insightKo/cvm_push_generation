#!/bin/bash
# Выкладка output/CVM_101552.sql в обменник MCI (D:\cvm_api\exchange): этот компьютер → kz → ru → контейнер cvm-api → SFTP.
# Скрипт встаёт в конец manifest.json, записи других скриптов сохраняются. Задание CVM_API_nightly выполнит его в ближайшие
# 10 минут; пока витрина покупок занята загрузкой (вечер/ночь) — скрипт откажет ошибкой 16 и раннер повторит через 10 минут.
# Файл уходит в UTF-16 LE с BOM (sqlcmd -i без -f читает его верно, кириллица в N'…' сохраняется).
# После успеха раннер сам переносит файл в done\ и убирает из манифеста.
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
SQL="$DIR/CVM_101552.sql"
NAME="CVM_101552.sql"
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run 'rm -rf /tmp/c552_up; mkdir -p /tmp/c552_up; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > /tmp/kh_mci; ssh-keygen -lf /tmp/kh_mci' </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
TMP16="$(mktemp).sql"
printf '\xff\xfe' > "$TMP16"
iconv -f UTF-8 -t UTF-16LE "$SQL" >> "$TMP16"
run "cat > /tmp/c552_up/$NAME" < "$TMP16"
rm -f "$TMP16"
run 'cat > /tmp/c552_up/upload.sh' <<'UP'
set -e
cd /tmp/c552_up
H=/tmp/kh_mci
K=/srv/cvm_api/.ssh/id_ed25519
T=cvm-portal@185.130.112.179
NAME=CVM_101552.sql
printf 'get manifest.json /tmp/c552_up/manifest.json\n' > b1.txt
sftp -b b1.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
SHA=$(sha256sum $NAME | cut -c1-64)
python3 - "$SHA" "$NAME" <<'PY'
import json, sys, datetime
sha, name = sys.argv[1], sys.argv[2]
m = json.load(open('/tmp/c552_up/manifest.json', encoding='utf-8-sig'))
m['scripts'] = [s for s in m['scripts'] if s['name'] != name] + [{'name': name, 'sha256': sha}]
m['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
m['publishedBy'] = 'Елена (скрипт Claude): список 101552 «Вернём 150р. монетами с покупки от 1000р.», старт 03.11.2026'
json.dump(m, open('/tmp/c552_up/manifest.new.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
PY
printf 'put %s %s\nput manifest.new.json manifest.json\nget manifest.json /tmp/c552_up/check.json\n' "$NAME" "$NAME" > b2.txt
sftp -b b2.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 -c "import json;print('В манифесте:', ', '.join(s['name'] for s in json.load(open('/tmp/c552_up/check.json',encoding='utf-8-sig'))['scripts']))"
echo "sha256 $SHA"
cd /tmp && rm -rf /tmp/c552_up
UP
run 'sh /tmp/c552_up/upload.sh'
echo "Выложено: $NAME. Проверка: dbo.CVM_API_JOB_LOG (nolock) по имени файла, затем I_PROMO и I_PROMO_OFFER по 101552."
