#!/bin/bash
# Выкладка output/CVM_101432_101434.sql (списки и КГ чековой лестницы 101432–101434, старт 29.10.2026) в обменник MCI с Mac: Mac → kz → ru → cvm-api → SFTP.
# Скрипт встаёт в конец manifest.json, записи других скриптов сохраняются. Задание CVM_API_nightly выполнит его в ближайшие
# 10 минут (скрипты идут по имени, CVM_1… — раньше CVM_EFFECT_…); при неполном I_CVM_CONTACT или несходимости КГ — ошибка 16
# и раннер повторит через 10 минут; собранные списки второй раз не пишутся.
# Файл уходит в UTF-16 LE с BOM (sqlcmd -i без -f читает его верно, кириллица в N'…' сохраняется).
# Запуск:  bash "/Users/elenakoryakova/new projects/cvm_push_generation/output/CVM_101432_101434_upload.sh"
# После успеха раннер сам переносит файл в done\ и убирает из манифеста; если не убрал — выложить манифест без него тем же способом.
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
SQL="$DIR/CVM_101432_101434.sql"
NAME="CVM_101432_101434.sql"
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run 'rm -rf /tmp/lad_up; mkdir -p /tmp/lad_up; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > /tmp/kh_mci; ssh-keygen -lf /tmp/kh_mci' </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
# UTF-8 → UTF-16 LE с BOM (на Mac), затем в контейнер
TMP16="$(mktemp -t lad).sql"
printf '\xff\xfe' > "$TMP16"
iconv -f UTF-8 -t UTF-16LE "$SQL" >> "$TMP16"
run "cat > /tmp/lad_up/$NAME" < "$TMP16"
rm -f "$TMP16"
run 'cat > /tmp/lad_up/upload.sh' <<'UP'
set -e
cd /tmp/lad_up
H=/tmp/kh_mci
K=/srv/cvm_api/.ssh/id_ed25519
T=cvm-portal@185.130.112.179
NAME=CVM_101432_101434.sql
printf 'get manifest.json /tmp/lad_up/manifest.json\n' > b1.txt
sftp -b b1.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
SHA=$(sha256sum $NAME | cut -c1-64)
python3 - "$SHA" "$NAME" <<'PY'
import json, sys, datetime
sha, name = sys.argv[1], sys.argv[2]
m = json.load(open('/tmp/lad_up/manifest.json', encoding='utf-8-sig'))   # манифест может быть с BOM
m['scripts'] = [s for s in m['scripts'] if s['name'] != name] + [{'name': name, 'sha256': sha}]
m['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
m['publishedBy'] = 'Елена (скрипт Claude): списки и КГ чековой лестницы 101432–101434 (старт 29.10.2026), правило ОК 05.10.2026'
json.dump(m, open('/tmp/lad_up/manifest.new.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
PY
printf 'put %s %s\nput manifest.new.json manifest.json\nget manifest.json /tmp/lad_up/check.json\n' "$NAME" "$NAME" > b2.txt
sftp -b b2.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 -c "import json;print('В манифесте:', ', '.join(s['name'] for s in json.load(open('/tmp/lad_up/check.json',encoding='utf-8-sig'))['scripts']))"
cd /tmp && rm -rf /tmp/lad_up
UP
run 'sh /tmp/lad_up/upload.sh'
echo "Выложено: $NAME. Проверка: dbo.CVM_API_JOB_LOG (nolock) по JOB = CVM_101432_101434 и I_PROMO_OFFER по ID_PROMO 101432–101434."
