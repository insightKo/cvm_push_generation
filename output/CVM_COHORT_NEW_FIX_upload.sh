#!/bin/bash
# Выкладка output/CVM_COHORT_NEW_FIX.sql в обменник MCI (D:\cvm_api\exchange) с Mac: Mac → kz → ru → контейнер cvm-api → SFTP.
# Скрипт встаёт в конец manifest.json, записи других скриптов сохраняются. Задание CVM_API_nightly выполнит его
# в ближайшие 10 минут окна 06:00–21:00; если идёт «MCI Regular update» / перезаливка — скрипт откажет ошибкой 16
# и раннер повторит позже. После успеха раннер переносит файл в done\. Повторный запуск — безопасен.
# Запуск:  bash "/Users/elenakoryakova/new projects/cvm_push_generation/output/CVM_COHORT_NEW_FIX_upload.sh"
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
SQL="$DIR/CVM_COHORT_NEW_FIX.sql"
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run 'rm -rf /tmp/cohort_up; mkdir -p /tmp/cohort_up; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > /tmp/kh_mci; ssh-keygen -lf /tmp/kh_mci' </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
run 'cat > /tmp/cohort_up/CVM_COHORT_NEW_FIX.sql' < "$SQL"
run 'cat > /tmp/cohort_up/upload.sh' <<'UP'
set -e
cd /tmp/cohort_up
H=/tmp/kh_mci
K=/srv/cvm_api/.ssh/id_ed25519
T=cvm-portal@185.130.112.179
printf 'get manifest.json /tmp/cohort_up/manifest.json\n' > b1.txt
sftp -b b1.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
SHA=$(sha256sum CVM_COHORT_NEW_FIX.sql | cut -c1-64)
python3 - "$SHA" <<'PY'
import json, sys, datetime
m = json.load(open('/tmp/cohort_up/manifest.json', encoding='utf-8'))
m['scripts'] = [s for s in m['scripts'] if s['name'] != 'CVM_COHORT_NEW_FIX.sql'] + [{'name': 'CVM_COHORT_NEW_FIX.sql', 'sha256': sys.argv[1]}]
m['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
m['publishedBy'] = 'Елена (скрипт Claude)'
json.dump(m, open('/tmp/cohort_up/manifest.new.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
PY
printf 'put CVM_COHORT_NEW_FIX.sql CVM_COHORT_NEW_FIX.sql\nput manifest.new.json manifest.json\nget manifest.json /tmp/cohort_up/check.json\n' > b2.txt
sftp -b b2.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 -c "import json;print('В манифесте:', ', '.join(s['name'] for s in json.load(open('/tmp/cohort_up/check.json',encoding='utf-8'))['scripts']))"
echo "Готово, sha256 $SHA"
UP
run 'sh /tmp/cohort_up/upload.sh' </dev/null
