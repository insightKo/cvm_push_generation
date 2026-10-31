#!/bin/bash
# Выкладка CVM_JOB_WEEKLY_T9_20261101.sql (раз в неделю шесть шагов «MCI Regular update», бэклог ночного задания Т9) в обменник MCI
# (D:\cvm_api\exchange). Путь: эта машина → kz → ru → контейнер cvm-api → SFTP. Записи других скриптов в manifest.json
# сохраняются. Задание CVM_API_nightly выполняет скрипт каждые 10 минут 06:00–23:59; до 01.11 17:00 и пока идёт «MCI Regular update» или
# загрузка MCI, скрипт падает своей ошибкой без изменений и повторяется; после успеха раннер сам уносит его в done\.
# Файл уходит в UTF-16 LE с BOM (sqlcmd -i без -f читает его верно); BOM UTF-8 исходника снимается.
# Запуск:  bash output/CVM_JOB_WEEKLY_T9_upload.sh
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
NAME="CVM_JOB_WEEKLY_T9_20261101.sql"
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run 'rm -rf /tmp/xt9_up; mkdir -p /tmp/xt9_up; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > /tmp/kh_mci_t9; ssh-keygen -lf /tmp/kh_mci_t9' </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
TMP16="$(mktemp)"
printf '\xff\xfe' > "$TMP16"
sed '1s/^\xEF\xBB\xBF//' "$DIR/$NAME" | iconv -f UTF-8 -t UTF-16LE >> "$TMP16"
echo "$NAME: $(sha256sum "$TMP16" | cut -c1-64)"
run "cat > /tmp/xt9_up/$NAME" < "$TMP16"
rm -f "$TMP16"
run 'cat > /tmp/xt9_up/upload.sh' <<'UP'
set -e
cd /tmp/xt9_up
H=/tmp/kh_mci_t9
K=/srv/cvm_api/.ssh/id_ed25519
T=cvm-portal@185.130.112.179
printf 'get manifest.json /tmp/xt9_up/manifest.json\n' > b1.txt
sftp -b b1.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 - <<'PY'
import json, datetime, hashlib
name = 'CVM_JOB_WEEKLY_T9_20261101.sql'
m = json.load(open('/tmp/xt9_up/manifest.json', encoding='utf-8-sig'))   # манифест может быть с BOM
print('В манифесте до:', ', '.join(s['name'] for s in m['scripts']) or '(пусто)')
m['scripts'] = [s for s in m['scripts'] if s['name'] != name] + [
    {'name': name, 'sha256': hashlib.sha256(open('/tmp/xt9_up/' + name, 'rb').read()).hexdigest()}]
m['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
m['publishedBy'] = 'Елена (скрипт Claude): раз в неделю шесть шагов MCI Regular update (бэклог Т9)'
json.dump(m, open('/tmp/xt9_up/manifest.new.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
PY
printf 'put CVM_JOB_WEEKLY_T9_20261101.sql CVM_JOB_WEEKLY_T9_20261101.sql\nput manifest.new.json manifest.json\nget manifest.json /tmp/xt9_up/check.json\n' > b2.txt
sftp -b b2.txt -i $K -o UserKnownHostsFile=$H $T >/dev/null
python3 -c "import json;print('В манифесте после:', ', '.join(s['name'] for s in json.load(open('/tmp/xt9_up/check.json',encoding='utf-8-sig'))['scripts']))"
cd /tmp && rm -rf /tmp/xt9_up /tmp/kh_mci_t9
UP
run 'sh /tmp/xt9_up/upload.sh' </dev/null
echo "Выложено: $NAME. Применится в первый проход обменника после 01.11 17:00 и окончания «MCI Regular update»."
echo "Проверка: CVM_API_JOB_LOG (до 01.11 17:00 и конца задания — ошибки «Wait/Job running», потом ok), sp_help_jobstep — команды шести шагов с /* T9 */."
