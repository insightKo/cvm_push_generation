#!/bin/bash
# Выкладка SQL-скрипта из output/ в обменник MCI (D:\cvm_api\exchange): этот компьютер → kz → ru → контейнер cvm-api → SFTP.
# Запуск: bash output/exchange_upload.sh <файл.sql> "<кто и зачем>" [имя_убрать_из_манифеста ...]
# Файл встаёт в конец manifest.json (записи других скриптов сохраняются), перечисленные после описания имена из манифеста убираются.
# Файл уходит в UTF-16 LE с BOM (sqlcmd -i без -f читает его верно). Раннер CVM_API_nightly выполняет манифест каждые 10 минут,
# после успеха сам переносит файл в done\ и убирает из манифеста.
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
NAME="$1"; WHY="$2"; shift 2; DROP="$*"
SQL="$DIR/$NAME"
[ -f "$SQL" ] || { echo "Нет файла $SQL"; exit 1; }
EXPECTED_KEY='SHA256:vv6HrusUM0LeV8Q+4PKVQHs97+/EbgJ7vOO5clUSetk'
W=/tmp/xu_$$
run() { ssh kz "ssh ru \"docker exec -i cvm-api sh -c '$1'\""; }

run "rm -rf $W; mkdir -p $W; ssh-keyscan -t ed25519 185.130.112.179 2>/dev/null > $W/kh; ssh-keygen -lf $W/kh" </dev/null \
  | grep -q "$EXPECTED_KEY" || { echo 'Ключ сервера обменника не совпал — стоп'; exit 1; }
TMP16="$(mktemp).sql"
printf '\xff\xfe' > "$TMP16"
iconv -f UTF-8 -t UTF-16LE "$SQL" >> "$TMP16"
run "cat > $W/$NAME" < "$TMP16"
rm -f "$TMP16"
cat > "$TMP16.py" <<PY
import json, sys, datetime, subprocess
W, name, why, drop = "$W", "$NAME", """$WHY""", "$DROP".split()
sha = subprocess.check_output(["sha256sum", f"{W}/{name}"]).decode()[:64]
m = json.load(open(f"{W}/manifest.json", encoding="utf-8-sig"))
m["scripts"] = [s for s in m["scripts"] if s["name"] != name and s["name"] not in drop] + [{"name": name, "sha256": sha}]
m["publishedAt"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
m["publishedBy"] = why
json.dump(m, open(f"{W}/manifest.new.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print("sha256", sha)
PY
run "cat > $W/m.py" < "$TMP16.py"
rm -f "$TMP16.py"
cat > "$TMP16.sh" <<SH
set -e
cd $W
printf 'get manifest.json $W/manifest.json
' > b1.txt
sftp -b b1.txt -i /srv/cvm_api/.ssh/id_ed25519 -o UserKnownHostsFile=$W/kh cvm-portal@185.130.112.179 >/dev/null
python3 m.py
printf 'put $NAME $NAME
put manifest.new.json manifest.json
get manifest.json $W/check.json
' > b2.txt
sftp -b b2.txt -i /srv/cvm_api/.ssh/id_ed25519 -o UserKnownHostsFile=$W/kh cvm-portal@185.130.112.179 >/dev/null
python3 -c 'import json; print("В манифесте:", ", ".join(s["name"] for s in json.load(open("$W/check.json", encoding="utf-8-sig"))["scripts"]))'
rm -rf $W
SH
run "cat > $W/up.sh" < "$TMP16.sh"
rm -f "$TMP16.sh"
run "sh $W/up.sh" </dev/null
echo "Выложено: $NAME"
