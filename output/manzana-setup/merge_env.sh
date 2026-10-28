#!/bin/bash
# Перенос доступов Manzana с Mac на ru (28.10.2026): лежит на ru как ~/merge_env.sh, запуск с Mac:
#   grep -E '^MC_(URL|LOGIN|PASSWORD|TOTP_SECRET)=' credentials/manzana.env | ssh kz 'ssh ru "bash ~/merge_env.sh /srv/cvm_portal/manzana.env"'
# Файл на kz не пишется (поток). Пароль — только в credentials/manzana.env (в git не уходит) и на ru.
# stdin: строки KEY=value; заменяет эти ключи в файле $1 (остальное без изменений), бэкап рядом, права 600; значения не печатает
set -euo pipefail
F="$1"; umask 077
NEW=$(mktemp); trap 'rm -f "$NEW"' EXIT; cat > "$NEW"
[ -f "$F" ] && cp -p "$F" "$F.bak-$(date +%Y%m%d-%H%M%S)"
awk -v nf="$NEW" 'BEGIN{while((getline l < nf)>0){k=substr(l,1,index(l,"=")-1); v[k]=l}}
  /^[A-Z_]+=/{k=substr($0,1,index($0,"=")-1); if(k in v){print v[k]; seen[k]=1; next}} {print}
  END{for(k in v) if(!(k in seen)) print v[k]}' "$F" > "$F.tmp"
chmod 600 "$F.tmp"; mv "$F.tmp" "$F"
awk -F= '/^MC_(URL|LOGIN|PASSWORD)=/{print $1"="($2==""?"<пусто>":($1~/PASS/?"<есть>":$2))}' "$F"; stat -c "%a %U" "$F"
