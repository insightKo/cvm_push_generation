#!/bin/bash
# Запускает Елена на ru:  /srv/cvm_portal/manzana-setup/set-secrets.sh
# 1) создаёт в базе портала роль cvm_manzana_ro только на чтение нужных колонок cvm.promo и cvm.push (случайный пароль);
#    в базе списков (cvm-lists-db) ставит пароль роли lists_manzana — её создаёт миграция портала db/lists/002_manzana.sql;
# 2) спрашивает адреса и учётки Manzana (пароли — без отображения; пустой ввод оставляет прежнее значение);
# 3) пишет /srv/cvm_portal/manzana.env (права 600) по образцу manzana.env.example. На экран пароли не выводятся.
# Повторный запуск безопасен: пароль роли меняется, остальные значения — только те, что ввели заново.
set -euo pipefail
OUT=/srv/cvm_portal/manzana.env
# Пароли читаются без эха только с терминала: через `ssh ru скрипт` (без -t) они бы отобразились на экране.
[ -t 0 ] || { echo "нужен терминал: войти ssh ru и запустить скрипт там" >&2; exit 1; }

# прежние значения — по умолчанию
declare -A OLD
if [ -s "$OUT" ]; then
  while IFS='=' read -r k v; do
    [[ "$k" =~ ^[A-Z_]+$ ]] && OLD["$k"]="$v"
  done < <(grep -E '^[A-Z_]+=' "$OUT")
fi
ask()  { local k="$1" p="$2" v; read -r -p "$p [${OLD[$k]:-}]: " v; echo "${v:-${OLD[$k]:-}}"; }
asks() { local k="$1" p="$2" v; read -r -s -p "$p [$([ -n "${OLD[$k]:-}" ] && echo 'есть' || echo 'пусто')]: " v; echo >&2; echo "${v:-${OLD[$k]:-}}"; }

PG_PW="$(openssl rand -hex 24)"
set -a; . /srv/cvm_portal/db/db.env; set +a

# Колонки по списку: что из него есть в базе, то и выдаётся (миграции на сервере могут отставать от репозитория).
/srv/cvm_portal/app/db/psql.sh -q -v ON_ERROR_STOP=1 -f - <<SQL
do \$\$ begin
  if not exists (select 1 from pg_roles where rolname = 'cvm_manzana_ro') then
    create role cvm_manzana_ro login;
  end if;
end \$\$;
alter role cvm_manzana_ro with login password '$PG_PW' connection limit 2;
alter role cvm_manzana_ro set default_transaction_read_only = on;
grant connect on database "$POSTGRES_DB" to cvm_manzana_ro;
grant usage on schema cvm to cvm_manzana_ro;
revoke all on cvm.promo from cvm_manzana_ro;
revoke all on cvm.push from cvm_manzana_ro;
do \$\$
declare cols text;
begin
  select string_agg(column_name, ', ') into cols from information_schema.columns
   where table_schema = 'cvm' and table_name = 'promo' and column_name in
   ('number','title','setup','segments_raw','channels','start_date','end_date','description','mechanic','mechanic_online',
    'category','bonus_expiry','coupon','coupon_name','slip_text','button','coupon_layout_url','discount_sum','restrictions',
    'status','updated_by');
  execute format('grant select (%s) on cvm.promo to cvm_manzana_ro', cols);
  select string_agg(column_name, ', ') into cols from information_schema.columns
   where table_schema = 'cvm' and table_name = 'push' and column_name in
   ('id','promo_number','msg_no','segment_raw','channel','send_date','send_time','client_settings','title','body',
    'title_edited','body_edited','screen_link','button','coupon_text','status','deleted_at');
  execute format('grant select (%s) on cvm.push to cvm_manzana_ro', cols);
end \$\$;
SQL

# База списков: роль lists_manzana создаёт миграция 002_manzana.sql (db/lists/migrate.sh); здесь только пароль. Пароль идёт
# в psql через stdin — в списке процессов его нет. Роли ещё нет — списки и кампании робота ждут, остальное работает.
LISTS_PW=""
if [ "$(docker exec -u postgres cvm-lists-db psql -q -X -At -d cvm_lists -c "select 1 from pg_roles where rolname = 'lists_manzana'" 2>/dev/null)" = "1" ]; then
  LISTS_PW="$(openssl rand -hex 24)"
  printf "alter role lists_manzana with login password '%s' connection limit 2;\n" "$LISTS_PW" \
    | docker exec -i -u postgres cvm-lists-db psql -q -X -v ON_ERROR_STOP=1 -d cvm_lists
else
  echo "в базе списков нет роли lists_manzana — сначала миграция 002_manzana.sql (db/lists/migrate.sh); списки робота ждут" >&2
fi

echo "Доступы Manzana (пустой ввод — оставить прежнее значение; пока их нет — просто Enter):"
MC_URL="$(ask MC_URL 'Manzana Campaign, адрес (https://…)')"
MC_LOGIN="$(ask MC_LOGIN 'Manzana Campaign, логин робота')"
MC_PASSWORD="$(asks MC_PASSWORD 'Manzana Campaign, пароль')"
MC_TOTP_SECRET="$(asks MC_TOTP_SECRET 'Manzana Campaign, секрет TOTP (если есть)')"
MC_PARTNER="$(ask MC_PARTNER 'Manzana Campaign, партнёр / проект (если выбирается)')"
# Manzana Online = раздел Loyalty того же приложения, та же учётка (Елена 20.10.2026) — отдельных доступов нет.
LOYALTY_ADMIN_URL="$(ask LOYALTY_ADMIN_URL 'Админка купонов ДИКСИ, адрес (https://…, по умолчанию https://loyalty-admin.dixy.ru)')"
LOYALTY_ADMIN_LOGIN="$(ask LOYALTY_ADMIN_LOGIN 'Админка купонов ДИКСИ, логин (e-mail) робота')"
LOYALTY_ADMIN_PASSWORD="$(asks LOYALTY_ADMIN_PASSWORD 'Админка купонов ДИКСИ, пароль')"
ENABLED="${OLD[MANZANA_SETUP_ENABLED]:-0}"
NOTIFY_TO="$(ask NOTIFY_TO 'Адреса для писем о сбоях, через запятую')"

umask 077
cat > "$OUT.tmp" <<ENV
# Обходной путь «Заведение в Manzana» (cvm-manzana-setup). Создано set-secrets.sh $(date -u +%F). Вне git, права 600.
# Образец и описание полей — /srv/cvm_portal/manzana-setup/manzana.env.example.
MANZANA_SETUP_ENABLED=$ENABLED
MANZANA_SETUP_SYSTEMS=${OLD[MANZANA_SETUP_SYSTEMS]:-online,campaign,lists}
MC_URL=$MC_URL
MC_LOGIN=$MC_LOGIN
MC_PASSWORD=$MC_PASSWORD
MC_TOTP_SECRET=$MC_TOTP_SECRET
MC_PARTNER=$MC_PARTNER
LOYALTY_ADMIN_URL=${LOYALTY_ADMIN_URL:-https://loyalty-admin.dixy.ru}
LOYALTY_ADMIN_LOGIN=$LOYALTY_ADMIN_LOGIN
LOYALTY_ADMIN_PASSWORD=$LOYALTY_ADMIN_PASSWORD
COUPON_DRAFTS=/state/coupon_drafts.json
BROWSER_PROFILE_DIR=/profile
BROWSER_HEADLESS=${OLD[BROWSER_HEADLESS]:-1}
BROWSER_TIMEOUT_SEC=${OLD[BROWSER_TIMEOUT_SEC]:-120}
BROWSER_SCREENSHOT_ON_ERROR=${OLD[BROWSER_SCREENSHOT_ON_ERROR]:-1}
LISTS_STATE=/state
RETENTION_DAYS=${OLD[RETENTION_DAYS]:-14}
PG_HOST=cvm-db
PG_PORT=5432
PG_DB=$POSTGRES_DB
PG_USER=cvm_manzana_ro
PG_PASSWORD=$PG_PW
LISTS_PG_HOST=cvm-lists-db
LISTS_PG_PORT=5432
LISTS_PG_DB=cvm_lists
LISTS_PG_USER=lists_manzana
LISTS_PG_PASSWORD=$LISTS_PW
NOTIFY_TO=$NOTIFY_TO
ENV
# Ключи прежнего файла, которых нет в шаблоне (селекторы формы входа, адреса API, настройки списков), сохраняются как были.
# LISTS_STORAGE и LISTS_WORKDIR — от схемы с SFTP-обменом (до 20.10.2026), роботу больше не нужны.
for k in "${!OLD[@]}"; do
  case "$k" in LISTS_STORAGE|LISTS_WORKDIR) continue ;; esac
  grep -q "^$k=" "$OUT.tmp" || printf '%s=%s\n' "$k" "${OLD[$k]}" >> "$OUT.tmp"
done
mv "$OUT.tmp" "$OUT"
chmod 600 "$OUT"
echo "готово: $OUT записан (600), роль cvm_manzana_ro в базе портала создана$([ -n "$LISTS_PW" ] && echo ', пароль lists_manzana в базе списков задан'). MANZANA_SETUP_ENABLED=$ENABLED — включение робота только руками в этом файле."
