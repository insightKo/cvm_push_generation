#!/bin/bash
# Установка задания «Заведение в Manzana» на ru. Запускать с фермы (классификатор авторежима агента это блокирует —
# запускает Елена):
#   tools/manzana-setup/install.sh            — код, образ, каталоги, юниты; таймер НЕ включается
#   tools/manzana-setup/install.sh --enable   — то же и включить таймер (только по слову Елены, нужен manzana.env)
# Повторный запуск безопасен. Ничего из работающего (cvm-campaign-lists, SFTP-обмен, cvm-sheets-sync) не меняет.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ENABLE="${1:-}"

ssh ru 'mkdir -p /srv/cvm_portal/manzana-setup/sql'
scp -q "$HERE/robot.py" "$HERE/campaign_lists.py" "$HERE/manzana_session.py" "$HERE/campaign_push.py" "$HERE/coupon_setup.py" "$HERE/loyalty_admin.py" "$HERE/Dockerfile" "$HERE/README.md" "$HERE/manzana.env.example" "$HERE/set-secrets.sh" \
       "$HERE/cvm-manzana-setup.service" "$HERE/cvm-manzana-setup.timer" ru:/srv/cvm_portal/manzana-setup/
scp -q "$HERE"/sql/*.sql ru:/srv/cvm_portal/manzana-setup/sql/

ssh ru "ENABLE='$ENABLE' bash -s" <<'REMOTE'
set -euo pipefail
cd /srv/cvm_portal/manzana-setup
chmod 755 set-secrets.sh

# Списки робот берёт из базы списков (cvm-lists-db), хранилище SFTP-обмена ему не нужно. Журнал и отметки — свои, uid 10010.
sudo install -d -m 700 -o 10010 -g 10010 /srv/cvm_portal/manzana-state
# Профиль Chromium (вход робота в Manzana).
sudo install -d -m 755 -o root -g root /srv/cvm_portal/manzana
sudo install -d -m 700 -o 10010 -g 10010 /srv/cvm_portal/manzana/profile

docker build --quiet -t cvm-manzana-setup:latest . >/dev/null
echo "образ cvm-manzana-setup:latest собран"

sudo install -m 644 -o root -g root cvm-manzana-setup.service cvm-manzana-setup.timer /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/cvm-manzana-setup.service /etc/systemd/system/cvm-manzana-setup.timer
sudo systemctl daemon-reload

if [ "$ENABLE" = "--enable" ]; then
  test -s /srv/cvm_portal/manzana.env || { echo "нет /srv/cvm_portal/manzana.env — сначала set-secrets.sh (Елена)"; exit 1; }
  sudo systemctl enable --now cvm-manzana-setup.timer
  systemctl list-timers cvm-manzana-setup.timer --no-pager
else
  echo "таймер не включён; проверка: docker run --rm --network app_default --env-file /srv/cvm_portal/manzana.env -v /srv/cvm_portal/manzana-state:/state cvm-manzana-setup:latest --check"
fi
REMOTE
