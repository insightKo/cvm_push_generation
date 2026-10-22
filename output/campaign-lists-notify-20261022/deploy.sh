#!/bin/bash
# Установка на ru задания cvm-campaign-lists с письмом «Готовы списки к загрузке в Campaign» (решение Елены 22.10.2026).
# Запуск с Mac: bash "/Users/elenakoryakova/new projects/cvm_push_generation/output/campaign-lists-notify-20261022/deploy.sh"
# Основа — HEAD, где tools/campaign-lists = ffdcded (то, что стоит на ru) + только письмо (папка head/); незакоммиченная работа по купонам в общем дереве
# не уезжает. На kz: отдельная копия /tmp/deploy-notify-20261022 на ветке notify-mail-20261022, коммит, install.sh
# (без --enable: таймер уже работает). В конце — сверка md5 export.py на ru с копией.
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)/head"
W=/tmp/deploy-notify-20261022
ssh kz "set -e; cd /srv/projects/cvm_platform; git diff --quiet ffdcded HEAD -- tools/campaign-lists || { echo 'tools/campaign-lists в HEAD отличается от ffdcded — не ставлю'; exit 1; }
  git worktree remove --force $W 2>/dev/null || true; git branch -D notify-mail-20261022 2>/dev/null || true
  git worktree add -q -b notify-mail-20261022 $W HEAD"
(cd "$D" && COPYFILE_DISABLE=1 tar cf - export.py test_export.py cvm-campaign-lists.service) | ssh kz "tar xf - -C $W/tools/campaign-lists"
ssh kz "set -e; cd $W
  git add tools/campaign-lists/export.py tools/campaign-lists/test_export.py tools/campaign-lists/cvm-campaign-lists.service
  git commit -q -m 'Списки для Campaign: письмо «Готовы списки к загрузке в Campaign» от cvm@mci-team.pro на M.Panov@dixy.ru после выгрузки (Елена 22.10.2026)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>'
  git log -1 --format='коммит %h на ветке notify-mail-20261022'
  bash tools/campaign-lists/install.sh
  echo \"kz: \$(md5sum < tools/campaign-lists/export.py | cut -c1-32)\"
  ssh ru 'echo \"ru: \$(md5sum < /srv/cvm_portal/campaign-lists/export.py | cut -c1-32)\"; systemctl list-timers cvm-campaign-lists.timer --no-pager | head -2'"
echo "Готово: письмо уйдёт после ближайшей выгрузки нового списка."
