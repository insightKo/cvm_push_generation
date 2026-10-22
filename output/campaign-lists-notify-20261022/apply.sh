#!/bin/bash
# Письмо «Готовы списки к загрузке в Campaign» (решение Елены 22.10.2026) — правка задания cvm-campaign-lists.
# Запуск с Mac: bash "output/campaign-lists-notify-20261022/apply.sh"
# Кладёт export.py, test_export.py, cvm-campaign-lists.service в общее дерево kz
# (/srv/projects/cvm_platform/tools/campaign-lists) с бэкапами *.bak-20261022-notify и прогоняет тесты.
# Только если export.py в дереве тот же, что при подготовке (md5 ea45fe4b…): другая сессия могла его поменять.
# На ru ничего не ставит — выкладка отдельно, после ОК.
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)/tree"
cd "$D"
tar cf - export.py test_export.py cvm-campaign-lists.service | ssh kz 'set -e
cd /srv/projects/cvm_platform/tools/campaign-lists
[ "$(md5sum < export.py | cut -c1-32)" = ea45fe4bacb7a3ce6ea9579bff19ca44 ] || { echo "export.py изменился после подготовки — ничего не записано"; exit 1; }
for f in export.py test_export.py cvm-campaign-lists.service; do cp -p "$f" "$f.bak-20261022-notify"; done
tar xf -
python3 -m pytest -q test_export.py 2>&1 | tail -3'
