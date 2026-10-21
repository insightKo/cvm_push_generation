#!/bin/bash
# Выкладка 30 подборок 101445–101474 на SFTP (ru: /srv/cvm_lists/lists/podborki, владелец cvm-lists, права 600).
# Существующие файлы с теми же именами не перезаписываются (печатается SKIP).
cd "/Users/elenakoryakova/new projects/cvm_push_generation/output/подборки_101445-101474" || exit 1
tar cf - *.xlsx | ssh kz 'ssh ru "rm -rf /tmp/podb_1014 && mkdir -p /tmp/podb_1014 && tar xf - -C /tmp/podb_1014 && cd /tmp/podb_1014 && for f in *.xlsx; do if sudo test -e /srv/cvm_lists/lists/podborki/\$f; then echo SKIP \$f; else sudo install -o cvm-lists -g 10010 -m 600 \$f /srv/cvm_lists/lists/podborki/; fi; done; rm -rf /tmp/podb_1014; echo выложено: \$(sudo ls /srv/cvm_lists/lists/podborki/ | grep -c -E 1014[4-7][0-9])"'
