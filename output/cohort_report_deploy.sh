#!/bin/bash
# Выкладка отчёта «Когортный анализ новых клиентов» (экран как слайд августа 2025) на прод портала:
# коммит в ветке kz /tmp/deploy-cohort-new → перебазирование на ru/main → push (хук пересобирает фронт) →
# пересборка cvm-api (хук её не трогает) → проверка ручки /dixy/cohort-new.
# Запуск:  bash "/Users/elenakoryakova/new projects/cvm_push_generation/output/cohort_report_deploy.sh"
set -e
ssh kz 'set -e
cd /tmp/deploy-cohort-new
git add -A api/app/routers/dixy.py api/tests/test_dixy.py api/sql/dixy api/scripts src/features/dixy-reports/components/cohort-new src/features/dixy-reports/pages/CohortNewPage.tsx
git -c user.name=insightKo commit -q -m "Когорты новых клиентов: экран как слайд «Когортный анализ новых клиентов» отчёта августа 2025 — все показатели витрины по периодам 0–5, средние по поколениям, выбор поколений и сравнения календарём, история всех поколений (Елена 23.10.2026)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>" || true
git fetch -q ru
git rebase -q ru/main
git push ru HEAD:main
ssh ru "cd /srv/cvm_portal/app && docker compose -f docker-compose.prod.yml --env-file /srv/cvm_portal/.env --profile api up -d --build cvm-api > /srv/cvm_portal/cvm-api-rebuild-cohort-slide.log 2>&1 && tail -1 /srv/cvm_portal/cvm-api-rebuild-cohort-slide.log"
sleep 25
curl -s -m 120 -H "X-Forwarded-User: elena" -H "X-User-Id: u_admin" localhost:18000/api/v1/dixy/cohort-new | head -c 300; echo
git log --oneline -1'
