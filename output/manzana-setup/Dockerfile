# Задание «Заведение в Manzana» (robot.py). Собирает install.sh на ru из каталога /srv/cvm_portal/manzana-setup.
FROM python:3.12-slim

# Браузер ставится в общий каталог, а не в /root/.cache: процесс идёт под uid 10010, и путь должен быть задан ДО
# `playwright install` (ENV действует только на следующие инструкции). Права — на чтение всем.
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
# Playwright с Chromium и системными библиотеками (как у скрапера конкурентов, та же версия 1.63.0); xvfb — на случай,
# если расширение ДИКСИ не работает в headless-режиме (BROWSER_HEADLESS=0). Ставится до смены пользователя: нужен apt.
RUN pip install --no-cache-dir "playwright==1.63.0" "psycopg[binary]==3.2.3" "pyotp==2.9.0" \
    && playwright install --with-deps chromium \
    && chmod -R a+rX /ms-playwright \
    && apt-get update && apt-get install -y --no-install-recommends xvfb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY robot.py campaign_lists.py manzana_session.py campaign_push.py coupon_setup.py loyalty_admin.py ./
COPY sql ./sql
# HOME=/profile: туда Chromium пишет кэш и профиль; каталог на ru принадлежит uid 10010 (как хранилище списков).
ENV PYTHONUNBUFFERED=1 LISTS_STORAGE=/data LISTS_STATE=/state HOME=/profile
# uid совпадает с системным пользователем cvm-lists на хосте — владельцем /srv/cvm_lists/lists: робот читает inbox/
# и пишет receipts/ как партнёр по обмену, других прав на диске у него нет.
USER 10010:10010
ENTRYPOINT ["python", "/app/robot.py"]
CMD ["--check"]
