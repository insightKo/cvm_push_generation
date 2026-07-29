"""Конфигурация нового пайплайн-приложения.

Берёт значения из общего .env репозитория (тот же, что использует Streamlit-версия).
Новый сервис не конфликтует со старым: отдельный порт 8503.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Корень репозитория = родитель pipeline_app/. Добавляем в путь, чтобы
# переиспользовать существующие модули forecast.py, ai_generator.py, dixy_parser.py
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env", override=True)

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY")
GOOGLE_CREDENTIALS_PATH = str(ROOT / os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials/service_account.json"))
AI_PROVIDER = os.getenv("AI_PROVIDER", "anthropic")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

SHEET_CVM_OFFLINE = "CVM offline"
SHEET_PUSH = "PUSH"

DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
CACHE_DIR = ROOT / "cache"

# Отдельный порт, чтобы не конфликтовать со Streamlit-версией (8502)
PORT = int(os.getenv("PIPELINE_PORT", "8503"))

# Этапы пайплайна (схема + шаг 0 приоритизации)
STEPS = [
    ("0", "Приоритизация", "приоритет акций: трафик · ТО · чек · маржа", "step0"),
    ("1", "Идея акции", "генерация акций-кандидатов", "step1"),
    ("2", "Прогноз", "uplift и отклик", "step2"),
    ("3", "Выбор предложения", "отбор лучших акций по прогнозу", "step3"),
    ("4", "Механика + экономика", "механика, юнит-экономика, маржа", "step4"),
    ("5", "Сегмент", "узкий подсегмент под акцию", "step5"),
    ("6", "План / сетка", "календарь, частоты, анти-конфликт", "step6"),
    ("7", "Заведение", "Manzana Processing + Campaign", "step7"),
    ("8", "Коммуникации и запуск", "тексты · ревью · старт акции", "step8"),
    ("9", "Постанализ", "факт vs прогноз → будущие прогнозы", "step9"),
]
