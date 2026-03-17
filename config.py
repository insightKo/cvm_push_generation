import os
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials/service_account.json")
AI_PROVIDER = os.getenv("AI_PROVIDER", "anthropic")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

SHEET_CVM_OFFLINE = "CVM offline"
SHEET_PUSH = "PUSH"

# Ограничения по умолчанию для push-текстов
DEFAULT_TITLE_MAX_LEN = 35
DEFAULT_BODY_MAX_LEN = 120

# Порт
PORT = int(os.getenv("PORT", "8502"))
