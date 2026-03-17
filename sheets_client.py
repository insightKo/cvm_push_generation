"""Модуль для работы с Google Sheets API."""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from config import SPREADSHEET_ID, GOOGLE_CREDENTIALS_PATH, SHEET_CVM_OFFLINE, SHEET_PUSH

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet():
    client = get_client()
    return client.open_by_key(SPREADSHEET_ID)


def load_cvm_offline() -> pd.DataFrame:
    """Загрузить данные из вкладки CVM offline."""
    ss = get_spreadsheet()
    ws = ss.worksheet(SHEET_CVM_OFFLINE)
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    # Фильтруем только строки с каналом PUSH
    if "Каналы коммуникации" in df.columns:
        df = df[df["Каналы коммуникации"].str.strip().str.upper() == "PUSH"]
    return df.reset_index(drop=True)


def load_push_data() -> pd.DataFrame:
    """Загрузить данные из вкладки PUSH."""
    ss = get_spreadsheet()
    ws = ss.worksheet(SHEET_PUSH)
    data = ws.get_all_records()
    return pd.DataFrame(data)


def save_push_rows(rows: list[dict]):
    """Сохранить (дописать) строки push во вкладку PUSH.

    rows — список словарей с ключами, соответствующими заголовкам вкладки PUSH.
    """
    ss = get_spreadsheet()
    ws = ss.worksheet(SHEET_PUSH)
    headers = ws.row_values(1)

    for row_data in rows:
        row_values = [str(row_data.get(h, "")) for h in headers]
        ws.append_row(row_values, value_input_option="USER_ENTERED")


def update_push_row(row_index: int, row_data: dict):
    """Обновить конкретную строку во вкладке PUSH (row_index — 1-based, включая заголовок)."""
    ss = get_spreadsheet()
    ws = ss.worksheet(SHEET_PUSH)
    headers = ws.row_values(1)

    for col_idx, header in enumerate(headers, start=1):
        if header in row_data:
            ws.update_cell(row_index, col_idx, str(row_data[header]))
