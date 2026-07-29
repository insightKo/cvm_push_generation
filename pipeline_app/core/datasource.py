"""Загрузка данных без Streamlit.

Источники:
  - Google Sheets «CVM offline» (акции) и «PUSH» (результат) через gspread,
    с fallback на публичный CSV-экспорт gviz.
  - Локальные расчётные файлы КД (data/расчет_КД_*.xlsx) — фактические цифры
    отклика/ТО/PL/среднего чека для приоритизации и постанализа.

Кэш — простой in-memory с TTL, чтобы не дёргать API на каждый запрос.
"""
from __future__ import annotations

import glob
import io
import time
import urllib.request

import pandas as pd

from pipeline_app import settings

_CACHE: dict[str, tuple[float, object]] = {}
_TTL = 300  # сек


def _cached(key: str, loader, ttl: int = _TTL):
    now = time.time()
    hit = _CACHE.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = loader()
    _CACHE[key] = (now, val)
    return val


def clear_cache() -> None:
    _CACHE.clear()


def _gspread_values(sheet_name: str) -> list[list[str]] | None:
    import os
    if not os.path.exists(settings.GOOGLE_CREDENTIALS_PATH):
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(settings.GOOGLE_CREDENTIALS_PATH, scopes=scopes)
        client = gspread.authorize(creds)
        ss = client.open_by_key(settings.SPREADSHEET_ID)
        ws = ss.worksheet(sheet_name)
        return ws.get_all_values()
    except Exception:
        return None


def _dedup_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            out.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            out.append(h)
    return out


def _csv_fallback(sheet_name: str) -> pd.DataFrame:
    try:
        sheet_q = sheet_name.replace(" ", "%20")
        url = (
            f"https://docs.google.com/spreadsheets/d/{settings.SPREADSHEET_ID}"
            f"/gviz/tq?tqx=out:csv&sheet={sheet_q}"
        )
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode("utf-8")
        return pd.read_csv(io.StringIO(content))
    except Exception:
        return pd.DataFrame()


def _load_sheet(sheet_name: str, push_only: bool = False) -> pd.DataFrame:
    values = _gspread_values(sheet_name)
    if values:
        headers = _dedup_headers(values[0])
        df = pd.DataFrame(values[1:], columns=headers)
    else:
        df = _csv_fallback(sheet_name)
    if df.empty:
        return df
    if "НОМЕР" in df.columns:
        df["НОМЕР"] = pd.to_numeric(df["НОМЕР"], errors="coerce")
    if push_only and "Каналы коммуникации" in df.columns:
        df = df[df["Каналы коммуникации"].astype(str).str.strip().str.upper() == "PUSH"]
    return df.reset_index(drop=True)


def load_cvm(push_only: bool = True) -> pd.DataFrame:
    return _cached(f"cvm:{push_only}", lambda: _load_sheet(settings.SHEET_CVM_OFFLINE, push_only))


def load_push() -> pd.DataFrame:
    return _cached("push", lambda: _load_sheet(settings.SHEET_PUSH))


def load_kd_facts() -> pd.DataFrame:
    """Все локальные расчёты КД (факт/план) — объединённая таблица аналогов."""
    def _loader() -> pd.DataFrame:
        frames = []
        for path in sorted(glob.glob(str(settings.DATA_DIR / "расчет_КД_*.xlsx"))):
            try:
                df = pd.read_excel(path)
                df["__file"] = path.split("/")[-1]
                frames.append(df)
            except Exception:
                continue
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    return _cached("kd_facts", _loader, ttl=3600)
