"""Мелкие утилиты: фильтр по месяцу (дефолт — последний месяц)."""
from __future__ import annotations

import pandas as pd

MONTH_ORDER = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль",
               "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]


def norm_num(v) -> str:
    """НОМЕР → чистая строка без хвоста .0 (в данных он приходит как float)."""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _month_key(m: str):
    s = str(m).strip().lower()
    for i, name in enumerate(MONTH_ORDER):
        if name in s:
            return i
    try:
        return int(float(s)) - 1
    except (ValueError, TypeError):
        return 99


def available_months(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty or "Месяц" not in df.columns:
        return []
    months = [str(m).strip() for m in df["Месяц"].dropna().unique() if str(m).strip()]
    return sorted(set(months), key=_month_key)


def default_month(df: pd.DataFrame) -> str | None:
    """Последний (самый поздний) месяц — дефолт фильтра."""
    months = available_months(df)
    return months[-1] if months else None


def filter_month(df: pd.DataFrame, month: str | None) -> pd.DataFrame:
    if df is None or df.empty or not month or "Месяц" not in df.columns:
        return df
    return df[df["Месяц"].astype(str).str.strip() == str(month).strip()].reset_index(drop=True)


def month_promos():
    """(df акций текущего месяца, выбранный месяц, список месяцев). Дефолт — последний."""
    from pipeline_app.core import datasource, state
    cvm = datasource.load_cvm(push_only=True)
    month = state.get("month") or default_month(cvm)
    state.set("month", month)
    return filter_month(cvm, month), month, available_months(cvm)


def selected_full() -> list[dict]:
    """Полные строки выбранных акций + назначенная механика и сегмент."""
    from pipeline_app.core import state
    df, _, _ = month_promos()
    if df is None or df.empty:
        return []
    sel = set(state.get("selected", []))
    mechs = state.get("mechanics", {})
    segs = state.get("segments", {})
    out = []
    for _, r in df.iterrows():
        d = r.to_dict()
        num = norm_num(d.get("НОМЕР", ""))
        if num not in sel:
            continue
        d["__num"] = num
        d["mech_id"] = mechs.get(num)
        d["assigned_segment"] = segs.get(num)
        # удобные алиасы для модулей плана
        d["name"] = d.get("Название промо", "")
        d["segment"] = segs.get(num) or d.get("Сегмент", "")
        out.append(d)
    return out
