"""Шаг 0 — приоритизация акций-кандидатов.

Каждая акция оценивается по 4 осям бизнес-целей:
  traffic   — трафик/частота: ожидаемое число откликнувшихся (clients × отклик)
  turnover  — товарооборот: Доп ТО, ₽
  avg_check — средний чек: Доп ТО / число покупателей (инкрементальный чек)
  margin    — маржа: PL, ₽ (= Доп ТО × 0.30 − Скидка)

Сырые значения по каждой оси нормируются 0–100 (min-max по набору кандидатов),
затем считается взвешенный композитный приоритет по весам пользователя.
Прогнозные цифры берём из forecast.py (исторические аналоги или план из CVM offline).
"""
from __future__ import annotations

import pandas as pd

import forecast as fc  # переиспользуем существующий модуль прогноза

from pipeline_app.core import util

AXES = ["traffic", "turnover", "avg_check", "margin"]
AXIS_LABELS = {
    "traffic": "Трафик (частота)",
    "turnover": "Товарооборот",
    "avg_check": "Средний чек",
    "margin": "Маржа",
}


def _safe_forecast_row(row: dict, analogues: dict) -> dict:
    """Безопасный прогноз: «план» из CVM offline, иначе оценка по аналогам
    через медиану на клиента. Обходит баг forecast.forecast_row (dop_to_pc)."""
    plan = fc._existing_plan(row)
    if plan:
        return plan

    key = fc.classify(row)
    matches, level = fc._match_analogues(key, analogues)
    if not matches:
        return {"source": "нет аналогов", "otklik": None, "dop_to": None,
                "disc": None, "pl": None, "clients_est": None}

    clients = fc._int(row.get("Примерное количество клиентов"))
    clients_est = False
    if not clients:
        pool = [m["clients"] for m in matches if m.get("clients")]
        if not pool:
            return {"source": "нет клиентов", "otklik": None, "dop_to": None,
                    "disc": None, "pl": None, "clients_est": None}
        import statistics
        clients = int(statistics.median(pool))
        clients_est = True

    import statistics
    otklik = statistics.median([m["otklik"] for m in matches])
    dop_to_pc = statistics.median([m["dop_to"] / m["clients"] for m in matches if m.get("clients")])
    disc_pc = statistics.median([m["disc"] / m["clients"] for m in matches if m.get("clients")])
    dop_to = clients * dop_to_pc
    disc = clients * disc_pc
    pl = dop_to * fc.MARGIN - disc
    src = f"прогноз: {level} ({len(matches)} аналогов)"
    if clients_est:
        src += f"; клиенты оценены ({clients})"
    return {"source": src, "otklik": otklik, "dop_to": dop_to, "disc": disc,
            "pl": pl, "clients_est": clients if clients_est else None}


def forecast_rows(promos_df: pd.DataFrame, history_df: pd.DataFrame) -> list[dict]:
    """Прогноз по каждой акции: отклик, Доп ТО, скидка, PL, источник."""
    if promos_df is None or promos_df.empty:
        return []
    analogues = fc.build_analogue_db(history_df) if history_df is not None and not history_df.empty else {}
    out = []
    for _, r in promos_df.iterrows():
        d = r.to_dict()
        f = _safe_forecast_row(d, analogues)
        clients = fc._int(d.get("Примерное количество клиентов")) or f.get("clients_est")
        out.append({
            "num": util.norm_num(d.get("НОМЕР", "")),
            "name": d.get("Название промо", ""),
            "segment": d.get("Сегмент", ""),
            "category": d.get("Категория", ""),
            "clients": clients,
            "otklik": round(f["otklik"] * 100, 2) if f.get("otklik") is not None else None,
            "dop_to": round(f["dop_to"]) if f.get("dop_to") is not None else None,
            "disc": round(f["disc"]) if f.get("disc") is not None else None,
            "pl": round(f["pl"]) if f.get("pl") is not None else None,
            "source": f.get("source", ""),
        })
    return out


def _normalize(values: list[float]) -> list[float]:
    vals = [v if v is not None else 0.0 for v in values]
    lo, hi = min(vals), max(vals)
    if hi <= lo:
        return [50.0 for _ in vals]  # все равны → нейтрально
    return [round((v - lo) / (hi - lo) * 100, 1) for v in vals]


def score(promos_df: pd.DataFrame, history_df: pd.DataFrame,
          weights: dict[str, float]) -> list[dict]:
    """Вернуть список акций с метриками по осям и композитным приоритетом."""
    if promos_df is None or promos_df.empty:
        return []

    analogues = fc.build_analogue_db(history_df) if history_df is not None and not history_df.empty else {}
    rows: list[dict] = []
    for _, r in promos_df.iterrows():
        d = r.to_dict()
        f = _safe_forecast_row(d, analogues)
        real_clients = fc._int(d.get("Примерное количество клиентов"))
        clients = real_clients or f.get("clients_est") or 0
        otklik = f.get("otklik")
        dop_to = f.get("dop_to")
        pl = f.get("pl")
        buyers = (clients * otklik) if (clients and otklik) else None
        avg_check = (dop_to / buyers) if (dop_to and buyers) else None
        rows.append({
            "num": util.norm_num(d.get("НОМЕР", "")),
            "name": d.get("Название промо", ""),
            "segment": d.get("Сегмент", ""),
            "category": d.get("Категория", ""),
            "start": d.get("Старт акции", ""),
            "channel": d.get("Каналы коммуникации", ""),
            "source": f.get("source", ""),
            "raw": {
                "traffic": buyers,
                "turnover": dop_to,
                "avg_check": avg_check,
                "margin": pl,
            },
        })

    # нормировка по каждой оси
    norm: dict[str, list[float]] = {}
    for ax in AXES:
        norm[ax] = _normalize([row["raw"][ax] for row in rows])

    total_w = sum(weights.get(ax, 0) for ax in AXES) or 1
    for i, row in enumerate(rows):
        row["norm"] = {ax: norm[ax][i] for ax in AXES}
        composite = sum(weights.get(ax, 0) * row["norm"][ax] for ax in AXES) / total_w
        row["priority"] = round(composite, 1)

    rows.sort(key=lambda x: x["priority"], reverse=True)
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    return rows
