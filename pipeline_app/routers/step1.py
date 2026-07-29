"""Шаг 1 — Идея акции: генерация акций-кандидатов.

Переиспользует forecast.py: gap-анализ покрытия сегментов, сезонные подсказки
и топ исторических акций по PL/клиент (шаблоны для копирования).
"""
from __future__ import annotations

from fastapi import APIRouter, Request

import forecast as fc
from pipeline_app.core import datasource, util
from pipeline_app.web import templates

router = APIRouter()


def _month_num(month: str | None) -> int:
    if not month:
        return 0
    s = str(month).strip().lower()
    for i, name in enumerate(util.MONTH_ORDER, start=1):
        if name in s:
            return i
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


@router.get("/step1")
def page(request: Request):
    df, month, months = util.month_promos()
    history = datasource.load_cvm(push_only=False)
    mnum = _month_num(month)
    ideas = fc.gap_ideas(df, mnum) if df is not None and not df.empty else []
    seasonal = fc.SEASONAL_HINTS.get(mnum, [])
    coverage = fc.segment_coverage(df) if df is not None and not df.empty else {}
    top = fc.top_historical_by_pl_per_client(history, n=10)
    top_rows = top.to_dict("records") if not top.empty else []
    return templates.TemplateResponse(request, "step1.html", {
        "request": request, "active": "step1", "month": month, "months": months,
        "ideas": ideas, "seasonal": seasonal, "coverage": coverage, "top_rows": top_rows,
    })
