"""Шаг 2 — Прогноз: uplift и отклик по каждой акции (forecast.py)."""
from __future__ import annotations

from fastapi import APIRouter, Request

from pipeline_app.core import datasource, prioritization, util
from pipeline_app.web import templates

router = APIRouter()


@router.get("/step2")
def page(request: Request):
    df, month, months = util.month_promos()
    history = datasource.load_cvm(push_only=False)
    rows = prioritization.forecast_rows(df, history)
    total_pl = sum(r["pl"] for r in rows if r.get("pl"))
    total_to = sum(r["dop_to"] for r in rows if r.get("dop_to"))
    return templates.TemplateResponse(request, "step2.html", {
        "request": request, "active": "step2", "month": month, "months": months,
        "rows": rows, "total_pl": total_pl, "total_to": total_to,
    })
