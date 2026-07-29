"""Шаг 3 — Выбор предложения: отбор лучших акций по прогнозу/приоритету."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request

from pipeline_app.core import datasource, prioritization, state, util
from pipeline_app.web import templates

router = APIRouter()


def _rows():
    df, month, months = util.month_promos()
    history = datasource.load_cvm(push_only=False)
    rows = prioritization.score(df, history, state.get("weights"))
    return rows, month, months


@router.get("/step3")
def page(request: Request):
    rows, month, months = _rows()
    return templates.TemplateResponse(request, "step3.html", {
        "request": request, "active": "step3", "month": month, "months": months,
        "rows": rows, "selected": set(state.get("selected", [])),
    })


@router.post("/step3/select")
def select(request: Request, num: list[str] = Form(default=[])):
    state.set("selected", [str(n) for n in num])
    rows, _, _ = _rows()
    sel = set(state.get("selected", []))
    return templates.TemplateResponse(request, "partials/selection_summary.html", {
        "request": request, "rows": rows, "selected": sel,
    })
