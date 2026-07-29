"""Шаг 0 — приоритизация акций по 4 осям с настраиваемыми весами."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request

from pipeline_app.core import datasource, prioritization, state, util
from pipeline_app.web import templates

router = APIRouter()


def _compute(month: str | None, weights: dict):
    cvm = datasource.load_cvm(push_only=True)
    if month is None:
        month = util.default_month(cvm)
    df = util.filter_month(cvm, month)
    history = datasource.load_cvm(push_only=False)
    rows = prioritization.score(df, history, weights)
    return rows, month, util.available_months(cvm)


@router.get("/step0")
def page(request: Request):
    weights = state.get("weights")
    month = state.get("month")
    rows, month, months = _compute(month, weights)
    state.set("month", month)
    return templates.TemplateResponse(request, "step0.html", {
        "request": request, "active": "step0",
        "rows": rows, "weights": weights, "months": months, "month": month,
        "axes": prioritization.AXES, "axis_labels": prioritization.AXIS_LABELS,
        "selected": set(state.get("selected", [])),
    })


@router.post("/step0/score")
def rescore(request: Request,
            traffic: int = Form(25), turnover: int = Form(25),
            avg_check: int = Form(25), margin: int = Form(25),
            month: str = Form("")):
    weights = {"traffic": traffic, "turnover": turnover,
               "avg_check": avg_check, "margin": margin}
    state.set("weights", weights)
    state.set("month", month or None)
    rows, month, _ = _compute(month or None, weights)
    return templates.TemplateResponse(request, "partials/priority_table.html", {
        "request": request, "rows": rows, "selected": set(state.get("selected", [])),
        "axes": prioritization.AXES, "axis_labels": prioritization.AXIS_LABELS,
    })
