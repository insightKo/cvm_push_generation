"""Шаг 6 — План / сетка: календарь по неделям, частоты, анти-конфликт."""
from __future__ import annotations

from fastapi import APIRouter, Request

from pipeline_app.core import mechanics, planning, util
from pipeline_app.web import templates

router = APIRouter()


@router.get("/step6")
def page(request: Request):
    promos = util.selected_full()
    grid = planning.build_grid(promos)
    warns = planning.conflicts(promos)
    return templates.TemplateResponse(request, "step6.html", {
        "request": request, "active": "step6",
        "grid": grid, "warns": warns, "mech_get": mechanics.get,
        "count": len(promos),
    })
