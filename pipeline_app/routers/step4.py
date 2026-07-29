"""Шаг 4 — Механика + экономика.

Библиотека механик (core/mechanics) + назначение механики выбранным акциям +
калькулятор юнит-экономики (PL = Доп ТО × 0.30 − Скидка).
"""
from __future__ import annotations

from fastapi import APIRouter, Form, Request

from pipeline_app.core import datasource, mechanics, state, util
from pipeline_app.web import templates

router = APIRouter()


def _selected_promos():
    df, _, _ = util.month_promos()
    sel = set(state.get("selected", []))
    if df is None or df.empty:
        return []
    out = []
    for _, r in df.iterrows():
        d = r.to_dict()
        num = util.norm_num(d.get("НОМЕР", ""))
        if num in sel:
            out.append({"num": num, "name": d.get("Название промо", ""),
                        "segment": d.get("Сегмент", ""), "category": d.get("Категория", "")})
    return out


@router.get("/step4")
def page(request: Request):
    promos = _selected_promos()
    assigned = state.get("mechanics", {})
    return templates.TemplateResponse(request, "step4.html", {
        "request": request, "active": "step4",
        "library": mechanics.all(), "goals": mechanics.GOALS,
        "promos": promos, "assigned": assigned, "mech_get": mechanics.get,
    })


@router.post("/step4/recommend")
def recommend(request: Request, goal: str = Form("traffic"), segment: str = Form("")):
    recs = mechanics.for_goal(goal, segment or None, top=6)
    return templates.TemplateResponse(request, "partials/mech_recommend.html", {
        "request": request, "recs": recs, "goal": goal,
        "goal_label": dict(mechanics.GOALS).get(goal, goal),
    })


@router.post("/step4/assign")
def assign(request: Request, num: str = Form(...), mech_id: str = Form(...)):
    assigned = dict(state.get("mechanics", {}))
    if mech_id:
        assigned[num] = int(mech_id)
    else:
        assigned.pop(num, None)
    state.set("mechanics", assigned)
    m = mechanics.get(int(mech_id)) if mech_id else None
    return templates.TemplateResponse(request, "partials/mech_assigned.html", {
        "request": request, "m": m,
    })


@router.post("/step4/econ")
def econ(request: Request, clients: int = Form(...), otklik: float = Form(...),
         avg_check: float = Form(...), discount: float = Form(...)):
    res = mechanics.estimate_economics(clients, otklik / 100.0, avg_check, discount / 100.0)
    return templates.TemplateResponse(request, "partials/econ_result.html", {
        "request": request, "r": res,
    })
