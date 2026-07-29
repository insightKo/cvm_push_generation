"""Шаг 5 — Сегмент: узкий подсегмент под акцию."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request

from pipeline_app.core import mechanics, segments, state, util
from pipeline_app.web import templates

router = APIRouter()


def _selected_promos():
    df, _, _ = util.month_promos()
    sel = set(state.get("selected", []))
    assigned = state.get("mechanics", {})
    if df is None or df.empty:
        return []
    out = []
    for _, r in df.iterrows():
        d = r.to_dict()
        num = util.norm_num(d.get("НОМЕР", ""))
        if num not in sel:
            continue
        category = d.get("Категория", "")
        m = mechanics.get(assigned.get(num))
        recs, why = segments.recommend(category, m["type"] if m else None)
        out.append({
            "num": num, "name": d.get("Название промо", ""),
            "current": d.get("Сегмент", ""), "category": category,
            "recs": recs, "why": why,
        })
    return out


@router.get("/step5")
def page(request: Request):
    promos = _selected_promos()
    return templates.TemplateResponse(request, "step5.html", {
        "request": request, "active": "step5",
        "promos": promos, "segments": segments.SEGMENTS,
        "assigned": state.get("segments", {}),
    })


@router.post("/step5/assign")
def assign(request: Request, num: str = Form(...), segment: str = Form("")):
    seg = dict(state.get("segments", {}))
    if segment:
        seg[num] = segment
    else:
        seg.pop(num, None)
    state.set("segments", seg)
    return templates.TemplateResponse(request, "partials/seg_assigned.html", {
        "request": request, "segment": segment,
    })
