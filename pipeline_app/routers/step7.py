"""Шаг 7 — Заведение: подготовка к Manzana Processing + Campaign.

Маппинг выбранных акций на ID справочников (механика, сегмент, канал) и
выгрузка в Excel для заведения.
"""
from __future__ import annotations

import io

import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from pipeline_app.core import mechanics, segments, util
from pipeline_app.web import templates

router = APIRouter()
CHANNEL_PUSH = 1


def _export_rows():
    rows = []
    for p in util.selected_full():
        m = mechanics.get(p.get("mech_id"))
        seg = p.get("segment", "")
        rows.append({
            "НОМЕР": p["__num"],
            "Название промо": p.get("Название промо", ""),
            "ID сегмента": segments.segment_id(seg) or "",
            "Сегмент": seg,
            "ID механики": m["id"] if m else "",
            "Механика": m["name"] if m else "",
            "ID канала": CHANNEL_PUSH,
            "Канал": "PUSH",
            "Категория": p.get("Категория", ""),
            "Старт акции": p.get("Старт акции", ""),
            "Окончание акции": p.get("Окончание акции", ""),
        })
    return rows


@router.get("/step7")
def page(request: Request):
    rows = _export_rows()
    incomplete = [r for r in rows if not r["ID механики"] or not r["ID сегмента"]]
    return templates.TemplateResponse(request, "step7.html", {
        "request": request, "active": "step7",
        "rows": rows, "incomplete": incomplete,
    })


@router.get("/step7/export.xlsx")
def export():
    rows = _export_rows()
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="Manzana")
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=manzana_setup.xlsx"},
    )
