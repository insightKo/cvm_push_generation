"""Заготовка месячного плана — Gantt как в Streamlit-версии (вкладка «Расписание»).

Читает все output/план_<месяц>_<год>_заготовка.json; ?m=ГГГГ-ММ выбирает месяц,
по умолчанию — последний (самый поздний) месяц списка.
"""
from __future__ import annotations

import json
import re
from datetime import date, timedelta

from fastapi import APIRouter, Request

from pipeline_app import settings
from pipeline_app.web import templates

router = APIRouter()

PLAN_FILE = settings.OUTPUT_DIR / "план_сентябрь_2026_заготовка.json"
PLAN_GLOB = "план_*_заготовка.json"
DOW = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
STREAM_NAME = {101426: "Дарим 100 монет на неделю", 101428: "Отток 50% кешбэка по дням"}


def _plans() -> list[dict]:
    """Все заготовки месяцев по возрастанию (год, месяц)."""
    out = []
    for f in settings.OUTPUT_DIR.glob(PLAN_GLOB):
        try:
            p = json.loads(f.read_text(encoding="utf-8"))
            out.append({"key": f"{int(p['year'])}-{int(p['month_num']):02d}", "file": f,
                        "month": p["month"], "year": int(p["year"]), "month_num": int(p["month_num"])})
        except (ValueError, KeyError, OSError):
            continue
    return sorted(out, key=lambda x: (x["year"], x["month_num"]))


def _fmt(x) -> str:
    if isinstance(x, (int, float)):
        return f"{int(round(x)):,}".replace(",", " ")
    return str(x or "")


@router.get("/plan")
def page(request: Request, m: str | None = None):
    plans = _plans()
    cur = next((p for p in plans if p["key"] == m), plans[-1])
    plan = json.loads(cur["file"].read_text(encoding="utf-8"))
    def _is_series(p):
        return p["channel"] == "slip" or p["mech"] == "Коммуникация"
    # слипы — первыми, затем серии коммуникаций, затем акции по дате старта
    plan["promos"].sort(key=lambda p: (p["channel"] != "slip", not _is_series(p), p["start"], p["end"]))
    y, mn = plan["year"], plan["month_num"]
    start = date(y, mn, 1)
    end = (date(y + (mn == 12), mn % 12 + 1, 1) - timedelta(days=1))
    days = [start + timedelta(i) for i in range((end - start).days + 1)]

    rows = []
    total_msgs = 0
    for p in plan["promos"]:
        s, e = date.fromisoformat(p["start"]), date.fromisoformat(p["end"])
        pushes = {date.fromisoformat(x["date"]): x["msg"] for x in p.get("push", [])}
        total_msgs += len(pushes)
        cells = []
        for d in days:
            cells.append({"in": s <= d <= e, "msg": pushes.get(d, ""), "we": d.weekday() >= 5})
        p["dop_to_fmt"] = _fmt(p.get("dop_to"))
        p["pl_fmt"] = _fmt(p.get("pl"))
        rows.append({**p, "cells": cells})
    # потоки (101426, 101428): одна строка на выборку с пушами потока, акции по дням — раскрываются под ней
    grouped, streams = [], {}
    for r in rows:
        base = r.get("stream")
        if not base:
            grouped.append(r); continue
        if base not in streams:
            seg = re.sub(r"\s*\(выборка[^)]*\)", "", r["segment"]).strip()
            streams[base] = {"num": base, "name": STREAM_NAME.get(base, str(base)), "segment": f"{seg} · выборка {base} на месяц",
                             "channel": "PUSH", "mech": r["mech"], "is_parent": True, "children": [], "dop_to_fmt": "", "pl_fmt": ""}
            grouped.append(streams[base])
        streams[base]["children"].append(r)
    for base, g in streams.items():
        ch = sorted(g["children"], key=lambda r: (r["start"], int(str(r["num"]).split("_")[1])))
        g["children"] = ch
        g["start"], g["end"] = min(c["start"] for c in ch), max(c["end"] for c in ch)
        g["cells"] = [{"in": any(c["cells"][i]["in"] for c in ch),
                       "msg": ",".join(c["cells"][i]["msg"] for c in ch if c["cells"][i]["msg"]),
                       "we": ch[0]["cells"][i]["we"]} for i in range(len(days))]
        g["cat"] = f"{len(ch)} акций, по одной выборке"
        g["why"] = "одна выборка на месяц, пуши по ней; нажми ▸ — акции по дням"
    rows = grouped
    has_forecast = any(p.get("dop_to") is not None for p in plan["promos"])
    sum_dop_to = sum(p["dop_to"] for p in plan["promos"] if isinstance(p.get("dop_to"), (int, float)))
    sum_pl = sum(p["pl"] for p in plan["promos"] if isinstance(p.get("pl"), (int, float)))

    return templates.TemplateResponse(request, "plan.html", {
        "request": request, "active": "plan", "plan": plan,
        "months": plans, "cur": cur["key"],
        "show_pushes": cur["key"] == "2026-09" and PUSH_FILE.exists(),
        "has_forecast": has_forecast, "sum_dop_to": _fmt(sum_dop_to), "sum_pl": _fmt(sum_pl),
        "days": [{"d": d.day, "dow": DOW[d.weekday()], "we": d.weekday() >= 5} for d in days],
        "rows": rows, "total": len(plan["promos"]), "total_msgs": total_msgs,
        "with_push": sum(1 for p in plan["promos"] if p.get("push")),
    })


PUSH_FILE = settings.OUTPUT_DIR / "пуши_сентябрь_нед36.json"
COND_FILE = settings.OUTPUT_DIR / "условия_сентябрь_2026.json"


@router.get("/plan/pushes")
def pushes(request: Request):
    data = json.loads(PUSH_FILE.read_text(encoding="utf-8"))
    cond = json.loads(COND_FILE.read_text(encoding="utf-8")) if COND_FILE.exists() else {}
    plan = json.loads(PLAN_FILE.read_text(encoding="utf-8"))
    names = {p["num"]: p for p in plan["promos"]}
    return templates.TemplateResponse(request, "plan_pushes.html", {
        "request": request, "active": "plan", "data": data, "cond": cond, "names": names,
    })
