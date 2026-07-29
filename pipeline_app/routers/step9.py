"""Шаг 9 — Постанализ: факт vs прогноз.

Факт берём из локальных расчётов КД (data/расчет_КД_*.xlsx): фактические
отклик/Доп ТО/PL. Сопоставляем с прогнозом forecast.py по тем же акциям —
дельта показывает точность модели и питает будущие прогнозы (аналоги).
"""
from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, Request

import forecast as fc
from pipeline_app.core import datasource, prioritization, util
from pipeline_app.web import templates

router = APIRouter()


def _num(v):
    return fc._num(v)


@router.get("/step9")
def page(request: Request):
    facts = datasource.load_kd_facts()
    history = datasource.load_cvm(push_only=False)

    rows = []
    tot_fact_pl = tot_fcst_pl = tot_fact_to = tot_fcst_to = 0.0
    if facts is not None and not facts.empty:
        fcst = prioritization.forecast_rows(facts.rename(columns={
            "Доп ТО, р.": "Доп ТО (план), р.",
        }), history)
        fcst_by_num = {f["num"]: f for f in fcst}
        for _, r in facts.iterrows():
            d = r.to_dict()
            num = util.norm_num(d.get("НОМЕР", ""))
            fact_to = _num(d.get("Доп ТО, р.")) or _num(d.get("Доп ТО, ₽"))
            fact_pl = _num(d.get("PL, р.")) or _num(d.get("PL, ₽"))
            f = fcst_by_num.get(num, {})
            fcst_pl = f.get("pl")
            fcst_to = f.get("dop_to")
            delta_pl = (fact_pl - fcst_pl) if (fact_pl is not None and fcst_pl is not None) else None
            if fact_pl is not None: tot_fact_pl += fact_pl
            if fcst_pl is not None: tot_fcst_pl += fcst_pl
            if fact_to is not None: tot_fact_to += fact_to
            if fcst_to is not None: tot_fcst_to += fcst_to
            rows.append({
                "num": num, "name": d.get("Название промо", ""),
                "segment": d.get("Сегмент", ""),
                "fact_to": fact_to, "fcst_to": fcst_to,
                "fact_pl": fact_pl, "fcst_pl": fcst_pl, "delta_pl": delta_pl,
                "file": d.get("__file", ""),
            })

    return templates.TemplateResponse(request, "step9.html", {
        "request": request, "active": "step9", "rows": rows,
        "tot_fact_pl": round(tot_fact_pl), "tot_fcst_pl": round(tot_fcst_pl),
        "tot_fact_to": round(tot_fact_to), "tot_fcst_to": round(tot_fcst_to),
    })
