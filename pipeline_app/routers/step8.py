"""Шаг 8 — Коммуникации и запуск: тексты пушей, условия, контент.

Переиспользует ai_generator.py:
  • generate_builtin — бесплатная генерация push-текстов в стиле Дикси;
  • calculate_push_schedule — расписание отправок;
  • classify_promo — тип акции;
  • generate_promo_conditions — условия акции (платный AI, по кнопке).
"""
from __future__ import annotations

import ai_generator as ai
from fastapi import APIRouter, Form, Request

import config
from pipeline_app.core import datasource, state, util
from pipeline_app.web import templates

router = APIRouter()


def _selected_list():
    return util.selected_full()


def _find(num: str):
    for p in _selected_list():
        if p["__num"] == str(num):
            return p
    return None


@router.get("/step8")
def page(request: Request, promo: str = ""):
    promos = _selected_list()
    chosen = _find(promo) if promo else (promos[0] if promos else None)
    pushes, classification, schedule = None, None, None
    if chosen:
        try:
            schedule = ai.calculate_push_schedule(chosen)
            classification = ai.classify_promo(chosen)
            result = ai.generate_builtin(
                chosen, rules="", num_variants=2,
                title_max_len=config.DEFAULT_TITLE_MAX_LEN,
                body_max_len=config.DEFAULT_BODY_MAX_LEN,
                schedule=schedule,
            )
            pushes = result.get("pushes", [])
        except Exception as e:
            classification = {"error": str(e)}
    return templates.TemplateResponse(request, "step8.html", {
        "request": request, "active": "step8",
        "promos": promos, "chosen": chosen,
        "pushes": pushes, "classification": classification, "schedule": schedule,
        "has_key": bool(config.ANTHROPIC_API_KEY),
    })


@router.post("/step8/conditions")
def conditions(request: Request, promo: str = Form(...)):
    chosen = _find(promo)
    if not chosen:
        return templates.TemplateResponse(request, "partials/conditions.html",
                                          {"request": request, "error": "Акция не найдена"})
    if not config.ANTHROPIC_API_KEY:
        return templates.TemplateResponse(request, "partials/conditions.html",
                                          {"request": request, "error": "Не задан ANTHROPIC_API_KEY в .env"})
    try:
        all_promos = datasource.load_cvm(push_only=False).to_dict("records")
        examples = ai.get_similar_examples(chosen, all_promos, n=5)
        result = ai.generate_promo_conditions(chosen, examples)
    except Exception as e:
        return templates.TemplateResponse(request, "partials/conditions.html",
                                          {"request": request, "error": str(e)})
    return templates.TemplateResponse(request, "partials/conditions.html",
                                      {"request": request, "result": result})
