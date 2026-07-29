"""Общий веб-слой: Jinja2-шаблоны с глобалами пайплайна."""
from __future__ import annotations

from pathlib import Path

from fastapi.templating import Jinja2Templates

from pipeline_app import settings

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.globals["STEPS"] = settings.STEPS


def fmt_money(v) -> str:
    if v is None or v == "":
        return "—"
    try:
        return f"{round(float(v)):,}".replace(",", " ") + " ₽"
    except (ValueError, TypeError):
        return str(v)


def fmt_int(v) -> str:
    if v is None or v == "":
        return "—"
    try:
        return f"{int(float(v)):,}".replace(",", " ")
    except (ValueError, TypeError):
        return str(v)


templates.env.filters["money"] = fmt_money
templates.env.filters["intf"] = fmt_int
