"""Состояние пайплайна между шагами (in-memory, single-user локально).

Хранит выбор пользователя по ходу воронки: веса приоритизации, отобранные
акции-кандидаты (шаг 3), назначенные механики (шаг 4), сегменты (шаг 5) и т.д.
Этого достаточно для локального инструмента; при необходимости позже заменим
на персистентное хранилище.
"""
from __future__ import annotations

from typing import Any

_STATE: dict[str, Any] = {
    # шаг 0 — веса осей приоритизации
    "weights": {"traffic": 25, "turnover": 25, "avg_check": 25, "margin": 25},
    # шаг 3 — отобранные НОМЕРа акций
    "selected": [],
    # шаг 4 — назначенные механики: {номер: mechanic_id}
    "mechanics": {},
    # шаг 5 — назначенные сегменты: {номер: segment}
    "segments": {},
    # выбранный месяц фильтрации
    "month": None,
}


def get(key: str, default: Any = None) -> Any:
    return _STATE.get(key, default)


def set(key: str, value: Any) -> None:
    _STATE[key] = value


def update(patch: dict) -> None:
    _STATE.update(patch)


def all() -> dict:
    return dict(_STATE)
