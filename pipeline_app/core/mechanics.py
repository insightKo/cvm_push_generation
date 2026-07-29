"""Библиотека механик акций.

Основа — справочник прода I_PROMO (data/cvm_dictionaries.md, ID_MECHANICS 1–16).
К каждому типу добавлена прикладная метаинформация для шагов 4 (механика +
экономика) и 0 (приоритизация):

  goal_fit   — насколько механика работает на каждую из 4 целей (0–3):
               traffic (частота визитов), turnover (товарооборот),
               avg_check (средний чек), margin (маржа/PL).
  segments   — для каких сегментов уместна (правила сегментации Дикси:
               большой чек/бандлы — только Активные; частотные — Активные/Новые;
               реактивация — Спящие/Отток).
  threshold  — нужен ли порог чека.
  econ       — как влияет на юнит-экономику (по формуле PL = Доп ТО × 0.30 − Скидка).

Это «каталог», из которого шаг 4 предлагает механику под цель и сегмент.
"""
from __future__ import annotations

MARGIN = 0.30  # маржа из CVM offline (PL = Доп ТО × MARGIN − Скидка)

ALL_SEGMENTS = ["Новые", "Активные", "Спящие", "Отток", "Случайные"]
ACTIVE_NEW = ["Активные", "Новые"]
REACTIVATION = ["Спящие", "Отток"]

# goal_fit: 0 = не работает, 1 = слабо, 2 = средне, 3 = сильно
MECHANICS: list[dict] = [
    {
        "id": 1, "name": "Кешбэк", "type": "cashback",
        "desc": "Возврат части суммы чека баллами. Стимулирует возврат за повторной покупкой.",
        "threshold": False, "segments": ALL_SEGMENTS,
        "goal_fit": {"traffic": 3, "turnover": 2, "avg_check": 1, "margin": 1},
        "econ": "Баллы списываются позже → отложенная стоимость; гонит частоту визитов.",
        "example": "Кешбэк 10% баллами на следующую покупку.",
        "big_check_only": False,
    },
    {
        "id": 2, "name": "Целевые бонусы", "type": "bonus",
        "desc": "Начисление бонусов за покупку в конкретной категории.",
        "threshold": False, "segments": ALL_SEGMENTS,
        "goal_fit": {"traffic": 2, "turnover": 3, "avg_check": 2, "margin": 1},
        "econ": "Гонит товарооборот в нужной категории; стоимость = номинал бонусов.",
        "example": "+200 бонусов за покупку молочной категории.",
        "big_check_only": False,
    },
    {
        "id": 3, "name": "Скидка", "type": "discount",
        "desc": "Прямая скидка минус N% на категорию/товар.",
        "threshold": False, "segments": ALL_SEGMENTS,
        "goal_fit": {"traffic": 2, "turnover": 3, "avg_check": 1, "margin": 0},
        "econ": "Прямо режет маржу (вычитается из PL); сильный драйвер ТО и трафика.",
        "example": "Минус 20% на всю категорию «бакалея».",
        "big_check_only": False,
    },
    {
        "id": 4, "name": "Активируемая скидка", "type": "discount",
        "desc": "Скидка, которую клиент активирует в приложении — фильтрует неактивных.",
        "threshold": False, "segments": ALL_SEGMENTS,
        "goal_fit": {"traffic": 3, "turnover": 2, "avg_check": 1, "margin": 1},
        "econ": "Активация повышает отклик и отсекает «холодных» → лучше PL/клиент.",
        "example": "Активируй и получи минус 25% на кофе.",
        "big_check_only": False,
    },
    {
        "id": 5, "name": "Купон", "type": "coupon",
        "desc": "Купон на скидку/подарок, печатается на чеке (slip) или в приложении.",
        "threshold": True, "segments": ALL_SEGMENTS,
        "goal_fit": {"traffic": 3, "turnover": 2, "avg_check": 2, "margin": 1},
        "econ": "Возвратный визит за вторым чеком; порог чека защищает маржу.",
        "example": "Купон минус 15% при следующей покупке от 700 ₽.",
        "big_check_only": False,
    },
    {
        "id": 6, "name": "Купон активируемый", "type": "coupon",
        "desc": "Купон с обязательной активацией в приложении.",
        "threshold": True, "segments": ACTIVE_NEW,
        "goal_fit": {"traffic": 3, "turnover": 2, "avg_check": 2, "margin": 2},
        "econ": "Активация → выше качество отклика и PL/клиент.",
        "example": "Активируй купон — минус 20% на чек от 1000 ₽.",
        "big_check_only": False,
    },
    {
        "id": 7, "name": "Коммуникация", "type": "communication",
        "desc": "Сервисный/контентный пуш без прямой выгоды (баланс, новости, рецепт).",
        "threshold": False, "segments": ALL_SEGMENTS,
        "goal_fit": {"traffic": 1, "turnover": 0, "avg_check": 0, "margin": 0},
        "econ": "Стоимость ≈ 0; поддерживает контакт и вовлечённость.",
        "example": "Напоминание о балансе баллов / рецепт недели.",
        "big_check_only": False,
    },
    {
        "id": 8, "name": "Кешбэк X баллов", "type": "cashback",
        "desc": "Фиксированное число баллов за покупку.",
        "threshold": False, "segments": ALL_SEGMENTS,
        "goal_fit": {"traffic": 3, "turnover": 1, "avg_check": 1, "margin": 1},
        "econ": "Фикс-стоимость на клиента; предсказуемый бюджет, гонит частоту.",
        "example": "Дарим 300 баллов за покупку.",
        "big_check_only": False,
    },
    {
        "id": 9, "name": "Кешбэк X баллов за чек", "type": "cashback",
        "desc": "Баллы при достижении порога чека.",
        "threshold": True, "segments": ["Активные", "Новые"],
        "goal_fit": {"traffic": 2, "turnover": 2, "avg_check": 3, "margin": 1},
        "econ": "Порог тянет средний чек вверх; стоимость только при выполнении.",
        "example": "300 баллов за чек от 1500 ₽.",
        "big_check_only": True,
    },
    {
        "id": 10, "name": "Кратный кешбэк", "type": "cashback",
        "desc": "Повышенный множитель баллов (x2/x3) на период.",
        "threshold": False, "segments": ["Активные"],
        "goal_fit": {"traffic": 3, "turnover": 2, "avg_check": 2, "margin": 0},
        "econ": "Сильный краткосрочный драйвер частоты; дорого по баллам.",
        "example": "x3 баллов на все покупки в выходные.",
        "big_check_only": False,
    },
    {
        "id": 11, "name": "Промокод", "type": "coupon",
        "desc": "Код для онлайн/офлайн со скидкой или бонусом.",
        "threshold": True, "segments": ALL_SEGMENTS,
        "goal_fit": {"traffic": 2, "turnover": 2, "avg_check": 2, "margin": 1},
        "econ": "Легко атрибутировать отклик; порог защищает маржу.",
        "example": "Промокод LETO — минус 15% от 800 ₽.",
        "big_check_only": False,
    },
    {
        "id": 12, "name": "Отложенная скидка", "type": "discount",
        "desc": "Скидка действует на следующий визит/период.",
        "threshold": False, "segments": REACTIVATION,
        "goal_fit": {"traffic": 3, "turnover": 1, "avg_check": 1, "margin": 1},
        "econ": "Инструмент возврата спящих/оттока — обещание выгоды на возврат.",
        "example": "Вернись на неделе — получи минус 20%.",
        "big_check_only": False,
    },
    {
        "id": 13, "name": "Скидка x% от чека", "type": "discount",
        "desc": "Процент скидки на весь чек при пороге.",
        "threshold": True, "segments": ["Активные", "Новые"],
        "goal_fit": {"traffic": 2, "turnover": 3, "avg_check": 3, "margin": 0},
        "econ": "Порог + % на чек тянут средний чек и ТО, но режут маржу.",
        "example": "Минус 10% на чек от 2000 ₽.",
        "big_check_only": True,
    },
    {
        "id": 14, "name": "Скидка Xр. от чека", "type": "discount",
        "desc": "Фиксированная скидка в рублях при пороге чека.",
        "threshold": True, "segments": ["Активные", "Новые"],
        "goal_fit": {"traffic": 2, "turnover": 2, "avg_check": 3, "margin": 1},
        "econ": "Фикс-рубли предсказуемы; порог поднимает средний чек.",
        "example": "Минус 300 ₽ при чеке от 1500 ₽.",
        "big_check_only": True,
    },
    {
        "id": 15, "name": "Предначисленные бонусы", "type": "bonus",
        "desc": "Бонусы зачислены заранее, сгорают к дедлайну — драйвер возврата.",
        "threshold": False, "segments": REACTIVATION,
        "goal_fit": {"traffic": 3, "turnover": 1, "avg_check": 1, "margin": 1},
        "econ": "Реактивация: «у тебя уже есть баллы, потрать до даты».",
        "example": "Тебе начислено 500 баллов — потрать до воскресенья.",
        "big_check_only": False,
    },
    {
        "id": 16, "name": "Кешбэк активируемый", "type": "cashback",
        "desc": "Кешбэк с активацией в приложении.",
        "threshold": False, "segments": ACTIVE_NEW,
        "goal_fit": {"traffic": 3, "turnover": 2, "avg_check": 1, "margin": 2},
        "econ": "Активация повышает качество отклика и PL/клиент.",
        "example": "Активируй кешбэк 15% на категорию.",
        "big_check_only": False,
    },
]

_BY_ID = {m["id"]: m for m in MECHANICS}

GOALS = [
    ("traffic", "Трафик (частота)"),
    ("turnover", "Товарооборот"),
    ("avg_check", "Средний чек"),
    ("margin", "Маржа"),
]


def get(mech_id: int) -> dict | None:
    return _BY_ID.get(int(mech_id)) if mech_id is not None else None


def all() -> list[dict]:
    return MECHANICS


def for_goal(goal: str, segment: str | None = None, top: int = 5) -> list[dict]:
    """Лучшие механики под цель (и сегмент, если задан)."""
    items = MECHANICS
    if segment:
        seg = segment.strip()
        items = [m for m in items if any(seg.lower() in s.lower() or s.lower() in seg.lower()
                                         for s in m["segments"])]
        # большой чек — только Активные
        if not any(k in seg.lower() for k in ("актив",)):
            items = [m for m in items if not m["big_check_only"]]
    ranked = sorted(items, key=lambda m: m["goal_fit"].get(goal, 0), reverse=True)
    return [m for m in ranked if m["goal_fit"].get(goal, 0) > 0][:top]


def estimate_economics(clients: int, otklik: float, avg_check: float,
                       discount_pct: float) -> dict:
    """Юнит-экономика по формуле CVM offline.

    clients      — размер аудитории
    otklik       — доля откликнувшихся (0..1)
    avg_check    — средний чек, ₽
    discount_pct — глубина скидки/выгоды на чек (0..1)
    """
    buyers = clients * otklik
    dop_to = buyers * avg_check
    discount = dop_to * discount_pct
    pl = dop_to * MARGIN - discount
    return {
        "buyers": round(buyers),
        "dop_to": round(dop_to),
        "discount": round(discount),
        "pl": round(pl),
        "pl_per_client": round(pl / clients, 2) if clients else 0.0,
        "roi": round(pl / discount, 2) if discount else None,
    }
