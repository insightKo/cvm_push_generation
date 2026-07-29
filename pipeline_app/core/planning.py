"""Шаг 6 — план/сетка и анти-конфликт.

Правила анти-конфликта (память проекта Дикси):
  • Монетные акции (частотные, «Дарим X монет») — только первая декада месяца.
  • Частотные акции («за N-ю покупку») — только Активные/Новые.
  • Большой чек (>1500₽: прогр. кешбэк, бандлы) — только Активные.
  • Не дублировать одну механику в одну неделю на смежных категориях.
"""
from __future__ import annotations

import re

from pipeline_app.core import mechanics


def _week(promo: dict) -> str:
    return str(promo.get("Неделя", "") or promo.get("week", "")).strip() or "—"


def build_grid(promos: list[dict]) -> dict[str, list[dict]]:
    """Группировка акций по неделям."""
    grid: dict[str, list[dict]] = {}
    for p in promos:
        grid.setdefault(_week(p), []).append(p)
    return dict(sorted(grid.items(), key=lambda kv: (kv[0] == "—", kv[0])))


def conflicts(promos: list[dict]) -> list[dict]:
    """Список предупреждений анти-конфликта по выбранным акциям."""
    warns: list[dict] = []

    # дубли механики в одну неделю
    by_week_mech: dict[tuple, list[str]] = {}
    for p in promos:
        m = mechanics.get(p.get("mech_id"))
        if not m:
            continue
        key = (_week(p), m["id"])
        by_week_mech.setdefault(key, []).append(p.get("name", "?"))
    for (week, mid), names in by_week_mech.items():
        if len(names) > 1:
            warns.append({"level": "warn",
                          "text": f"Неделя {week}: механика «{mechanics.get(mid)['name']}» повторяется в акциях: {', '.join(names)} — клиент получит дубли, CTR падает."})

    for p in promos:
        name = p.get("name", "?")
        seg = (p.get("segment") or "").lower()
        text = " ".join([p.get("name", ""), p.get("category", "")]).lower()
        m = mechanics.get(p.get("mech_id"))

        # частотные — только Активные/Новые
        if re.search(r"\bза\s+\d+", text) or "частот" in text or "n-ю" in text:
            if not ("актив" in seg or "нов" in seg):
                warns.append({"level": "warn",
                              "text": f"«{name}»: частотная механика на сегмент «{p.get('segment')}» — каскад «за N-ю покупку» только для Активных/Новых; Спящим/Оттоку нужна реактивация."})

        # большой чек — только Активные
        if m and m.get("big_check_only") and "актив" not in seg:
            warns.append({"level": "warn",
                          "text": f"«{name}»: механика «{m['name']}» (большой чек) на «{p.get('segment')}» — применять только к Активным."})

        # монетные акции — первая декада
        if ("монет" in text or (m and m["type"] == "cashback")) and _week(p) not in ("1", "—"):
            warns.append({"level": "info",
                          "text": f"«{name}»: монетная/кешбэк-механика на неделе {_week(p)} — ставить в первую декаду, чтобы монеты успели сгореть."})

    return warns
