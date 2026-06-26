"""Модуль генерации текстов push-уведомлений.

Поддерживает 3 режима:
  - builtin: бесплатный встроенный генератор (без API)
  - anthropic: Claude API (платный)
  - openai: OpenAI API (платный)
"""

import json
import random
import re
from datetime import datetime, timedelta, date
from pathlib import Path
from config import AI_PROVIDER, ANTHROPIC_API_KEY, OPENAI_API_KEY


# ── Справочник deeplinks ────────────────────────────────────────────────────

_DEEPLINK_DF = None


def _load_deeplinks():
    """Загрузить справочник deeplinks из xlsx."""
    global _DEEPLINK_DF
    if _DEEPLINK_DF is not None and not _DEEPLINK_DF.empty:
        return _DEEPLINK_DF
    import pandas as pd
    path = Path(__file__).resolve().parent / "data" / "deeplink.xlsx"
    if path.exists():
        try:
            _DEEPLINK_DF = pd.read_excel(path)
            _DEEPLINK_DF.columns = [c.strip() for c in _DEEPLINK_DF.columns]
            _DEEPLINK_DF["_search"] = _DEEPLINK_DF["КАТЕГОРИЯ"].astype(str).str.strip().str.lower()
        except Exception:
            _DEEPLINK_DF = pd.DataFrame(columns=["Id", "КАТЕГОРИЯ", "deeplink", "_search"])
    else:
        _DEEPLINK_DF = pd.DataFrame(columns=["Id", "КАТЕГОРИЯ", "deeplink", "_search"])
    return _DEEPLINK_DF


def find_best_deeplink(category_text: str) -> dict | None:
    """Найти наиболее подходящий deeplink по тексту категории.

    Returns: {"id": 712, "category": "Молочные продукты, яйцо", "deeplink": "dixyapp://..."} или None
    """
    df = _load_deeplinks()
    if df.empty or not category_text:
        return None

    query = category_text.strip().lower()

    # Сервисный пуш «Баланс баллов» — исторически уходит на каталог,
    # чтобы клиент мог сразу пойти и потратить монеты на любые покупки.
    if "баланс баллов" in query or "баланс монет" in query:
        return {"id": 0, "category": "каталог", "deeplink": "dixyapp://app/catalog"}

    # 1. Точное совпадение
    exact = df[df["_search"] == query]
    if not exact.empty:
        r = exact.iloc[0]
        return {"id": int(r["Id"]), "category": r["КАТЕГОРИЯ"], "deeplink": r["deeplink"]}

    # 2. Поиск по вхождению ключевых слов с синонимами
    _synonyms = {
        "детск": ["детей", "детск", "малыш", "ребён", "ребенк"],
        "детей": ["детск", "детей", "малыш"],
        "зоо": ["животн", "зоо", "корм", "кошач", "собач"],
        "животн": ["зоо", "животн", "корм"],
        "алкогол": ["вино", "пиво", "водк", "алкогол", "крепк"],
        "химии": ["бытов", "хими", "моющ", "стирк"],
        "бытов": ["хими", "бытов", "моющ"],
        "пп": ["правильн", "здоров", "диет", "фитнес"],
        "готов": ["готов", "перекус", "сэндвич"],
    }
    keywords = re.findall(r"[а-яёa-z]{3,}", query)
    # Стемминг: берём первые 4 символа каждого слова (пива → пив, соусов → соус)
    # Игнорируем стоп-слова и слова из акционной лексики
    _stop_words = {"для", "при", "что", "это", "акти", "купи", "поку", "напр",
                   "акци", "скид", "купон", "комб", "пром", "вернем", "верне",
                   "вернё", "вернем", "монет", "монет", "кешб", "кешбэ"}
    expanded_kw = set()
    for kw in keywords:
        stem = kw[:4]
        if stem in _stop_words:
            continue
        if len(stem) >= 3:
            expanded_kw.add(stem)
    # Добавляем синонимы. ВАЖНО: сопоставляем по началу слова, а не по
    # произвольному вхождению — иначе «пп» внутри «ре[пп]еленты» подтянет
    # синонимы здорового питания и даст неверный deeplink.
    for kw in list(expanded_kw):
        for syn_key, syn_vals in _synonyms.items():
            if kw.startswith(syn_key) or syn_key.startswith(kw):
                expanded_kw.update(s[:4] for s in syn_vals)

    # Полностью отбрасываем категории-акции (скидка/комбо/%/акция/цене)
    _campaign_markers = ("скидк", "комбо", "акци", "%", "купон", "цене", "+чипс", "+пиво")
    best_score = 0
    best_row = None
    for _, r in df.iterrows():
        cat = r["_search"]
        if any(m in cat for m in _campaign_markers):
            continue
        score = 0
        # Поиск с word boundary: ищем стем как НАЧАЛО слова (избегаем "това" в "готовая")
        for kw in expanded_kw:
            # Слово начинается с kw — ищем по \b<kw>
            if re.search(r"\b" + re.escape(kw), cat):
                score += len(kw)
        # Бонус за более короткое (общее) название категории
        if score > 0:
            score += max(0, 30 - len(cat))
        if score > best_score:
            best_score = score
            best_row = r

    if best_row is not None and best_score >= 3:
        return {"id": int(best_row["Id"]), "category": best_row["КАТЕГОРИЯ"], "deeplink": best_row["deeplink"]}

    return None


# Справочник механик ID_MECHANICS (прод, [mci_model].[dbo].[I_PROMO]).
# Поле «Механика» должно содержать ТОЧНО одно из этих значений.
MECHANICS_DICT = [
    "Кэшбек",
    "Целевые бонусы",
    "Скидка",
    "Активируемая скидка",
    "Купон",
    "Купон активируемый",
    "Коммуникация",
    "Кэшбек X баллов",
    "Кэшбек Х баллов за чек",
    "Кратный кэшбек",
    "Промокод",
    "Отложенная скидка",
    "Скидка x% от чека",
    "Скидка Xр. от чека",
    "Предначисленные бонусы",
    "Кэшбек активируемый",
]


def _normalize_mechanic(value: str, promo: dict | None = None) -> str:
    """Привести «Механику» к точному значению из справочника ID_MECHANICS.

    Если модель вернула значение из справочника — оставляем как есть.
    Иначе — подбираем ближайшее по типу выгоды и признаку активации.
    """
    v = _clean(value).strip()
    for m in MECHANICS_DICT:
        if v.lower() == m.lower():
            return m  # уже корректное значение справочника

    name = _clean((promo or {}).get("Название промо")).lower()
    text = f"{v.lower()} {name}"
    activ = any(w in text for w in ("активир", "активуем", "активируем", "активац", "акцепт"))

    if "коммуник" in text:
        return "Коммуникация"
    if "промокод" in text:
        return "Промокод"
    if "отложен" in text:
        return "Отложенная скидка"
    if "кратн" in text:
        return "Кратный кэшбек"
    if "предначисл" in text or "акцепт" in text or ("дарим" in text and "монет" in text):
        return "Предначисленные бонусы"
    if any(w in text for w in ("кешб", "кэшб", "cashback", "вернём", "вернем", "возвраща", "обратно на счет")):
        return "Кэшбек активируемый" if activ else "Кэшбек"
    if "купон" in text:
        return "Купон активируемый" if activ else "Купон"
    if "скидк" in text and "чек" in text:
        return "Скидка x% от чека"
    if "скидк" in text or "минус" in text:
        return "Активируемая скидка" if activ else "Скидка"
    if any(w in text for w in ("бонус", "монет", "балл")):
        return "Целевые бонусы"
    # Ничего не распознали — возвращаем как есть, чтобы не терять данные.
    return v


def _is_prepaid_bonus(mechanic: str, name: str) -> bool:
    """Механика «Предначисленные бонусы» / подарок монет (акцептные, «Активируй/Дарим N монет»)."""
    m = (mechanic or "").lower()
    n = (name or "").lower()
    if "предначисл" in m or "акцепт" in m:
        return True
    if "монет" in n and any(w in n for w in ("активируй", "акцепт", "дарим")):
        return True
    return False


def _cap_prepaid_bonus_expiry(result: dict, promo: dict) -> None:
    """Для предначисленных бонусов срок сгорания = окончание акции + 1 день (МАКСИМУМ).

    Чинит и завышенный срок (+7 дней), и неверный год.
    """
    name = _clean(promo.get("Название промо"))
    if not _is_prepaid_bonus(result.get("Механика", ""), name):
        return
    end_raw = _clean(promo.get("Окончание акции", ""))
    m = re.match(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", end_raw)
    if not m:
        return
    day, month = int(m.group(1)), int(m.group(2))
    if m.group(3):
        year = int(m.group(3))
        if year < 100:
            year += 2000
    else:
        year = _campaign_year(promo) or datetime.now().year
    try:
        expiry = date(year, month, day) + timedelta(days=1)
    except ValueError:
        return
    result["Срок сгорания бонусов"] = expiry.strftime("%d.%m.%Y") + " 23:59:00"


_MONTHS_RU = [
    ("январ", 1), ("феврал", 2), ("март", 3), ("апрел", 4), ("июн", 6), ("июл", 7),
    ("август", 8), ("сентябр", 9), ("октябр", 10), ("ноябр", 11), ("декабр", 12), ("ма", 5),
]


def _campaign_year(promo: dict) -> int | None:
    """Год кампании из «Год» (валидный 2024–2031), иначе из дат старта/окончания."""
    y = _clean(promo.get("Год", ""))
    if y.isdigit() and 2024 <= int(y) <= 2031:
        return int(y)
    for f in ("Окончание акции", "Старт акции"):
        m = re.search(r"\.(\d{4})", _clean(promo.get(f, "")))
        if m and 2024 <= int(m.group(1)) <= 2031:
            return int(m.group(1))
    return None


def _campaign_start_month(promo: dict) -> int:
    mm = _clean(promo.get("Месяц", ""))
    if mm.isdigit() and 1 <= int(mm) <= 12:
        return int(mm)
    m = re.match(r"\d{1,2}\.(\d{1,2})", _clean(promo.get("Старт акции", "")))
    return int(m.group(1)) if m else 1


def _month_num(word: str) -> int | None:
    w = word.lower()
    for stem, num in _MONTHS_RU:
        if w.startswith(stem):
            return num
    return None


def _fix_campaign_years(result: dict, promo: dict) -> None:
    """Принудительно проставить ГОД КАМПАНИИ во все даты (срок сгорания, текст купона).

    Модель часто пишет прошлый год (2025) вопреки данным — чиним пост-обработкой.
    Учитываем переход через НГ: если месяц даты < месяца старта, берём следующий год.
    """
    year = _campaign_year(promo)
    if not year:
        return
    sm = _campaign_start_month(promo)

    def yfor(month: int) -> int:
        return year if month >= sm else year + 1

    def fix_dotdate(m: re.Match) -> str:
        d, mo = m.group(1), int(m.group(2))
        if not (1 <= mo <= 12):
            return m.group(0)
        return f"{d}.{m.group(2)}.{yfor(mo)}"

    def fix_wordmonth(m: re.Match) -> str:
        d, word = m.group(1), m.group(2)
        mn = _month_num(word)
        return f"{d} {word} {yfor(mn)}" if mn else m.group(0)

    for fld in ("Срок сгорания бонусов", "Текст на информационном купоне / слип-чеке", "Описание акции"):
        v = result.get(fld)
        if not isinstance(v, str) or not v:
            continue
        v = re.sub(r"(\d{1,2})\.(\d{1,2})\.(20\d\d)", fix_dotdate, v)
        v = re.sub(r"(\d{1,2})\s+([А-Яа-яё]+)\s+(20\d\d)", fix_wordmonth, v)
        result[fld] = v


def _is_slip_channel(channel: str) -> bool:
    """Канал = слип-чек (офлайн, клиент без приложения).

    Для слипа купон бумажный: НЕТ кнопки/deeplink, в тексте нет упоминаний
    приложения/онлайн-заказа, скидка применяется при предъявлении купона и карты.
    """
    c = (channel or "").strip().lower()
    return "slip" in c or "слип" in c


# Постоянные (evergreen) разделы каталога приложения — только из них берём deeplink
# для акций. В справочнике помимо них куча кампанийных строк («Пикник», «Итс Ауч Тайм»…) —
# их в deeplink акции ставить нельзя (нужна КАТЕГОРИЯ, а не частная промо-подборка).
EVERGREEN_DEEPLINK_NAMES = (
    "молочные продукты, яйцо", "овощи, фрукты", "вода, соки, напитки", "готовая еда",
    "мясо, птица", "соусы, специи", "сыры", "чипсы, орехи и снеки", "мясная гастрономия",
    "для здорового питания", "хлеб и выпечка", "алкогольные напитки", "замороженные продукты",
    "рыба, морепродукты, икра", "бакалея", "кондитерские изделия, торты", "консервы",
    "чай, кофе, какао", "детское питание", "товары для детей", "товары для животных",
    "бытовая химия", "гигиена и косметика", "товары для дома и дачи",
)


def _norm_cat_name(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def _is_evergreen_category(name: str) -> bool:
    return _norm_cat_name(name) in EVERGREEN_DEEPLINK_NAMES


def _evergreen_deeplinks() -> dict[str, int]:
    """{нормализованное имя категории: Id} только для evergreen-разделов справочника."""
    df = _load_deeplinks()
    out: dict[str, int] = {}
    if df.empty:
        return out
    for _, r in df.iterrows():
        nm = _norm_cat_name(r["КАТЕГОРИЯ"])
        if nm in EVERGREEN_DEEPLINK_NAMES and nm not in out:
            out[nm] = int(r["Id"])
    return out


def resolve_deeplink_ai(category: str, name: str = "") -> dict | None:
    """AI-фолбэк подбора deeplink: выбрать БЛИЖАЙШИЙ ПОСТОЯННЫЙ раздел каталога
    (а не каталог целиком и не частную промо-подборку).

    НЕ выдумывает ID — выбирает только из evergreen-категорий справочника.
    Возвращает {"id","category","deeplink"} или None (если ничего не близко → каталог).
    """
    key = ANTHROPIC_API_KEY
    df = _load_deeplinks()
    q = (category or name).strip()
    if not key or df.empty or not q:
        return None
    seen = _evergreen_deeplinks()  # {имя: id} — только постоянные разделы
    listing = "\n".join(f"  {i}: {c}" for c, i in seen.items())
    prompt = (
        f"Акция ДИКСИ: категория «{category}» (название акции: «{name}»).\n"
        "Ниже список реальных категорий приложения (ID: название). Выбери ОДНУ — БЛИЖАЙШУЮ по смыслу "
        "для навигации клиента. Предпочитай широкие постоянные разделы (например «Товары для дома и дачи», "
        "«Бытовая химия», «Гигиена и косметика») перед узкими промо-подборками.\n"
        "Если НИ ОДНА категория не близка по смыслу — верни id = 0 (каталог).\n\n"
        f"КАТЕГОРИИ:\n{listing}\n\n"
        'Ответь СТРОГО JSON: {"id": <число>, "category": "<название>"}'
    )
    try:
        res = _parse_response(_anthropic_message(prompt, key, 300))
        cid = int(res.get("id", 0))
    except Exception:
        return None
    if not cid:
        return None
    row = df[df["Id"] == cid]
    if row.empty:
        return None
    r = row.iloc[0]
    return {"id": int(r["Id"]), "category": str(r["КАТЕГОРИЯ"]), "deeplink": str(r["deeplink"])}


def search_deeplinks(query: str, limit: int = 10) -> list[dict]:
    """Поиск deeplinks по запросу. Для UI."""
    df = _load_deeplinks()
    if df.empty or not query:
        return []
    q = query.strip().lower()
    matches = df[df["_search"].str.contains(q, na=False)]
    results = []
    for _, r in matches.head(limit).iterrows():
        results.append({"id": int(r["Id"]), "category": r["КАТЕГОРИЯ"], "deeplink": r["deeplink"]})
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Парсинг дат (поддержка форматов: 25.03., 25.03.2026, 2026-03-25 и т.д.)
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_date(d, year_hint=None) -> date | None:
    """Парсит дату из разных форматов, включая '25.03.' без года."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    if not d or str(d).strip() in ("", "nan", "NaT", "None"):
        return None

    s = str(d).strip().rstrip(".")  # убираем точку на конце: "25.03." → "25.03"

    # Если год не указан (формат DD.MM) — берём hint или текущий
    if not year_hint:
        year_hint = date.today().year

    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    # Формат DD.MM (без года)
    try:
        parsed = datetime.strptime(s, "%d.%m")
        return parsed.replace(year=int(year_hint)).date()
    except (ValueError, TypeError):
        pass

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Автоматический расчёт дат push из дат акции (правило 10)
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_push_schedule(promo: dict) -> list[dict]:
    """Рассчитать расписание push на основе дат акции.

    Правила:
      1 день (duration=0) — 1 push в день акции
      2 дня (duration=1) — 1 push на старт
      3-7 дней (неделя) — 2 push: старт + последний день (напоминание)
      8+ дней (месяц) — push каждую неделю в тот же день недели что и старт
    """
    year_hint = promo.get("Год", date.today().year)
    start = _parse_date(promo.get("Старт акции"), year_hint)
    end = _parse_date(promo.get("Окончание акции"), year_hint)

    if not start or not end:
        today = date.today()
        return [{"date": today.strftime("%d.%m.%Y"), "time": "12:00",
                 "date_obj": today, "type": "start"}]

    if end < start:
        start, end = end, start

    duration = (end - start).days

    schedule = []

    if duration <= 1:
        # 1-2 дня — одно сообщение на старт
        schedule.append({
            "date": start.strftime("%d.%m.%Y"),
            "time": "10:00",
            "date_obj": start,
            "type": "start",
        })

    elif duration <= 7:
        # 3-7 дней (неделя) — старт + напоминание через 1 день после старта
        schedule.append({
            "date": start.strftime("%d.%m.%Y"),
            "time": "10:00",
            "date_obj": start,
            "type": "start",
        })
        reminder = start + timedelta(days=2)  # через 1 день = на 2-й день после старта
        if reminder > end:
            reminder = end
        schedule.append({
            "date": reminder.strftime("%d.%m.%Y"),
            "time": "11:00",
            "date_obj": reminder,
            "type": "reminder",
        })

    else:
        # 8+ дней (месяц) — каждую неделю в тот же день недели
        current = start
        push_num = 0
        while current <= end:
            push_type = "start" if push_num == 0 else "reminder"
            schedule.append({
                "date": current.strftime("%d.%m.%Y"),
                "time": "10:00" if push_num == 0 else "11:00",
                "date_obj": current,
                "type": push_type,
            })
            push_num += 1
            current += timedelta(days=7)

    return schedule


# ═══════════════════════════════════════════════════════════════════════════════
# Извлечение данных из акции
# ═══════════════════════════════════════════════════════════════════════════════

def _clean(val) -> str:
    """Очистить значение от nan/None."""
    s = str(val).strip() if val is not None else ""
    return "" if s in ("nan", "None", "NaT") else s


def _extract_benefit(promo: dict) -> dict:
    """Умное извлечение выгоды из всех полей акции.

    ДИКСИ: программа лояльности — «монеты», НЕ «бонусы».

    ПРИОРИТЕТ ИСТОЧНИКОВ (от самого точного к общему):
      1. Название промо / механика / купон — содержат реальную выгоду за покупку
      2. Поле «Скидка» — если заполнено, обычно корректно
      3. Поле «Бонусы» — ОСТОРОЖНО: может содержать бюджет/лимит, а не сумму за чек!
         Поэтому сверяем с суммой из названия/механики.

    Номинальные значения (50, 100…) → «+50р. монетами».
    Процентные → «20% кешбэк» / «скидка 20%».
    """
    name = _clean(promo.get("Название промо"))
    discount = _clean(promo.get("Скидка"))
    bonus_field = _clean(promo.get("Бонусы"))
    coupon_text = _clean(promo.get("Текст на информационном купоне / слип-чеке"))
    coupon_name = _clean(promo.get("Название информационного купона для МП"))
    mech = _clean(promo.get("Механика")) or _clean(promo.get("Механика для Manzana Online"))
    description = _clean(promo.get("Описание акции"))

    # Собираем все текстовые источники для поиска реальной выгоды
    all_text = f"{name} {mech} {coupon_text} {coupon_name} {description}"
    all_text_lower = all_text.lower()

    benefit_type = "general"
    benefit_value = ""
    benefit_text = ""

    # ── 1. Ищем СКИДКУ (поле или в тексте) ──
    if discount:
        benefit_type = "discount"
        benefit_value = discount
        benefit_text = f"скидка {discount}"
        return {"type": benefit_type, "value": benefit_value, "text": benefit_text}

    # ── 1b. Ищем "скидку/скидка Xр" в названии ──
    _discount_rub_match = re.search(r"скидк\w*\s+(\d+)\s*(?:р\.?|₽|руб\.?)", name, re.IGNORECASE)
    if _discount_rub_match:
        _val = _discount_rub_match.group(1)
        return {"type": "discount", "value": f"{_val}₽", "text": f"-{_val}₽"}

    # ── 2. Ищем ПРОЦЕНТ в названии/механике ──
    pct_match = re.search(r"(\d+)\s*%", name)
    is_cashback = any(kw in all_text_lower for kw in ("кешбэк", "кешбэк", "cashback", "монет"))

    if pct_match:
        pct_val = pct_match.group(1)
        if is_cashback:
            return {"type": "cashback", "value": f"{pct_val}%", "text": f"{pct_val}% кешбэк"}
        else:
            return {"type": "discount", "value": f"{pct_val}%", "text": f"скидка {pct_val}%"}

    # ── 3. Ищем НОМИНАЛ МОНЕТ в названии/механике/купоне ──
    # Это самый надёжный источник: "начислим 50 монет", "+50 монет за чек"
    # Паттерны: "50 монет", "начислим 50", "+50р монетами", "50 р. монетами"
    # "Возвращаем 300р. с 1000р. на счет"
    nom_patterns = [
        r"(?:начисл\w*|получи\w*|верн\w*|возвращ\w*|дарим|подарим|\+)\s*(\d+)\s*(?:р\.?|₽|руб\.?)?\s*монет",
        r"(\d+)\s*(?:р\.?|₽|руб\.?)?\s*монет",
        r"(?:начисл\w*|получи\w*|верн\w*|возвращ\w*)\s*(\d+)\s*(?:р\.?|₽|руб\.?)",
        # "Возвращаем 300р. с 1000р. на счет" — номинал + "на счет/на карту"
        r"(?:начисл\w*|получи\w*|верн\w*|возвращ\w*|дарим|подарим)\s*(\d+)\s*(?:р\.?|₽|руб\.?)?\s*(?:с\s+\d+|.*?на\s+(?:счет|карт))",
    ]
    nom_from_text = None
    for pat in nom_patterns:
        m = re.search(pat, all_text, re.IGNORECASE)
        if m:
            nom_from_text = m.group(1)
            break

    if nom_from_text:
        return {
            "type": "bonus",
            "value": f"{nom_from_text}₽",
            "text": f"+{nom_from_text}₽ монетами",
        }

    # ── 4. Ищем процент в механике (fallback) ──
    if mech:
        m_pct = re.search(r"(\d+)\s*%", mech)
        if m_pct:
            pct_val = m_pct.group(1)
            if "монет" in mech.lower() or "кешбэк" in mech.lower():
                return {"type": "cashback", "value": f"{pct_val}%", "text": f"{pct_val}% кешбэк"}
            else:
                return {"type": "discount", "value": f"{pct_val}%", "text": f"скидка {pct_val}%"}

    # ── 5. Поле «Бонусы» — ПОСЛЕДНИЙ fallback ──
    # Может содержать бюджет/лимит (250), а не сумму за покупку (50)
    # Используем ТОЛЬКО если ничего не нашли выше
    if bonus_field:
        benefit_type = "bonus"
        bonus_clean = bonus_field.strip().replace(" ", "")
        if re.match(r"^\d+$", bonus_clean):
            benefit_value = f"{bonus_clean}₽"
            benefit_text = f"+{bonus_clean}₽ монетами"
        elif "%" in bonus_clean:
            benefit_value = bonus_clean
            benefit_text = f"{bonus_clean} монетами"
        else:
            benefit_value = bonus_clean
            benefit_text = f"+{bonus_clean} монетами"
        return {"type": benefit_type, "value": benefit_value, "text": benefit_text}

    return {
        "type": benefit_type,
        "value": benefit_value,
        "text": benefit_text,
    }


def _extract_condition(promo: dict) -> str:
    """Извлечь условие акции (мин. чек, мин. кол-во и т.д.) из механики и купона.

    Формат: «при чеке от 1000₽» — начинается с «при», чтобы не было двойного «за».
    """
    mech = _clean(promo.get("Механика")) or _clean(promo.get("Механика для Manzana Online"))
    coupon = _clean(promo.get("Текст на информационном купоне / слип-чеке"))
    name = _clean(promo.get("Название промо"))
    description = _clean(promo.get("Описание акции"))
    all_text = f"{name} {mech} {coupon} {description}"

    # Ищем "от Xр." / "от X руб" / "от X₽"
    m = re.search(r"(?:от|свыше|более)\s*(\d[\d\s]*)\s*(?:р\.?|руб\.?|₽)", all_text, re.IGNORECASE)
    if m:
        amount = m.group(1).replace(" ", "")
        return f"при чеке от {amount}₽"

    # Ищем "чек от X"
    m2 = re.search(r"чек\w*\s+(?:от|свыше)\s*(\d[\d\s]*)", all_text, re.IGNORECASE)
    if m2:
        amount = m2.group(1).replace(" ", "")
        return f"при чеке от {amount}₽"

    # Ищем "с Xр." / "с X₽" — "Возвращаем 300р. с 1000р."
    m_s = re.search(r"\d+\s*(?:р\.?|₽|руб\.?)\s+с\s+(\d[\d\s]*)\s*(?:р\.?|₽|руб\.?)", all_text, re.IGNORECASE)
    if m_s:
        amount = m_s.group(1).replace(" ", "")
        return f"при чеке от {amount}₽"

    # Ищем "при покупке от X шт."
    m3 = re.search(r"(?:от|свыше)\s*(\d+)\s*(?:шт|штук|единиц)", all_text, re.IGNORECASE)
    if m3:
        return f"при покупке от {m3.group(1)} шт."

    # Ищем "покупку на сумму от X"
    m4 = re.search(r"(?:на сумму|сумм[аеой])\s+(?:от\s+)?(\d[\d\s]*)\s*(?:р\.?|руб\.?|₽)?", all_text, re.IGNORECASE)
    if m4:
        amount = m4.group(1).replace(" ", "")
        return f"при чеке от {amount}₽"

    return ""


def _extract_promo_code(promo: dict) -> dict | None:
    """Извлечь промокод из полей акции.

    Ищет в механике, купоне и названии паттерны:
      - «промокод XXXX», «промо-код XXXX», «код XXXX»
      - и связанную скидку: «-300р», «скидка 300», «300₽ скидка»

    Возвращает {"code": "nastol", "benefit": "-300₽"} или None.
    """
    mech = _clean(promo.get("Механика")) or _clean(promo.get("Механика для Manzana Online"))
    coupon = _clean(promo.get("Текст на информационном купоне / слип-чеке"))
    coupon_name = _clean(promo.get("Название информационного купона для МП"))
    button = _clean(promo.get("Текст кнопки (действие)"))
    name = _clean(promo.get("Название промо"))
    all_text = f"{name} {mech} {coupon} {coupon_name} {button}"

    # Ищем промокод
    code_match = re.search(
        r"(?:промо[- ]?код|promo[- ]?code|код)\s+[«\"']?([A-Za-zА-Яа-я0-9_-]+)[»\"']?",
        all_text, re.IGNORECASE)
    if not code_match:
        return None

    code = code_match.group(1)

    # Ищем связанную скидку и мин. сумму заказа
    # Расширенный контекст: 120 символов вокруг промокода
    pos = code_match.start()
    context = all_text[max(0, pos - 120):pos + 120]

    benefit_amount = ""
    min_order = ""

    # "скидка 300р" / "-300р" / "300₽ скидка" / "скидку 300"
    ben_match = re.search(r"(?:скидк\w*|минус|-)\s*(\d+)\s*(?:р\.?|руб\.?|₽)?", context, re.IGNORECASE)
    if ben_match:
        benefit_amount = ben_match.group(1)
    else:
        ben_match2 = re.search(r"(\d+)\s*(?:р\.?|руб\.?|₽)\s*(?:скидк)", context, re.IGNORECASE)
        if ben_match2:
            benefit_amount = ben_match2.group(1)

    # Ищем мин. сумму заказа: "от 1500р", "заказ от 1500", "на сумму от 1500"
    min_match = re.search(r"(?:заказ\w*|сумм\w*|чек\w*)\s+(?:от|свыше)\s*(\d+)\s*(?:р\.?|руб\.?|₽)?", context, re.IGNORECASE)
    if min_match:
        min_order = min_match.group(1)
    else:
        # "от 1500р" рядом с промокодом
        min_match2 = re.search(r"от\s*(\d{3,})\s*(?:р\.?|руб\.?|₽)?", context, re.IGNORECASE)
        if min_match2:
            min_order = min_match2.group(1)

    return {
        "code": code,
        "benefit_amount": benefit_amount,
        "min_order": min_order,
    }


def _extract_category_details(promo: dict) -> dict:
    """Извлечь реальную категорию и конкретные товары."""
    name = _clean(promo.get("Название промо"))
    category_raw = _clean(promo.get("Категория"))
    coupon_text = _clean(promo.get("Текст на информационном купоне / слип-чеке"))
    button = _clean(promo.get("Кнопка"))
    mech = _clean(promo.get("Механика")) or _clean(promo.get("Механика для Manzana Online"))

    # Если категория — числовые коды, очищаем
    if category_raw:
        _cleaned_cat = re.sub(r"[\d\s\n]+", "", category_raw).strip()
        if not _cleaned_cat:
            # Категория состоит только из цифр/пробелов/переводов строки — это коды товаров
            category_raw = ""

    # Если категория — просто "список" или пусто, извлекаем из названия/купона
    category = category_raw
    if not category or category.lower() in ("список", "nan", ""):
        # Попробуем из названия: "20% КЕШБЭК на бытовую химию и средства для ухода за обувью"
        m = re.search(r"(?:на|в)\s+(.+?)$", name, re.IGNORECASE)
        if m:
            candidate = m.group(1).strip()
            # Отфильтровываем мусор: "ДИКСИ", "счет", "карту" — это не категории товаров
            _junk = ("дикси", "счет", "счёт", "карт", "кассу", "магазин", "период")
            if not any(j in candidate.lower() for j in _junk):
                category = candidate

    # Извлечь конкретные товары из текста купона
    products = []
    if coupon_text:
        # Ищем полные фразы: "средств для ухода за обувью" → "уход за обувью"
        # "средств для чистки посуды, ванной и туалета" → "чистка посуды, ванной"
        full_matches = re.findall(
            r"средств[ао]?\s+(?:для\s+)([\w\s,]+?)(?=\n|и\s+друг|$)",
            coupon_text, re.IGNORECASE
        )
        for m in full_matches:
            # "ухода за обувью" → "уход за обувью"
            # "чистки посуды, ванной и туалета" → "чистка посуды и ванной"
            p = m.strip().rstrip(",. ")
            p = re.sub(r"^ухода\s+за\s+", "уход за ", p)
            p = re.sub(r"^чистки\s+", "чистка ", p)
            if len(p) > 3:
                products.append(p)

        # Ищем "покупку <товаров>" как fallback
        if not products:
            m2 = re.search(r"покупк[уи]\s+(.+?)(?:\n|с\s+картой|в\s+период)", coupon_text, re.IGNORECASE)
            if m2:
                txt = m2.group(1).strip()
                parts = re.split(r",\s*", txt)
                products = [p.strip() for p in parts if len(p.strip()) > 3]

    # Из кнопки — короткое название (но НЕ deeplinks и системные кнопки)
    if button and button.lower() not in ("nan", ""):
        btn_clean = button.strip()
        # Убираем deeplinks, URL, системные кнопки
        _btn_junk = ("dixy", "http", "каталог", "catalog", "app/", "://", "активир", "магазин")
        if not any(j in btn_clean.lower() for j in _btn_junk):
            btn_clean = re.sub(r"^средств[ао]?\s+(?:для\s+)?", "", btn_clean, flags=re.IGNORECASE)
            btn_clean = re.sub(r"^чистки\s+", "чистка ", btn_clean, flags=re.IGNORECASE)
            btn_clean = re.sub(r"^ухода\s+за\s+", "уход за ", btn_clean, flags=re.IGNORECASE)
            if btn_clean and len(btn_clean) > 2:
                products.append(btn_clean)

    # Убираем дубли
    seen = set()
    unique_products = []
    for p in products:
        key = p.lower()[:15]
        if key not in seen:
            seen.add(key)
            unique_products.append(p)

    # Формируем текст с товарами — если слишком длинный, берём категорию
    products_list = unique_products[:3]
    products_text = ", ".join(products_list) if products_list else category

    # Если текст товаров > 50 символов, используем категорию
    if len(products_text) > 50 and category:
        products_text = category

    return {
        "category": category,
        "products": products_list,
        "products_text": products_text,
    }


def _needs_activation(promo: dict) -> bool:
    """Проверить, нужна ли активация."""
    name = _clean(promo.get("Название промо")).lower()
    mech = (_clean(promo.get("Механика")) or _clean(promo.get("Механика для Manzana Online"))).lower()
    coupon = _clean(promo.get("Текст на информационном купоне / слип-чеке")).lower()
    coupon_name = _clean(promo.get("Название информационного купона для МП")).lower()
    return ("активир" in name or "активир" in mech or "активац" in mech
            or "активац" in coupon or "активир" in coupon_name)


def _is_online(promo: dict) -> bool:
    """Проверить, доступна ли акция онлайн."""
    coupon = _clean(promo.get("Текст на информационном купоне / слип-чеке")).lower()
    return "онлайн" in coupon or "приложени" in coupon


# ═══════════════════════════════════════════════════════════════════════════════
# Многомерная классификация акций (5 осей)
# ═══════════════════════════════════════════════════════════════════════════════

def classify_promo(promo: dict) -> dict:
    """Классифицировать акцию по 5 осям.

    Returns dict:
        activation: "yes" | "no"
        benefit: "cashback_pct" | "cashback_rub" | "discount_pct" | "discount_rub" | "gift" | "communication" | "present"
        scope: "all" | "category"
        check: "no_check" | "check_from"
        period: "one_day" | "week" | "month"
        value: "20%" | "300₽" | "50₽" и т.д.
        check_amount: "1000₽" | "" (если нет чека)
    """
    year_hint = promo.get("Год", date.today().year)
    name = _clean(promo.get("Название промо")).lower()
    desc = _clean(promo.get("Описание акции")).lower()
    mech = (_clean(promo.get("Механика")) or _clean(promo.get("Механика для Manzana Online"))).lower()
    text = f"{name} {desc} {mech}"

    # ── Ось 1: Активация ──
    activation = "no"
    coupon_name = _clean(promo.get("Название информационного купона для МП")).lower()
    coupon_flag = _clean(promo.get("Купон")).lower()
    if "активируй" in name or "активир" in name or "акцептн" in name:
        activation = "yes"
    elif "активируй" in text or "акцептн" in text:
        activation = "yes"
    elif "активируй" in coupon_name or "активир" in coupon_name:
        activation = "yes"
    elif coupon_flag == "да" and ("активир" in mech or "активация" in mech):
        activation = "yes"

    # ── Ось 2: Тип выгоды ──
    # Синонимы кешбэка
    _cashback_words = ("кешбэк", "кешбэк", "cashback", "вернём", "вернем",
                       "возвращаем", "обратно на счет", "на счет", "монетами за")

    has_cashback_word = any(w in text for w in _cashback_words)
    has_discount_word = "скидк" in text or "купон на скидк" in text
    has_gift_word = "дарим" in text
    has_comm_word = any(w in text for w in ("коммуникац", "тематик", "подборка", "рассылка"))
    has_present_word = "подарок" in text or "подарки за" in text

    # Определяем значение (число + % или ₽)
    benefit = _extract_benefit(promo)
    value = benefit.get("value", "")
    is_pct = "%" in value
    is_rub = "₽" in value or "р" in value

    if has_comm_word and not has_cashback_word and not has_discount_word and not has_gift_word:
        benefit_type = "communication"
    elif has_present_word:
        benefit_type = "present"
    elif has_gift_word and not has_cashback_word and not has_discount_word:
        benefit_type = "gift"
    elif has_cashback_word:
        benefit_type = "cashback_pct" if is_pct else "cashback_rub"
    elif has_discount_word:
        benefit_type = "discount_pct" if is_pct else "discount_rub"
    else:
        # Fallback: число + "монет" без других маркеров = gift
        if re.search(r'\d+\s*монет', text):
            benefit_type = "gift"
        elif is_pct:
            benefit_type = "discount_pct"
        elif is_rub:
            benefit_type = "discount_rub"
        else:
            benefit_type = "communication"

    # ── Ось 3: Товарный охват ──
    cat = _extract_category_details(promo)
    has_category = bool(cat.get("category") or cat.get("products_text"))
    # "все товары" / "на покупки" = all
    _cat_text = (cat.get("category", "") + " " + cat.get("products_text", "")).lower().strip()
    _all_markers = ("все товары", "все", "на покупки", "на покупки в дикси")
    # Проверяем: пустое ИЛИ содержит только "все товары"
    _cat_words = set(_cat_text.split())
    _is_all = (not _cat_text
               or _cat_text in _all_markers
               or _cat_words <= {"все", "товары", "на", "покупки", "в", "дикси"})
    if _is_all:
        scope = "all"
    elif has_category:
        scope = "category"
    else:
        scope = "all"

    # ── Ось 4: Условие чека ──
    condition_raw = _extract_condition(promo)
    if condition_raw and re.search(r'\d+', condition_raw):
        check = "check_from"
        _m = re.search(r'(\d[\d\s]*)₽?', condition_raw)
        check_amount = f"{_m.group(1).strip()}₽" if _m else ""
    else:
        check = "no_check"
        check_amount = ""

    # ── Ось 5: Период ──
    try:
        start_str = _clean(promo.get("Старт акции"))
        end_str = _clean(promo.get("Окончание акции"))
        if start_str and end_str:
            from datetime import datetime
            start = _parse_date(start_str)
            end = _parse_date(end_str)
            days = (end - start).days
            if days <= 0:
                period = "one_day"
            elif days <= 7:
                period = "week"
            else:
                period = "month"
        else:
            period = "week"
    except Exception:
        period = "week"

    # ── Ось 6: Контекст (сезон / праздник / погода) ──
    context_tags = []
    try:
        _start_d = _parse_date(_clean(promo.get("Старт акции")), year_hint) if promo.get("Старт акции") else date.today()
        if not _start_d:
            _start_d = date.today()
        _month = _start_d.month
        _day = _start_d.day

        # Сезон
        if _month in (12, 1, 2):
            context_tags.append("зима")
        elif _month in (3, 4, 5):
            context_tags.append("весна")
        elif _month in (6, 7, 8):
            context_tags.append("лето")
        else:
            context_tags.append("осень")

        # Праздники (по дате старта push, проверяем ±3 дня)
        _holidays = {
            (1, 1): "новый_год", (1, 2): "новый_год", (1, 3): "новый_год",
            (1, 7): "рождество",
            (2, 14): "валентинов_день",
            (2, 23): "23_февраля",
            (3, 8): "8_марта",
            (5, 1): "1_мая", (5, 9): "день_победы",
            (6, 1): "день_детей", (6, 12): "день_россии",
            (9, 1): "1_сентября",
            (11, 4): "день_народного_единства",
            (12, 31): "новый_год",
        }
        # Пасха — подвижная дата, приблизительно
        _easter_dates = {
            2025: (4, 20), 2026: (4, 12), 2027: (5, 2), 2028: (4, 16),
        }
        _yr = _start_d.year
        if _yr in _easter_dates:
            em, ed = _easter_dates[_yr]
            _holidays[(em, ed)] = "пасха"

        # Проверяем праздники: за 7 дней до и 1 день после
        for (hm, hd), tag in _holidays.items():
            try:
                _holiday_date = date(_start_d.year, hm, hd)
                _delta = (_holiday_date - _start_d).days
                if -1 <= _delta <= 7:  # push за неделю до праздника или в сам день
                    if tag not in context_tags:
                        context_tags.append(tag)
            except ValueError:
                continue

        # Погода / активности по сезону
        if _month in (5, 6, 7, 8, 9):
            context_tags.append("шашлыки")
        if _month in (4, 5):
            context_tags.append("уборка")  # весенняя уборка
        if _month in (6, 7, 8):
            context_tags.append("жара")
        if _month in (12, 1, 2):
            context_tags.append("тепло_уют")

    except Exception:
        context_tags = ["весна"]  # safe fallback

    # ── Онлайн ──
    is_online = _is_online(promo)

    return {
        "activation": activation,
        "benefit": benefit_type,
        "scope": scope,
        "check": check,
        "period": period,
        "value": value,
        "check_amount": check_amount,
        "context": context_tags,
        "is_online": is_online,
    }


# Фразы для контекста — используются в заголовке и body
_CONTEXT_TITLE_HINTS = {
    "пасха": ["🐣Пасхальн", "🥚К Пасхе: "],
    "новый_год": ["🎄Новогодн", "🎅К Новому году: "],
    "8_марта": ["🌷К 8 марта: ", "💐Праздничн"],
    "23_февраля": ["🎖К 23 февраля: ", "💪Мужской "],
    "1_мая": ["🌸Майск", "☀️Майск"],
    "день_победы": ["🎗9 мая: "],
    "валентинов_день": ["❤️К 14 февраля: "],
}

_CONTEXT_BODY_PHRASES = {
    "пасха": [
        " Куличи пекутся, яйца красятся — а монеты копятся 🐣",
        " Пасхальный стол собирается сам — осталось зайти в ДИКСИ 🥚",
        " Красим яйца, печём куличи — экономим с умом 🐣",
        " К пасхальному столу — с выгодой и без суеты 🥚",
    ],
    "новый_год": [
        " Оливье, мандарины и монеты на карте — Новый год близко 🎄",
        " Ёлка есть, подарки есть — осталось закупиться 🎅",
        " Праздничный стол сам себя не накроет — но мы поможем 🎄",
        " Шампанское найдётся, а выгода — уже тут 🥂",
    ],
    "8_марта": [
        " Цветы — отдельно, а продукты — с выгодой 🌷",
        " 8 марта — праздник, а экономия — привычка 💐",
        " Весна, тюльпаны и монеты на карте 🌷",
    ],
    "23_февраля": [
        " Мужской праздник — мужская закупка 💪",
        " Стейк, пиво и монеты — что ещё нужно? 🥩",
        " 23 февраля — день серьёзных покупок 🎖",
    ],
    "зима": [
        " Под пледом теплее, с монетами — веселее ❄️",
        " Зимний вечер + горячий ужин = идеально ☕",
        " Мороз крепчает — запасы пополняются 🧣",
        " Зима — время сытных ужинов и умных покупок 🏠",
    ],
    "весна": [
        " Весна — значит пора обновить холодильник 🌱",
        " Природа просыпается, аппетит растёт 🌸",
        " Авитаминоз? ДИКСИ поможет 🥗",
        " Весна пришла — пора за витаминами 🌿",
    ],
    "лето": [
        " Мороженое тает, цены — тоже 🍦",
        " Жара? Холодильник полный — проблема решена 🧊",
        " Лето, солнце, барбекю — и монеты на карте ☀️",
        " Ледяной лимонад и горячие скидки 🧊",
    ],
    "осень": [
        " Листья падают — цены тоже 🍂",
        " Осень — время тёплых супов и умных покупок 🍁",
        " Дождь за окном, а в корзине — выгода 🌧",
        " Запасаемся на осень — монеты копятся 🍂",
    ],
    "шашлыки": [
        " Мангал ждёт, угли готовы — осталось мясо 🔥",
        " Шашлык + друзья + монеты = идеальный выходной 🌳",
        " На природу? Корзину собрали, монеты начислили 🔥",
        " Сезон открыт: мясо на мангал, монеты на карту 🍖",
    ],
    "уборка": [
        " Квартира блестит — карта пополняется 🧹",
        " Весна — из шкафов пыль, на карту — монеты ✨",
        " Генуборка с кешбэком — двойная чистота 🧽",
        " Моем, чистим, экономим — весна! 🧹",
    ],
    "жара": [
        " В +30 спасает только холодное и выгодное 🧊",
        " Жара плавит цены — бери пока холодно в ДИКСИ ❄️",
        " Водичка, мороженое и монеты на карте 💧",
    ],
    "тепло_уют": [
        " Горячий чай, тёплый плед и полный холодильник 🏠",
        " Уют = вкусный ужин + экономия ☕",
        " Когда за окном холод — дома должно быть вкусно 🫖",
    ],
    "1_мая": [
        " Майские — на дачу! Корзину собрали? 🌸",
        " Праздники = шашлыки + закупка в ДИКСИ 🔥",
    ],
    "день_победы": [
        " К праздничному столу — с уважением и выгодой 🎗",
        " 9 мая — день памяти и вкусного стола 🎗",
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# Готовые пары шаблонов (title, body) по комбинации осей
# Ключ: (activation, benefit, scope, check, period)  — "*" = любое значение
# ═══════════════════════════════════════════════════════════════════════════════

_PUSH_PAIRS = {
    # ── NO ACTIVATION + CASHBACK % + CATEGORY ──
    ("no", "cashback_pct", "category", "*", "*"): [
        ("{emoji}{value} кешбэк на {products_short}",
         "{date_context}. Покупаешь — монеты копятся.{humor} {cta}"),
        ("{emoji}{value} назад за {products_short}",
         "{date_context}. {value} вернутся монетами на карту.{humor} {cta}"),
        ("{emoji}{value} вернём за {products_short}",
         "{date_context}.{humor} {cta}"),
        ("{emoji}{products_short} + {value} назад",
         "{date_context}. Монеты — сразу на карту.{humor} {cta}"),
    ],
    # ── NO + CASHBACK % + ALL + NO_CHECK ──
    ("no", "cashback_pct", "all", "no_check", "*"): [
        ("{emoji}{value} вернём монетами",
         "{date_context}. Покупаешь — монеты копятся.{humor} {cta}"),
        ("{emoji}{value} кешбэк с покупки",
         "{date_context}. {value} вернутся монетами на карту.{humor} {cta}"),
        ("{emoji}{value} монетами с покупки",
         "{date_context}.{humor} {cta}"),
    ],
    # ── NO + CASHBACK % + ALL + CHECK ──
    ("no", "cashback_pct", "all", "check_from", "*"): [
        ("{emoji}{value} с чека от {check_amount}",
         "{date_context}. Монеты начисляются сразу.{humor} {cta}"),
        ("{emoji}{value} монетами с чека от {check_amount}",
         "{date_context}.{humor} {cta}"),
        ("{emoji}{value} кешбэк за чек от {check_amount}",
         "{date_context}. {value} вернутся на карту.{humor} {cta}"),
    ],
    # ── NO + CASHBACK ₽ + CATEGORY ──
    ("no", "cashback_rub", "category", "*", "*"): [
        ("{emoji}+{value} монетами за {products_short}",
         "{products} {date_context}.{humor} {cta}"),
        ("{emoji}{value} вернём за {products_short}",
         "{products} {date_context}. Монеты — на карту.{humor} {cta}"),
        ("{emoji}{products_short} = +{value} монет",
         "{products} {date_context}.{humor} {cta}"),
    ],
    # ── NO + CASHBACK ₽ + ALL + CHECK ──
    ("no", "cashback_rub", "all", "check_from", "*"): [
        ("{emoji}{value} с чека от {check_amount}",
         "{date_context}. Монеты — сразу на карту.{humor} {cta}"),
        ("{emoji}+{value} за чек от {check_amount}",
         "{date_context}.{humor} {cta}"),
        ("{emoji}{value} вернём за чек от {check_amount}",
         "{date_context}. Покупай и копи!{humor} {cta}"),
    ],
    # ── NO + CASHBACK ₽ + ALL + NO_CHECK ──
    ("no", "cashback_rub", "all", "no_check", "*"): [
        ("{emoji}{value} вернём монетами",
         "{date_context}.{humor} {cta}"),
        ("{emoji}+{value} монетами с покупки",
         "{date_context}. Монеты — на карту.{humor} {cta}"),
    ],
    # ── NO + DISCOUNT % + CATEGORY ──
    ("no", "discount_pct", "category", "*", "one_day"): [
        ("{emoji}-{value} на {products_short}",
         "{products} — только сегодня!{humor} {cta}"),
        ("{emoji}{products_short}: -{value}",
         "{products} — один день!{humor} {cta}"),
    ],
    ("no", "discount_pct", "category", "*", "*"): [
        ("{emoji}-{value} на {products_short}",
         "{products} {date_context}.{humor} {cta}"),
        ("{emoji}{products_short} дешевле на {value}",
         "{products} {date_context}. Цены уже снижены.{humor} {cta}"),
        ("{emoji}{products_short}: -{value}",
         "{products} {date_context}.{humor} {cta}"),
    ],
    # ── NO + DISCOUNT % + ALL ──
    ("no", "discount_pct", "all", "*", "*"): [
        ("{emoji}-{value} на всё",
         "{date_context}. Цены снижены!{humor} {cta}"),
        ("{emoji}-{value} с чека",
         "{date_context}.{humor} {cta}"),
    ],
    # ── NO + DISCOUNT ₽ ──
    ("no", "discount_rub", "*", "*", "*"): [
        ("{emoji}Скидка {value} по купону",
         "{date_context}. Покажи купон на кассе.{humor} {cta}"),
        ("{emoji}Купон на {value} — забирай",
         "{date_context}. Действует при предъявлении купона.{humor} {cta}"),
        ("{emoji}Скидка {value} с покупки",
         "{date_context}.{humor} {cta}"),
    ],
    # ── NO + GIFT + ALL + NO_CHECK ──
    ("no", "gift", "all", "no_check", "*"): [
        ("{emoji}{value} монетами — дарим!",
         "{date_context}. Монеты на карте — трать с удовольствием!{humor} {cta}"),
        ("{emoji}+{value} на карту монетами",
         "{date_context}. Приятно, когда дарят 😊 {cta}"),
        ("{emoji}{value} уже на карте",
         "{date_context}. Монеты ждут — заходи потратить!{humor} {cta}"),
        ("{emoji}{value} в подарок монетами",
         "{date_context}.{humor} {cta}"),
    ],
    # ── NO + GIFT + ALL + CHECK ──
    ("no", "gift", "all", "check_from", "*"): [
        ("{emoji}{value} за чек от {check_amount}",
         "{date_context}. Монеты начисляются сразу.{humor} {cta}"),
        ("{emoji}+{value} монетами за чек от {check_amount}",
         "{date_context}. Покупай — копи — трать!{humor} {cta}"),
    ],
    # ── NO + GIFT + CATEGORY ──
    ("no", "gift", "category", "*", "*"): [
        ("{emoji}+{value} монетами за {products_short}",
         "{products} {date_context}. Монеты — на карту.{humor} {cta}"),
        ("{emoji}{value} дарим за {products_short}",
         "{products} {date_context}.{humor} {cta}"),
    ],
    # ── NO + COMMUNICATION ──
    ("no", "communication", "category", "*", "*"): [
        ("{emoji}{products_short} — цены ниже рынка",
         "{products} {date_context}. Загляни — приятно удивишься!{humor} {cta}"),
        ("{emoji}{products_short} — цены вау 🤩",
         "{products} {date_context}.{humor} {cta}"),
        ("{emoji}{products_short} — цены приятные",
         "{products} {date_context}. В ДИКСИ сейчас выгодно!{humor} {cta}"),
        ("{emoji}{products_short} — цены класс 👌",
         "{products} {date_context}.{humor} {cta}"),
        ("{emoji}Комфортные цены: {products_short}",
         "{products} {date_context}. ДИКСИ радует ценами!{humor} {cta}"),
    ],
    ("no", "communication", "all", "*", "*"): [
        ("{emoji}Цены ниже рынка в ДИКСИ",
         "{date_context}. Загляни — приятно удивишься!{humor} {cta}"),
        ("{emoji}Цены вау в ДИКСИ 🤩",
         "{date_context}.{humor} {cta}"),
        ("{emoji}Комфортные цены в ДИКСИ",
         "{date_context}. Заходи — порадуешься!{humor} {cta}"),
    ],
    # ── NO + PRESENT ──
    ("no", "present", "*", "*", "*"): [
        ("{emoji}Подарки за покупку",
         "{date_context}.{humor} {cta}"),
        ("{emoji}Подарок за чек",
         "{date_context}. Покупай и получай подарки!{humor} {cta}"),
    ],
    # ── YES + CASHBACK % ──
    ("yes", "cashback_pct", "category", "*", "*"): [
        ("{emoji}Активируй {value} кешбэк",
         "{products} {date_context}. Монеты начисляются сразу после покупки.{humor} {cta}"),
        ("{emoji}{value} кешбэк — активируй",
         "{products} {date_context}.{humor} {cta}"),
        ("{emoji}Включи {value} кешбэк",
         "{products} {date_context}. Активируй и {value} вернутся монетами.{humor} {cta}"),
    ],
    ("yes", "cashback_pct", "all", "*", "*"): [
        ("{emoji}Активируй {value} кешбэк",
         "{date_context}. Монеты начисляются сразу после покупки.{humor} {cta}"),
        ("{emoji}{value} кешбэк — активируй",
         "{date_context}.{humor} {cta}"),
    ],
    # ── YES + CASHBACK ₽ ──
    ("yes", "cashback_rub", "*", "*", "*"): [
        ("{emoji}Активируй {value} монетами",
         "{date_context}. Монеты — сразу на карту после покупки.{humor} {cta}"),
        ("{emoji}{value} монетами — активируй",
         "{date_context}.{humor} {cta}"),
        ("{emoji}Забери {value} монетами",
         "{date_context}. Активируй и покупай — монеты на карте!{humor} {cta}"),
    ],
    # ── YES + DISCOUNT % ──
    ("yes", "discount_pct", "*", "*", "*"): [
        ("{emoji}Активируй -{value}",
         "{products} {date_context}. Цена снизится после активации.{humor} {cta}"),
        ("{emoji}-{value} — активируй",
         "{products} {date_context}.{humor} {cta}"),
    ],
    # ── YES + DISCOUNT ₽ ──
    ("yes", "discount_rub", "*", "*", "*"): [
        ("{emoji}Активируй скидку {value}",
         "{date_context}. Покажи купон на кассе.{humor} {cta}"),
        ("{emoji}Скидка {value} — активируй",
         "{date_context}.{humor} {cta}"),
    ],
    # ── YES + GIFT ──
    ("yes", "gift", "all", "*", "*"): [
        ("{emoji}Активируй {value} монетами",
         "{date_context}. Монеты ждут на карте!{humor} {cta}"),
        ("{emoji}{value} монетами — активируй!",
         "{date_context}. Активируй и трать в магазине.{humor} {cta}"),
        ("{emoji}Забери {value} монетами",
         "{date_context}. Активируй — монеты на карте!{humor} {cta}"),
        ("{emoji}{value} ждут активации",
         "{date_context}. Активируй и не забудь потратить!{humor} {cta}"),
    ],
    ("yes", "gift", "category", "*", "*"): [
        ("{emoji}Активируй {value} за {products_short}",
         "{products} {date_context}. Монеты начисляются после покупки.{humor} {cta}"),
        ("{emoji}{value} за {products_short} — активируй",
         "{products} {date_context}.{humor} {cta}"),
    ],
    # ── НАПОМИНАНИЯ: коммуникация (без value, без urgency) ──
    ("no", "communication", "*", "*", "reminder"): [
        ("{emoji}{products_short} — обновляем",
         "{products} {date_context}. Загляни — обновили ассортимент!{humor} {cta}"),
        ("{emoji}{products_short} — напоминаем",
         "{products} {date_context}. Цены всё ещё радуют!{humor} {cta}"),
        ("{emoji}{products_short} ждут в ДИКСИ",
         "{products} {date_context}.{humor} {cta}"),
    ],
    # ── НАПОМИНАНИЯ (reminder) — акции с выгодой ──
    ("*", "*", "*", "*", "reminder"): [
        ("⏳{value} — заканчивается",
         "{products} {date_context}. Не пропусти!{humor} {cta}"),
        ("🏃Успей: {value} {date_context}",
         "{products} Последний шанс!{humor} {cta}"),
        ("⚡Последний шанс: {value}",
         "{products} {date_context}. Успей забрать!{humor} {cta}"),
        ("🔔Напоминаем: {value}",
         "{products} {date_context}. Не забудь!{humor} {cta}"),
    ],
}


def _match_pairs(key: tuple) -> list[tuple]:
    """Найти пары шаблонов по ключу с поддержкой wildcard '*'.

    Приоритет: более специфичные (меньше *) идут первыми.
    Если есть специфичный матч — общий wildcard НЕ добавляется.
    """
    matches = []
    for tpl_key, tpl_pairs in _PUSH_PAIRS.items():
        match = True
        specificity = 0
        for k, t in zip(key, tpl_key):
            if t != "*" and k != t:
                match = False
                break
            if t != "*":
                specificity += 1
        if match:
            matches.append((specificity, tpl_pairs))

    if not matches:
        return []

    # Если есть специфичные матчи (specificity > 0), не берём полностью wildcard
    max_spec = max(s for s, _ in matches)
    results = []
    for spec, pairs in matches:
        # Берём только матчи с максимальной или близкой специфичностью
        if spec >= max_spec - 1:
            results.extend(pairs)
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Расшифровка товаров по категориям (правило 16)
# ═══════════════════════════════════════════════════════════════════════════════

_PRODUCT_DETAILS = {
    # Одежда / аксессуары (одна запись чтобы не дублировать)
    "колготк": "колготки, чулки, носки",
    "носк": "носки, колготки, чулки",
    "чулк": "чулки, колготки, носки",
    # Бытовая химия — порядок важен! Специфичные сначала
    "обув": "средства для обуви",
    "стир": "порошки, гели для стирки",
    "чист": "средства для кухни и ванной",
    "хими": "средства для стирки, мытья посуды, уборки",
    "бытов": "средства для стирки, мытья посуды, уборки",
    # ── ЕДА: мясо и птица ──
    "мясо": "говядина, свинина, фарш",
    "курица": "филе, окорочка, крылышки",
    "куриц": "филе, окорочка, крылышки",
    "птиц": "курица, индейка, утка",
    "индейк": "индейка, филе, стейки",
    "свинин": "свинина, шейка, рёбра, фарш",
    "говядин": "говядина, стейки, фарш",
    "фарш": "фарш свиной, говяжий, куриный",
    "шашлык": "свинина, курица, маринады",
    "полуфабрикат": "пельмени, котлеты, блинчики",
    "пельмен": "пельмени, вареники, хинкали",
    "варenik": "вареники, пельмени",
    "сосиск": "сосиски, сардельки",
    "колбас": "колбаса, сосиски, ветчина",
    "ветчин": "ветчина, буженина, карбонад",
    # ── ЕДА: молочное ──
    "молоко": "молоко, кефир, сметана",
    "молочн": "творог, йогурты, сметана",
    "кефир": "кефир, ряженка, простокваша",
    "сметан": "сметана, творог",
    "творог": "творог, сырки, запеканки",
    "йогурт": "йогурты, творожки, десерты",
    "масло": "масло сливочное, растительное",
    "сыр": "сыры твёрдые, плавленые, моцарелла",
    "яйц": "яйца куриные, перепелиные",
    # ── ЕДА: рыба и морепродукты ──
    "рыб": "рыба свежая, замороженная, консервы",
    "морепрод": "креветки, кальмары, мидии",
    "лосос": "лосось, сёмга, форель",
    "сёмг": "сёмга, лосось, форель",
    "креветк": "креветки, кальмары",
    # ── ЕДА: овощи и фрукты ──
    "овощ": "помидоры, огурцы, перец, зелень",
    "фрукт": "яблоки, бананы, апельсины",
    "помидор": "помидоры, огурцы, зелень",
    "огурц": "огурцы, помидоры, зелень",
    "картоф": "картофель, морковь, лук",
    "картошк": "картофель, морковь, лук",
    "зелен": "укроп, петрушка, салат",
    "банан": "бананы, яблоки, апельсины",
    "яблок": "яблоки, груши, бананы",
    "цитрус": "апельсины, лимоны, мандарины",
    "ягод": "клубника, черника, малина",
    "грибы": "шампиньоны, вёшенки",
    "гриб": "шампиньоны, вёшенки",
    # ── ЕДА: хлеб и выпечка ──
    "хлеб": "хлеб, батон, лаваш",
    "выпечк": "булочки, круассаны, пирожки",
    "торт": "торты, пирожные, рулеты",
    "печень": "печенье, вафли, пряники",
    # ── ЕДА: бакалея и крупы ──
    "круп": "гречка, рис, овсянка",
    "гречк": "гречка, рис, пшено",
    "рисов": "рис, гречка, булгур",
    "макарон": "спагетти, пенне, лапша",
    "мук": "мука пшеничная, блинная",
    "сахар": "сахар, сахарная пудра",
    "крупа": "гречка, рис, овсянка, пшено",
    # ── ЕДА: сладости и снеки ──
    "конфет": "конфеты в коробках, шоколад",
    "шокол": "шоколад, конфеты в коробках",
    "мороженое": "мороженое, пломбир, эскимо",
    "морожен": "мороженое, пломбир, эскимо",
    "чипс": "чипсы, снеки, сухарики",
    "снек": "чипсы, снеки, орешки",
    "орех": "орехи, сухофрукты, семечки",
    # ── ЕДА: напитки ──
    "кофе": "кофе молотый, в зёрнах, растворимый",
    "чай": "чай чёрный, зелёный, травяной",
    "сок": "соки, нектары, морсы",
    "вода": "вода, газировка, лимонад",
    "газиров": "газировка, лимонад, кола",
    "пиво": "пиво светлое, тёмное, крафтовое",
    "вин": "вино красное, белое, игристое",
    # ── ЕДА: консервы и соусы ──
    "консерв": "тушёнка, горошек, кукуруза",
    "соус": "кетчуп, майонез, горчица",
    "кетчуп": "кетчуп, соусы, майонез",
    "майонез": "майонез, кетчуп, соусы",
    # ── ЕДА: готовая еда, заморозка ──
    "заморо": "пельмени, овощи, пицца, ягоды",
    "пицц": "пицца замороженная, готовая",
    "салат": "салаты готовые, нарезки",
    "готов": "готовые блюда, салаты, нарезки",
    # ── ЕДА: детское, специальное ──
    "детск": "пюре, каши, смеси, соки детские",
    "подгузн": "подгузники, салфетки, кремы",
    # ── Корм для животных ──
    "корм": "корм для кошек и собак",
    "кошач": "корм для кошек, наполнитель",
    "собач": "корм для собак, лакомства",
}


_RECIPE_INGREDIENTS = {
    # phrase для detect → {dish, ingredients, emoji}
    # Самые специфичные многословные фразы — иначе «греческий йогурт» матчит «греческ»
    "греческий салат": {"dish": "греческий салат", "emoji": "🍅",
                        "ingredients": "помидоры черри, фету, огурцы, красный лук, маслины, оливковое масло"},
    "клубник":         {"dish": "клубнику с йогуртом", "emoji": "🍓",
                        "ingredients": "клубнику, черешню, греческий йогурт"},
    "черешн":          {"dish": "черешню с йогуртом", "emoji": "🍒",
                        "ingredients": "черешню, клубнику, греческий йогурт"},
    "окрошк":          {"dish": "окрошку", "emoji": "🥒🥔",
                        "ingredients": "квас, редис, огурцы, картофель, варёную колбасу, яйца, сметану, зелень"},
    "ябло":            {"dish": "яблоки", "emoji": "🍎",
                        "ingredients": "Гала, Голден, Фуджи, Гренни Смит и другие сочные сорта"},
    "оливье":          {"dish": "оливье", "emoji": "🥗",
                        "ingredients": "колбасу, картофель, морковь, яйца, огурцы, горошек, лук, майонез"},
    "крабов":          {"dish": "крабовый салат", "emoji": "🦀",
                        "ingredients": "крабовые палочки, рис, кукурузу, яйца, огурцы, майонез"},
    "шуба":            {"dish": "сельдь под шубой", "emoji": "🐟",
                        "ingredients": "сельдь, картофель, морковь, свёклу, лук, яйца, майонез"},
    "сморреброд":      {"dish": "сморреброды", "emoji": "🥪",
                        "ingredients": "ржаной хлеб, лосось, креветки, сыр, авокадо, огурец, масло, укроп"},
    "смузи":           {"dish": "смузи", "emoji": "🥤",
                        "ingredients": "бананы, клубнику, малину, йогурт, мёд, киви, мяту"},
    "мохито":          {"dish": "мохито", "emoji": "🍹",
                        "ingredients": "мяту, лайм, тростниковый сахар, содовую, лёд, ром"},
}


def _detect_recipe(name: str, category: str, coupon_text: str) -> dict | None:
    """Если в названии/категории/купоне есть рецептное блюдо — вернуть {dish, ingredients}.

    Используется для:
      • _build_product_details — подставить ингредиенты как «details» в push;
      • generate_builtin — подставить название блюда как «dish_name» в заголовок.
    """
    haystack = (name + " " + category + " " + coupon_text).lower()
    for phrase, rec in _RECIPE_INGREDIENTS.items():
        if phrase in haystack:
            return rec
    return None


def _recipe_ingredients_text(name: str, category: str, coupon_text: str) -> str:
    """Совместимость: только список ингредиентов (без названия блюда)."""
    rec = _detect_recipe(name, category, coupon_text)
    return rec["ingredients"] if rec else ""


def _build_product_details(category: str, products_text: str, coupon_text: str) -> str:
    """Собрать расшифровку товаров из ВСЕХ совпавших категорий + купона.

    Для акции «бытовая химия и средства для ухода за обувью» найдёт и химию, и обувь.
    Для рецептных акций (окрошка, греческий салат, клубника-черешня с йогуртом и т.п.) —
    возвращает список ингредиентов блюда, чтобы клиент видел в push, на что скидка.
    Дедуплицирует отдельные товары.
    """
    # 1) Сначала проверяем — это рецептная акция?
    recipe = _recipe_ingredients_text("", category, coupon_text)
    if recipe:
        return recipe

    text = (category + " " + products_text + " " + coupon_text).lower()
    found_parts = []
    seen_keywords = set()

    for keyword, details in _PRODUCT_DETAILS.items():
        if keyword in text and keyword not in seen_keywords:
            found_parts.append(details)
            seen_keywords.add(keyword)
            # Пропускаем дублирующие синонимы
            if keyword in ("хими", "бытов"):
                seen_keywords.update(("хими", "бытов"))
            if keyword in ("колготк", "носк", "чулк"):
                seen_keywords.update(("колготк", "носк", "чулк"))
            if keyword in ("курица", "куриц", "птиц"):
                seen_keywords.update(("курица", "куриц", "птиц"))
            if keyword in ("молоко", "молочн", "кефир", "сметан"):
                seen_keywords.update(("молоко", "молочн", "кефир", "сметан"))
            if keyword in ("конфет", "шокол"):
                seen_keywords.update(("конфет", "шокол"))
            if keyword in ("мясо", "говядин", "свинин", "фарш"):
                seen_keywords.update(("мясо", "говядин", "свинин", "фарш"))
            if keyword in ("овощ", "помидор", "огурц"):
                seen_keywords.update(("овощ", "помидор", "огурц"))
            if keyword in ("фрукт", "банан", "яблок", "цитрус"):
                seen_keywords.update(("фрукт", "банан", "яблок", "цитрус"))
            if keyword in ("круп", "гречк", "крупа"):
                seen_keywords.update(("круп", "гречк", "крупа"))
            if keyword in ("картоф", "картошк"):
                seen_keywords.update(("картоф", "картошк"))
            if keyword in ("сосиск", "колбас", "ветчин"):
                seen_keywords.update(("сосиск", "колбас", "ветчин"))
            if keyword in ("рыб", "лосос", "сёмг"):
                seen_keywords.update(("рыб", "лосос", "сёмг"))
            if keyword in ("чипс", "снек"):
                seen_keywords.update(("чипс", "снек"))
            if keyword in ("соус", "кетчуп", "майонез"):
                seen_keywords.update(("соус", "кетчуп", "майонез"))
            if keyword in ("творог", "йогурт"):
                seen_keywords.update(("творог", "йогурт"))
            if keyword in ("корм", "кошач", "собач"):
                seen_keywords.update(("корм", "кошач", "собач"))
            if keyword in ("морожен", "мороженое"):
                seen_keywords.update(("морожен", "мороженое"))
            if keyword in ("пельмен", "полуфабрикат"):
                seen_keywords.update(("пельмен", "полуфабрикат"))

    if not found_parts:
        return ""

    # Разбиваем все части на отдельные товары и дедуплицируем
    all_items = []
    seen_items = set()
    for part in found_parts:
        for item in part.split(","):
            item = item.strip()
            # Ключ для дедупликации — первые 10 символов
            key = item.lower()[:10]
            if key not in seen_items and len(item) > 2:
                seen_items.add(key)
                all_items.append(item)

    combined = ", ".join(all_items)
    # Ограничиваем длину до 70 символов
    if len(combined) > 70:
        combined = combined[:70].rsplit(",", 1)[0]
    return combined

# Эмодзи для категорий (ставится в начало заголовка)
_CATEGORY_EMOJI = {
    # ══ Одежда / аксессуары ══
    "колготк": "🧦", "носк": "🧦", "чулк": "🧦",
    "одежд": "👗", "белье": "👙", "бельё": "👙",
    "перчатк": "🧤", "шапк": "🧣", "шарф": "🧣",
    # ══ Бытовая химия / гигиена ══
    "хими": "🧼", "бытов": "🧼", "стир": "🧼", "чист": "✨",
    "обув": "👟", "мыл": "🧴", "шампун": "💇", "гигиен": "🧴", "зуб": "🪥",
    "порош": "🧼", "средств": "🧹", "губк": "🧽",
    # ══ Мясо и птица ══
    "мясо": "🥩", "говядин": "🥩", "свинин": "🥩", "фарш": "🥩",
    "баранин": "🥩", "телятин": "🥩", "рёбрышк": "🥩",
    "курица": "🍗", "куриц": "🍗", "птиц": "🍗", "индейк": "🍗",
    "филе": "🍗", "окорочк": "🍗", "крылышк": "🍗",
    "шашлык": "🔥", "полуфабрикат": "🥟", "пельмен": "🥟",
    "варenik": "🥟", "хинкал": "🥟", "манты": "🥟",
    "сосиск": "🌭", "колбас": "🌭", "ветчин": "🥩",
    "бекон": "🥓", "копчён": "🥩", "копчен": "🥩",
    # ══ Молочное ══
    "молоко": "🥛", "молочн": "🥛", "кефир": "🥛", "сметан": "🥛",
    "творог": "🧀", "йогурт": "🧀", "масло": "🧈", "сыр": "🧀",
    "ряженк": "🥛", "простокваш": "🥛", "сливк": "🥛",
    "яйц": "🍳", "яйцо": "🥚",
    # ══ Рыба и морепродукты ══
    "рыб": "🐟", "морепрод": "🦐", "лосос": "🐟", "сёмг": "🐟",
    "креветк": "🦐", "кальмар": "🦑", "краб": "🦀",
    "форел": "🐟", "скумбри": "🐟", "минтай": "🐟",
    "селёдк": "🐟", "селедк": "🐟", "шпрот": "🐟",
    "икр": "🐟", "устриц": "🦪", "мидии": "🦐",
    # ══ Фрукты ══
    "фрукт": "🍎", "банан": "🍌", "яблок": "🍎", "цитрус": "🍊",
    "апельсин": "🍊", "мандарин": "🍊", "лимон": "🍋",
    "груш": "🍐", "виноград": "🍇", "персик": "🍑",
    "арбуз": "🍉", "дын": "🍈", "ананас": "🍍",
    "манго": "🥭", "кокос": "🥥", "киви": "🥝",
    "вишн": "🍒", "черешн": "🍒", "слив": "🍑",
    "гранат": "🍎", "хурм": "🍑", "авокадо": "🥑",
    # ══ Ягоды ══
    "ягод": "🍓", "клубник": "🍓", "малин": "🍓",
    "черник": "🫐", "голубик": "🫐", "смородин": "🍇",
    "ежевик": "🫐", "крыжовник": "🍇",
    # ══ Овощи ══
    "овощ": "🥒", "помидор": "🍅", "томат": "🍅",
    "огурц": "🥒", "картоф": "🥔", "картошк": "🥔",
    "зелен": "🥬", "грибы": "🍄", "гриб": "🍄",
    "морков": "🥕", "капуст": "🥬", "брокколи": "🥦",
    "перец": "🌶️", "лук": "🧅", "чеснок": "🧄",
    "кукуруз": "🌽", "свёкл": "🥕", "свекл": "🥕",
    "баклажан": "🍆", "кабачк": "🥒", "тыкв": "🎃",
    "редис": "🥕", "горох": "🫛", "фасол": "🫘",
    # ══ Хлеб и выпечка ══
    "хлеб": "🍞", "батон": "🥖", "багет": "🥖",
    "выпечк": "🥐", "круассан": "🥐", "булочк": "🥐",
    "торт": "🎂", "печень": "🍪", "пирожн": "🧁",
    "пирог": "🥧", "пирожк": "🥐", "кекс": "🧁",
    "блин": "🥞", "оладь": "🥞", "вафл": "🧇",
    "лаваш": "🫓", "лепёшк": "🫓",
    # ══ Бакалея и крупы ══
    "круп": "🌾", "гречк": "🌾", "рисов": "🍚", "макарон": "🍝",
    "мук": "🌾", "крупа": "🌾", "овсян": "🌾",
    "паста": "🍝", "спагетти": "🍝", "лапш": "🍝",
    "каш": "🥣",
    # ══ Сладости и снеки ══
    "конфет": "🍬", "шокол": "🍫", "морожен": "🍦", "мороженое": "🍦",
    "пломбир": "🍦", "эскимо": "🍦",
    "чипс": "🍿", "снек": "🍿", "орех": "🥜",
    "сухарик": "🍿", "крекер": "🍪",
    "мёд": "🍯", "мед": "🍯", "варень": "🍯", "джем": "🍯",
    "зефир": "🍬", "мармелад": "🍬", "пастил": "🍬",
    "халв": "🍬", "козинак": "🍬",
    "жвачк": "🍬", "леденц": "🍭",
    # ══ Напитки ══
    "кофе": "☕", "чай": "🍵", "сок": "🧃", "вода": "💧",
    "газиров": "🥤", "лимонад": "🥤", "морс": "🧃",
    "компот": "🧃", "квас": "🥤", "какао": "☕",
    "пиво": "🍺", "вин": "🍷", "шампанск": "🍾",
    "игрист": "🍾", "коньяк": "🥃", "виски": "🥃",
    "водк": "🥃", "настойк": "🥃", "ликёр": "🍸",
    "коктейл": "🍹", "смузи": "🥤",
    "энергетик": "⚡",
    # ══ Консервы и соусы ══
    "консерв": "🥫", "соус": "🫙", "кетчуп": "🫙", "майонез": "🫙",
    "горчиц": "🫙", "уксус": "🫙", "масл": "🫒",
    # ══ Готовая еда, заморозка ══
    "заморо": "❄️", "пицц": "🍕", "салат": "🥗", "готов": "🍱",
    "суп": "🥣", "бульон": "🥣",
    "бургер": "🍔", "сэндвич": "🥪",
    "суши": "🍣", "ролл": "🍣",
    # ══ Детское ══
    "детск": "👶", "подгузн": "👶", "салфет": "🧻",
    "пюре детск": "👶", "смес": "🍼",
    # ══ Корм для животных ══
    "корм": "🐱", "кошач": "🐱", "собач": "🐶",
    "наполнител": "🐱",
    # ══ Дом и сад ══
    "цвет": "💐", "букет": "💐", "рассад": "🌱",
    "свеч": "🕯️", "посуд": "🍽️",
    # ══ Сезонное / праздники ══
    "пасх": "🥚", "кулич": "🥚", "ёлк": "🎄", "елк": "🎄",
    "новогод": "🎄", "подарок": "🎁", "подар": "🎁",
    # ══ Здоровье / ЗОЖ ══
    "витамин": "💊", "бад": "💊", "протеин": "💪",
}


def _get_category_emoji(category: str, products_text: str, benefit_type: str = "") -> str:
    """Подобрать эмодзи категории.

    Если категория не определена:
      - для бонусов/монет → 💰
      - для кешбэка → 💰
      - иначе → 🎯
    """
    text = (category + " " + products_text).lower()
    for keyword, emoji in _CATEGORY_EMOJI.items():
        if keyword in text:
            return emoji
    # Дефолтный эмодзи зависит от типа выгоды
    if benefit_type in ("bonus",):
        return random.choice(["💰", "💎", "🫰"])
    elif benefit_type in ("cashback",):
        return "💰"
    elif benefit_type in ("discount",):
        return random.choice(["💰", "💎", "🫰", "％", "👛", "🛒"])
    return "🎯"


def _get_product_details(category: str, products_text: str) -> str:
    """Получить расшифровку конкретных товаров из категории."""
    text = (category + " " + products_text).lower()
    for keyword, details in _PRODUCT_DETAILS.items():
        if keyword in text:
            return details
    return ""


def _truncate(text: str, max_len: int) -> str:
    """Обрезать текст до max_len аккуратно по слову."""
    if len(text) <= max_len:
        return text
    cut = text[:max_len - 1]
    last_space = cut.rfind(" ")
    if last_space > max_len * 0.5:
        cut = cut[:last_space]
    return cut.rstrip(".,!?;: ") + "…"


# ═══════════════════════════════════════════════════════════════════════════════
# Встроенный генератор (на основе реальных push ДИКСИ)
# ═══════════════════════════════════════════════════════════════════════════════

# ── ПРИНЦИПЫ (из обратной связи):
# 1. Заголовок ПЕРЕТЕКАЕТ в текст (без ! на конце заголовка)
# 2. Выгода — ТОЛЬКО в заголовке, в тексте НЕ дублируем
# 3. Текст начинается с расшифровки товаров (видно в превью ~24 символа)
# 4. ОДИН CTA: если нужна активация → АКТИВИРУЙ; иначе → забегай/закажи
# 5. Онлайн — упоминаем в контексте покупки, а не как отдельный CTA
# 6. НЕ ВРАТЬ клиенту (20% ≠ "половина", точные цифры)
# 7. Юмор — тонкий, органичный, не навязанный вопрос

# ── ПАРЫ заголовок + текст (грамматически согласованные!) ──────
# Заголовок перетекает в текст. Каждая пара проверена на согласование.
# {body_details} = расшифровка товаров, уже включает "за покупку" / "на" и т.д.

# СТАРТ: кешбэк
# Заголовок — креативный, с выгодой. Body — товары в именительном падеже (без «на/за»).
_PAIRS_START_CASHBACK = [
    ("{emoji}Кешбэк {value} — твой",
     "{details} {date_context}. {condition}Вернём монетами.{promo_code} {purchase_ctx}{cta}"),

    ("{emoji}Щедрый кешбэк {value}",
     "{details} и не только {date_context}. {condition}{promo_code} {purchase_ctx}{cta}"),

    ("{emoji}Вернём {value} монетами",
     "{details} {date_context}. {condition}{promo_code} {purchase_ctx}{cta}"),

    ("{emoji}{value} обратно монетами",
     "{details} и другое {date_context}. {condition}{promo_code} {purchase_ctx}{cta}"),

    ("{emoji}Особенный кешбэк {value}",
     "{details} {date_context}. {condition}{promo_code} {purchase_ctx}{cta}"),
]

# СТАРТ: скидка
_PAIRS_START_DISCOUNT = [
    ("{emoji}Скидка {value} — лови",
     "{details} {date_context}. {condition}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}-{value} {date_context}",
     "{details} и другое со скидкой. {condition}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}Цены тают — {value}",
     "{details} {date_context}. {condition}{promo_code} {purchase_ctx}{cta}"),
]

# СТАРТ: монеты (ДИКСИ: «монеты», не «бонусы»!)
# Монеты начисляются ПОСЛЕ покупки — «вернём», «держи», «верни часть покупки».
# Образец (101048): Заголовок: "Вернём 100р. монетами 😉"
#   Body: "За чек на 1500₽ - держи 100 монет! Только три дня, до 12.10. Забегай сегодня — верни часть покупки уже сейчас."
_PAIRS_START_BONUS = [
    # Условие чека — В ЗАГОЛОВКЕ (как в реальных push ДИКСИ)
    # Body — товары + юмор + промокод + CTA
    # {condition_title} = «с чека от 1500₽ » или пусто
    ("{emoji}Верни {value} с каждого чека",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}Вернём {value} монетами",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}Возвращаем {value} монетами",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}Получи {value} монетами",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}{value} вернём монетами",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
]

# Бонусные пары С УСЛОВИЕМ ЧЕКА (когда есть condition_check)
_PAIRS_START_BONUS_WITH_CHECK = [
    ("{emoji}Верни {value} с чека от {check_amount}",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}Вернём {value} с чека от {check_amount}",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}{value} с каждого чека от {check_amount}",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}Покупай от {check_amount} — верни {value}",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
    ("{emoji}Чек от {check_amount} = +{value} монет",
     "{details_bonus}{date_context}.{humor}{promo_code} {purchase_ctx}{cta}"),
]

# СТАРТ: общий
_PAIRS_START_GENERAL = [
    ("{emoji}Выгода {date_context}",
     "{details} — не пропусти! {purchase_ctx}{cta}"),
    ("{emoji}Акция {date_context}",
     "{details} и другое с выгодой. {purchase_ctx}{cta}"),
    ("{emoji}Время выгоды {date_context}",
     "{details} — лови момент! {purchase_ctx}{cta}"),
]

# НАПОМИНАНИЕ: кешбэк
_PAIRS_REMIND_CASHBACK = [
    ("⏳Кешбэк {value} — не упусти",
     "{details} — ещё начислим {value} монетами. Акция {date_context}. {condition}{cta}"),
    ("⏰Кешбэк {value} заканчивается",
     "{details} — ещё можно успеть. {condition}Акция {date_context}. {cta}"),
    ("🏃Кешбэк {value} — успей забрать",
     "{details} {date_context}. {condition}Последний шанс забрать монеты. {cta}"),
    ("⚡Последний день: кешбэк {value}",
     "{details} {date_context}. {condition}Не упусти! {cta}"),
]

# НАПОМИНАНИЕ: скидка
_PAIRS_REMIND_DISCOUNT = [
    ("⏳-{value} скоро закончится",
     "{details} со скидкой {date_context}. {condition}Успей! {cta}"),
    ("🏃Скидка {value} — успей",
     "{details} {date_context}. {condition}Ещё можно купить с выгодой. {cta}"),
    ("⚡Последний шанс -{value}",
     "{details} {date_context}. {condition}Не упусти! {cta}"),
]

# НАПОМИНАНИЕ: монеты
_PAIRS_REMIND_BONUS = [
    ("⏳{value} монетами — заканчивается",
     "{condition_check}{details_bonus}{date_context}. Успей забрать монеты! {cta}"),
    ("🏃Успей забрать {value} монетами",
     "{condition_check}{details_bonus}{date_context}. Последний шанс! {cta}"),
    ("⚡Последний день: +{value} монетами",
     "{condition_check}{details_bonus}{date_context}. Монеты ждут! {cta}"),
    ("🔔Напоминаем: {value} монетами",
     "{condition_check}{details_bonus}{date_context}. Не забудь! {cta}"),
]

# НАПОМИНАНИЕ: общий
_PAIRS_REMIND_GENERAL = [
    ("⏰Успей {date_context}",
     "{details} с выгодой — осталось мало времени. {condition}{cta}"),
    ("⚡Последний шанс",
     "{details} — акция {date_context}. {condition}Не упусти! {cta}"),
]

# ── Юмор — конкретный, ситуационный, по-человечески ────

_HUMOR = {
    # ── Бытовая химия ──
    "хими": [
        " Весенняя уборка с кешбэком — красота 🧹",
        " Плита и ванная скажут спасибо ✨",
        " Готовимся к чистому четвергу красиво ✨",
        " После зимы дом точно заслужил генуборку 😄",
        " Чистота — в доме! Монеты — на карте! ✨",
        " И блеск, и свежесть — а карман не пуст 😏",
    ],
    "бытов": [
        " Весна — самое время навести блеск 🧹",
        " Дома засияет после зимы ✨",
        " Готовимся к чистому четвергу красиво ✨",
        " Чистый дом — душе бальзам ✨",
    ],
    "обув": [
        " Весна, лужи, реагенты — обувь кричит о помощи 👟",
        " После зимы кроссовки выглядят как ветераны 😅",
        " Сезон луж открыт — вооружайся 👟",
        " Зима ушла, а соль на ботинках осталась 😬",
        " Белые кроссовки после марта? Спасём 😎",
    ],
    # ── Одежда / аксессуары ──
    "колготк": [
        " Весна — пора обновить гардероб 🌸",
        " Колготки заканчиваются в самый неподходящий момент 😅",
        " Весенний марафон по лужам? Запасись 🧦",
    ],
    "носк": [
        " Носков много не бывает — проверено 🧦",
        " Тот случай, когда запас точно не помешает 😄",
        " Куда пропадают носки из стиралки? Загадка века 🧦",
    ],
    "чулк": [
        " Весна — время лёгких нарядов 🌸",
        " Обновляем гардероб к весне ✨",
    ],
    # ── Мясо и птица ──
    "мясо": [
        " Шашлычный сезон объявляется открытым 🥩",
        " Мангал ждёт, мясо ждёт, ты чего ждёшь? 😋",
        " Холодильник без мяса — грустный холодильник 😄",
        " Мясо есть — и жизнь прекрасна, мяса нет — напрасно 🥩",
    ],
    "курица": [
        " Курочка не подведёт — проверено поколениями 🍗",
        " Наггетсы + сериал = идеальный вечер 🎬",
        " Курица — универсальный ответ на «что на ужин?» 🍗",
    ],
    "куриц": [
        " Курочка спасает каждый ужин 🍗",
        " Что приготовить? Курица знает ответ 😄",
    ],
    "птиц": [
        " Птичку жалко? А если со скидкой? 😄",
        " Индейка, курица — выбирай и экономь 🍗",
    ],
    "индейк": [
        " Индейка — ЗОЖ-одобрено и вкусно 💪",
        " Диетическое мясо по недиетической скидке 😎",
    ],
    "свинин": [
        " Шашлычный сезон на носу — запасаемся 🔥",
        " Рёбрышки на гриле — мечта, а не ужин 😋",
    ],
    "говядин": [
        " Стейк-вечер? Отличная идея 🥩",
        " Бургер дома вкуснее и дешевле, проверено 😋",
    ],
    "шашлык": [
        " Погода шепчет: «мангааал» 🔥",
        " Шашлычный сезон не ждёт — и ты не жди 😋",
    ],
    "колбас": [
        " Бутерброд без колбасы — просто хлеб 😄",
        " Классика перекуса — всегда в деле 🌭",
    ],
    "сосиск": [
        " Сосиска — спасение, когда лень готовить 😄",
        " 5 минут — и ужин готов. Магия? Нет, сосиски 🌭",
    ],
    "пельмен": [
        " Пельмени — еда настоящих стратегов 🥟",
        " Лень готовить? Пельмени всё понимают 😄",
        " Холостяцкий ужин? Мы не осуждаем 😅",
    ],
    "полуфабрикат": [
        " Когда лень — полуфабрикаты спасают мир 🥟",
        " Быстро, вкусно и без лишних вопросов 😄",
    ],
    # ── Молочное ──
    "молоко": [
        " Молоко — классика, которая не устаревает 🥛",
        " Утро начинается с кефира. Или со сметаны 😄",
    ],
    "молочн": [
        " Холодильник пустой — это не про тебя 🥛",
        " Утро без творожка — утро потеряно 😄",
        " Йогурт на завтрак — маленькое счастье 😋",
    ],
    "кефир": [
        " Кефир на ночь — ЗОЖ-привычка со скидкой 🥛",
        " Бабушка одобряет: кефир — это классика 😊",
    ],
    "творог": [
        " Творожок — завтрак чемпионов 💪",
        " Утро без творога — утро без смысла 😄",
    ],
    "йогурт": [
        " Йогурт — маленькая радость в большом дне 😋",
        " Перекус, который всегда кстати 🧀",
    ],
    "сыр": [
        " Сыра много не бывает — это закон 🧀",
        " Что делает бутерброд идеальным? Правильно, сыр 😋",
        " Без сыра пицца — просто лепёшка 🧀",
    ],
    "масло": [
        " Хлеб с маслом — проще рецепта не найти 🧈",
    ],
    "яйц": [
        " Яичница — блюдо, которое никогда не предаст 🍳",
        " Утро бывает добрым, когда есть яйца на завтрак 😄",
    ],
    # ── Рыба и морепродукты ──
    "рыб": [
        " Пятница — рыбный день, а скидка — каждый 🐟",
        " Уха по-домашнему — а вкуснее и не бывает 😋",
        " Омега-3 со скидкой — мозг скажет спасибо 🧠",
    ],
    "морепрод": [
        " Креветки к пятничному вечеру — идеально 🦐",
        " Морепродукты — когда хочется чего-то особенного 😋",
    ],
    "лосос": [
        " Красная рыба — праздник без повода 🐟",
        " Сёмга + авокадо = инста-завтрак 😎",
    ],
    # ── Овощи и фрукты ──
    "овощ": [
        " Витамины по скидке — весна одобряет 🥒",
        " Салат сам себя не приготовит. Или?.. 😄",
        " Весна — время салатов и витаминов 🥗",
    ],
    "фрукт": [
        " Фрукты — лучший перекус (мама была права) 🍎",
        " Витаминный заряд по выгодной цене 😋",
        " Весна — время свежих фруктов 🍎",
    ],
    "помидор": [
        " Салат без помидора? Даже не начинай 🍅",
    ],
    "картоф": [
        " Картошка — королева гарниров 👑",
        " Жареная, варёная, пюре — картошка всегда к месту 🥔",
    ],
    "зелен": [
        " Весна пахнет укропом и петрушкой 🌿",
        " Свежая зелень — и любое блюдо оживает 🥬",
    ],
    "ягод": [
        " Ягоды — маленькие витаминные бомбочки 🍓",
        " Клубника — вкус, который ждали всю зиму 😋",
    ],
    "гриб": [
        " Грибы — когда хочется чего-то уютного 🍄",
        " Грибной суп в дождливый день — это терапия 😊",
    ],
    # ── Хлеб и выпечка ──
    "хлеб": [
        " Хлеб всему голова — особенно свежий 🍞",
        " Запах свежего хлеба — лучший будильник 😋",
    ],
    "выпечк": [
        " Круассан утром — день удался 🥐",
        " Выпечка к чаю — маленький праздник каждый день ☕",
    ],
    "торт": [
        " Торт без повода — лучший повод 🎂",
        " Для тортика повод не нужен, проверено 😋",
    ],
    "печень": [
        " Печенье к чаю — традиция, которая не стареет 🍪",
        " Одну печеньку? Ладно, пачку 😅",
    ],
    # ── Бакалея и крупы ──
    "круп": [
        " Гречка — наш стратегический запас 🌾",
        " Каша утром — сила весь день 💪",
    ],
    "макарон": [
        " Макароны — еда, которая всех объединяет 🍝",
        " Паста-вечер? Всегда хорошая идея 😋",
    ],
    # ── Сладости ──
    "конфет": [
        " Сладкая жизнь — это когда ещё и с кешбэком 🍬",
        " К чаю — идеально, к кешбэку — вдвойне 😋",
        " Конфеты — лекарство от плохого настроения 🍬",
    ],
    "шокол": [
        " Шоколад лечит всё. Научно доказано 🍫",
        " Настроение на максимум, кошелёк не страдает 😋",
        " Долька? Нет, целую плитку 😄",
        " Плитку — в руку! Настроение — в гору! 🍫",
    ],
    "морожен": [
        " Мороженое — счастье в вафельном стаканчике 🍦",
        " Пломбир как в детстве, только со скидкой 😋",
    ],
    "чипс": [
        " Чипсы + фильм = идеальный вечер 🎬",
        " Одну чипсинку? Кого мы обманываем 😅",
    ],
    "орех": [
        " Полезный перекус — и вкусно, и совесть чиста 🥜",
    ],
    # ── Напитки ──
    "кофе": [
        " Утро без кофе — не утро, а недоразумение ☕",
        " Кофеман оценит, кошелёк тоже ☕",
        " Кофе — топливо для великих дел ☕",
        " Пей кофе! Твори дела! День не ждёт! ☕",
        " И утро доброе, когда с арабикой, а не с будильником 😏",
    ],
    "чай": [
        " Чайку? Всегда хорошая идея 🍵",
        " Вечер, плед и чай — рецепт уюта 🍵",
    ],
    "сок": [
        " Утро начинается с витаминов 🧃",
        " Стакан сока — заряд бодрости на день 😊",
    ],
    "пиво": [
        " Пятница + пиво = классика 🍺",
        " К футболу и не только 😎",
    ],
    "вин": [
        " Бокал вина — маленький праздник 🍷",
        " Вечер заслуживает чего-то особенного 🍷",
    ],
    # ── Консервы и соусы ──
    "консерв": [
        " Стратегический запас на все случаи жизни 🥫",
    ],
    "соус": [
        " Хороший соус — и любое блюдо звучит иначе 😋",
    ],
    # ── Заморозка и готовое ──
    "заморо": [
        " Морозилка полная — душа спокойна ❄️",
        " Запас в морозилке — это как деньги на карте 😄",
    ],
    "пицц": [
        " Пицца — ответ на любой вопрос 🍕",
        " Вечер пиццы? Всегда уместно 😋",
    ],
    "салат": [
        " Салат — когда хочется быть молодцом 🥗",
    ],
    # ── Детское ──
    "детск": [
        " Для маленьких гурманов — самое лучшее 👶",
        " Малышу — вкусно, маме — выгодно 😊",
    ],
    # ── Корм для животных ──
    "корм": [
        " Котик (или пёсик) будет доволен 🐱",
        " Миска пустая — это не вариант 🐶",
    ],
    # ── Общее / все товары (когда нет конкретной категории) ──
    "__general__": [
        " Набирай полную корзину — всё считается 🛒",
        " Закупка недели? Самое время 🔥",
        " Повод пополнить запасы 😄",
        " Корзину — полную! Кошелёк — целый! 🔥",
        " Налетай! Выгода не ждёт! 💥",
        " Выгода ждёт — не откладывай 😉",
    ],
}

# ── СИТУАЦИОННЫЕ ПАРЫ — для категорий с яркой историей ────
# Подмешиваются в пул когда категория совпадает

_SITUATIONAL_PAIRS = {
    # ── Бытовая химия / обувь / одежда ──
    "обув": [
        ("👟{value} за чистую обувь",
         "Весна, лужи, реагенты — обувь кричит о помощи. Средства для обуви и другая бытовая химия {date_context}. {purchase_ctx}{cta}"),
        ("👟{value} кешбэк на обувь",
         "После зимы кроссовки выглядят как ветераны 😅 Средства для обуви + бытовая химия {date_context}. {purchase_ctx}{cta}"),
        ("{emoji}{value} — обувь скажет спасибо",
         "Сезон луж открыт — вооружайся! Средства для обуви, мытья посуды, уборки {date_context}. {purchase_ctx}{cta}"),
        ("👟Весна + кешбэк {value}",
         "Зима ушла, а соль на ботинках осталась 😬 Средства для обуви и бытовая химия {date_context}. {purchase_ctx}{cta}"),
        ("{emoji}Обувь как новая + {value}",
         "Белые кроссовки после марта? Спасём 😎 Средства для обуви, уборки и другое {date_context}. {purchase_ctx}{cta}"),
    ],
    "колготк": [
        ("🧦{value} на колготки и носки",
         "Колготки заканчиваются в самый неподходящий момент 😅 Пополни запасы {date_context}. {purchase_ctx}{cta}"),
        ("🧦Весна + скидка {value}",
         "обновляем гардероб! Колготки, чулки и носки {date_context} со скидкой. {purchase_ctx}{cta}"),
        ("🧦{value} — носки ждут",
         "Куда пропадают носки из стиралки? Загадка 🧦 Пополни запас {date_context} со скидкой. {purchase_ctx}{cta}"),
        ("🌸{value} к весне",
         "колготки, чулки и носки — обновляем к сезону {date_context}. Носков много не бывает 🧦 {purchase_ctx}{cta}"),
    ],
    "носк": [
        ("🧦{value} на носки и колготки",
         "Носков много не бывает — проверено 😄 Колготки, чулки тоже {date_context} со скидкой. {purchase_ctx}{cta}"),
        ("🧦Запасайся — {value} скидка",
         "носки, колготки и чулки {date_context}. Куда они пропадают из стиралки — загадка века 🧦 {purchase_ctx}{cta}"),
    ],
    # ── Мясо и птица ──
    "мясо": [
        ("🥩{value} на мясо",
         "шашлычный сезон открыт! Говядина, свинина, фарш {date_context}. Мангал ждёт 😋 {purchase_ctx}{cta}"),
        ("🥩Мясо + {value}",
         "холодильник без мяса — грустный холодильник 😄 Говядина, свинина, фарш {date_context}. {purchase_ctx}{cta}"),
    ],
    "курица": [
        ("🍗{value} на курицу",
         "филе, окорочка, крылышки с кешбэком {date_context}. Наггетсы + сериал = вечер 🎬 {purchase_ctx}{cta}"),
        ("🍗Курица + {value}",
         "универсальный ответ на «что на ужин?» 🍗 Филе, окорочка, крылышки {date_context}. {purchase_ctx}{cta}"),
    ],
    "шашлык": [
        ("🔥{value} на шашлык",
         "погода шепчет: «мангааал» 🔥 Свинина, курица, маринады {date_context}. {purchase_ctx}{cta}"),
    ],
    "колбас": [
        ("🌭{value} на колбасу",
         "бутерброд без колбасы — просто хлеб 😄 Колбаса, сосиски, ветчина {date_context}. {purchase_ctx}{cta}"),
    ],
    "пельмен": [
        ("🥟{value} на пельмени",
         "лень готовить? Пельмени всё понимают 😄 Пельмени, вареники, хинкали {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Молочное ──
    "молочн": [
        ("{emoji}{value} на молочное",
         "утро без творожка — утро потеряно 😄 Творог, йогурты, сметана {date_context}. {purchase_ctx}{cta}"),
        ("{emoji}Молочка + {value}",
         "холодильник пустой — это не про тебя 🥛 Творог, кефир, йогурты {date_context}. {purchase_ctx}{cta}"),
    ],
    "сыр": [
        ("🧀{value} на сыр",
         "сыра много не бывает — это закон 🧀 Твёрдые, плавленые, моцарелла {date_context}. {purchase_ctx}{cta}"),
    ],
    "яйц": [
        ("🍳{value} на яйца",
         "яичница — блюдо, которое никогда не предаст 🍳 Куриные, перепелиные {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Рыба ──
    "рыб": [
        ("🐟{value} на рыбу",
         "пятница — рыбный день, а скидка — каждый 🐟 Свежая, замороженная {date_context}. {purchase_ctx}{cta}"),
    ],
    "морепрод": [
        ("🦐{value} на морепродукты",
         "креветки к пятничному вечеру — идеально 🦐 Креветки, кальмары, мидии {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Овощи и фрукты ──
    "овощ": [
        ("🥒{value} на овощи",
         "витамины по скидке — весна одобряет 🥒 Помидоры, огурцы, зелень {date_context}. {purchase_ctx}{cta}"),
        ("🥗Весна + {value} на овощи",
         "салат сам себя не приготовит 😄 Помидоры, огурцы, перец {date_context}. {purchase_ctx}{cta}"),
    ],
    "фрукт": [
        ("🍎{value} на фрукты",
         "мама была права — фрукты лучший перекус 🍎 Яблоки, бананы, апельсины {date_context}. {purchase_ctx}{cta}"),
        ("🍎Витамины + {value}",
         "весна — время свежих фруктов 😋 Яблоки, бананы, апельсины {date_context}. {purchase_ctx}{cta}"),
    ],
    "ягод": [
        ("🍓{value} на ягоды",
         "маленькие витаминные бомбочки 🍓 Клубника, черника, малина {date_context}. {purchase_ctx}{cta}"),
    ],
    "картоф": [
        ("🥔{value} на картофель",
         "королева гарниров — всегда к месту 🥔 Картофель, морковь, лук {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Хлеб и выпечка ──
    "хлеб": [
        ("🍞{value} на хлеб",
         "хлеб всему голова — особенно свежий 🍞 Хлеб, батон, лаваш {date_context}. {purchase_ctx}{cta}"),
    ],
    "выпечк": [
        ("🥐{value} на выпечку",
         "круассан утром — день удался 🥐 Булочки, круассаны, пирожки {date_context}. {purchase_ctx}{cta}"),
    ],
    "торт": [
        ("🎂{value} на торты",
         "для тортика повод не нужен 😋 Торты, пирожные, рулеты {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Бакалея ──
    "круп": [
        ("🌾{value} на крупы",
         "гречка — наш стратегический запас 🌾 Гречка, рис, овсянка {date_context}. {purchase_ctx}{cta}"),
    ],
    "макарон": [
        ("🍝{value} на макароны",
         "паста-вечер? Всегда хорошая идея 😋 Спагетти, пенне, лапша {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Сладости и снеки ──
    "конфет": [
        ("🍬{value} на сладкое",
         "конфеты в коробках и шоколад с кешбэком {date_context}. Сделай вечер сладким 😋 {purchase_ctx}{cta}"),
        ("🍬Сладкая жизнь + {value}",
         "конфеты — лекарство от плохого настроения 🍬 К чаю — идеально {date_context}. {purchase_ctx}{cta}"),
    ],
    "шокол": [
        ("🍫{value} на шоколад",
         "лучший антистресс теперь с кешбэком {date_context}. Научно доказано 😄 {purchase_ctx}{cta}"),
        ("🍫Шоколад + {value}",
         "долька? Нет, целую плитку 😄 Шоколад, конфеты {date_context}. {purchase_ctx}{cta}"),
    ],
    "морожен": [
        ("🍦{value} на мороженое",
         "счастье в вафельном стаканчике 🍦 Пломбир, эскимо {date_context}. {purchase_ctx}{cta}"),
    ],
    "чипс": [
        ("🍿{value} на снеки",
         "чипсы + фильм = идеальный вечер 🎬 Чипсы, снеки, сухарики {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Напитки ──
    "кофе": [
        ("☕{value} на кофе",
         "утро без кофе — не утро ☕ Молотый, в зёрнах, растворимый {date_context}. {purchase_ctx}{cta}"),
        ("☕Кофе + {value}",
         "топливо для великих дел теперь с выгодой ☕ Молотый, в зёрнах {date_context}. {purchase_ctx}{cta}"),
    ],
    "чай": [
        ("🍵{value} на чай",
         "вечер, плед и чай — рецепт уюта 🍵 Чёрный, зелёный, травяной {date_context}. {purchase_ctx}{cta}"),
    ],
    "сок": [
        ("🧃{value} на соки",
         "утро начинается с витаминов 🧃 Соки, нектары, морсы {date_context}. {purchase_ctx}{cta}"),
    ],
    "пиво": [
        ("🍺{value} на пиво",
         "пятница + пиво = классика 🍺 Светлое, тёмное, крафтовое {date_context}. {purchase_ctx}{cta}"),
    ],
    "вин": [
        ("🍷{value} на вино",
         "бокал вина — маленький праздник 🍷 Красное, белое, игристое {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Заморозка и готовое ──
    "заморо": [
        ("❄️{value} на заморозку",
         "морозилка полная — душа спокойна ❄️ Пельмени, овощи, ягоды {date_context}. {purchase_ctx}{cta}"),
    ],
    "пицц": [
        ("🍕{value} на пиццу",
         "пицца — ответ на любой вопрос 🍕 Замороженная, готовая {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Детское ──
    "детск": [
        ("👶{value} на детское",
         "малышу — вкусно, маме — выгодно 😊 Пюре, каши, смеси {date_context}. {purchase_ctx}{cta}"),
    ],
    # ── Корм для животных ──
    "корм": [
        ("🐱{value} на корм",
         "котик (или пёсик) будет доволен 🐱 Корм для кошек и собак {date_context}. {purchase_ctx}{cta}"),
    ],
}


def _get_humor(category: str, products_text: str) -> str:
    """Подобрать тонкий юмор — собираем ВСЕ совпавшие шутки и выбираем случайную.

    Если ни одна категория не совпала — берём общие шутки (__general__).
    """
    text = (category + " " + products_text).lower()
    all_jokes = []
    for keyword, jokes in _HUMOR.items():
        if keyword == "__general__":
            continue  # общие — только как fallback
        if keyword in text:
            all_jokes.extend(jokes)
    # Если ничего не нашли — используем общие шутки
    if not all_jokes:
        all_jokes = _HUMOR.get("__general__", [])
    return random.choice(all_jokes) if all_jokes else ""


# ═══════════════════════════════════════════════════════════════════════════════
# Сегменты клиентов — разный тон и CTA для разных аудиторий
# ═══════════════════════════════════════════════════════════════════════════════

# Сегменты: Активные, Новые, Спящие, Отток
# Каждый сегмент влияет на:
#   - тон обращения (приветствие/тёплые слова)
#   - CTA (призыв к действию)
#   - дополнительный контекст (соскучились, рады видеть и т.д.)

# ── Лайфстайл-сегменты (НЕ активные/отток — те игнорируем) ──
# Поле «Сегмент» может содержать лайфстайл-сегменты клиентов: семьи, ЗОЖ, сладкоежки и т.д.
# Именно они влияют на тон и подачу push.

_SEGMENT_CONFIG = {
    "семь": {
        "name": "Семья",
        "tone": "family",
        "cta_suffix": [
            "",
            " Семья оценит!",
            " Порадуй близких!",
        ],
        "greeting": [
            "",
            " Для всей семьи 👨👩👧👦",
            " Домашние скажут спасибо 😊",
        ],
    },
    "зож": {
        "name": "ЗОЖ",
        "tone": "healthy",
        "cta_suffix": [
            "",
            " Полезно и выгодно!",
        ],
        "greeting": [
            "",
            " Для тех, кто следит за собой 💪",
            " ЗОЖ-выбор со скидкой 🥗",
        ],
    },
    "сладкоежк": {
        "name": "Сладкоежка",
        "tone": "sweet",
        "cta_suffix": [
            "",
            " Сладкая жизнь ждёт!",
        ],
        "greeting": [
            "",
            " Для сладкоежек 🍬",
            " Побалуй себя 😋",
        ],
    },
    "эконом": {
        "name": "Экономный",
        "tone": "saving",
        "cta_suffix": [
            "",
            " Экономия — наше всё!",
            " Выгодно, проверено!",
        ],
        "greeting": [
            "",
            " Умная экономия 💰",
            " Для ценителей выгоды 😉",
        ],
    },
    "гурман": {
        "name": "Гурман",
        "tone": "gourmet",
        "cta_suffix": [
            "",
            " Для ценителей вкуса!",
        ],
        "greeting": [
            "",
            " Для настоящих гурманов 👨🍳",
            " Вкус, который оценишь 😋",
        ],
    },
    "мясоед": {
        "name": "Мясоед",
        "tone": "meat",
        "cta_suffix": [
            "",
            " Мясо ждёт!",
        ],
        "greeting": [
            "",
            " Для мясоедов 🥩",
            " Мясная выгода 🔥",
        ],
    },
    "молод": {
        "name": "Молодёжь",
        "tone": "young",
        "cta_suffix": [
            "",
            " Го в ДИКСИ!",
        ],
        "greeting": [
            "",
            " Выгода — это база 🔥",
            " Лови момент 😎",
        ],
    },
    "пенсион": {
        "name": "Пенсионер",
        "tone": "senior",
        "cta_suffix": [
            "",
            " Ждём вас!",
            " Приятных покупок!",
        ],
        "greeting": [
            "",
            " Специально для вас 🌸",
            " С заботой о вас 💛",
        ],
    },
    "питомц": {
        "name": "Питомец",
        "tone": "pet",
        "cta_suffix": [
            "",
            " Питомец оценит!",
        ],
        "greeting": [
            "",
            " Для любимцев 🐱🐶",
            " Хвостатые одобряют 😺",
        ],
    },
    "кошат": {
        "name": "Кошатник",
        "tone": "pet",
        "cta_suffix": ["", " Котик оценит 🐱"],
        "greeting": ["", " Для кошатников 🐱"],
    },
    "собач": {
        "name": "Собачник",
        "tone": "pet",
        "cta_suffix": ["", " Пёсик оценит 🐶"],
        "greeting": ["", " Для собачников 🐶"],
    },
    "детск": {
        "name": "Дети",
        "tone": "kids",
        "cta_suffix": [
            "",
            " Малышу — лучшее!",
        ],
        "greeting": [
            "",
            " Для заботливых родителей 👶",
            " Малышам — самое лучшее 💛",
        ],
    },
}


def _detect_segment(promo: dict) -> dict | None:
    """Определить лайфстайл-сегмент клиента из поля Сегмент акции.

    Игнорирует CRM-сегменты (Активные, Новые, Спящие, Отток) —
    смотрит ТОЛЬКО лайфстайл-сегменты (Семья, ЗОЖ, Сладкоежка и т.д.).
    """
    segment_raw = _clean(promo.get("Сегмент")).lower()
    if not segment_raw:
        return None

    # Игнорируем CRM-сегменты
    _IGNORE = ("активн", "нов", "спящ", "отток", "лояльн", "регуляр")

    for key, config in _SEGMENT_CONFIG.items():
        if key in segment_raw:
            return config

    return None


def _apply_segment_cta(cta: str, segment_config: dict | None) -> str:
    """CTA не меняем — сегмент не должен перегружать CTA.

    Раньше добавляли суффиксы, но это создаёт «спамный» эффект.
    """
    return cta


def _get_segment_greeting(segment_config: dict | None) -> str:
    """Получить сегмент-специфичное приветствие (тонкое, не спамное).

    Используется ВМЕСТО юмора, когда юмор не подходит к категории.
    Возвращает пустую строку в ~50% случаев, чтобы не перегружать.
    """
    if not segment_config:
        return ""
    greetings = segment_config.get("greeting", [""])
    choice = random.choice(greetings)
    return choice


def generate_builtin(promo: dict, rules: str, num_variants: int,
                     title_max_len: int, body_max_len: int,
                     schedule: list[dict]) -> dict:
    """Бесплатная генерация push-текстов на основе стиля ДИКСИ."""

    benefit = _extract_benefit(promo)
    cat = _extract_category_details(promo)
    needs_act = _needs_activation(promo)
    is_online = _is_online(promo)
    segment = _detect_segment(promo)

    year_hint = promo.get("Год", date.today().year)
    start_date = _parse_date(promo.get("Старт акции"), year_hint)
    end_date = _parse_date(promo.get("Окончание акции"), year_hint)

    # Определяем однодневная ли акция
    is_one_day = (start_date and end_date and start_date == end_date)

    # Формат даты: "до 29.03" или "только 26 марта"
    _MONTH_NAMES = {1: "января", 2: "февраля", 3: "марта", 4: "апреля",
                    5: "мая", 6: "июня", 7: "июля", 8: "августа",
                    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"}
    if is_one_day and end_date:
        end_str = f"только {end_date.day} {_MONTH_NAMES.get(end_date.month, '')}"
        date_context = end_str  # "только 26 марта"
    elif end_date:
        end_str = end_date.strftime("%d.%m")
        date_context = f"до {end_str}"  # "до 29.03"
    else:
        end_str = ""
        date_context = ""

    # Эмодзи категории (с учётом типа выгоды для дефолта)
    name_for_emoji = _clean(promo.get("Название промо"))
    emoji = _get_category_emoji(
        cat["category"] + " " + name_for_emoji, cat["products_text"],
        benefit_type=benefit["type"])

    # Расшифровка конкретных товаров (правило 16) — из ВСЕХ категорий в акции
    coupon_text = _clean(promo.get("Текст на информационном купоне / слип-чеке"))
    name_for_details = _clean(promo.get("Название промо"))
    details = _build_product_details(
        cat["category"] + " " + name_for_details, cat["products_text"], coupon_text)
    if not details:
        details = cat["products_text"] or cat["category"] or "все товары"
    # Capitalize first letter
    if details:
        details = details[0].upper() + details[1:]
    # details_bonus: для бонусных шаблонов — пустое если «все товары», иначе «Чипсы, снеки — »
    _details_lower = details.lower().strip()
    if _details_lower in ("все товары", "все", ""):
        details_bonus = ""
    else:
        details_bonus = f"{details} — "

    # Короткая версия details для заголовка (первые 1-2 слова)
    details_short = details.split(",")[0].strip()[:20]
    # Не вставляем "Все товары" в заголовок
    if details_short.lower() in ("все товары", "все"):
        details_short = ""

    # ── Распознавание рецептной акции (окрошка, греческий салат, клубника с йогуртом и т.п.) ──
    # Заголовок push — КОРОТКОЕ имя блюда (соблюдаем title_max_len).
    # Полное «красивое» описание блюда (если есть) выносим в начало body,
    # чтобы не раздувать title.
    _recipe = _detect_recipe(name_for_details, cat["category"], coupon_text)
    dish_name = ""    # короткое — для title
    dish_full = ""    # полное описание — для body
    if _recipe:
        dish_name = _recipe.get("dish", "")
        # Пробуем вытащить полное описание блюда из названия акции — то, что идёт после «на».
        _name_m = re.search(r"\bна\s+(.+)$", name_for_details, re.IGNORECASE)
        if _name_m:
            _full = _name_m.group(1).strip().rstrip(".!?,")
            _full = re.split(r"\s+и\s+друг", _full, maxsplit=1)[0].strip()
            if _full and _full[0].isupper():
                _full = _full[0].lower() + _full[1:]
            # Полное описание стоит выносить в body только если оно ЯВНО богаче короткого
            # (иначе будет дублирование «окрошку» / «окрошка»).
            if len(_full) > len(dish_name) + 6:
                dish_full = _full
        details_short = dish_name
        # Эмодзи берём из рецепта (🥒🥔 окрошка, 🍅 греческий, 🍓 клубника),
        # а не по случайному совпадению ингредиента с «морковка» и подобным.
        if _recipe.get("emoji"):
            emoji = _recipe["emoji"]
    # Для body-шаблонов: «Окрошка на ледяном квасе… — » если есть, иначе пусто
    dish_full_prefix = f"{dish_full[0].upper()}{dish_full[1:]} — " if dish_full else ""

    # ── Извлечение условия (мин. чек, кол-во шт. и т.д.) ──
    condition_raw = _extract_condition(promo)
    # condition: для вставки внутри body → "При чеке от 1000₽. "
    condition = f"{condition_raw[0].upper()}{condition_raw[1:]}. " if condition_raw else ""
    # condition_on: для начала body с тире → "При чеке от 1000₽ — "
    condition_on = f"{condition_raw[0].upper()}{condition_raw[1:]} — " if condition_raw else ""
    # condition_check: живой формат «За чек на 1500₽ » (как в реальных push ДИКСИ)
    _cond_amount = re.search(r"(\d+)₽", condition_raw) if condition_raw else None
    if _cond_amount:
        condition_check = f"за чек от {_cond_amount.group(1)}₽ "
    else:
        condition_check = ""

    # ── Извлечение промокода ──
    promo_code = _extract_promo_code(promo)
    promo_code_text = ""
    has_promo_code = False
    if promo_code:
        has_promo_code = True
        code = promo_code["code"].upper()
        pc_amount = promo_code.get("benefit_amount", "")
        pc_min = promo_code.get("min_order", "")
        # Полный формат: «И скидка 300₽ на заказ от 1500₽ онлайн по промокоду NASTOL»
        if pc_amount and pc_min:
            promo_code_text = f" И скидка {pc_amount}₽ на заказ от {pc_min}₽ онлайн по промокоду {code}."
        elif pc_amount:
            promo_code_text = f" И скидка {pc_amount}₽ онлайн по промокоду {code}."
        elif pc_min:
            promo_code_text = f" И скидка на заказ от {pc_min}₽ онлайн по промокоду {code}."
        else:
            promo_code_text = f" И доп. скидка онлайн по промокоду {code}."

        # Если condition содержит ту же сумму что и промокод — убираем condition
        # (чтобы не дублировать «от 1500₽» дважды)
        if pc_min and condition_raw and pc_min in condition_raw:
            condition = ""
            condition_on = ""
            condition_check = ""

    # ── ОДИН CTA (правило: не перегружать, не дублировать ДИКСИ) ──
    # Если есть промокод — используем креативный CTA на основе ситуации
    # (промокод_text уже содержит «онлайн», не нужно повторять)
    if has_promo_code:
        # Креативные CTA по ситуации (категория → «к столу», «к вечеру» и т.д.)
        _cta_situations = {
            "вин": "Смотреть что есть к столу",
            "пиво": "Собрать корзину к вечеру",
            "напит": "Выбрать напитки",
            "мясо": "Выбрать к ужину",
            "рыб": "Выбрать к ужину",
            "сладк": "Выбрать к чаю",
            "шокол": "Выбрать к чаю",
            "конфет": "Выбрать к чаю",
            "молочн": "Собрать к завтраку",
        }
        cta = "Смотреть в ДИКСИ"  # default
        cat_lower_cta = (cat["category"] + " " + cat["products_text"]).lower()
        for kw, creative_cta in _cta_situations.items():
            if kw in cat_lower_cta:
                cta = creative_cta
                break
    elif needs_act:
        # Для акций с активацией CTA = "Активируй акцию" (всегда, даже если "активируй" в title)
        cta = "Активируй акцию"
    elif is_online:
        cta = "Забегай в ДИКСИ или закажи онлайн"
    else:
        cta = "Забегай в ДИКСИ"

    # Контекст покупки — НЕ дублировать CTA
    purchase_ctx = ""

    # ── Сегментная персонализация ──
    segment_greeting = _get_segment_greeting(segment)
    segment_name = segment["name"] if segment else ""

    fill = {
        "value": benefit["value"] or "супер",
        "benefit": benefit["text"] or "выгода",
        "details": details,
        "details_short": details_short,
        "dish_name": dish_name,
        "end": end_str or "конца акции",
        "date": end_str or "конца акции",
        "products": details,
        "date_context": date_context or "сегодня",
        "condition": condition,
        "condition_on": condition_on,
        "condition_check": condition_check,
        "cta": cta,
        "purchase_ctx": purchase_ctx,
        "emoji": emoji,
        "humor": "",
        "situation": "",
        "segment_greeting": segment_greeting,
        "promo_code": promo_code_text,
        "details_bonus": details_bonus,
        "check_amount": _cond_amount.group(1) + "₽" if _cond_amount else "",
    }

    # Ситуации: будни vs пятница/выходные — разный ход мыслей
    _SITUATIONS_WEEKDAY = {
        # Бытовая химия
        "хими": "уборки", "бытов": "уборки", "обув": "ухода за обувью",
        "стир": "стирки", "чист": "уборки",
        # Мясо и птица
        "мясо": "вкусного ужина", "курица": "вкусного ужина", "куриц": "вкусного ужина",
        "птиц": "вкусного ужина", "индейк": "полезного ужина",
        "свинин": "шашлыка", "говядин": "стейк-вечера",
        "шашлык": "пикника", "колбас": "перекуса", "сосиск": "быстрого ужина",
        "пельмен": "быстрого ужина", "полуфабрикат": "быстрого ужина",
        # Молочное
        "молоко": "завтрака", "молочн": "завтрака", "кефир": "завтрака",
        "творог": "завтрака", "йогурт": "перекуса",
        "сыр": "завтрака", "масло": "завтрака", "яйц": "завтрака",
        # Рыба
        "рыб": "ужина", "морепрод": "праздничного вечера",
        "лосос": "праздничного ужина",
        # Овощи и фрукты
        "овощ": "салата", "фрукт": "перекуса", "ягод": "перекуса",
        "картоф": "гарнира", "зелен": "салата", "гриб": "уютного ужина",
        # Хлеб и выпечка
        "хлеб": "завтрака", "выпечк": "чаепития", "торт": "праздника",
        "печень": "чаепития",
        # Бакалея
        "круп": "полезного обеда", "макарон": "паста-вечера", "мук": "домашней выпечки",
        # Сладости и снеки
        "конфет": "сладкого вечера", "шокол": "поднятия настроения",
        "морожен": "сладкого перекуса", "чипс": "киновечера", "орех": "перекуса",
        # Напитки
        "кофе": "бодрого утра", "чай": "уютного вечера",
        "сок": "витаминного утра", "пиво": "вечера с друзьями", "вин": "особого вечера",
        # Заморозка
        "заморо": "быстрого ужина", "пицц": "вечера", "салат": "лёгкого обеда",
        # Другое
        "детск": "малыша", "корм": "питомца",
    }
    # Пт/Сб/Вс — выходной ход мыслей: отдых, друзья, семья, гости, шашлык
    _SITUATIONS_WEEKEND = {
        # Бытовая химия — генуборка выходного дня
        "хими": "генуборки на выходных", "бытов": "генуборки на выходных",
        "обув": "обуви после рабочей недели", "стир": "большой стирки",
        "чист": "чистоты к выходным",
        # Мясо — шашлык, гости, семейный ужин
        "мясо": "семейного ужина на выходных", "курица": "семейного обеда",
        "куриц": "семейного обеда", "птиц": "воскресного обеда",
        "свинин": "шашлыка на выходных", "говядин": "субботнего стейка",
        "шашлык": "шашлыка с друзьями", "колбас": "пикника",
        "сосиск": "лёгкого завтрака", "пельмен": "ленивых выходных",
        "полуфабрикат": "ленивых выходных", "индейк": "воскресного обеда",
        # Молочное — неспешный завтрак
        "молоко": "неспешного завтрака", "молочн": "воскресного завтрака",
        "кефир": "утра после пятницы", "творог": "неспешного утра",
        "йогурт": "завтрака в постели", "сыр": "бранча с друзьями",
        "масло": "утренних тостов", "яйц": "субботней яичницы",
        # Рыба — пятничный вечер
        "рыб": "пятничного ужина", "морепрод": "особенного вечера",
        "лосос": "вечера при свечах",
        # Овощи и фрукты — готовим дома
        "овощ": "домашней готовки", "фрукт": "витаминного дня",
        "ягод": "летнего десерта", "картоф": "семейного обеда",
        "зелен": "свежего салата", "гриб": "воскресного жульена",
        # Хлеб и выпечка — домашний уют
        "хлеб": "субботних бутербродов", "выпечк": "домашнего чаепития",
        "торт": "праздника на выходных", "печень": "чаепития с семьёй",
        # Бакалея
        "круп": "закупки на неделю", "макарон": "вечера итальянской кухни",
        "мук": "домашних блинов",
        # Сладости и снеки — кино, отдых
        "конфет": "сладких выходных", "шокол": "уютного вечера с фильмом",
        "морожен": "прогулки в парке", "чипс": "киномарафона", "орех": "перекуса на диване",
        # Напитки — вечеринка, расслабление
        "кофе": "субботнего латте", "чай": "воскресного вечера",
        "сок": "утреннего детокса", "пиво": "пятничного вечера",
        "вин": "пятничного вечера", "газиров": "вечеринки",
        # Заморозка
        "заморо": "закупки на неделю", "пицц": "вечера с друзьями",
        "салат": "лёгкого обеда на свежем воздухе",
        # Другое
        "детск": "семейных выходных", "корм": "питомца",
    }
    # Выбираем ситуации в зависимости от того, в какой день push
    # Пока берём первый push (start) — определим день позже в цикле
    # Здесь задаём дефолтные будничные, а в цикле по push перезапишем для пт/вых
    fill["_situations_weekday"] = _SITUATIONS_WEEKDAY
    fill["_situations_weekend"] = _SITUATIONS_WEEKEND
    cat_lower = cat["category"].lower()
    for kw, sit in _SITUATIONS_WEEKDAY.items():
        if kw in cat_lower:
            fill["situation"] = sit
            break
    if not fill["situation"]:
        fill["situation"] = "дома"

    pushes = []

    # ── Контекст дня недели ──
    _WEEKDAY_NAMES = {
        0: "понедельник", 1: "вторник", 2: "среда", 3: "четверг",
        4: "пятница", 5: "суббота", 6: "воскресенье",
    }
    _WEEKDAY_VIBE = {
        0: "",  # понедельник — нейтрально
        1: "",
        2: "",
        3: " Четверг — почти пятница!",
        4: " Пятница — лучший день для покупок!",
        5: " Выходной — время для себя!",
        6: " Воскресенье — закупаемся на неделю!",
    }

    for s in schedule:
        push_type = s.get("type", "start")
        push_num = s.get("push_number", schedule.index(s) + 1)

        # День недели push-а
        push_date_obj = s.get("date_obj")
        weekday_num = push_date_obj.weekday() if push_date_obj else date.today().weekday()
        weekday_name = _WEEKDAY_NAMES.get(weekday_num, "")
        weekday_vibe = _WEEKDAY_VIBE.get(weekday_num, "")
        is_friday = weekday_num == 4
        is_weekend = weekday_num in (5, 6)

        # Обновляем fill с контекстом дня недели
        fill["weekday"] = weekday_name
        fill["weekday_vibe"] = weekday_vibe

        # Переключаем ситуации на выходные (пт/сб/вс)
        if weekday_num >= 4:  # пятница, суббота, воскресенье
            situations = fill.get("_situations_weekend", {})
        else:
            situations = fill.get("_situations_weekday", {})
        cat_lower_sit = cat["category"].lower()
        fill["situation"] = "дома"
        for kw, sit in situations.items():
            if kw in cat_lower_sit:
                fill["situation"] = sit
                break

        # ═══ НОВАЯ СИСТЕМА: готовые пары по 5 осям classify_promo ═══
        cl = classify_promo(promo)
        _act = cl["activation"]
        _ben = cl["benefit"]
        _sco = cl["scope"]
        _chk = cl["check"]
        _per = cl["period"]
        _ctx = cl.get("context", [])

        # Подбираем готовые пары (title, body)
        if push_type == "reminder":
            # Сначала ищем специфичные reminder для этого типа акции
            pairs_pool = _match_pairs((_act, _ben, _sco, _chk, "reminder"))
            if not pairs_pool:
                pairs_pool = _match_pairs((_act, _ben, "*", "*", "reminder"))
            if not pairs_pool:
                pairs_pool = _match_pairs(("*", "*", "*", "*", "reminder"))
        else:
            pairs_pool = _match_pairs((_act, _ben, _sco, _chk, _per))
            if not pairs_pool:
                pairs_pool = _match_pairs((_act, _ben, _sco, _chk, "*"))
            if not pairs_pool:
                pairs_pool = _match_pairs((_act, _ben, "*", "*", "*"))
            if not pairs_pool:
                pairs_pool = [("{emoji}{value} {date_context}", "{products} {date_context}.{humor} {cta}")]

        # ── Рецептные акции: подменяем пул на «-X% на блюдо / найди ингредиенты» ──
        # Структура (по требованию заказчика): рецепт в заголовке, перечисление ингредиентов в body.
        if dish_name and push_type != "reminder":
            if _ben == "discount_pct":
                pairs_pool = [
                    ("{emoji}-{value} на {dish_name}",
                     "{date_context}. Найди все ингредиенты в ДИКСИ: {products}. {cta}"),
                    ("{emoji}{value} скидка на {dish_name}",
                     "{date_context}. Собери корзину: {products}. {cta}"),
                ]
            elif _ben == "cashback_pct":
                pairs_pool = [
                    ("{emoji}{value} кешбэк на {dish_name}",
                     "{date_context}. Ингредиенты в ДИКСИ: {products}. {cta}"),
                    ("{emoji}{value} монетами за {dish_name}",
                     "{date_context}. Бери: {products}. {cta}"),
                ]
            elif _ben == "discount_rub":
                pairs_pool = [
                    ("{emoji}-{value} на {dish_name}",
                     "{date_context}. Ингредиенты в ДИКСИ: {products}. {cta}"),
                ]
            elif _ben == "cashback_rub":
                pairs_pool = [
                    ("{emoji}{value} монетами за {dish_name}",
                     "{date_context}. Бери в ДИКСИ: {products}. {cta}"),
                ]

        # Пятничные/выходные пары — добавляем (НЕ для рецептных, чтобы не размывать блюдо)
        if push_type != "reminder" and not dish_name:
            if is_friday:
                pairs_pool.append(("{emoji}Пятничная выгода {value}",
                                   "{products} {date_context}.{humor} {cta}"))
            elif is_weekend:
                pairs_pool.append(("{emoji}Выходные + {value}",
                                   "{products} {date_context}.{humor} {cta}"))

        fill["products_short"] = details_short
        # Для scope=all: не вставляем "Все товары", а пишем "" или "на любые товары"
        _details_lower_clean = details.lower().strip()
        if _details_lower_clean in ("все товары", "все", ""):
            fill["products"] = ""
        else:
            fill["products"] = details

        random.shuffle(pairs_pool)

        variants = []
        for vi in range(num_variants):
            # Генерируем юмор (правило 6)
            humor = _get_humor(cat["category"], cat["products_text"])

            fill_vi = {**fill, "humor": humor, "cta": cta}

            pair = pairs_pool[vi % len(pairs_pool)]
            title_tmpl, body_tmpl = pair

            # Антидублирование: если продукт в заголовке, убираем из body
            fill_body = dict(fill_vi)
            if ("{products_short}" in title_tmpl or "{products}" in title_tmpl) and "{products}" in body_tmpl:
                fill_body["products"] = ""

            try:
                title = title_tmpl.format(**fill_vi)
            except (KeyError, IndexError):
                title = title_tmpl
            try:
                body = body_tmpl.format(**fill_body)
            except (KeyError, IndexError):
                body = body_tmpl

            # Чистим двойные пробелы
            title = re.sub(r"  +", " ", title).strip()
            body = re.sub(r"  +", " ", body).strip()

            # ── Защита CTA в рецептных push: перерендерим body с подрезкой ингредиентов ──
            # Если body длиннее лимита и это рецептный шаблон с {products} — обрежем список
            # ингредиентов так, чтобы CTA «Активируй акцию» точно остался в конце.
            if dish_name and "{products}" in body_tmpl and len(body) > body_max_len:
                full_products = fill_body.get("products", "")
                items = [s.strip() for s in full_products.split(",") if s.strip()]
                rendered_cta = fill_vi.get("cta", "")
                # Подбираем максимальное число ингредиентов, при котором body влезает целиком
                fit = full_products
                for keep in range(len(items), 0, -1):
                    trial_products = ", ".join(items[:keep])
                    trial_fill = {**fill_body, "products": trial_products}
                    try:
                        trial_body = body_tmpl.format(**trial_fill)
                    except (KeyError, IndexError):
                        continue
                    trial_body = re.sub(r"  +", " ", trial_body).strip()
                    if len(trial_body) <= body_max_len and trial_body.endswith(rendered_cta):
                        fit = trial_products
                        body = trial_body
                        break

            title = _truncate(title, title_max_len)
            body = _truncate(body, body_max_len)

            variants.append({
                "title": title,
                "title_length": len(title),
                "body": body,
                "body_length": len(body),
                "segment": segment_name,
            })

        pushes.append({
            "push_number": push_num,
            "date": s["date"],
            "time": s.get("time", "12:00"),
            "type": push_type,
            "variants": variants,
        })

    return {"pushes": pushes}


# ═══════════════════════════════════════════════════════════════════════════════
# AI-генераторы (платные)
# ═══════════════════════════════════════════════════════════════════════════════

def build_prompt(promo: dict, rules: str, num_variants: int,
                 title_max_len: int, body_max_len: int,
                 schedule: list[dict]) -> str:
    """Собрать промпт для AI."""
    schedule_text = ""
    for i, s in enumerate(schedule, 1):
        stype = s.get("type", "start")
        schedule_text += f"  Push #{i}: дата {s['date']}, время {s.get('time','12:00')}, тип: {stype}\n"

    # Собираем ВСЕ данные акции для AI
    all_fields = ""
    for key, val in promo.items():
        v = _clean(val)
        if v:
            all_fields += f"- {key}: {v}\n"

    # Извлекаем данные для контекста
    benefit = _extract_benefit(promo)
    cat = _extract_category_details(promo)
    condition_raw = _extract_condition(promo)
    promo_code = _extract_promo_code(promo)
    needs_act = _needs_activation(promo)
    is_online = _is_online(promo)

    benefit_context = f"Тип выгоды: {benefit['type']}, значение: {benefit['value']}, текст: {benefit['text']}"
    condition_context = f"Условие: {condition_raw}" if condition_raw else "Условие: нет"
    promo_code_context = ""
    if promo_code:
        pc = promo_code
        promo_code_context = f"Промокод: {pc['code']}, скидка: {pc.get('benefit_amount','')}₽, мин. заказ: {pc.get('min_order','')}₽"

    prompt = f"""Ты — лучший копирайтер сети магазинов ДИКСИ. Сгенерируй тексты push-уведомлений.

ВСЕ ДАННЫЕ АКЦИИ:
{all_fields}

ИЗВЛЕЧЁННЫЕ ДАННЫЕ:
- {benefit_context}
- {condition_context}
- Категория: {cat['category']}, товары: {cat['products_text']}
- Активация: {'да' if needs_act else 'нет'}
- Онлайн: {'да' if is_online else 'нет'}
- {promo_code_context}

РАСПИСАНИЕ PUSH:
{schedule_text}

═══════════════════════════════════════════
ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА (НАРУШЕНИЕ = БРАК):
═══════════════════════════════════════════

1. ТОЛЬКО КИРИЛЛИЦА. Никакой латиницы! Fairy → «Фейри», Ariel → «Ариэль», Domestos → «Доместос».
   Все бренды, названия — ТОЛЬКО кириллицей.

2. ТОЧНЫЕ ЦИФРЫ. 20% — это 20%, НЕ «половина». 50% — это 50%, НЕ «половина».
   Не округляй, не преувеличивай, не приукрашивай.

3. ПРОГРАММА ЛОЯЛЬНОСТИ ДИКСИ = «монеты», НЕ «бонусы», НЕ «баллы».
   Правильно: «вернём монетами», «+100₽ монетами», «кешбэк монетами».
   НЕПРАВИЛЬНО: «бонусы», «баллы», «бонусные рубли».

4. НЕ ВРИ И НЕ ДОДУМЫВАЙ. Не добавляй информацию, которой нет в данных акции.
   Не придумывай даты сгорания монет, доп. условия, бренды.
   Используй ТОЛЬКО факты из данных акции.

5. ОБРАЩЕНИЕ НА «ТЫ», но БЕЗ ТЫКАНЬЯ в заголовке.
   В body можно: «забегай», «покупай», «лови».
   В заголовке НЕ НАДО: «для тебя», «твой кешбэк».

6. ЗНАК РУБЛЯ = ₽ (символ), НЕ «р.» и НЕ «руб.»
   Правильно: 100₽, 1500₽. Неправильно: 100р., 100 руб.

7. ЭМОДЗИ:
   - НЕ используй 🪙 (монета) — некрасивый.
   - Для скидки: 💰💎🫰％👛🛒
   - Для бонусов/монет: 💰💎🫰
   - Для кешбэка: 💰
   - Для категорий: подбери по смыслу (🍗 курица, 🧼 химия, 🍷 вино и т.д.)
   - Один эмодзи в начале заголовка, можно один в body для настроения.

8. СТРУКТУРА PUSH:
   ЗАГОЛОВОК: [эмодзи][выгода + условие чека если есть]
   Примеры хороших заголовков:
   - «💰Вернём 100₽ монетами» (бонус без чека)
   - «🫰Верни 100₽ с чека от 1500₽» (бонус с чеком — условие В ЗАГОЛОВКЕ)
   - «🍷Кешбэк 20% — твой» (кешбэк)
   - «🧼Скидка 30% — лови» (скидка)

   BODY: [товары] [дата]. [юмор/мотивация]. [промокод если есть]. [CTA]
   Примеры:
   - «Вода, газировка, вино до 03.11. И скидка 300₽ на заказ от 1500₽ онлайн по промокоду NASTOL. Выбрать напитки»
   - «до 12.10. Закупка недели? Самое время 🔥 Забегай в ДИКСИ»
   - «Чипсы, снеки, сухарики — до 30.11. Чипсы + фильм = идеальный вечер 🎬 Забегай в ДИКСИ»

9. ТОВАРЫ В ИМЕНИТЕЛЬНОМ ПАДЕЖЕ. Не «на воду, газировку» а «Вода, газировка, вино».
   Товарный список — отдельное предложение в body, без предлогов «на/за» перед ним.

10. «ВСЕ ТОВАРЫ» — НЕ ПИСАТЬ. Если акция на все товары, не нужно писать это в body.

11. НЕ ДУБЛИРОВАТЬ:
    - Не повторяй выгоду в body если она уже в заголовке.
    - Не повторяй «ДИКСИ» дважды.
    - Не повторяй «верни» / «вернём» дважды.

12. ПРОМОКОД — если есть, описывай ПОЛНО:
    «И скидка 300₽ на заказ от 1500₽ онлайн по промокоду NASTOL.»
    НЕ: «Промокод NASTOL — дополнительная скидка онлайн!» (размыто).

13. CTA (призыв к действию) — ОДИН, в конце:
    - Если активация: «АКТИВИРУЙ акцию»
    - Если онлайн: «Забегай в ДИКСИ или закажи онлайн»
    - Если промокод: креативный CTA по ситуации («Выбрать напитки», «Смотреть что есть к столу»)
    - Иначе: «Забегай в ДИКСИ»

14. ЮМОР — тонкий, ситуационный, в body:
    - Привязан к категории: снеки → «Чипсы + фильм = идеальный вечер 🎬»
    - Пятница/выходные: учитывай день недели.
    - НЕ СПАМ: «Когда выгодно — надо брать» = спам. Не используй.
    - НЕ ПАФОС: «Вечер заслуживает чего-то особенного» = пафос. Не используй.

15. ДЕНЬ НЕДЕЛИ: учитывай какой день у push.
    Пятница → «пятничный кешбэк», выходные → «закупаемся на неделю».

16. ПОДАРОК МОНЕТ. Если акция = «дарим N монет» (без кешбэка, без скидки):
    - Заголовок: «💰150₽ монетами — дарим!», «🎁Дарим 150₽ монетами», «💎+150₽ на карту монетами»
    - Body: дата + юмор + CTA. НЕ ПЕРЕСКАЗЫВАЙ текст купона.
    - НЕ ПИШИ: «У тебя есть N монет — это N₽» (масло масляное).
    - НЕ ПИШИ: «трать в магазине или заказывай онлайн» (общие слова).
    - Пример хорошего push: «💰Дарим 150₽ монетами» / «до 26.10. Приятно, когда дарят 😊 Забегай в ДИКСИ»

17. НЕ ПЕРЕСКАЗЫВАЙ ТЕКСТ КУПОНА. Поле «Текст на информационном купоне» — это юридический текст для купона.
    Из него бери ТОЛЬКО: суммы, даты, условия. НЕ копируй фразы, НЕ пересказывай.

18. НЕ ВСТАВЛЯЙ КОДЫ ТОВАРОВ, АРТИКУЛЫ, ID. Если в данных есть числовые коды (SKU, артикулы, ID категорий) —
    НЕ включай их в текст push. Только человекочитаемые названия товаров и категорий.

{f'''
ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА ОТ ПОЛЬЗОВАТЕЛЯ:
{rules}
''' if rules.strip() else ''}

ЖЁСТКИЕ ОГРАНИЧЕНИЯ:
- Заголовок: СТРОГО до {title_max_len} символов (включая эмодзи)
- Текст body: СТРОГО до {body_max_len} символов (включая эмодзи)

ЗАДАНИЕ:
Для каждого push из расписания сгенерируй {num_variants} вариантов.

Ответь СТРОГО в JSON (без markdown, без ```):
{{
  "pushes": [
    {{
      "push_number": 1,
      "date": "...",
      "time": "...",
      "variants": [
        {{"title": "заголовок", "title_length": 25, "body": "текст", "body_length": 95}}
      ]
    }}
  ]
}}"""
    return prompt


def generate_push_texts(promo: dict, rules: str, num_variants: int,
                        title_max_len: int, body_max_len: int,
                        schedule: list[dict], provider: str = None,
                        anthropic_key: str = None, openai_key: str = None) -> dict:
    """Вызвать генератор push-текстов."""
    provider = provider or AI_PROVIDER or "builtin"

    # Сервисный пуш «Баланс баллов» — отдельный промпт без маркетинговой выгоды.
    if is_balance_promo(promo):
        return generate_balance_promo(
            promo=promo, schedule=schedule,
            num_variants=num_variants,
            title_max_len=title_max_len, body_max_len=body_max_len,
            rules=rules, provider=provider,
            anthropic_key=anthropic_key, openai_key=openai_key,
        )

    if provider == "builtin":
        return generate_builtin(promo, rules, num_variants,
                                title_max_len, body_max_len, schedule)
    elif provider == "anthropic":
        key = anthropic_key or ANTHROPIC_API_KEY
        if not key:
            raise ValueError("API ключ Anthropic не указан")
        prompt = build_prompt(promo, rules, num_variants, title_max_len, body_max_len, schedule)
        return _call_anthropic(prompt, key)
    elif provider == "openai":
        key = openai_key or OPENAI_API_KEY
        if not key:
            raise ValueError("API ключ OpenAI не указан")
        prompt = build_prompt(promo, rules, num_variants, title_max_len, body_max_len, schedule)
        return _call_openai(prompt, key)
    else:
        raise ValueError(f"Неизвестный провайдер: {provider}")


def build_template_prompt(target_promo: dict, template_promo: dict,
                          template_messages: list[dict], schedule: list[dict],
                          title_max_len: int, body_max_len: int,
                          rules: str = "") -> str:
    """Собрать промпт для генерации push по шаблону другой акции.

    template_messages — список одобренных push-сообщений шаблонной акции:
      [{"push_number": 1, "title": "...", "body": "...", "date": "...", "time": "..."}, ...]

    Цель: сгенерировать тексты для target_promo ПОЛНОСТЬЮ ПО АНАЛОГИИ с шаблоном,
    подменив только условия (суммы, проценты, чек, даты).
    """
    # Расписание целевой акции
    schedule_text = ""
    for i, s in enumerate(schedule, 1):
        stype = s.get("type", "start")
        schedule_text += f"  Push #{i}: дата {s['date']}, время {s.get('time','12:00')}, тип: {stype}\n"

    # Извлекаем условия шаблонной и целевой акций
    tmpl_benefit = _extract_benefit(template_promo)
    tmpl_condition = _extract_condition(template_promo)
    tmpl_cat = _extract_category_details(template_promo)
    tmpl_needs_act = _needs_activation(template_promo)

    tgt_benefit = _extract_benefit(target_promo)
    tgt_condition = _extract_condition(target_promo)
    tgt_cat = _extract_category_details(target_promo)
    tgt_needs_act = _needs_activation(target_promo)

    # Полные данные целевой акции (чтобы AI мог увидеть все нюансы)
    tgt_fields = ""
    for key, val in target_promo.items():
        v = _clean(val)
        if v:
            tgt_fields += f"- {key}: {v}\n"

    # Шаблонные сообщения
    tmpl_messages_text = ""
    for m in template_messages:
        pn = m.get("push_number", "?")
        title = m.get("title", "")
        body = m.get("body", "")
        d = m.get("date", "")
        t = m.get("time", "")
        tmpl_messages_text += f"\nPush #{pn} ({d} {t}):\n"
        tmpl_messages_text += f"  Заголовок: {title}\n"
        tmpl_messages_text += f"  Текст: {body}\n"

    prompt = f"""Ты — лучший копирайтер сети магазинов ДИКСИ. Тебе нужно сгенерировать push-уведомления ПО АНАЛОГИИ с уже одобренными текстами для похожей акции.

═══════════════════════════════════════════
ШАБЛОННАЯ АКЦИЯ (одобренные тексты-образцы):
═══════════════════════════════════════════
- Номер: {_clean(template_promo.get('НОМЕР'))}
- Название: {_clean(template_promo.get('Название промо'))}
- Выгода: {tmpl_benefit['text']} (тип: {tmpl_benefit['type']}, значение: {tmpl_benefit['value']})
- Условие чека: {tmpl_condition or 'нет'}
- Категория/товары: {tmpl_cat['products_text'] or tmpl_cat['category'] or 'все товары'}
- Активация: {'да' if tmpl_needs_act else 'нет'}
- Старт: {_clean(template_promo.get('Старт акции'))}
- Окончание: {_clean(template_promo.get('Окончание акции'))}

ОДОБРЕННЫЕ PUSH-СООБЩЕНИЯ ШАБЛОНА (используй их как ОБРАЗЕЦ структуры, тона, юмора, CTA):
{tmpl_messages_text}

═══════════════════════════════════════════
ЦЕЛЕВАЯ АКЦИЯ (для неё нужно сгенерировать тексты):
═══════════════════════════════════════════
{tgt_fields}

ИЗВЛЕЧЁННЫЕ УСЛОВИЯ ЦЕЛЕВОЙ АКЦИИ:
- Выгода: {tgt_benefit['text']} (тип: {tgt_benefit['type']}, значение: {tgt_benefit['value']})
- Условие чека: {tgt_condition or 'нет'}
- Категория/товары: {tgt_cat['products_text'] or tgt_cat['category'] or 'все товары'}
- Активация: {'да' if tgt_needs_act else 'нет'}

РАСПИСАНИЕ PUSH ДЛЯ ЦЕЛЕВОЙ АКЦИИ:
{schedule_text}

═══════════════════════════════════════════
ГЛАВНОЕ ПРАВИЛО — ГЕНЕРАЦИЯ ПО ШАБЛОНУ:
═══════════════════════════════════════════

Возьми каждое одобренное сообщение шаблона и сделай ПОЛНОСТЬЮ АНАЛОГИЧНОЕ для целевой акции:
  • Сохрани СТРУКТУРУ заголовка и body (порядок частей, длину, тон, юмор, CTA, эмодзи).
  • Сохрани ЭМОДЗИ из шаблона (если он подходит к категории целевой акции).
  • Сохрани ЮМОР, фразы-связки, мотивацию — переноси их слово в слово, если они общие.
  • ПОДМЕНИ ТОЛЬКО УСЛОВИЯ:
      – суммы выгоды (например, шаблон «100₽» → целевая «{tgt_benefit['value']}»)
      – проценты (например, шаблон «20%» → целевая «{tgt_benefit['value']}»)
      – минимальный чек (например, шаблон «от 1000₽» → целевая «{tgt_condition or 'без чека'}»)
      – даты (бери из расписания целевой акции)
      – категорию/товары (если в шаблоне они упомянуты — заменяй на товары целевой)

Если в шаблоне 2 push, в целевой тоже должно быть столько push, сколько в расписании выше — не больше и не меньше.
Если push в расписании больше, чем в шаблоне — последний шаблонный push копируй для остальных push, подменяя дату.
Если push в расписании меньше — генерируй только нужное количество.

═══════════════════════════════════════════
ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА (НАРУШЕНИЕ = БРАК):
═══════════════════════════════════════════

1. ТОЛЬКО КИРИЛЛИЦА. Никакой латиницы! Бренды — кириллицей.
2. ТОЧНЫЕ ЦИФРЫ из целевой акции, НЕ из шаблона.
3. ПРОГРАММА ЛОЯЛЬНОСТИ ДИКСИ = «монеты», НЕ «бонусы», НЕ «баллы».
4. НЕ ВРИ И НЕ ДОДУМЫВАЙ. Только факты из целевой акции.
5. ЗНАК РУБЛЯ = ₽, НЕ «р.» и НЕ «руб.».
6. ОБРАЩЕНИЕ НА «ТЫ», без тыканья в заголовке.

{f'''
ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА ОТ ПОЛЬЗОВАТЕЛЯ:
{rules}
''' if rules.strip() else ''}

ЖЁСТКИЕ ОГРАНИЧЕНИЯ:
- Заголовок: СТРОГО до {title_max_len} символов (включая эмодзи)
- Текст body: СТРОГО до {body_max_len} символов (включая эмодзи)

ЗАДАНИЕ:
Сгенерируй по 1 варианту для каждого push из расписания целевой акции, ПО АНАЛОГИИ с шаблонными.

Ответь СТРОГО в JSON (без markdown, без ```):
{{
  "pushes": [
    {{
      "push_number": 1,
      "date": "...",
      "time": "...",
      "variants": [
        {{"title": "заголовок", "title_length": 25, "body": "текст", "body_length": 95}}
      ]
    }}
  ]
}}"""
    return prompt


def _first_int_str(s) -> str:
    """Первое целое число из строки как строка. '+100₽ монетами' → '100'."""
    if not s:
        return ""
    m = re.search(r"\d+", str(s))
    return m.group(0) if m else ""


def generate_push_from_template_builtin(target_promo: dict, template_promo: dict,
                                        template_messages: list[dict],
                                        schedule: list[dict]) -> dict:
    """Шаблонная генерация без AI — копируем текст шаблона и подменяем числа/даты.

    Принципы:
      • для каждого push из расписания целевой акции берём шаблонное сообщение
        с тем же индексом (или последнее, если шаблонных меньше);
      • подменяем число выгоды (100→150), число чека (1000→1500), дату окончания (31.05→30.06);
      • дату/время push берём ИЗ РАСПИСАНИЯ целевой акции, а не из шаблона.
    """
    tmpl_b = _extract_benefit(template_promo)
    tgt_b = _extract_benefit(target_promo)
    tmpl_c = _extract_condition(template_promo)
    tgt_c = _extract_condition(target_promo)

    tmpl_val_num = _first_int_str(tmpl_b.get("value", ""))
    tgt_val_num = _first_int_str(tgt_b.get("value", ""))
    tmpl_check_num = _first_int_str(tmpl_c)
    tgt_check_num = _first_int_str(tgt_c)

    # Дата окончания шаблона и цели — для замены "до 31.05"
    def _end_short(promo):
        yh = promo.get("Год", date.today().year)
        try:
            yh = int(yh)
        except (ValueError, TypeError):
            yh = date.today().year
        d = _parse_date(_clean(promo.get("Окончание акции")), yh)
        return d.strftime("%d.%m") if d else ""

    tmpl_end_short = _end_short(template_promo)
    tgt_end_short = _end_short(target_promo)

    # Список (старое → новое), отсортированный по убыванию длины старого числа,
    # чтобы 1000→1500 не мешало 100→150 (или наоборот).
    num_subs = []
    if tmpl_check_num and tgt_check_num and tmpl_check_num != tgt_check_num:
        num_subs.append((tmpl_check_num, tgt_check_num))
    if tmpl_val_num and tgt_val_num and tmpl_val_num != tgt_val_num \
            and tmpl_val_num != tmpl_check_num:
        num_subs.append((tmpl_val_num, tgt_val_num))
    num_subs.sort(key=lambda x: -len(x[0]))

    def _substitute(text: str) -> str:
        if not text:
            return text
        new_text = text

        # 1) Дата окончания (заменяем сначала, чтобы не пересеклась с числами)
        if tmpl_end_short and tgt_end_short and tmpl_end_short != tgt_end_short:
            new_text = new_text.replace(tmpl_end_short, tgt_end_short)

        # 2) Числа — через плейсхолдеры, чтобы не было повторных подмен
        for i, (old, _) in enumerate(num_subs):
            new_text = re.sub(rf"(?<!\d){re.escape(old)}(?!\d)", f"\u0000SUB{i}\u0000", new_text)
        for i, (_, new) in enumerate(num_subs):
            new_text = new_text.replace(f"\u0000SUB{i}\u0000", new)

        return new_text

    output_pushes = []
    if not template_messages or not schedule:
        return {"pushes": []}

    for i, sched in enumerate(schedule):
        tmpl_idx = min(i, len(template_messages) - 1)
        tmpl_msg = template_messages[tmpl_idx]
        title_new = _substitute(tmpl_msg.get("title", ""))
        body_new = _substitute(tmpl_msg.get("body", ""))
        output_pushes.append({
            "push_number": i + 1,
            "date": sched.get("date", ""),
            "time": sched.get("time", "10:00"),
            "variants": [{
                "title": title_new,
                "body": body_new,
                "title_length": len(title_new),
                "body_length": len(body_new),
            }],
        })

    return {"pushes": output_pushes}


def generate_push_from_template(target_promo: dict, template_promo: dict,
                                template_messages: list[dict],
                                schedule: list[dict],
                                title_max_len: int = 35,
                                body_max_len: int = 120,
                                rules: str = "",
                                provider: str = None,
                                anthropic_key: str = None,
                                openai_key: str = None) -> dict:
    """Сгенерировать push для целевой акции по аналогии с шаблонной.

    builtin — простая регулярная подмена чисел/дат без AI.
    anthropic / openai — генерация через LLM с промптом «по аналогии».
    """
    provider = provider or AI_PROVIDER or "builtin"

    if provider == "builtin":
        return generate_push_from_template_builtin(
            target_promo, template_promo, template_messages, schedule,
        )

    prompt = build_template_prompt(
        target_promo=target_promo,
        template_promo=template_promo,
        template_messages=template_messages,
        schedule=schedule,
        title_max_len=title_max_len,
        body_max_len=body_max_len,
        rules=rules,
    )

    if provider == "anthropic":
        key = anthropic_key or ANTHROPIC_API_KEY
        if not key:
            raise ValueError("API ключ Anthropic не указан")
        return _call_anthropic(prompt, key)
    elif provider == "openai":
        key = openai_key or OPENAI_API_KEY
        if not key:
            raise ValueError("API ключ OpenAI не указан")
        return _call_openai(prompt, key)
    else:
        raise ValueError(f"Неизвестный провайдер: {provider}")


def _anthropic_message(prompt: str, api_key: str, max_tokens: int = 4096) -> str:
    """Запрос к Anthropic с увеличенным таймаутом, повторами и понятной ошибкой при обрыве."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key, timeout=90.0, max_retries=4)
    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.APIConnectionError as e:
        raise RuntimeError(
            "Не удалось подключиться к Anthropic API. Проверьте интернет/VPN "
            "(API недоступен с российских IP) и попробуйте ещё раз."
        ) from e
    return message.content[0].text


def _call_anthropic(prompt: str, api_key: str) -> dict:
    return _parse_response(_anthropic_message(prompt, api_key, 4096))


def _call_openai(prompt: str, api_key: str) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=4096, temperature=0.8,
    )
    return _parse_response(response.choices[0].message.content)


def _parse_response(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1])
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
        raise ValueError(f"Не удалось разобрать ответ AI: {text[:200]}")


# ═══════════════════════════════════════════════════════════════════════════════
# Подбор cat5-категорий из ассортимента (поле «Категории» для CVM offline)
# ═══════════════════════════════════════════════════════════════════════════════
_CAT5_CATALOG = None
# Широкие акции — конкретные cat5 не нужны (акция на весь ассортимент).
_CAT5_BROAD_MARKERS = (
    "все товары", "всё товары", "все товаров", "любые товары", "любых товаров",
    "на покупки", "на любые покупки", "весь ассортимент", "на всё", "на все товары",
)


def _load_cat5_catalog() -> dict:
    """Индекс cat5 из data/cat5_catalog.json (строится build_cat5_catalog.py)."""
    global _CAT5_CATALOG
    if _CAT5_CATALOG is not None:
        return _CAT5_CATALOG
    path = Path(__file__).resolve().parent / "data" / "cat5_catalog.json"
    try:
        _CAT5_CATALOG = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        _CAT5_CATALOG = {}
    return _CAT5_CATALOG


def _cat4_candidates(keywords: list[str], limit: int = 50) -> list[dict]:
    """Кандидаты-ГРУППЫ cat4: ключевые слова (как начало слова) встречаются в названиях
    характерных товаров группы. Возвращает [{cat4, names, cat5:[...]}].

    Работаем на уровне cat4, а не cat5: слово «банан» есть в названии товаров группы
    «БАНАНЫ», и вся группа разворачивается в свои cat5 — так покрываются широкие категории.
    """
    cat4 = _load_cat5_catalog().get("cat4", {})
    terms = {k.strip().lower() for k in keywords if len(k.strip()) >= 3}
    if not terms:
        return []
    scored = []
    for code, info in cat4.items():
        names = info.get("names", [])
        hay = " ".join(names).lower()
        # сколько разных слов совпало + у скольких товаров группы есть совпадение
        distinct = sum(1 for t in terms if re.search(r"\b" + re.escape(t), hay))
        if not distinct:
            continue
        match_names = sum(1 for nm in names if any(re.search(r"\b" + re.escape(t), nm.lower()) for t in terms))
        scored.append((distinct + match_names, info.get("n", 0), code, info))
    scored.sort(key=lambda x: (-x[0], -x[1]))
    return [
        {"cat4": code, "names": info.get("names", [])[:5], "cat5": info.get("cat5", [])}
        for _, _, code, info in scored[:limit]
    ]


def _expand_category_to_terms(category: str, name: str, key: str) -> list[str]:
    """AI: абстрактная категория акции → конкретные НАЗВАНИЯ ТОВАРОВ этой категории."""
    prompt = (
        f"Категория промо-акции ДИКСИ: «{category or name}» (название акции: «{name}»).\n"
        "Назови 6–15 КОНКРЕТНЫХ слов-основ — так НАЗЫВАЮТ сами товары этой категории на полке "
        "(не абстрактно «фрукты», а именно «банан», «яблок», «груш»).\n"
        "Примеры:\n"
        "  «фрукты и ягоды» → банан, яблок, груш, виноград, апельсин, мандарин, лимон, клубник, черешн, малин, слив, персик\n"
        "  «овощи зелень и салаты» → картофел, томат, огурц, лук, морков, перец, капуст, зелен, укроп, петрушк, салат\n"
        "  «вода и соки» → вода, сок, нектар, морс, лимонад, минерал\n"
        "  «реппеленты» → комар, репеллент, клещ, москит, фумигатор, спираль, гардекс, раптор\n"
        "  «товары для уборки» → чистящ, моющ, унитаз, стекл, губк, тряпк, перчатк, порошок\n"
        "Только основы (без окончаний), по-русски, нижний регистр.\n"
        'Ответь СТРОГО JSON: {"terms": ["...", "..."]}'
    )
    try:
        res = _parse_response(_anthropic_message(prompt, key, 400))
        return [str(t).strip().lower() for t in res.get("terms", []) if str(t).strip()]
    except Exception:
        return []


def resolve_cat5_codes(promo: dict) -> dict:
    """Подобрать cat5-коды (CVM offline) для акции из ассортимента (через группы cat4).

    Шаги: 1) AI разворачивает категорию в названия товаров → 2) отбор групп-кандидатов cat4 →
    3) AI-скептик оставляет только подходящие группы → 4) разворачиваем их в cat5.
    Для широких акций («все товары») — пустой список (cat5 не нужен).
    """
    key = ANTHROPIC_API_KEY
    name = _clean(promo.get("Название промо"))
    category = _clean(promo.get("Категория")) or _clean(promo.get("Описание акции"))
    q = f"{category} {name}".lower()
    if not key or not category or any(m in q for m in _CAT5_BROAD_MARKERS):
        return {"codes": [], "rejected": [], "candidates": 0, "broad": True}

    terms = _expand_category_to_terms(category, name, key)
    terms += re.findall(r"[а-яёa-z]{4,}", category.lower())
    cat4_cands = _cat4_candidates(terms, limit=90)
    if not cat4_cands:
        return {"codes": [], "rejected": [], "candidates": 0, "broad": False}

    # Группы cat4 дают ПОЛНОТУ (нашли «БАНАНЫ», «ЯБЛОКИ»…), но выбираем на уровне cat5 —
    # чтобы для узкой категории (репелленты) не захватить всю dacha-группу (грунт, удобрения).
    names_map = _load_cat5_catalog().get("cat5_names", {})
    candidates: list[dict] = []
    seen5: set[str] = set()
    for c4 in cat4_cands:
        for c5 in c4["cat5"]:
            if c5 in seen5:
                continue
            seen5.add(c5)
            candidates.append({"code": c5, "names": names_map.get(c5, [])})
        if len(candidates) >= 300:
            break

    cand_text = "\n".join(f'  {c["code"]}: {", ".join(c["names"])}' for c in candidates)
    prompt = (
        f"Акция ДИКСИ на категорию: «{category}» (название акции: «{name}»).\n"
        "Ниже cat5-подкатегории ассортимента (код: примеры товаров). Верни ВСЕ коды, чьи товары "
        f"относятся к категории «{category}» — НЕ ПРОПУСКАЙ ни одной подходящей (для «фрукты» это и бананы, "
        "и яблоки, и цитрусовые, и виноград, и ягоды — всё сразу). Исключай только явно посторонние "
        "(сливки при «слив», конфеты «с фруктами», корма, косметику — если категория про свежие фрукты).\n\n"
        f"ПОДКАТЕГОРИИ:\n{cand_text}\n\n"
        'Ответь ТОЛЬКО JSON-объектом: {"codes": ["<код>", "<код>", ...]}'
    )
    try:
        res = _parse_response(_anthropic_message(prompt, key, 2000))
    except Exception:
        return {"codes": [], "rejected": [], "candidates": len(candidates), "broad": False}
    valid = {c["code"] for c in candidates}
    codes = [str(c) for c in res.get("codes", []) if str(c) in valid]
    return {
        "codes": codes,
        "rejected": [],
        "candidates": len(candidates),
        "broad": False,
    }


def verify_cat5_codes(category: str, codes: list[str]) -> dict:
    """Скилл проверки: перепроверить, что выбранные cat5 реально соответствуют категории.

    Независимый строгий проход. Возвращает {"ok": [...], "wrong": [{code, reason}]}.
    """
    key = ANTHROPIC_API_KEY
    names_map = _load_cat5_catalog().get("cat5_names", {})
    items = [(str(c), names_map.get(str(c), [])) for c in codes]
    items = [(c, n) for c, n in items if n]
    if not key or not items:
        return {"ok": [str(c) for c in codes], "wrong": []}
    listing = "\n".join(f'  {c}: {", ".join(n[:4])}' for c, n in items)
    prompt = (
        f"Категория акции: «{category}». Проверь каждый cat5-код — реально ли его товары относятся "
        "к этой категории? Будь строгим скептиком, сомнительное считай несоответствующим.\n"
        f"{listing}\n\n"
        'Ответь СТРОГО JSON: {"ok": ["<код>"...], "wrong": [{"code": "<код>", "reason": "<...>"}]}'
    )
    try:
        res = _parse_response(_anthropic_message(prompt, key, 1200))
        return {"ok": [str(c) for c in res.get("ok", [])], "wrong": res.get("wrong", [])}
    except Exception:
        return {"ok": [str(c) for c in codes], "wrong": []}


# ═══════════════════════════════════════════════════════════════════════════════
# Генератор условий акций (заполнение полей из названия)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_promo_conditions_prompt(promo: dict, examples: list[dict]) -> str:
    """Собрать промпт для AI-генерации условий акции."""
    name = _clean(promo.get("Название промо", ""))
    segment = _clean(promo.get("Сегмент", ""))
    start = _clean(promo.get("Старт акции", ""))
    end = _clean(promo.get("Окончание акции", ""))
    channel = _clean(promo.get("Каналы коммуникации", ""))
    num = _clean(promo.get("НОМЕР", ""))
    nastroyka = _clean(promo.get("Настройка", ""))
    skip_coupon = nastroyka.lower() == "нет"

    # Слип-чек — офлайн-канал (клиенты без приложения): купон бумажный, кнопки/deeplink НЕТ.
    is_slip = _is_slip_channel(channel)

    # Поиск подходящего deeplink из справочника — ищем по названию, категории, описанию.
    # Для слипа deeplink не нужен вообще (нет приложения) — пропускаем поиск.
    deeplink_hint = ""
    _cat_raw = _clean(promo.get("Категория", ""))
    _desc_raw = _clean(promo.get("Описание акции", ""))
    if not is_slip:
        dl = find_best_deeplink(name)
        if not dl and _cat_raw:
            dl = find_best_deeplink(_cat_raw)
        if not dl and _desc_raw:
            dl = find_best_deeplink(_desc_raw)
        # В deeplink акции нужен ПОСТОЯННЫЙ раздел каталога, а не частная промо-подборка.
        # Если keyword-матч попал в кампанийную строку — отбрасываем.
        if dl and not _is_evergreen_category(dl.get("category", "")):
            dl = None
        # Нет подходящего постоянного раздела — подбираем ближайший через AI (не каталог).
        if not dl:
            dl = resolve_deeplink_ai(_cat_raw or _desc_raw or name, name)
        if dl:
            deeplink_hint = f"\nПОДХОДЯЩИЙ DEEPLINK ИЗ СПРАВОЧНИКА:\n  Категория: {dl['category']} (ID: {dl['id']})\n  Deeplink: {dl['deeplink']}\n  Используй этот deeplink в поле «Кнопка» если акция на эту категорию.\n"

    # Год акции — чтобы во ВСЕХ датах (срок сгорания, даты в тексте купона) был верный год.
    _year = _clean(promo.get("Год", "")) or str(datetime.now().year)
    m_end = re.search(r"\.(\d{4})\b", end)
    if m_end:
        _year = m_end.group(1)
    year_hint = f"\n📅 ГОД АКЦИИ: {_year}. ВО ВСЕХ ДАТАХ (срок сгорания, даты в тексте купона) используй ТОЛЬКО {_year}. НЕ ставь прошлый/следующий год.\n"

    # Вычислить выходные/пятницы если акция "в выходные"
    weekend_hint = ""
    name_lower = name.lower()
    if any(w in name_lower for w in ("выходн", "пятниц", "суббот", "воскресен")):
        try:
            year = int(_clean(promo.get("Год", "")) or datetime.now().year)
            start_raw = _clean(promo.get("Старт акции", ""))
            end_raw = _clean(promo.get("Окончание акции", ""))
            s_match = re.match(r"(\d{1,2})\.(\d{1,2})", start_raw)
            e_match = re.match(r"(\d{1,2})\.(\d{1,2})", end_raw)
            if s_match and e_match:
                d_start = date(year, int(s_match.group(2)), int(s_match.group(1)))
                d_end = date(year, int(e_match.group(2)), int(e_match.group(1)))
                weekends = []
                d = d_start
                while d <= d_end:
                    if d.weekday() in (4, 5, 6):  # пт, сб, вс
                        weekends.append(d.strftime("%d.%m"))
                    d += timedelta(days=1)
                if weekends:
                    weekend_hint = f"\nВЫХОДНЫЕ И ПЯТНИЦЫ В ПЕРИОДЕ АКЦИИ:\n  {', '.join(weekends)}\n  Используй эти даты в тексте купона.\n"
        except Exception:
            pass

    # Форматируем примеры
    examples_text = ""
    for ex in examples[:5]:
        ex = _sanitize_promo(ex)
        examples_text += f"""
--- Пример: {ex.get('Название промо','')} ---
  Описание акции: {ex.get('Описание акции','')}
  Скидка: {ex.get('Скидка','')}
  Бонусы: {ex.get('Бонусы','')}
  Механика: {ex.get('Механика','')}
  Категория: {ex.get('Категория','')}
  Срок сгорания бонусов: {ex.get('Срок сгорания бонусов','')}
  Название информационного купона для МП: {ex.get('Название информационного купона для МП','')}
  Текст на информационном купоне / слип-чеке: {str(ex.get('Текст на информационном купоне / слип-чеке',''))[:200]}
  Кнопка: {ex.get('Кнопка','')}
"""

    # Явное предупреждение если в названии "Активируй" / "Акцептные"
    activation_warning = ""
    _name_lower = name.lower()
    if "активируй" in _name_lower or "активир" in _name_lower:
        activation_warning = "\n🚨🚨🚨 ВНИМАНИЕ: В НАЗВАНИИ АКЦИИ ЕСТЬ СЛОВО «АКТИВИРУЙ»!\n   → «Механика» = активируемая из справочника по типу выгоды: «Активируемая скидка» / «Кэшбек активируемый» / «Купон активируемый»\n   → «Название информационного купона для МП» начинай с «Активируй»\n   → В тексте купона должно быть указание про активацию в приложении.\n"

    # Готовые рецепты для тематических акций (с учётом месяца и сезона)
    _RECIPES = {
        "коктейл": {
            "default": (
                "🍹 МОХИТО — два варианта:\n"
                "  БЕЗАЛКОГОЛЬНЫЙ МОХИТО (ингредиенты): мята свежая, лайм, тростниковый сахар, газированная вода (содовая) или Sprite, лёд\n"
                "    Рецепт: размять мяту с сахаром и лаймом, добавить лёд, залить газировкой\n"
                "  АЛКОГОЛЬНЫЙ МОХИТО (ингредиенты): мята свежая, лайм, тростниковый сахар, белый ром, содовая, лёд\n"
                "    Рецепт: размять мяту с сахаром и лаймом, добавить ром, лёд, залить содовой\n"
                "  💡 В описании акции: расскажи про оба варианта — для всей семьи или для вечеринки"
            ),
            5: (
                "🍹 МАЙСКИЙ МОХИТО — два варианта:\n"
                "  БЕЗАЛКОГОЛЬНЫЙ: мята свежая, лайм, тростниковый сахар, газированная вода или Sprite, лёд\n"
                "    Рецепт: размять мяту с сахаром и лаймом, добавить лёд, залить газировкой\n"
                "  АЛКОГОЛЬНЫЙ: мята свежая, лайм, тростниковый сахар, белый ром, содовая, лёд\n"
                "    Рецепт: размять мяту с сахаром и лаймом, добавить ром, лёд, залить содовой\n"
                "  💡 Май = первая зелень и первая свежая мята! В описании — оба варианта мохито: для семьи и для вечеринки"
            ),
            6: (
                "🍹 ЛЕТНИЙ МОХИТО — два варианта:\n"
                "  БЕЗАЛКОГОЛЬНЫЙ: мята, лайм, тростниковый сахар, газировка, лёд\n"
                "  АЛКОГОЛЬНЫЙ: мята, лайм, сахар, белый ром, содовая, лёд\n"
                "  💡 Идея: освежающий мохито для жары — есть и алко, и без"
            ),
        },
        "смузи": {
            "default": "СМУЗИ — рецепт: банан, клубника, малина, йогурт, мёд, мята, киви",
        },
        "оливье": {
            "default": "САЛАТ ОЛИВЬЕ — ингредиенты:\n  колбаса варёная, картофель, морковь, яйца, огурцы маринованные, горошек консервированный, лук, майонез\n  Тема: классика к новому году/празднику",
        },
        "шуба": {
            "default": "СЕЛЁДКА ПОД ШУБОЙ — ингредиенты:\n  сельдь слабосолёная, картофель, морковь, свёкла, лук, яйца, майонез\n  Тема: классический праздничный салат",
        },
        "крабов": {
            "default": "КРАБОВЫЙ САЛАТ — ингредиенты:\n  крабовые палочки, рис, кукуруза консервированная, яйца, огурцы свежие, майонез",
        },
        "сморреброд": {
            "default": "СМОРРЕБРОДЫ (датские бутерброды) — ингредиенты:\n  ржаной хлеб, лосось / красная рыба, креветки, сыр, авокадо, огурец, сливочное масло, укроп\n  Тема: скандинавская кухня, эстетика",
        },
        "пасх": {
            "default": "ПАСХАЛЬНЫЙ СТОЛ — ингредиенты:\n  яйца, кулич, творожная пасха, сливочное масло, мука, дрожжи, изюм, ваниль\n  Тема: пасхальная выпечка и творожная пасха",
        },
        "кулич": {
            "default": "ПАСХАЛЬНЫЙ КУЛИЧ — ингредиенты:\n  мука, яйца, сахар, масло сливочное, молоко, дрожжи сухие, изюм, ваниль, цукаты\n  Тема: пасхальная выпечка",
        },
        "постим": {
            "default": "ПОСТНЫЕ БЛЮДА — ингредиенты:\n  крупы (гречка, рис, овсянка), овощи свежие, овощи замороженные, грибы, бобовые, постные соусы, фрукты\n  Тема: простая постная кухня без молока, мяса, яиц",
        },
        "ужин": {
            "default": "ИДЕИ ДЛЯ УЖИНА:\n  курица/индейка, овощи (брокколи, цветная капуста, морковь), картофель, рис, паста, соусы\n  Тема: семейный ужин",
        },
        "завтрак": {
            "default": "ИДЕИ ДЛЯ ЗАВТРАКА:\n  овсянка, гранола, йогурт, творог, сырники, омлет, тосты, кофе, какао, фрукты\n  Тема: бодрое утро",
        },
        "перекус": {
            "default": "ПЕРЕКУСЫ И СНЕКИ:\n  сэндвичи, бутерброды, орехи, чипсы, готовая еда, йогурт питьевой, фрукты, шоколадки, батончики\n  Тема: быстрый сытный перекус",
        },
        "коктейл алкогол": {
            "default": "АЛКОГОЛЬНЫЕ КОКТЕЙЛИ:\n  водка, ром, виски, ликёр, сок, тоник, лимон, мята, лёд",
        },
        "окрошк": {
            "default": "ОКРОШКА — ингредиенты:\n  квас, редис, картофель, огурцы свежие, яйца, варёная колбаса (Докторская) или ветчина, сметана, зелёный лук, укроп, петрушка\n  Тема: летний холодный суп",
        },
        "греческ": {
            "default": "ГРЕЧЕСКИЙ САЛАТ — ингредиенты:\n  помидоры черри, сыр фета (или брынза/фетакса), огурцы свежие, красный лук, маслины и оливки, оливковое масло, орегано/базилик\n  Тема: лёгкий средиземноморский салат",
        },
        "клубник": {
            "default": "КЛУБНИКА И ЧЕРЕШНЯ С ГРЕЧЕСКИМ ЙОГУРТОМ — ингредиенты:\n  клубника свежая, черешня свежая, греческий йогурт натуральный\n  Тема: летний полезный десерт",
        },
        "черешн": {
            "default": "ЧЕРЕШНЯ С ЙОГУРТОМ — ингредиенты:\n  черешня свежая, клубника свежая, греческий йогурт натуральный\n  Тема: летний полезный десерт",
        },
    }

    # Для тематических/рецептных акций — парсим актуальные товары с dixy.ru + рецепт
    products_hint = ""
    _recipe_keywords = (
        "тематик", "подборк", "постим", "коктейл", "эстетик",
        "тонус", "правильн", "кулич", "оливье", "шуба", "сморреброд",
        "крабов", "ужин", "завтрак", "перекус", "пасх",
        "8 март", "23 феврал", "новый год", "шашлык", "смузи", "витамин",
        "окрошк", "греческ", "клубник", "черешн",
    )
    if any(kw in _name_lower for kw in _recipe_keywords):
        # Получаем месяц
        try:
            _month_int = int(str(_clean(promo.get("Месяц", "")) or "0"))
        except (ValueError, TypeError):
            _month_int = 0

        # Подбираем рецепт по теме+месяцу
        _recipe_text = ""
        for _r_key, _r_versions in _RECIPES.items():
            if _r_key in _name_lower:
                _recipe_text = _r_versions.get(_month_int, _r_versions.get("default", ""))
                break

        # Парсим товары с dixy.ru
        _products_text = ""
        try:
            from dixy_parser import search_discounts
            _products = search_discounts(name)[:15]
            if _products:
                _items = []
                for _p in _products:
                    _line = _p.get("name", "")
                    _disc = _p.get("discount", "")
                    if _disc:
                        _line += f" ({_disc})"
                    _items.append(_line)
                _products_text = "📦 АКТУАЛЬНЫЕ ТОВАРЫ ИЗ КАТАЛОГА ДИКСИ:\n  " + "\n  ".join(_items)
        except Exception:
            pass

        if _recipe_text or _products_text:
            products_hint = "\n"
            if _recipe_text:
                products_hint += f"🍳 ГОТОВЫЙ РЕЦЕПТ/ПОДБОРКА ДЛЯ ЭТОЙ АКЦИИ:\n  {_recipe_text}\n"
            if _products_text:
                products_hint += f"\n{_products_text}\n"
            products_hint += "  → Используй РЕАЛЬНЫЕ ингредиенты из рецепта в описании, тексте купона и кнопке.\n  → Не выдумывай товары!\n"
    elif "акцептн" in _name_lower:
        activation_warning = "\n🚨🚨🚨 ВНИМАНИЕ: АКЦИЯ «АКЦЕПТНЫЕ» — ПРЕДНАЧИСЛЕННЫЕ МОНЕТЫ!\n   → «Механика» = «Предначисленные бонусы» (ОБЯЗАТЕЛЬНО, из справочника)\n   → «Название информационного купона для МП» = «Активируй N монет»\n"

    # Слип-чек — офлайн-канал. Перебивает все «приложение/онлайн/deeplink» инструкции ниже.
    slip_block = ""
    if is_slip:
        slip_block = (
            "\n🧾🧾🧾 КАНАЛ = СЛИП-ЧЕК (ОФЛАЙН). ЭТИ ПРАВИЛА ВАЖНЕЕ ВСЕХ ОСТАЛЬНЫХ:\n"
            "  • Купон БУМАЖНЫЙ (печатается на чеке после покупки). Клиент БЕЗ приложения.\n"
            "  • Поле «Кнопка» ОБЯЗАТЕЛЬНО ПУСТОЕ (\"\"). НИКАКИХ deeplink, НИКАКОГО «В КАТАЛОГ dixyapp://app/catalog».\n"
            "  • В тексте купона НЕЛЬЗЯ упоминать приложение, онлайн-заказ, доставку, «активируй в приложении».\n"
            "  • Скидка действует ТОЛЬКО офлайн: «при предъявлении купона и карты клуба Друзей Дикси на кассе магазина».\n"
            "  • Обязательно добавляй: «Скидка по купону не суммируется с жёлтыми ценниками» и «на 1 покупку до [дата окончания]».\n"
            "  • «Механика» из справочника, НЕ активируемая: обычно «Купон» или «Скидка» (без активации в приложении).\n"
        )

    prompt = f"""Ты — менеджер CVM-программы сети магазинов ДИКСИ. Нужно заполнить условия новой акции.
{slip_block}{year_hint}{deeplink_hint}{weekend_hint}{activation_warning}{products_hint}
ДАННЫЕ АКЦИИ:
- НОМЕР: {num}
- Название промо: {name}
- Сегмент: {segment}
- Старт акции: {start}
- Окончание акции: {end}
- Каналы коммуникации: {channel}
- Настройка: {nastroyka or 'да'}

ПРИМЕРЫ ЗАПОЛНЕННЫХ АКЦИЙ (для понимания формата):
{examples_text}

ЗАДАНИЕ:
Проанализируй название акции и на основе примеров заполни поля. Строго следуй паттернам из примеров.

🚨 КРИТИЧЕСКИ ВАЖНО — НЕ ПРИДУМЫВАЙ УСЛОВИЯ:
- Бери ТОЛЬКО то, что есть в НАЗВАНИИ акции и предоставленных полях.
- НЕ ДОБАВЛЯЙ комбо («4 пива + чипсы»), мин. количества («от 3 шт»), сочетания товаров, сроки годности, доп. условия.
- Если в названии «20% с покупки пива и снеков» — это скидка/кешбэк на пиво и снеки. НИКАКИХ комбо!
- Все доп. условия (мин. чек, акцептные, активация) — ТОЛЬКО если они явно присутствуют в названии акции.

ПРАВИЛА:
1. «Описание акции» — расшифровка: «Вернём 20% монетами за покупку зелени и овощей», «Скидка 10% на зубную пасту»
2. «Скидка» — число или процент если это скидочная акция (10%, 50р.), пусто если кешбэк/бонус
3. «Бонусы» — число или процент если кешбэк/бонус (20%, 100), пусто если скидка
4. 🚨 «Механика» — ВЫБЕРИ ТОЧНО ОДНО ЗНАЧЕНИЕ ИЗ СПРАВОЧНИКА (писать дословно, ничего не выдумывать):
   Кэшбек | Целевые бонусы | Скидка | Активируемая скидка | Купон | Купон активируемый | Коммуникация | Кэшбек X баллов | Кэшбек Х баллов за чек | Кратный кэшбек | Промокод | Отложенная скидка | Скидка x% от чека | Скидка Xр. от чека | Предначисленные бонусы | Кэшбек активируемый
   Как выбирать по названию акции:
   - кешбэк / вернём / возвращаем + % → «Кэшбек» (а если в названии «Активируй» → «Кэшбек активируемый»)
   - кратный кешбэк (x2, x3) → «Кратный кэшбек»
   - скидка / минус N% на категорию → «Скидка» (а если «Активируй» → «Активируемая скидка»)
   - скидка % или N₽ ОТ ЧЕКА (от суммы чека) → «Скидка x% от чека» или «Скидка Xр. от чека»
   - купон / купон на скидку (slip) → «Купон» (а если «Активируй» → «Купон активируемый»)
   - отложенная скидка (скидка на следующую покупку) → «Отложенная скидка»
   - промокод → «Промокод»
   - «Акцептные»/«Дарим N монет»/предначисленные монеты → «Предначисленные бонусы»
   - целевые бонусы/монеты за действие → «Целевые бонусы»
   - «Коммуникация по…»/тематическая рассылка без выгоды → «Коммуникация»
   ВАЖНО: значение ДОЛЖНО быть ровно из списка выше. Не пиши «активация», «автоматическая», «скидка 20%» и т.п. — это БРАК.
5. «Категория» — реальная категория товаров из названия: «зелень и овощи», «зубная паста», «яйца», «все товары»

6. «Срок сгорания бонусов» — ТОЛЬКО для бонусных/кешбэк акций. Для скидочных — пусто.
   РАЗНЫЕ ПРАВИЛА РАСЧЁТА В ЗАВИСИМОСТИ ОТ ТИПА:
   а) Предначисленные бонусы / подарок монет («Акцептные N монет», «Активируй N монет», «Дарим N монет», Механика = «Предначисленные бонусы»):
      срок = дата окончания акции + 1 день, и НЕ БОЛЬШЕ. Это МАКСИМУМ — сгорают на следующий день после окончания акции.
      Пример: акция до 19.04 → 20.04.2026 23:59:00; акция до 05.07 → 06.07.2026 23:59:00.
   б) Кешбэк (с активацией и без): срок = дата окончания акции + 7 дней. НО если акция заканчивается после 20-го числа месяца, то срок = ПРЕДПОСЛЕДНИЙ день этого месяца.
   Примеры кешбэка: акция до 05.04 → 12.04.2026 23:59:00; акция до 26.04 → 29.04.2026 23:59:00; акция до 03.05 → 10.05.2026 23:59:00.
   Формат: «DD.MM.YYYY 23:59:00». ГОД бери из даты окончания акции (тот же год, что у акции) — НЕ подставляй прошлый/следующий год.

7. «Название информационного купона для МП» — КОРОТКОЕ название: ТОЛЬКО ВЫГОДА, БЕЗ КАТЕГОРИИ/ТОВАРА!
   ⚠️ ВАЖНО: название купона + текст купона читаются СОВМЕСТНО как одно сообщение клиенту.
   🚫 КАТЕГОРИЯ/ТОВАР упоминается РОВНО ОДИН РАЗ — в ТЕКСТЕ купона, а НЕ в названии.
      Если категория есть в названии — НЕ повторяй её в тексте, и наоборот. Дубль слова = БРАК.
   🚫 Слово «скидка» в названии НЕ использовать — выгода по % пишется как «минус N%».

   Шаблоны названия (БЕЗ категории, БЕЗ повторов с текстом купона):
   - Для скидки: «Минус 10%» (только %!)
   - Для скидки с активацией: «Активируй минус 10%»
   - Для кешбэка с активацией: «Активируй 20% кешбэк»
   - Для кешбэка без активации (вернём): «Вернём 50%» или «Вернём 100%»
   - Для бонуса/монет: «50% монетами» или «100₽ монетами»
   - Для подарка монет: «Активируй 50 монет»

   ❌ НЕЛЬЗЯ:
   - Название «Скидка 20% на репелленты» + текст «на репелленты от комаров...» (двойное «репелленты» + слово «скидка»)
   - Название «Вернём 50% с покупки соусов» + текст «с покупки соусов...» (двойное «с покупки»/«соусы»)
   - Название «50% кешбэк за алкоголь» + текст «за покупку алкоголя...» (двойное «алкоголь»)
   ✅ ПРАВИЛЬНО:
   - Название «Минус 20%» + текст «на репелленты от комаров, мошек, клещей и другие средства защиты...»
   - Название «Вернём 50%» + текст «с покупки соусов с картой...»
   - Название «50% кешбэк» + текст «за покупку алкоголя с картой...»

8. «Текст на информационном купоне / слип-чеке» — подробный текст купона.
   ВАЖНО для PUSH-акций: НЕ ПИШИ «при предъявлении купона» — купон активируется в приложении.
   НО для slip-акций (канал = slip): купон БУМАЖНЫЙ, поэтому ОБЯЗАТЕЛЬНО пиши «при предъявлении купона».

   г) Купон на скидку (slip-чек, «Купон на скидку Nр.»): бумажный QR-код.
      Текст СТРОГО по шаблону:
      «[сумма]р.
      \nна покупки в ДИКСИ
      \n(QR-код)
      \n\nДействует при предъявлении купона и вашей карты на 1 покупку до [дата окончания].
      \nСкидка по купону не применяется на промотовары по желтым ценникам.»

   НАЧИСЛЕНИЕ МОНЕТ — РАЗНЫЕ ТИПЫ АКЦИЙ:
   ФОРМАТИРОВАНИЕ ТЕКСТА КУПОНА (ВАЖНО — не сливай всё в один абзац!):
   Текст купона ВСЕГДА разбивай на АБЗАЦЫ через \n\n — минимум 2-3 абзаца, одна мысль = один абзац.
   Условие/товары, условия применения и исключения/правила — РАЗНЫЕ абзацы (\n\n между ними).
   Внутри абзаца короткие строки можно разделять одиночным \n. Сплошной текст без \n\n — БРАК.
   Структура текста:
   [основное условие с датами]
   \nСрок использования монет...
   \n1 монета = 1 рубль
   \nПовышенный кешбэк начисляется...
   \n\nИспользование монет осуществляется...

   а) Кешбэк с активацией («Активируй 20% КЕШБЭК на...»): монеты начисляются СРАЗУ после покупки. НЕ ПИШИ «в течение 1 суток» — это неверно.
      Текст СТРОГО В ТАКОМ ПОРЯДКЕ:
      «за покупку [конкретные товары] и других [категория] с картой клуба Друзей Дикси на кассе или при онлайн-заказе в приложении ДИКСИ с [старт] по [окончание] включительно.
      \nСрок использования монет до [срок сгорания].
      \n1 монета = 1 рубль
      \nПовышенный кешбэк начисляется на товары без скидок и товары, не участвующие в других акциях, только на первые 2 покупки товаров за каждый день проведения акции.
      \n\nИспользование монет осуществляется согласно Правилам программы лояльности.»
      ПОРЯДОК БЛОКОВ ОБЯЗАТЕЛЕН: 1) условие покупки с датами → 2) срок использования монет → 3) 1 монета=1 рубль → 4) повышенный кешбэк → 5) правила программы. НЕ МЕНЯЙ МЕСТАМИ!

   б) Акцептные монеты («Акцептные 50 монет»): предначисленные монеты, активируются кнопкой. Монеты зачисляются в течение 1 дня ПОСЛЕ АКТИВАЦИИ.
      Текст БЕЗ пустых строк между строками основного блока:
      «для любых покупок \nс картой клуба друзей ДИКСИ в магазине \nили онлайн-заказа в приложении ДИКСИ с доставкой (от 40 мин) или самовывозом\nс [старт] по [окончание] включительно.\n\n1 монета = 1 рубль.\nИспользование монет осуществляется согласно Правилам программы лояльности.»

   в) Кешбэк без активации («Вернём 50% с покупки...»): монеты начисляются автоматически после покупки.
      Используй предлог «с покупки», НЕ «за покупку» (т.к. в названии акции «вернём с покупки»).
      Текст СТРОГО В ТАКОМ ПОРЯДКЕ:
      «с покупки [конкретные товары] с картой клуба Друзей Дикси на кассе или при онлайн-заказе в приложении ДИКСИ с [старт] по [окончание] включительно.
      \nСрок использования монет до [срок сгорания].
      \n1 монета = 1 рубль
      \nПовышенный кешбэк начисляется на товары без скидок и товары, не участвующие в других акциях, только на первые 2 покупки товаров за каждый день проведения акции.
      \n\nИспользование монет осуществляется согласно Правилам программы лояльности.»
      ПОРЯДОК БЛОКОВ ОБЯЗАТЕЛЕН: 1) условие покупки с датами → 2) срок использования монет → 3) 1 монета=1 рубль → 4) повышенный кешбэк → 5) правила программы. НЕ МЕНЯЙ МЕСТАМИ!

   д) Скидка (однодневная или многодневная) — для PUSH/онлайн-каналов.
      ПИШИ АБЗАЦАМИ — РОВНО ТРИ блока, разделённых \n\n (НЕ сливай в один абзац!):
      Блок 1 (товары): «на [конкретные товары] и другие [категория].»
      Блок 2 (условия применения — ОБЯЗАТЕЛЬНО, не пропускай!): «Скидка действует [только N день, ДД месяца ГГГГ | с ДД по ДД месяца ГГГГ] с картой клуба Друзей Дикси на кассе магазина или на онлайн-покупки в приложении ДИКСИ (Доставка от 40 мин).»
      Блок 3 (исключения — ОБЯЗАТЕЛЬНО): «Скидка не распространяется на промотовары по жёлтым ценникам.»
      Итоговый шаблон: «<Блок1>\n\n<Блок2>\n\n<Блок3>»
      Пример (1 день): «на репелленты от комаров, мошек, клещей и другие средства защиты от насекомых.\n\nСкидка действует только 1 день, 5 июля 2026, с картой клуба Друзей Дикси на кассе магазина или на онлайн-покупки в приложении ДИКСИ (Доставка от 40 мин).\n\nСкидка не распространяется на промотовары по жёлтым ценникам.»
      Пример (несколько дней): «на апельсиновый, яблочный, томатный и другие соки, а также воду.\n\nСкидка действует с 3 по 7 июля 2026 с картой клуба Друзей Дикси на кассе магазина или на онлайн-покупки в приложении ДИКСИ (Доставка от 40 мин).\n\nСкидка не распространяется на промотовары по жёлтым ценникам.»

   д-слип) Скидка по СЛИП-ЧЕКУ (канал = slip, бумажный купон, БЕЗ приложения):
      НЕ упоминай приложение/онлайн/доставку. Текст СТРОГО по шаблону:
      «минус [N]% на [конкретные товары] и другие [категория].
      \n\nДействует при предъявлении купона и карты клуба Друзей Дикси на кассе магазина на 1 покупку с [старт] по [окончание] включительно.
      \nСкидка по купону не суммируется с жёлтыми ценниками.»
      Для купона на сумму (например «Купон 100р. на покупку от 1000р.»):
      «[сумма]р. на покупку от [мин. чек]р.
      \n\nДействует при предъявлении купона и карты клуба Друзей Дикси на кассе магазина на 1 покупку с [старт] по [окончание] включительно.
      \nСкидка по купону не суммируется с жёлтыми ценниками.»

9. РАСШИФРОВКА ТОВАРОВ: Обязательно перечисляй КОНКРЕТНЫЕ товары внутри категории!

   🍳 РЕЦЕПТНЫЕ АКЦИИ (блюдо в названии: окрошка, греческий салат, клубника с йогуртом и т.п.):
   В тексте купона ОБЯЗАТЕЛЬНО перечисли ВСЕ ингредиенты блюда из подсказки «🍳 ГОТОВЫЙ РЕЦЕПТ» —
   клиент должен понимать, на что распространяется скидка.
   ❌ Нельзя: «на Окрошка с редиской и другую готовую еду» — клиент не понимает, что входит в скидку.
   ✅ Правильно: «на квас, редис, картофель, огурцы, варёную колбасу, яйца, сметану, зелёный лук и укроп — всё для окрошки.»
   ✅ Правильно: «на помидоры черри, сыр фета, огурцы, красный лук, маслины и оливковое масло — всё для греческого салата.»
   ✅ Правильно: «на клубнику, черешню и греческий йогурт.»
   Перечисляй компактно через запятую, в конце — «— всё для [название блюда]» (если блюдо в названии).


   - «овощи» → «капусту, помидоры, огурцы, морковь, перец и другие овощи»
   - «зелень» → «укроп, петрушку, базилик, салат и другую зелень»
   - «фрукты» → «яблоки, бананы, апельсины, груши и другие фрукты»
   - «птица» → «курицу, индейку и другую птицу»
   - «алкоголь» → «вино, пиво, просекко и другие алкогольные напитки»
   - «соусы» → «кетчуп, майонез, горчицу и другие соусы и специи»
   - «бытовая химия» → «средства для уборки, мытья посуды и другую бытовую химию»
   - «соки и вода» / «напитки» → «апельсиновый, яблочный, томатный и другие соки, питьевую и минеральную воду, лимонады, морсы, холодный чай и другие безалкогольные напитки»
   - «яйца» → «куриные, перепелиные и другие яйца»
   - «зубная паста» → «зубные пасты Colgate, Splat, Blend-a-med и другие»
   - «товары для животных» / «зоо» → «корма для кошек и собак, наполнители, лакомства и другие товары для животных»
   - «детские категории» / «дети» → «детское питание, подгузники, каши, пюре и другие товары для детей»
   - «готовая еда» → «салаты, сэндвичи, роллы, выпечку и другую готовую еду»
   - «ПП» / «правильное питание» → «протеиновые батончики, гранолу, хлебцы, орехи и другие товары для здорового питания»
   - «вино и просекко» → «вино, просекко, игристое и другие напитки»
   - «пиво» → «светлое, тёмное, нефильтрованное и другое пиво»
   НЕ ПИШИ просто «соки и воду» или «овощи» — ВСЕГДА расшифровывай конкретно!

10. Для «Коммуникация по X» — это информационная акция без скидки/кешбэка, описание = «Рассказываем о товарах [категория] в ДИКСИ»
11. Для «Акцептные N монет» — это подарок монет на активацию
12. Если «Настройка» = «нет», то купон НЕ формируется — оставь поля «Название информационного купона для МП» и «Текст на информационном купоне / слип-чеке» ПУСТЫМИ. Заполни только Описание, Категорию, Скидку/Бонусы, Механику.

13. АКЦИИ «В ВЫХОДНЫЕ» / «ПО ПЯТНИЦАМ»:
    Если в названии акции есть «в выходные», «по выходным», «в пятницу» — нужно:
    а) Вычислить ВСЕ пятницы, субботы и воскресенья в периоде акции (от старта до окончания).
    б) Перечислить конкретные даты в тексте купона: «Кешбэк действует только по пятницам, субботам и воскресеньям: 10.04, 11.04, 12.04, 17.04, 18.04, 19.04, 24.04, 25.04, 26.04, 01.05, 02.05, 03.05.»
    в) В описании акции тоже указать: «...только в выходные и пятницы в период с [старт] по [окончание]».

14. «Кнопка» — СТАНДАРТНЫЙ текст + deeplink. Формат: «В КАТАЛОГ deeplink» (или «ВЫБРАТЬ и КУПИТЬ deeplink»).
    ❌ НЕ пиши тематические слоганы/глагольные фразы («Побриться с комфортом», «Утолить жажду», «Активируй кешбэк на фрукты») — для кнопки это чужеродно. Только «В КАТАЛОГ» / «ВЫБРАТЬ и КУПИТЬ».
    🧾 ЕСЛИ КАНАЛ = СЛИП-ЧЕК: поле «Кнопка» ВСЕГДА ПУСТОЕ (""). Слип бумажный, кнопки в нём нет.
    📣 ЕСЛИ это КОММУНИКАЦИЯ / ТЕМАТИЧЕСКАЯ РАССЫЛКА (механика «Коммуникация»): «Кнопка» ПУСТАЯ ("") — на контентных рассылках кнопка не нужна.
    НИКОГДА НЕ ПРИДУМЫВАЙ DEEPLINKS! Используй ТОЛЬКО:
    - Deeplink из раздела «ПОДХОДЯЩИЙ DEEPLINK ИЗ СПРАВОЧНИКА» выше (если есть)
    - Или «dixyapp://app/catalog» для акций на все товары
    Если подходящий deeplink НЕ найден в справочнике — ставь «В КАТАЛОГ dixyapp://app/catalog».
    НЕ ВЫДУМЫВАЙ ссылки типа dixyapp://app/products_list?id=... самостоятельно!

Ответь СТРОГО в JSON (без markdown, без ```):\n"""

    if skip_coupon:
        prompt += """\n{"Описание акции": "...", "Скидка": "...", "Бонусы": "..."}"""
    else:
        prompt += """\n{"Описание акции": "...", "Скидка": "...", "Бонусы": "...", "Механика": "...", "Категория": "...", "Срок сгорания бонусов": "...", "Название информационного купона для МП": "...", "Текст на информационном купоне / слип-чеке": "...", "Кнопка": "..."}"""
    return prompt


def _sanitize_promo(promo: dict) -> dict:
    """Очистить NaN/float значения в promo dict."""
    import math
    clean = {}
    for k, v in promo.items():
        if v is None or (isinstance(v, float) and math.isnan(v)):
            clean[k] = ""
        else:
            clean[k] = v
    return clean


def generate_promo_conditions(promo: dict, examples: list[dict]) -> dict:
    """Сгенерировать условия акции через AI."""
    key = ANTHROPIC_API_KEY
    if not key:
        raise ValueError("API ключ Anthropic не указан")

    promo = _sanitize_promo(promo)
    examples = [_sanitize_promo(ex) for ex in examples]
    prompt = _build_promo_conditions_prompt(promo, examples)

    result = _parse_response(_anthropic_message(prompt, key, 2048))

    if isinstance(result, dict):
        # «Механика» — строго из справочника ID_MECHANICS.
        if _clean(result.get("Механика")):
            result["Механика"] = _normalize_mechanic(result.get("Механика"), promo)
        # Предначисленные бонусы: срок сгорания максимум = окончание + 1 день.
        _cap_prepaid_bonus_expiry(result, promo)
        # Принудительно проставить год кампании во все даты (модель пишет прошлый год).
        _fix_campaign_years(result, promo)
        # Слип-чек — офлайн: кнопки/deeplink быть не должно, даже если модель её предложила.
        if _is_slip_channel(_clean(promo.get("Каналы коммуникации", ""))):
            result["Кнопка"] = ""
        # Коммуникация/тематическая рассылка — кнопка не нужна.
        if "коммуникац" in _clean(result.get("Механика", "")).lower():
            result["Кнопка"] = ""
        # «Категории» (cat5 для CVM offline) — подбор из ассортимента + проверка.
        # Для рецептных акций cat5 берётся из data/promo_skus.json (в app.py), не трогаем.
        try:
            _enriched = dict(promo)
            _enriched["Категория"] = result.get("Категория") or promo.get("Категория")
            _enriched["Описание акции"] = result.get("Описание акции") or promo.get("Описание акции")
            _c5 = resolve_cat5_codes(_enriched)
            if _c5.get("codes"):
                result["Категории"] = "\n".join(_c5["codes"])
            result["__cat5_rejected"] = _c5.get("rejected", [])
            result["__cat5_candidates"] = _c5.get("candidates", 0)
        except Exception:
            pass
    return result


def get_similar_examples(promo: dict, all_promos: list[dict], n: int = 5) -> list[dict]:
    """Найти похожие заполненные акции для примера."""
    name = _clean(promo.get("Название промо", "")).lower()
    scored = []
    for p in all_promos:
        # Только заполненные акции (есть Описание или Текст купона)
        if not (_clean(p.get("Описание акции")) or _clean(p.get("Текст на информационном купоне / слип-чеке"))):
            continue
        pname = _clean(p.get("Название промо", "")).lower()
        # Простое сходство по ключевым словам
        score = 0
        # Тип акции
        if "кешбэк" in name and "кешбэк" in pname:
            score += 3
        if "кешбэк" in name and ("возвращаем" in pname or "вернем" in pname or "вернём" in pname):
            score += 3
        if "вернем" in name and ("возвращаем" in pname or "вернем" in pname or "вернём" in pname):
            score += 3
        if "скидк" in name and "скидк" in pname:
            score += 3
        if "активируй" in name and "активируй" in pname:
            score += 2
        if "акцептн" in name and "акцептн" in pname:
            score += 5
        if "коммуникация" in name and "коммуникация" in pname:
            score += 5
        if "купон" in name and "купон" in pname:
            score += 3
        if "монет" in name and "монет" in pname:
            score += 2
        # Категория
        keywords = re.findall(r'[а-яё]{4,}', name)
        for kw in keywords:
            if kw in pname:
                score += 1
        if score > 0:
            scored.append((score, p))
    scored.sort(key=lambda x: -x[0])
    return [p for _, p in scored[:n]]


# ═══════════════════════════════════════════════════════════════════════════════
# Контентные акции (коммуникация без финансовых условий)
# ═══════════════════════════════════════════════════════════════════════════════

# Маркеры контентной акции в названии
_CONTENT_MARKERS = (
    "коммуникац", "тематик", "рассылк", "сериал", "идеи что",
    "вайб", "контент", "подборка товаров", "гид ", "советы",
)
# Финансовые токены — их наличие означает, что это НЕ контентная, а скидочная акция
_FINANCIAL_MARKERS = (
    "скидк", "кешб", "кэшб", "cashback", "промокод", "по цене",
    "дарим", "монет", "подарк", "бонус",
)


def is_content_promo(promo: dict) -> bool:
    """Контентная акция — это коммуникация/тематическая рассылка без финансовых условий.

    Признаки (по названию промо):
      • есть маркер коммуникации: «Тематическая рассылка», «Коммуникация»,
        «Сериал», «Идеи что съесть», «Вайб-рассылка», «Контент», «Подборка товаров»…
      • НЕТ финансовой выгоды: ни %, ни рублей, ни «скидка/кешбэк/монеты/дарим».
      • это не сервисный пуш «Баланс баллов» / «Списание».
    Купон при этом нужен (информационный, без денежного условия).
    """
    name = _clean(promo.get("Название промо")).lower()
    if not name:
        return False
    if not any(m in name for m in _CONTENT_MARKERS):
        return False
    if "баланс баллов" in name or "списан" in name:
        return False
    if any(t in name for t in _FINANCIAL_MARKERS):
        return False
    if re.search(r"\d+\s*%", name):
        return False
    if re.search(r"\d+\s*(?:р\.?|₽|руб)", name):
        return False
    return True


# CTA-механика по умолчанию для контентных акций (в текст вшивается ненавязчиво)
CONTENT_DEFAULT_CTA = "Выбери товар любимым и получи минус 20%"

# Человекочитаемая аудитория по сегменту (для тона; не фактические утверждения о товарах)
_CONTENT_AUDIENCE = {
    "мам": "семьи с детьми",
    "зоо": "владельцы домашних питомцев",
    "перекус": "любители готовой еды и снеков",
    "п/ф": "те, кто готовит быстрые домашние ужины",
    "пп": "те, кто следит за питанием",
    "тонус": "любители кофе и энергетиков",
    "кофе": "любители кофе",
    "просекко": "любители игристого",
    "вино": "ценители вина",
    "пиво": "любители пива",
}


def _content_audience(segment: str) -> str:
    """Привести сегмент к человекочитаемой аудитории для тона текста."""
    s = (segment or "").lower()
    for k, v in _CONTENT_AUDIENCE.items():
        if k in s:
            return v
    return segment or "широкая аудитория"


def _build_content_prompt(promo: dict, schedule: list[dict], user_brief: str = "",
                          cta: str = None) -> str:
    """Собрать промпт для генерации контента (выпуск + купон + промпт картинки на каждую неделю).

    user_brief — свободные вводные от пользователя; встраиваются в промпт
    и имеют приоритет над общими требованиями.
    cta — механика/CTA акции, которую нужно вшить в текст (по умолчанию CONTENT_DEFAULT_CTA;
    пустая строка — без CTA).
    """
    name = _clean(promo.get("Название промо"))
    segment = _clean(promo.get("Сегмент"))
    audience = _content_audience(segment)
    num = _clean(promo.get("НОМЕР"))
    start = _clean(promo.get("Старт акции"))
    end = _clean(promo.get("Окончание акции"))

    if cta is None:
        cta = CONTENT_DEFAULT_CTA
    cta = (cta or "").strip()

    weeks_text = ""
    for i, s in enumerate(schedule, 1):
        weeks_text += f"  Выпуск {i}: дата {s['date']}\n"
    n = len(schedule)

    # Сезон по первой дате выпуска
    season = ""
    if schedule:
        d0 = schedule[0].get("date_obj") or _parse_date(schedule[0].get("date"))
        if d0:
            m = d0.month
            season = ("зима" if m in (12, 1, 2) else "весна" if m in (3, 4, 5)
                      else "лето" if m in (6, 7, 8) else "осень")

    brief = (user_brief or "").strip()
    brief_block = ""
    if brief:
        brief_block = (
            "\n═══════════════════════════════════════════\n"
            "🔥 ВВОДНЫЕ ОТ ЗАКАЗЧИКА — САМЫЙ ВЫСОКИЙ ПРИОРИТЕТ\n"
            "═══════════════════════════════════════════\n"
            "Эти вводные ВАЖНЕЕ всех общих требований ниже (кроме запретов на «скидку» и выдуманные факты).\n"
            "ПЕРЕД ГЕНЕРАЦИЕЙ ОБЯЗАТЕЛЬНО ВЫПОЛНИ 3 ШАГА:\n"
            "  ШАГ 1. Извлеки из брифа имена/роли действующих лиц (например: «Миша 8 лет», «Соня 5 лет», «мама»).\n"
            "          Эти герои — СКВОЗНЫЕ для всего сериала. Они ОБЯЗАНЫ присутствовать в КАЖДОМ выпуске:\n"
            "            • либо имя в push_title («Соня нашла русалку 🧜», «Миша строит крепость 🏰»),\n"
            "            • либо имена в первой фразе push_body («Миша и Соня раскопали…», «Соня решила, что…»),\n"
            "            • в content — точно как действующие лица сцены.\n"
            "          ⛔️ ЗАПРЕЩЕНО ставить главным героем сериала неодушевлённый объект (Холодильник, Школа, "
            "Лето, Каникулы), если бриф называет конкретных персонажей. У сериала про Мишу и Соню герои — Миша и Соня.\n"
            "  ШАГ 2. Извлеки ключевой механизм сюжета (например: «каждую неделю придумывают новую затею, "
            "мама помогает с продуктами»). Каждая серия должна следовать этой формуле: новая затея героев → "
            "продукт из ДИКСИ как помощник.\n"
            "  ШАГ 3. Только теперь придумывай конкретные затеи (выпуск 1, 2, 3…) и сцены.\n"
            "\n"
            "БРИФ:\n"
            f"{brief}\n"
            "═══════════════════════════════════════════\n"
        )

    if cta:
        cta_line = (f"4. В каждом выпуске органично используй механику акции: «{cta}». "
                    "Подавай её как приятный бонус по ходу/в конце, не превращай контент в рекламу скидки.")
        push_must = (f"Обязательно упомяни конкретный продукт, релевантный серии выпуска, "
                     f"и выгоду по механике («{cta}»).")
    else:
        cta_line = ("4. Не делай акцент на скидках и выгоде — это контентная рассылка; "
                    "выгоду упоминай только если она задана во вводных.")
        push_must = "Обязательно упомяни конкретный продукт, релевантный серии выпуска."

    return f"""Представь, что ты — ЛУЧШИЙ В МИРЕ продавец сети магазинов ДИКСИ. Готовишь регулярную КОНТЕНТНУЮ рассылку-СЕРИАЛ для клиентов мобильного приложения.

ЦЕЛЬ: замотивировать клиента прийти в магазин у дома ДИКСИ и показать, что ДИКСИ понимает все
заботы и мелкие радости целевой аудитории «{audience}». Тон — очень тёплый, очень много ЮМОРА,
лёгкая ирония, живые сцены из реальной жизни сегмента (а не картинка из рекламы).

ВАЖНО: это КОНТЕНТНАЯ (коммуникационная) акция: главное — полезный, тёплый и развлекательный
контент, который удерживает клиента и формирует привычку заходить в приложение. Это НЕ прямая
распродажа: контент не должен превращаться в рекламу скидки. Механику акции (если она задана ниже)
вплетай ненавязчиво, как приятный бонус.

ГЛАВНЫЙ КРИТЕРИЙ КАЧЕСТВА: каждое следующее сообщение в сериале должно ВЫЗЫВАТЬ ЖЕЛАНИЕ
ОТКРЫТЬ ЕГО И УЗНАТЬ ПРОДОЛЖЕНИЕ. Если после прочтения push-а не хочется кликнуть и узнать,
что там дальше — переделай.

ДАННЫЕ АКЦИИ:
- Номер: {num}
- Название (тема): {name}
- Сегмент: {segment}
- Аудитория (для тона): {audience}
- Период: {start}–{end}
- Сезон: {season or '—'}
- Количество выпусков: {n} (по одному на каждую неделю периода)

ГРАФИК ВЫПУСКОВ:
{weeks_text}{brief_block}
═══════════════════════════════════════════
СТРУКТУРА СЕРИАЛА (без эталонов — следуй принципам):
═══════════════════════════════════════════
  • Каждый push_title — МИНИ-СОБЫТИЕ или ИНТРИГА с героем из брифа (если есть), а НЕ описание сезона.
  • Каждый push_body начинается со СЦЕНЫ на 1-2 конкретные детали, без обобщений и без перечислений.
  • В конце каждого push_body — явный CTA с продуктом и «минус N%».
  • Со 2-го выпуска в push_body или content — ОТСЫЛКА к прошлому эпизоду одной фразой.
  • Финальный выпуск — закрывает арку и перечисляет ключевые события прошлых серий.
  • Тон — живой, тёплый, с лёгкой иронией. Без штампов и канцелярита.

═══════════════════════════════════════════
ТРЕБОВАНИЯ К КОНТЕНТУ:
═══════════════════════════════════════════
0. Если выше есть «ВВОДНЫЕ ОТ ЗАКАЗЧИКА» — следуй им в первую очередь; при конфликте с общими требованиями приоритет у вводных (кроме запрета на выдуманные факты).
1. Пиши под аудиторию «{audience}» — тон, примеры и темы должны быть релевантны именно ей.
2. Если в теме есть слово «сериал» (или сериальная структура очевидна из брифа) — ОБЯЗАТЕЛЬНО сделай выпуски
   ПОСЛЕДОВАТЕЛЬНЫМИ эпизодами одной истории с СКВОЗНЫМИ ГЕРОЯМИ и СЮЖЕТНОЙ ДУГОЙ:
     • в каждом push_title — конкретное событие/интрига этой серии (мини-событие героя), а НЕ объявление сезона;
     • в каждом push_body — живая сцена (1-2 конкретные детали) + явный CTA с продуктом и «минус N%»;
     • со 2-го выпуска в push_body или content — ЯВНАЯ отсылка к прошлой серии одной фразой («после того, как…», «помнишь, на прошлой неделе…»);
     • финальный выпуск — перечисляет ключевые события всех серий и закрывает арку.
   Иначе (не сериал) — каждый выпуск это самостоятельная свежая идея/подборка по теме.
3. Учитывай сезон ({season or 'текущий'}), актуальные поводы периода и повседневные продукты магазина у дома (ДИКСИ — это магазин у дома рядом, опирайся на привычные продукты, которые там есть).
{cta_line}
5. НЕ выдумывай факты: не указывай конкретные цены, не приписывай товарам несуществующее
   позиционирование, не утверждай наличие конкретных СТМ-линеек. Пиши о категориях и идеях в общем.
5a. ⛔️ ЗАПРЕЩЕНО слово «скидка» в любых текстах (push_title, push_body, content, coupon_*).
    Если нужно сказать про процент — пиши «минус N%» / «−N%» / «-N%». Например: «минус 20%» вместо «скидка 20%».
6. ═══════════════════════════════════════════
   TONE OF VOICE ДИКСИ (брендовый фильтр — соблюдай СТРОГО)
   ═══════════════════════════════════════════
   ДИКСИ — это районный магазин у дома. Не премиум, не дискаунтер с ярмаркой, не суровый сетевик.
   Это ТЁПЛЫЙ, БОДРЫЙ, КОНКРЕТНЫЙ СОСЕД, который знает, что у тебя в холодильнике закончилось молоко.

   ▸ ОБРАЩЕНИЕ: на «ты», как сосед/приятель. «Купи», «забери», «выбери», «узнай», «читай», «переходи».
     БЕЗ «дорогой клиент», «уважаемый покупатель», БЕЗ «Вы»/«Купите»/«Выберите» — это официоз.
     Можно «у тебя», «тебе», «твой».

   ▸ ЛЕКСИКА — ДА: купить, забрать, получить, выбрать, выгода, цена, акция, купон, бонусы, монеты,
     свежий/домашний/вкусный (всегда с конкретикой), «к ужину», «на завтрак», «в выходные»,
     «к Новому году», «всего», «только», «уже».

   ▸ ЛЕКСИКА — НЕТ (ни в каком виде):
       • эксклюзивный, премиальный, элитный (если это не премиум-категория);
       • инновационный, революционный, уникальный;
       • осуществить, реализовать, приобрести;
       • оптимальный выбор, выгодное решение;
       • не упустите, спешите, торопитесь — бьёт по доверию;
       • «скидка N%» → ВСЕГДА «минус N%»;
       • восклицания-обращения «Ура!», «Привет!», «Друзья!», «Дорогие клиенты»;
       • штампы-наполнители: «придумали план», «отлично проводят время», «всё для старта уже есть».

   ▸ КАЛЬКИ И НЕЕСТЕСТВЕННЫЙ РУССКИЙ (главный маркер AI-стиля) — НЕТ:
       «ребёнок открыт на полную», «лето на максимум», «активирован каникулярный режим»,
       сухие заголовки через точку («Каникулы. Холодильник.» — звучит как заголовок таблицы).
       Фейково-бюрократическое: «режим дня официально уволен», «портфель торжественно запущен в угол»,
       «лето официально объявлено открытым», «мама — директор лагеря, повар и переговорщик одновременно».
       Тест-фраза: ТАК ГОВОРИТ ЖИВАЯ МАМА на кухне? Если нет — переписывай.

   ▸ ЗНАКИ И ОФОРМЛЕНИЕ:
       • Восклицательный — МАКСИМУМ 1 на пуш. В заголовке обычно НЕ нужен.
       • Многоточие «…» — НИКОГДА, ни в каком виде.
       • Капс — НЕТ.
       • Скобки — НЕТ, выноси в отдельное предложение.
       • Тире — длинное «—», между смысловыми блоками, НЕ дефис «-».
       • Эмодзи — 0 или 1 в конце, только если есть РЕАЛЬНАЯ причина (праздник, продукт).
       • Точки — нужны, дроби длинные предложения.

   ▸ ДЛИНЫ (БРЕНДОВЫЙ СТАНДАРТ — соблюдай эти лимиты):
       • push_title: ≤ 30 символов (включая эмодзи).
       • push_body:  ≤ 100 символов, 1–2 предложения.
       • content (описание серии / посадочный экран): ≤ 200 символов.
       • coupon_name: ≤ 35 символов.

   ▸ {push_must} Тон дружелюбный, разговорный, без канцелярита и без слова «клиент».
   ▸ Заголовок — СОБЫТИЕ/ВОПРОС/ИНТРИГА С ГЕРОЕМ из брифа (если бриф называет героев),
     а не объявление сезона и не сухой список существительных.
6b. 🔥 МЕХАНИКА ДОЛЖНА БЫТЬ ВИДНА — это контентный сериал С ВЫГОДОЙ, не чистый сторителлинг:
    • Если в «Механике акции (CTA)» указан процент / номинал — push_title ОБЯЗАН содержать его в виде «-N%» / «минус N%» / «-N₽»
      (например: «🍦Мороженое -20%», «🥐Круассан -20%», «🔥Третье блюдо -50%» — формат как в майском сериале готовой еды).
    • push_body ОБЯЗАН явно содержать связку «продукт + механика» одной короткой фразой в конце:
        «Выбери мороженое любимым — минус 20%»
        «Добавь в любимые товары — заберёшь с минус 20%»
        «Положи в любимые — минус 20%»
      Никаких «в ДИКСИ выгоднее» / «забери приятно» без явной цифры — клиент должен СРАЗУ видеть, что он получает.
    • Структура push_body: [1 короткое предложение со сценой, 1-2 конкретные детали — без перечислений] + [явный CTA с продуктом и -N%].
      Одна сцена → одна выгода → конец. Не нагромождай по три-четыре шутки подряд.
7. Веди клиента из описания серии в каталог с товарами: придумай короткое цепляющее название кнопки-перехода (поле "button", до ~20 символов).
8. Описание серии (поле "content") — СТРОГО до 200 символов (брендовый лимит). Это посадочный экран после клика по push.
   ✅ ЦЕЛЬ — чётко описать ОДНУ проблему сегмента и подвести к товару-решению. БЕЗ воды и стека шуток.
   Структура (2–3 коротких предложения, без воды):
   • Предложение 1 — конкретная проблема/сцена 1 фразой: «Дети съели мороженое до завтрака. Снова.» / «В 7 утра уже грохот на кухне.»
   • Предложение 2 — со 2-го выпуска: отсылка к прошлой серии в одной фразе («Помнишь, на прошлой неделе…»). В выпуске 1 — практический мини-совет.
   • Предложение 3 — конкретный товар-якорь из каталога + механика: «Выбери мороженое любимым — минус 20%.»
   ⛔️ В content НЕЛЬЗЯ: длинные пояснения времени («1 июня, 7:43 утра»), фейково-бюрократические конструкции (см. пункт 6a),
       перечисления из 3+ ролей мамы, нагромождение шуток одна на другую, спойлеры в духе «дети нашли новую цель — это холодильник».
   ✅ В content ОБЯЗАТЕЛЬНО: явное «выбери / добавь в любимые» + конкретный продукт + «минус N%» (если механика задана).
9. Включай практические советы, полезные для аудитории «{audience}».

ДЛЯ КАЖДОГО ВЫПУСКА верни:
- "title": название выпуска/серии (яркое, до ~50 символов) — то же мини-событие, что и в push_title
- "push_title": заголовок пуша (≤30 символов, 0-1 эмодзи) — МИНИ-СОБЫТИЕ + продукт + «-N%» (если механика задана), см. пункт 6b
- "push_body": текст пуша (≤100 символов, 1-2 предложения) — 1 сцена + явный CTA «Выбери [продукт] любимым — минус N%», см. пункт 6b. Со 2-й серии — отсылка к прошлой
- "content": описание серии для карточки в приложении (СТРОГО ≤200 символов, 2-3 коротких предложения) — без воды, без «официально уволен», явный CTA с продуктом и «минус N%»; слово «скидка» запрещено
- "button": короткое название кнопки-перехода в каталог с товарами (до ~20 символов)
- "coupon_name": короткое название информационного купона для МП (инфо-карточка к выпуску)
- "coupon_text": текст на информационном купоне / карточке — тёплый, тематический; механику акции можно упомянуть, но без выдуманных цен и условий; слова «скидка» НЕ использовать
- "image_prompt": ОТДЕЛЬНЫЙ УНИКАЛЬНЫЙ промпт на русском для нейросети-генератора картинки — свой для каждого выпуска.
  Он должен отражать сцену, героя и продукты именно ЭТОГО эпизода; не повторяй формулировки между выпусками.
  Опиши сцену, объекты, композицию, освещение и настроение; картинка современная и аппетитная.
  Стиль — премиальный, дорогой объёмный 3D-рендер в духе анимации Pixar. Используй ту же фирменную
  цветовую гамму бренда ДИКСИ, что и в примере (тёплые оранжевый, фиолетовый, зелёный).
  Картинка БЕЗ встроенного текста и логотипов.

  🚨 ФОРМАТ — СТРОГО ГОРИЗОНТАЛЬНЫЙ БАННЕР 21:9 (широкая, не квадратная и не вертикальная):
    • Соотношение сторон 21:9 (≈ 2.33:1) — широкоформатный кинематографический кадр.
    • Композиция растянута по горизонтали: герой/продукт смещён влево или вправо, по бокам пространство.
    • Камера «панорама»/«широкий план» (cinematic wide shot), а не портрет.
    • ❌ ЗАПРЕЩЕНО писать в image_prompt слова: «вертикальный», «portrait», «vertical», «9:16», «карточка приложения», «stories», «формат для приложения».
    • ✅ ОБЯЗАТЕЛЬНО заверши промпт фразой: «горизонтальный баннер 21:9, cinematic widescreen, широкая композиция».

Верни СТРОГО валидный JSON без пояснений:
{{
  "theme": "краткое описание темы и идеи рассылки",
  "audience": "{audience}",
  "weeks": [
    {{
      "week": 1,
      "date": "{schedule[0]['date'] if schedule else ''}",
      "title": "...",
      "push_title": "...",
      "push_body": "...",
      "content": "...",
      "button": "...",
      "coupon_name": "...",
      "coupon_text": "...",
      "image_prompt": "..."
    }}
  ]
}}
В массиве "weeks" должно быть ровно {n} элемент(ов) — по одному на каждый выпуск из графика, с правильными датами."""


def _build_polish_prompt(content_pack: dict, audience: str, brief: str, cta: str) -> str:
    """Пост-проход: humanizer + beautiful-prose + voice-builder-dixy.

    Принимает уже сгенерированный набор выпусков и переписывает каждое поле так,
    чтобы:
      • уложиться в лимиты длины (push_title ≤30, push_body ≤100, content ≤200);
      • убрать кальки/канцелярит/AI-маркеры;
      • привести имена героев из брифа в каждый выпуск;
      • согласовать тексты между собой (одна и та же категория, бренд, дедлайн);
      • не нарушать ToV ДИКСИ (обращение на «ты», «минус N%» вместо «скидка»).
    """
    weeks_json = json.dumps(content_pack.get("weeks", []), ensure_ascii=False, indent=2)
    brief_block = ""
    if (brief or "").strip():
        brief_block = (
            "\nБРИФ (приоритет — высокий):\n"
            f"{brief.strip()}\n"
            "Если в брифе названы конкретные герои (имена) — они ОБЯЗАНЫ присутствовать "
            "по имени в каждом выпуске (в push_title или в первой фразе push_body или в content). "
            "Не оставляй обобщённое «ребёнок», «дети», «он» — пиши имена.\n"
        )
    cta_line = f"Механика акции (CTA): «{cta}»." if cta else "Без явной механики."

    return f"""Ты — редактор-полировщик клиентских текстов ДИКСИ. На входе — сгенерированный нейросетью набор выпусков контентного сериала. Твоя задача — переписать каждое поле так, чтобы тексты звучали как живая речь, укладывались в лимиты и сочетались между собой.

АУДИТОРИЯ: {audience}
{cta_line}{brief_block}
═══════════════════════════════════════════
ЖЁСТКИЕ ЛИМИТЫ ДЛИНЫ (НЕ НАРУШАТЬ — это не рекомендация):
═══════════════════════════════════════════
  • push_title:  ≤ 30 символов (включая эмодзи и пробелы). Если больше — СОКРАЩАЙ.
  • push_body:   ≤ 100 символов, 1-2 предложения. Если больше — СОКРАЩАЙ.
  • content:     ≤ 200 символов, 2-3 коротких предложения. Если больше — СОКРАЩАЙ.
  • coupon_name: ≤ 35 символов.
  • button:      ≤ 20 символов.
Считай символы перед возвратом каждого поля. Превышение даже на 1 символ — брак, перепиши короче.

═══════════════════════════════════════════
HUMANIZER — убрать AI-стиль:
═══════════════════════════════════════════
СТОП-СЛОВА (удалять или заменять):
  • канцелярит: «осуществлять», «производить», «являться», «данный», «оптимальный», «уникальный»;
  • усилители-наполнители: «по-настоящему», «действительно», «огромный», «невероятный», «потрясающий»,
    «приятно сообщить», «спешим порадовать»;
  • AI-связки: «стоит отметить», «важно понимать», «в современном мире», «в нашем магазине вы найдёте».

ЗАМЕНЫ:
  • «Осуществить покупку» → «Купить»
  • «В рамках акции» → «По акции»
  • «Данный товар» → «Этот»
  • «Скидка 30%» → «Минус 30%» (СТРОГО)
  • «Спешим сообщить» → удалить, начать с пользы
  • «Не упустите шанс» → «Успей до …»

КАЛЬКИ И ГРАММАТИЧЕСКИЕ ОШИБКИ (исправлять):
  • «вошли во вкусу» → «вошли во вкус»;
  • «ребёнок открыт на полную», «лето на максимум» — переписать живым языком;
  • согласование рода/числа: если героев двое (Миша и Соня) — НЕ «он», а «они» или поимённо;
  • сухие заголовки через точку («Каникулы. Холодильник.») — переписать в живую фразу.

ПРИНЦИПЫ:
  • Глагол > отглагольное существительное.
  • Конкретное > общее.
  • Короткая фраза > длинная. Точка — друг.
  • Обращение на «ты», как сосед: «Купи», «Выбери», «Читай», «Переходи».

═══════════════════════════════════════════
BEAUTIFUL PROSE — согласованность набора:
═══════════════════════════════════════════
  • Категория названа одинаково во всех текстах (не «макароны» в push и «паста» в content).
  • Цифра выгоды повторяется не более 2 раз на выпуск.
  • Тон одинаков во всех выпусках (либо везде ирония, либо везде нейтральный).
  • Восклицательный — макс 1 на пуш. Многоточий «…» — НЕТ.
  • Эмодзи — 0 или 1 в конце push_title с реальной причиной.
  • Имена героев пишутся ОДИНАКОВО во всех выпусках (если Миша — то везде Миша, не «мальчик»/«ребёнок»).

═══════════════════════════════════════════
ИНСТРУКЦИЯ:
═══════════════════════════════════════════
Возьми каждый выпуск из исходного набора ниже и перепиши push_title, push_body, content, coupon_name,
coupon_text, button — соблюдая ВСЕ правила выше. Поля title, week, date, image_prompt оставь как есть.

Если поле уже короткое, грамотное, на «ты», с именами героев и без AI-стиля — оставь его без изменений.
Если что-то не так — ПЕРЕПИШИ.

ИСХОДНЫЙ НАБОР:
{weeks_json}

Верни СТРОГО валидный JSON без пояснений того же формата:
{{
  "weeks": [
    {{
      "week": 1,
      "date": "...",
      "title": "...",
      "push_title": "...",
      "push_body": "...",
      "content": "...",
      "button": "...",
      "coupon_name": "...",
      "coupon_text": "...",
      "image_prompt": "..."
    }}
  ]
}}"""


def _polish_content_pack(content_pack: dict, audience: str, brief: str,
                         cta: str, api_key: str) -> dict:
    """Прогнать сгенерированный набор через humanizer + beautiful-prose.

    Если что-то падает — возвращаем оригинал без полировки, чтобы не терять данные.
    """
    if not content_pack or not content_pack.get("weeks"):
        return content_pack
    try:
        prompt = _build_polish_prompt(content_pack, audience, brief, cta)
        polished = _parse_response(_anthropic_message(prompt, api_key, 4096))
        polished_weeks = polished.get("weeks") if isinstance(polished, dict) else None
        if not polished_weeks:
            return content_pack
        # Сливаем: то что вернул полировщик — побеждает; остальные поля (theme, audience и т.п.) — из исходника
        out = dict(content_pack)
        # Сохраняем те поля, которые полировщик пропустил (image_prompt и т.п.) — берём из оригинала
        original_by_week = {w.get("week"): w for w in content_pack.get("weeks", [])}
        merged = []
        for pw in polished_weeks:
            ow = original_by_week.get(pw.get("week"), {})
            merged_week = {**ow, **{k: v for k, v in pw.items() if v}}
            merged.append(merged_week)
        out["weeks"] = merged
        out["__polished"] = True
        return out
    except Exception:
        # Любая ошибка — возвращаем оригинал, не теряем результат основной генерации
        return content_pack


def generate_content_promo(promo: dict, schedule: list[dict], user_brief: str = "",
                           cta: str = None) -> dict:
    """Сгенерировать контент (выпуски + купоны + промпты картинок) для контентной акции через Claude.

    Два прохода:
      1. Основная генерация — создаёт набор выпусков.
      2. Полировальный пасс (humanizer + beautiful-prose) — переписывает поля с превышением
         лимита, чистит кальки/канцелярит, приводит имена героев и согласует тексты между собой.
    """
    key = ANTHROPIC_API_KEY
    if not key:
        raise ValueError("API ключ Anthropic не указан (нужен для генерации контента)")

    promo = _sanitize_promo(promo)
    prompt = _build_content_prompt(promo, schedule, user_brief, cta)
    raw = _parse_response(_anthropic_message(prompt, key, 4096))

    audience = _content_audience(_clean(promo.get("Сегмент")))
    if cta is None:
        cta = CONTENT_DEFAULT_CTA
    polished = _polish_content_pack(raw, audience, user_brief or "", cta or "", key)
    return polished


# ═══════════════════════════════════════════════════════════════════════════════
# Сервисные пуши: «Баланс баллов» (регулярное напоминание о монетах)
# ═══════════════════════════════════════════════════════════════════════════════

def is_balance_promo(promo: dict) -> bool:
    """«Баланс баллов» — регулярная сервисная коммуникация о монетах на карте.

    Это НЕ акция: без скидки, без кешбэка, без чека, без категории.
    Цель — напомнить клиенту, сколько у него монет, и мягко мотивировать потратить.
    """
    name = _clean(promo.get("Название промо")).lower()
    if not name:
        return False
    # Главный маркер — «баланс баллов» / «баланс монет» в названии
    if "баланс баллов" in name or "баланс монет" in name:
        return True
    # Иногда зовут просто «Баланс» + слово «монет»/«балл» в описании/купоне
    if name.startswith("баланс") or " баланс" in name:
        desc = (_clean(promo.get("Описание акции")) + " "
                + _clean(promo.get("Текст на информационном купоне / слип-чеке"))).lower()
        if "монет" in desc or "балл" in desc:
            return True
    return False


def _build_balance_prompt(promo: dict, schedule: list[dict],
                          num_variants: int = 3,
                          title_max_len: int = 35,
                          body_max_len: int = 120,
                          rules: str = "") -> str:
    """Промпт для генерации СЕРВИСНОГО push «Баланс баллов».

    Стиль строго по историческим примерам из PUSH-таблицы:
      • заголовок ВСЕГДА содержит плейсхолдер X (CRM подставит реальную сумму
        баланса каждому клиенту), формат: «emoji + Xр. монетами на счету»
        или «emoji + На счете Xр. монетами»;
      • body — короткая фраза-контекст + образовательное «1 монета = 1₽»
        + ЯВНЫЙ CTA (Приходи / Используй / Закажи в приложении ДИКСИ);
      • тон лёгкий, дружелюбный, без распродажных штампов «Закупка недели?» и т.п.;
      • программа лояльности = «монеты», не «бонусы», не «баллы».
    """
    name = _clean(promo.get("Название промо"))
    segment = _clean(promo.get("Сегмент"))
    num = _clean(promo.get("НОМЕР"))

    # Сезон / месяц push — пригодится для контекстной фразы в body
    season_hint = ""
    month_hint = ""
    if schedule:
        try:
            d0 = schedule[0].get("date_obj") or _parse_date(schedule[0].get("date"))
            if d0:
                m = d0.month
                season_hint = ("зима" if m in (12, 1, 2) else "весна" if m in (3, 4, 5)
                               else "лето" if m in (6, 7, 8) else "осень")
                _MO = {1:"январь",2:"февраль",3:"март",4:"апрель",5:"май",6:"июнь",
                       7:"июль",8:"август",9:"сентябрь",10:"октябрь",11:"ноябрь",12:"декабрь"}
                month_hint = _MO.get(m, "")
        except Exception:
            pass

    schedule_text = ""
    for i, s in enumerate(schedule, 1):
        stype = s.get("type", "start")
        schedule_text += f"  Push #{i}: дата {s['date']}, время {s.get('time','12:00')}, тип: {stype}\n"

    rules_block = ""
    if (rules or "").strip():
        rules_block = (
            "\nДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА ОТ ПОЛЬЗОВАТЕЛЯ (приоритет выше общих, "
            "но не нарушают запреты ниже):\n"
            f"{rules.strip()}\n"
        )

    return f"""Ты — копирайтер сети магазинов ДИКСИ. Готовишь СЕРВИСНЫЙ push «Баланс баллов» — ежемесячное напоминание клиенту о монетах на его карте программы лояльности.

ВАЖНО ПРО X: это НЕ выдуманное число. X — это ПЛЕЙСХОЛДЕР, который CRM подставит каждому клиенту его реальной суммой баланса. Ты ОБЯЗАН использовать букву X (заглавную латинскую X) как placeholder в заголовке. НЕ подставляй конкретное число.

ДАННЫЕ ПУША:
- Номер промо: {num}
- Название: {name}
- Сегмент: {segment}
- Сезон/месяц: {season_hint} / {month_hint}

РАСПИСАНИЕ PUSH:
{schedule_text}
{rules_block}
═══════════════════════════════════════════
ШАБЛОН ЗАГОЛОВКА (СТРОГО по историческим образцам):
═══════════════════════════════════════════
Формат: «<эмодзи><Xр. монетами на счету>» или «<эмодзи>На счете Xр. монетами».

Исторические одобренные заголовки (используй ИМЕННО ТАКУЮ структуру, меняй только эмодзи под сезон/настроение):
  • «✨Xр. монетами на счету»
  • «💸Xр. монетами на счету»
  • «🍌На счете Xр. монетами»
  • «🎉На счете Xр. монетами»
  • «💰Xр. монетами на счету»
  • «🛒На счете Xр. монетами»

Эмодзи в начале — ОДИН, релевантный сезону/настроению ({season_hint or 'нейтральный'}).
Заглавная X — обязательно. После X — «р.», не «₽».

═══════════════════════════════════════════
ШАБЛОН BODY — должен ЧИТАТЬСЯ ПРОДОЛЖЕНИЕМ ЗАГОЛОВКА:
═══════════════════════════════════════════
ВАЖНО: заголовок + body вместе складываются в одну живую русскую фразу — без рассинхрона.

Пример (читать вслух как одно предложение):
  Заголовок: «🍓Xр. монетами на счету»
  Body:      «на любые летние покупки в ДИКСИ: свежие овощи, ягоды, мясо для шашлыка. 1 монета = 1₽. Приходи в ДИКСИ»
  Вслух:     «X рублей монетами на счету на любые летние покупки в ДИКСИ: свежие овощи, ягоды, мясо для шашлыка. Одна монета равно одному рублю. Приходи в ДИКСИ.» — ЭТО ПРАВИЛЬНЫЙ РУССКИЙ.

СТРУКТУРА BODY (СТРОГО в этом порядке):
  1. «на любые <ЛЕТНИЕ/ОСЕННИЕ/ЗИМНИЕ/ВЕСЕННИЕ> покупки в ДИКСИ:» — начинай с МАЛЕНЬКОЙ буквы, потому что это продолжение заголовка. Двоеточие ОБЯЗАТЕЛЬНО.
  2. <2–4 АППЕТИТНЫХ конкретных сезонных продукта через запятую>.
  3. «1 монета = 1₽.» — отдельным предложением.
  4. <ЯВНЫЙ CTA> — отдельным коротким предложением.

Body должен быть ПЛОТНЫМ, 120–{body_max_len} символов. НЕ короче 100. CTA — обязателен. У этого сервисного пуша лимит body = {body_max_len} (а не 120), не экономь — нужно уместить и продукты, и обе опции CTA, и «1 монета = 1₽».

Сезонное прилагательное (часть 1) выбирай по сезону ({season_hint or 'не определён'}):
  лето → «летние», осень → «осенние», зима → «зимние», весна → «весенние».

ЗАПРЕЩЕНО:
  • Начинать body С ЗАГЛАВНОЙ или с продуктов через тире — это рвёт связь с заголовком. НЕПРАВИЛЬНО: «Клубника, черешня — на любые покупки.» ПРАВИЛЬНО: «на любые летние покупки в ДИКСИ: клубника, черешня…».
  • Слова-абстракции БЕЗ конкретики после них («летние покупки», «зимние закупки» без двоеточия и списка) — БРАК.
  • Голое «мясо», голое «сыр», голое «фрукты» — слишком абстрактно. Нужно «мясо для шашлыка», «мясо для запекания», «праздничные сыры», «свежие овощи и фрукты», «спелые ягоды», «свежая зелень».
  • СМЕШИВАТЬ ОБЩУЮ КАТЕГОРИЮ И ЕЁ ЧАСТНЫЙ СЛУЧАЙ В ОДНОМ СПИСКЕ. Нельзя: «ягоды, клубника» (клубника — это ягода), «фрукты, яблоки», «мясо, курица», «овощи, помидоры», «сыры, моцарелла». Нужно: ИЛИ общее, ИЛИ частное — но не оба сразу.
  • Перечисление 3–4 непересекающихся позиций, объединённых через «,» и «и» перед последней: «свежие овощи, фрукты и ягоды» (а НЕ «свежие овощи, ягоды, клубника»).
  • Выдуманные цены/проценты («-40% на сосиски», «скидки до 50%»).
  • Конкретные бренды и СТМ-линейки.

═══════════════════════════════════════════
АППЕТИТНЫЕ ПРОДУКТЫ ПО МЕСЯЦАМ (3 непересекающиеся позиции):
═══════════════════════════════════════════
Текущий месяц push: {month_hint or '—'}.

Бери ровно 3 позиции из списка ниже (или эквивалентные по логике), соединяй через «,» и «и» перед последней. НЕ смешивай общую категорию и её частный случай (не «ягоды, клубника» — клубника это ягода).

  • ИЮНЬ — свежие овощи, ягоды и мясо для шашлыка
  • ИЮЛЬ — арбузы, мороженое и мясо для шашлыка
  • АВГУСТ — арбузы, дыни и виноград
  • СЕНТЯБРЬ — яблоки, груши и тыква
  • ОКТЯБРЬ — тыква, яблоки и выпечка
  • НОЯБРЬ — горячий кофе, чай и выпечка
  • ДЕКАБРЬ — мандарины, сыры и оливье
  • ЯНВАРЬ — выпечка, сыры и горячий кофе
  • ФЕВРАЛЬ — выпечка, шоколад и мясо для запекания
  • МАРТ — свежие фрукты, выпечка и цветы
  • АПРЕЛЬ — творог, яйца и зелень
  • МАЙ — мясо для шашлыка, свежие овощи и зелень

ПРИМЕРЫ ПРАВИЛЬНЫХ body (по нужной структуре):
  • (июнь) «на любые летние покупки: свежие овощи, ягоды и мясо для шашлыка. 1 монета = 1₽. Приходи в ДИКСИ или закажи доставку в приложении.»
  • (декабрь) «на любые зимние покупки в ДИКСИ: мандарины, сыры и оливье. 1 монета = 1₽. Зайди в магазин или закажи доставку в приложении.»
  • (сентябрь) «на любые осенние покупки в ДИКСИ: яблоки, груши и тыква. 1 монета = 1₽. Купи в магазине или закажи доставку в приложении.»

ВАЖНО про повторение «ДИКСИ» (правило beautiful-prose):
  • «ДИКСИ» в body должно встречаться РОВНО один раз. Либо в первой фразе («на любые летние покупки в ДИКСИ: …»), либо в CTA («Приходи в ДИКСИ или …») — но НЕ в обеих сразу. Заголовок «Xр. монетами на счету» — нейтральный, в нём ДИКСИ нет.

CTA — обязательный, ЯВНЫЙ, отдельным коротким предложением. ВСЕГДА предлагай ОБЕ опции (оффлайн + онлайн), потому что баланс монет работает и в магазине, и в приложении:
  • «Приходи в ДИКСИ или закажи доставку в приложении» (ПРЕДПОЧТИТЕЛЬНЫЙ)
  • «Зайди в магазин или закажи в приложении»
  • «Жми в каталог и выбирай» (если нужно покороче)

НЕ оставляй только одну опцию («Приходи в ДИКСИ» без онлайна или «Закажи доставку» без оффлайна) — клиенту нужно понять, что монеты работают и там, и там.

═══════════════════════════════════════════
СТРОГИЕ ЗАПРЕТЫ (нарушение = брак):
═══════════════════════════════════════════
1. НЕ выдумывай конкретное число баланса — это всегда X.
2. НЕ ИСПОЛЬЗОВАТЬ маркетинговые штампы дискаунт-пушей: «Закупка недели?», «Самое время», «Заходи — порадуешься», «Лови».
3. НЕ писать «Дарим монеты» / «Получи X монет» / «+X монетами на карту» — мы НЕ дарим, мы напоминаем.
4. НЕ выдумывать конкретные цены, проценты скидок, SKU, бренды, которых нет в данных. «-40% на сосиски» из исторического примера — это исторический пример, НЕ копируй цифры; общую идею «ещё и цены/скидки» — можно.
5. Программа лояльности = «монеты». НЕ «бонусы», НЕ «баллы», НЕ «бонусные рубли». «1 монета = 1₽» (или «1 монета = 1р.»).
6. Только кириллица. Обращение на «ты», без тыканья в заголовке.
7. Body НЕ ДОЛЖЕН быть короче 100 символов. CTA обязателен. Используй доступный лимит {body_max_len} знаков по максимуму — добавляй продукты, обе опции CTA, не «обрезай» в угоду компактности.

═══════════════════════════════════════════
ЖЁСТКИЕ ОГРАНИЧЕНИЯ ДЛИНЫ:
═══════════════════════════════════════════
- Заголовок: СТРОГО до {title_max_len} символов (включая эмодзи и X).
- Текст body: СТРОГО до {body_max_len} символов (включая эмодзи). И НЕ короче 100 симв.

═══════════════════════════════════════════
ЗАДАНИЕ:
═══════════════════════════════════════════
Для каждого push из расписания выше сгенерируй {num_variants} разных вариантов
(меняй эмодзи / контекстную фразу / CTA-формулировку, но СТРУКТУРА «X монетами на счету / 1 монета = 1₽ / явный CTA» — одинаковая во всех).

Ответь СТРОГО в JSON (без markdown, без ```):
{{
  "pushes": [
    {{
      "push_number": 1,
      "date": "...",
      "time": "...",
      "variants": [
        {{"title": "заголовок с X", "title_length": 25, "body": "текст с CTA и 1 монета = 1₽", "body_length": 110}}
      ]
    }}
  ]
}}"""


def generate_balance_promo(promo: dict, schedule: list[dict],
                           num_variants: int = 3,
                           title_max_len: int = 35,
                           body_max_len: int = 120,
                           rules: str = "",
                           provider: str = None,
                           anthropic_key: str = None,
                           openai_key: str = None) -> dict:
    """Сгенерировать сервисный push «Баланс баллов»."""
    provider = provider or AI_PROVIDER or "anthropic"
    # Сервисный пуш «Баланс баллов» — длинный сервисный текст, в нём нужно
    # уместить и продукты, и образовательное «1 монета = 1₽», и обе опции CTA.
    # Поднимаем лимит body до 200 (а не общих 120 для маркетинговых пушей).
    body_max_len = max(body_max_len, 200)
    prompt = _build_balance_prompt(promo, schedule, num_variants,
                                   title_max_len, body_max_len, rules)

    if provider == "anthropic":
        key = anthropic_key or ANTHROPIC_API_KEY
        if not key:
            raise ValueError("API ключ Anthropic не указан")
        return _call_anthropic(prompt, key)
    elif provider == "openai":
        key = openai_key or OPENAI_API_KEY
        if not key:
            raise ValueError("API ключ OpenAI не указан")
        return _call_openai(prompt, key)
    else:
        # builtin: 3 варианта по правилу
        # title:  «<emoji>Xр. монетами на счету»
        # body :  «на любые <сезон> покупки в ДИКСИ: <2-3 продукта>. 1 монета = 1₽. <CTA>»
        # Заголовок + body читаются одним предложением.
        _MONTH_INFO = {
            # (emoji, "сезон-прилагательное", "3 непересекающихся аппетитных продукта по месяцу")
            # Правило: НЕ смешиваем общую категорию и её частный случай в одном списке
            # (нельзя «ягоды, клубника» — клубника это ягода).
            1:  ("🧀", "зимние",  "выпечка, сыры и горячий кофе"),
            2:  ("🍫", "зимние",  "выпечка, шоколад и мясо для запекания"),
            3:  ("💐", "весенние", "свежие фрукты, выпечка и цветы"),
            4:  ("🥚", "весенние", "творог, яйца и зелень"),
            5:  ("🍖", "весенние", "мясо для шашлыка, овощи и зелень"),
            6:  ("🍓", "летние",  "свежие овощи, ягоды и мясо для шашлыка"),
            7:  ("🍉", "летние",  "арбузы, мороженое и мясо для шашлыка"),
            8:  ("🍇", "летние",  "арбузы, дыни и виноград"),
            9:  ("🍎", "осенние", "яблоки, груши и тыква"),
            10: ("🎃", "осенние", "тыква, яблоки и выпечка"),
            11: ("☕", "осенние", "горячий кофе, чай и выпечка"),
            12: ("🎄", "зимние",  "мандарины, сыры и оливье"),
        }
        month = 0
        if schedule:
            try:
                d0 = schedule[0].get("date_obj") or _parse_date(schedule[0].get("date"))
                if d0:
                    month = d0.month
            except Exception:
                pass
        emoji, season_adj, tops = _MONTH_INFO.get(
            month, ("💰", "", "свежие овощи, фрукты, выпечка"))

        season_phrase = f"на любые {season_adj} покупки в ДИКСИ" if season_adj else "на любые покупки в ДИКСИ"

        def _mk_variants():
            # CTA — везде обе опции (оффлайн + онлайн).
            # Правило beautiful-prose: «ДИКСИ» должно встречаться в body РОВНО один раз
            # (или в первой фразе, или в CTA — не в обеих сразу).
            # v1 — без «в ДИКСИ» в первой фразе, есть в CTA.
            # v2/v3 — с «в ДИКСИ» в первой фразе, без него в CTA.
            season_no_dixy = season_phrase.replace(" в ДИКСИ", "")
            v1_t = f"{emoji}Xр. монетами на счету"
            v1_b = f"{season_no_dixy}: {tops}. 1 монета = 1₽. Приходи в ДИКСИ или закажи доставку в приложении."
            v2_t = "🛒На счете Xр. монетами"
            v2_b = f"{season_phrase}: {tops}. 1 монета = 1₽. Зайди в магазин или закажи доставку в приложении."
            v3_t = "💎Xр. монетами на счету"
            v3_b = f"{season_phrase}: {tops}. 1 монета = 1₽. Купи в магазине или закажи доставку в приложении."
            out = []
            for t, b in [(v1_t, v1_b), (v2_t, v2_b), (v3_t, v3_b)]:
                if num_variants <= len(out):
                    break
                out.append({"title": t, "title_length": len(t),
                            "body": b, "body_length": len(b)})
            return out

        pushes = []
        for i, s in enumerate(schedule, 1):
            pushes.append({
                "push_number": i,
                "date": s.get("date", ""),
                "time": s.get("time", "10:00"),
                "variants": _mk_variants(),
            })
        return {"pushes": pushes}
