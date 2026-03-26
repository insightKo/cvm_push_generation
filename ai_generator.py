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
    # 1. Точное совпадение
    exact = df[df["_search"] == query]
    if not exact.empty:
        r = exact.iloc[0]
        return {"id": int(r["Id"]), "category": r["КАТЕГОРИЯ"], "deeplink": r["deeplink"]}

    # 2. Поиск по вхождению ключевых слов
    keywords = re.findall(r"[а-яёa-z]{3,}", query)
    best_score = 0
    best_row = None
    for _, r in df.iterrows():
        cat = r["_search"]
        score = 0
        for kw in keywords:
            if kw in cat:
                score += len(kw)  # Длинные слова = больше вес
        if score > best_score:
            best_score = score
            best_row = r

    if best_row is not None and best_score >= 3:
        return {"id": int(best_row["Id"]), "category": best_row["КАТЕГОРИЯ"], "deeplink": best_row["deeplink"]}

    return None


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

    Правило 10:
      10.1. один день (duration=0) — 1 push в день акции
      10.2. два+ дня — 2 push: 1-й день (старт) + последний день (напоминание)
            Между стартом и напоминанием минимум 1 день разницы.
            Если разница = 1 день → напоминание = конец.
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
        # 10.1: один-два дня — одно сообщение на старт
        schedule.append({
            "date": start.strftime("%d.%m.%Y"),
            "time": "10:00",
            "date_obj": start,
            "type": "start",
        })

    else:
        # 10.2: три+ дня — старт + последний день
        # Push 1: первый день акции
        schedule.append({
            "date": start.strftime("%d.%m.%Y"),
            "time": "10:00",
            "date_obj": start,
            "type": "start",
        })
        # Push 2: последний день акции (напоминание)
        # Гарантируем минимум 1 день между push-ами
        reminder = end
        if (reminder - start).days < 1:
            reminder = start + timedelta(days=1)
        schedule.append({
            "date": reminder.strftime("%d.%m.%Y"),
            "time": "11:00",
            "date_obj": reminder,
            "type": "reminder",
        })

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

    # ── 2. Ищем ПРОЦЕНТ в названии/механике ──
    pct_match = re.search(r"(\d+)\s*%", name)
    is_cashback = any(kw in all_text_lower for kw in ("кешбэк", "кэшбэк", "cashback", "монет"))

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


def _build_product_details(category: str, products_text: str, coupon_text: str) -> str:
    """Собрать расшифровку товаров из ВСЕХ совпавших категорий + купона.

    Для акции «бытовая химия и средства для ухода за обувью» найдёт и химию, и обувь.
    Дедуплицирует отдельные товары.
    """
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
    "обув": "👟", "мыл": "🧴", "шампун": "💇", "гигиен": "🧴",
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
            " Для всей семьи 👨‍👩‍👧‍👦",
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
            " Для настоящих гурманов 👨‍🍳",
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
        cta = "АКТИВИРУЙ акцию"
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
        "end": end_str or "конца акции",
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

        # Выбираем пулы ПАР (заголовок + текст, грамматически согласованные)
        if push_type == "reminder":
            if benefit["type"] == "cashback":
                pairs_pool = list(_PAIRS_REMIND_CASHBACK)
            elif benefit["type"] == "discount":
                pairs_pool = list(_PAIRS_REMIND_DISCOUNT)
            elif benefit["type"] == "bonus":
                pairs_pool = list(_PAIRS_REMIND_BONUS)
            else:
                pairs_pool = list(_PAIRS_REMIND_GENERAL)
        else:
            if benefit["type"] == "cashback":
                pairs_pool = list(_PAIRS_START_CASHBACK)
            elif benefit["type"] == "discount":
                pairs_pool = list(_PAIRS_START_DISCOUNT)
            elif benefit["type"] == "bonus":
                if condition_check:
                    pairs_pool = list(_PAIRS_START_BONUS_WITH_CHECK)
                else:
                    pairs_pool = list(_PAIRS_START_BONUS)
            else:
                pairs_pool = list(_PAIRS_START_GENERAL)

            # Подмешиваем ситуационные пары (обувь, шоколад и т.д.)
            # для стартовых push — они дают разнообразие и юмор
            cat_lower = (cat["category"] + " " + cat["products_text"]).lower()
            for kw, sit_pairs in _SITUATIONAL_PAIRS.items():
                if kw in cat_lower:
                    pairs_pool.extend(sit_pairs)

            # Пятничные / выходные пары — подмешиваем для атмосферы
            if is_friday and benefit["type"] == "cashback":
                pairs_pool.extend([
                    ("{emoji}Пятничный кешбэк {value}",
                     "{details} {date_context}. {condition}Лучший повод закупиться к выходным!{promo_code} {purchase_ctx}{cta}"),
                    ("{emoji}Пятница + {value} кешбэк",
                     "{details} {date_context}. {condition}Выходные начинаются с выгоды!{promo_code} {purchase_ctx}{cta}"),
                ])
            elif is_friday and benefit["type"] == "discount":
                pairs_pool.extend([
                    ("{emoji}Пятничная скидка {value}",
                     "{details} {date_context}. {condition}Закупаемся к выходным!{promo_code} {purchase_ctx}{cta}"),
                ])
            elif is_weekend and benefit["type"] == "cashback":
                pairs_pool.extend([
                    ("{emoji}Выходной + кешбэк {value}",
                     "{details} {date_context}. {condition}Выходные с выгодой!{promo_code} {purchase_ctx}{cta}"),
                ])
            elif is_weekend and benefit["type"] == "discount":
                pairs_pool.extend([
                    ("{emoji}Выходная скидка {value}",
                     "{details} {date_context}. {condition}Время закупок!{promo_code} {purchase_ctx}{cta}"),
                ])

        random.shuffle(pairs_pool)

        variants = []
        for vi in range(num_variants):
            # Генерируем юмор (правило 6)
            humor = _get_humor(cat["category"], cat["products_text"])

            fill_vi = {**fill, "humor": humor, "cta": cta}

            pair = pairs_pool[vi % len(pairs_pool)]
            title_tmpl, body_tmpl = pair

            try:
                title = title_tmpl.format(**fill_vi)
            except (KeyError, IndexError):
                title = title_tmpl
            try:
                body = body_tmpl.format(**fill_vi)
            except (KeyError, IndexError):
                body = body_tmpl

            # Чистим двойные пробелы
            title = re.sub(r"  +", " ", title).strip()
            body = re.sub(r"  +", " ", body).strip()

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


def _call_anthropic(prompt: str, api_key: str) -> dict:
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    return _parse_response(message.content[0].text)


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

    # Поиск подходящего deeplink из справочника
    deeplink_hint = ""
    dl = find_best_deeplink(name)
    if dl:
        deeplink_hint = f"\nПОДХОДЯЩИЙ DEEPLINK ИЗ СПРАВОЧНИКА:\n  Категория: {dl['category']} (ID: {dl['id']})\n  Deeplink: {dl['deeplink']}\n  Используй этот deeplink в поле «Кнопка» если акция на эту категорию.\n"

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

    prompt = f"""Ты — менеджер CVM-программы сети магазинов ДИКСИ. Нужно заполнить условия новой акции.
{deeplink_hint}{weekend_hint}
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

ПРАВИЛА:
1. «Описание акции» — расшифровка: «Вернём 20% монетами за покупку зелени и овощей», «Скидка 10% на зубную пасту»
2. «Скидка» — число или процент если это скидочная акция (10%, 50р.), пусто если кешбэк/бонус
3. «Бонусы» — число или процент если кешбэк/бонус (20%, 100), пусто если скидка
4. «Механика» — «активация» если в названии «Активируй», «автоматическая» если нет
5. «Категория» — реальная категория товаров из названия: «зелень и овощи», «зубная паста», «яйца», «все товары»

6. «Срок сгорания бонусов» — ТОЛЬКО для бонусных/кешбэк акций. Для скидочных — пусто.
   РАЗНЫЕ ПРАВИЛА РАСЧЁТА В ЗАВИСИМОСТИ ОТ ТИПА:
   а) Акцептные монеты («Акцептные N монет»): срок = дата окончания акции + 1 день. Пример: акция до 19.04 → 20.04.2026 23:59:00
   б) Кешбэк (с активацией и без): срок = дата окончания акции + 7 дней. НО если акция заканчивается после 20-го числа месяца, то срок = ПРЕДПОСЛЕДНИЙ день этого месяца.
   Примеры кешбэка: акция до 05.04 → 12.04.2026 23:59:00; акция до 26.04 → 29.04.2026 23:59:00; акция до 03.05 → 10.05.2026 23:59:00.
   Формат: «DD.MM.YYYY 23:59:00»

7. «Название информационного купона для МП» — КОРОТКОЕ название, БЕЗ дублирования категории!
   - Для скидки: «Активируй скидку 10%»
   - Для кешбэка с активацией: «Активируй 20% кешбэк»
   - Для кешбэка без активации: «Вернём 100%» или «50% кешбэк» — БЕЗ категории товаров (категория будет в тексте купона)
   - Для подарка монет: «Активируй 50 монет»
   ВАЖНО: НЕ дублируй категорию в названии и тексте купона одновременно!

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
   ФОРМАТИРОВАНИЕ ТЕКСТА КУПОНА:
   Используй переносы строк (\n) для разделения смысловых блоков и \n\n для абзацев — как в реальных купонах.
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

   д) Скидка (однодневная или многодневная):
      Текст СТРОГО по шаблону реальных акций:
      «на [конкретные товары] и другие [категория].
      \n\nСкидка действует только [срок], [дата полностью: ДД месяц ГГГГ]
      \nc картой клуба Друзей Дикси на кассе магазина
      \nили на онлайн покупки в приложении ДИКСИ (Доставка от 40 мин)»
      Пример для 1 дня: «на апельсиновый, яблочный, томатный и другие соки, а также воду.\n\nСкидка действует только 1 день, 15 апреля 2026\nc картой клуба Друзей Дикси на кассе магазина\nили на онлайн покупки в приложении ДИКСИ (Доставка от 40 мин)»

9. РАСШИФРОВКА ТОВАРОВ: Обязательно перечисляй КОНКРЕТНЫЕ товары внутри категории!
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

14. «Кнопка» — текст кнопки + deeplink. Формат: «ТЕКСТ КНОПКИ deeplink».
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

    import anthropic
    client = anthropic.Anthropic(api_key=key)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    return _parse_response(message.content[0].text)


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
