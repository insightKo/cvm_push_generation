"""Парсер каталога ДИКСИ — поиск скидок по категориям."""

import re
import requests
from functools import lru_cache

BASE_URL = "https://dixy.ru"

# Маппинг категорий акций → slug каталога ДИКСИ
_CATEGORY_SLUGS = {
    # Продукты
    "овощ": "ovoshchi-frukty", "фрукт": "ovoshchi-frukty", "зелен": "ovoshchi-frukty",
    "банан": "ovoshchi-frukty", "яблок": "ovoshchi-frukty", "картоф": "ovoshchi-frukty",
    "помидор": "ovoshchi-frukty", "огурц": "ovoshchi-frukty",
    "молок": "molochnye-produkty-yaytsa", "кефир": "molochnye-produkty-yaytsa",
    "сметан": "molochnye-produkty-yaytsa", "творог": "molochnye-produkty-yaytsa",
    "йогурт": "molochnye-produkty-yaytsa", "яйц": "molochnye-produkty-yaytsa",
    "сыр": "syry",
    "мяс": "myaso-ptitsa", "говядин": "myaso-ptitsa", "свинин": "myaso-ptitsa",
    "фарш": "myaso-ptitsa", "птиц": "myaso-ptitsa", "курин": "myaso-ptitsa",
    "куриц": "myaso-ptitsa",
    "колбас": "myasnaya-gastronomiya", "сосис": "myasnaya-gastronomiya",
    "ветчин": "myasnaya-gastronomiya",
    "хлеб": "khleb-i-vypechka", "выпечк": "khleb-i-vypechka", "кулич": "khleb-i-vypechka",
    "рыб": "ryba-moreprodukty-ikra", "лосос": "ryba-moreprodukty-ikra",
    "сёмг": "ryba-moreprodukty-ikra", "морепродукт": "ryba-moreprodukty-ikra",
    "круп": "bakaleya", "гречк": "bakaleya", "рисов": "bakaleya", "макарон": "bakaleya",
    "мук": "bakaleya", "масло подсолн": "bakaleya", "масло растит": "bakaleya",
    "сахар": "bakaleya",
    "консерв": "konservy",
    "заморож": "zamorojennye-produkty", "пельмен": "zamorojennye-produkty",
    "полуфабрикат": "zamorojennye-produkty", "морожен": "zamorojennye-produkty",
    "соус": "sousy-spetsii", "кетчуп": "sousy-spetsii", "майонез": "sousy-spetsii",
    "специ": "sousy-spetsii",
    "конфет": "konditerskie-izdeliya-torty", "шокол": "konditerskie-izdeliya-torty",
    "торт": "konditerskie-izdeliya-torty", "печень": "konditerskie-izdeliya-torty",
    "вафл": "konditerskie-izdeliya-torty",
    "чипс": "chipsy-orekhi-i-sneki", "снек": "chipsy-orekhi-i-sneki",
    "орех": "chipsy-orekhi-i-sneki", "сухар": "chipsy-orekhi-i-sneki",
    "чай": "chay-kofe-kakao", "кофе": "chay-kofe-kakao", "какао": "chay-kofe-kakao",
    "вод": "voda-soki-napitki", "сок": "voda-soki-napitki", "напит": "voda-soki-napitki",
    "газировк": "voda-soki-napitki", "лимонад": "voda-soki-napitki",
    "детск": "detskoe-pitanie",
    "животн": "tovary-dlya-jivotnykh", "корм": "tovary-dlya-jivotnykh",
    "кошач": "tovary-dlya-jivotnykh", "собач": "tovary-dlya-jivotnykh",
    "готов": "gotovaya-eda", "перекус": "gotovaya-eda", "салат": "gotovaya-eda",
    "здоров": "dlya-zdorovogo-pitaniya", "пп ": "dlya-zdorovogo-pitaniya",
    # Напитки
    "пив": "pivo-i-piv-napitki", "вин": "vino-i-igristoe",
    "игрист": "vino-i-igristoe", "просекк": "vino-i-igristoe",
    "алкогол": "krepkiy-alkogol", "водк": "krepkiy-alkogol",
    "виск": "krepkiy-alkogol", "коньяк": "krepkiy-alkogol",
    # Бытовые
    "хими": "bytovaya-khimiya", "бытов": "bytovaya-khimiya",
    "стирк": "bytovaya-khimiya", "моющ": "bytovaya-khimiya",
    "зуб": "gigiena-i-ukhod", "паст": "gigiena-i-ukhod",
    "шампун": "gigiena-i-ukhod", "гигиен": "gigiena-i-ukhod",
}


def _get_session() -> requests.Session:
    """Создать сессию с дефолтным магазином Москва."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://dixy.ru/catalog/",
    })
    # Установить магазин (Москва, центр)
    try:
        s.get(f"{BASE_URL}/catalog/", timeout=10)
        s.post(f"{BASE_URL}/ajax/ajax.php", data={
            "action": "setCity",
            "city": "Москва",
            "lat": "55.7558",
            "lon": "37.6173",
        }, timeout=10)
    except Exception:
        pass
    return s


def _find_slug(promo_text: str) -> str | None:
    """Найти slug каталога по тексту акции."""
    text = promo_text.lower()
    best_slug = None
    best_len = 0
    for keyword, slug in _CATEGORY_SLUGS.items():
        if keyword in text and len(keyword) > best_len:
            best_slug = slug
            best_len = len(keyword)
    return best_slug


def _get_sid(session: requests.Session, slug: str) -> int | None:
    """Получить sid категории из HTML страницы."""
    try:
        resp = session.get(f"{BASE_URL}/catalog/{slug}/", timeout=10)
        m = re.search(r'"sid"\s*:\s*(\d+)', resp.text)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def _fetch_products(session: requests.Session, sid: int, max_pages: int = 3) -> list[dict]:
    """Загрузить товары категории через JSON API."""
    all_products = []
    for page in range(1, max_pages + 1):
        try:
            resp = session.get(
                f"{BASE_URL}/ajax/listing-json.php",
                params={
                    "block": "product-list",
                    "sid": sid,
                    "perPage": 30,
                    "page": page,
                },
                timeout=15,
            )
            data = resp.json()
            if not data.get("response"):
                break
            cards = data["response"][0].get("cards", []) if data["response"] else []
            if not cards:
                break
            all_products.extend(cards)

            pagen = data["response"][0].get("pagenData", {})
            if pagen.get("isLastPage", True):
                break
        except Exception:
            break
    return all_products


def _fetch_discounts_page(session: requests.Session, max_pages: int = 5) -> list[dict]:
    """Загрузить страницу 'Скидки по карте' (sid=839)."""
    return _fetch_products(session, 839, max_pages)


def search_discounts(promo_text: str, use_discount_page: bool = True) -> list[dict]:
    """Найти скидки по тексту акции.

    Returns: список dict с полями:
        - name: название товара
        - price: текущая цена
        - old_price: старая цена
        - discount: скидка в %
        - discount_rub: скидка в рублях
        - by_card: скидка по карте (True/False)
        - date_to: дата окончания скидки (если есть)
        - brand: бренд
        - weight: вес/объём
        - image: URL картинки
    """
    session = _get_session()

    # 1. Ищем в категории акции
    slug = _find_slug(promo_text)
    products = []

    if slug:
        sid = _get_sid(session, slug)
        if sid:
            products = _fetch_products(session, sid, max_pages=3)

    # 2. Если не нашли категорию — берём общую страницу скидок
    if not products and use_discount_page:
        products = _fetch_discounts_page(session, max_pages=3)

    # 3. Фильтруем только товары со скидкой
    discounted = []
    for p in products:
        if not p.get("crossPrice"):
            continue

        try:
            price = float(p.get("priceSimple", "0").replace(",", "."))
            old_price = float(p.get("oldPriceSimple", "0").replace(",", "."))
        except (ValueError, TypeError):
            continue

        if old_price <= 0 or price >= old_price:
            continue

        discount_pct = round((1 - price / old_price) * 100)
        discount_rub = round(old_price - price, 2)

        # Определяем "по карте" — в ДИКСИ большинство скидок по карте
        by_card = True  # По умолчанию — по карте клуба

        name = p.get("title", "").strip()

        discounted.append({
            "name": name,
            "price": price,
            "old_price": old_price,
            "discount": discount_pct,
            "discount_rub": discount_rub,
            "by_card": by_card,
            "date_to": "",  # API не возвращает дату окончания скидки
            "brand": p.get("brand", ""),
            "weight": p.get("weight", ""),
            "image": BASE_URL + p.get("src", "") if p.get("src") else "",
        })

    # Сортируем по размеру скидки
    discounted.sort(key=lambda x: x["discount"], reverse=True)

    return discounted


def search_discounts_by_keywords(keywords: list[str]) -> list[dict]:
    """Поиск скидок по списку ключевых слов."""
    session = _get_session()
    all_discounted = []
    seen_ids = set()

    for kw in keywords:
        slug = _find_slug(kw)
        if not slug:
            continue
        sid = _get_sid(session, slug)
        if not sid:
            continue
        products = _fetch_products(session, sid, max_pages=2)
        for p in products:
            pid = p.get("id", "")
            if pid in seen_ids or not p.get("crossPrice"):
                continue
            seen_ids.add(pid)
            try:
                price = float(p.get("priceSimple", "0").replace(",", "."))
                old_price = float(p.get("oldPriceSimple", "0").replace(",", "."))
            except (ValueError, TypeError):
                continue
            if old_price <= 0 or price >= old_price:
                continue

            all_discounted.append({
                "name": p.get("title", "").strip(),
                "price": price,
                "old_price": old_price,
                "discount": round((1 - price / old_price) * 100),
                "discount_rub": round(old_price - price, 2),
                "by_card": True,
                "date_to": "",
                "brand": p.get("brand", ""),
                "weight": p.get("weight", ""),
                "image": BASE_URL + p.get("src", "") if p.get("src") else "",
            })

    all_discounted.sort(key=lambda x: x["discount"], reverse=True)
    return all_discounted
