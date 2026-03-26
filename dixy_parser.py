"""Парсер каталога ДИКСИ — поиск скидок по категориям через Playwright."""

import re

BASE_URL = "https://dixy.ru"

# Маппинг категорий акций → slug каталога ДИКСИ
_CATEGORY_SLUGS = {
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
    "специ": "sousy-spetsii", "майо": "sousy-spetsii",
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
    "пив": "pivo-i-piv-napitki", "вин": "vino-i-igristoe",
    "игрист": "vino-i-igristoe", "просекк": "vino-i-igristoe",
    "алкогол": "krepkiy-alkogol", "водк": "krepkiy-alkogol",
    "виск": "krepkiy-alkogol", "коньяк": "krepkiy-alkogol",
    "хими": "bytovaya-khimiya", "бытов": "bytovaya-khimiya",
    "стирк": "bytovaya-khimiya", "моющ": "bytovaya-khimiya",
    "зуб": "gigiena-i-ukhod", "паст": "gigiena-i-ukhod",
    "шампун": "gigiena-i-ukhod", "гигиен": "gigiena-i-ukhod",
}


# Категории с несколькими slug (нужно парсить все)
_MULTI_SLUGS = {
    "детск": ["detskoe-pitanie", "tovary-dlya-detey"],
    "малыш": ["detskoe-pitanie", "tovary-dlya-detey"],
    "мам": ["detskoe-pitanie", "tovary-dlya-detey"],
    "салфетк": ["tovary-dlya-detey", "gigiena-i-kosmetika"],
    "подгузник": ["tovary-dlya-detey"],
    "пасх": ["molochnye-produkty-yaytsa", "khleb-i-vypechka", "bakaleya"],
}


def _find_slugs(promo_text: str) -> list[str]:
    """Найти все slug-и каталога по тексту акции."""
    text = promo_text.lower()

    # Сначала проверяем мульти-slug маппинг
    for keyword, slugs in _MULTI_SLUGS.items():
        if keyword in text:
            return slugs

    # Затем обычный маппинг
    best_slug = None
    best_len = 0
    for keyword, slug in _CATEGORY_SLUGS.items():
        if keyword in text and len(keyword) > best_len:
            best_slug = slug
            best_len = len(keyword)
    return [best_slug] if best_slug else []


def _find_slug(promo_text: str) -> str | None:
    """Найти slug каталога (обратная совместимость)."""
    slugs = _find_slugs(promo_text)
    return slugs[0] if slugs else None


def _parse_price(text: str) -> float:
    """Извлечь цену из '119.90руб.' или '149,90 ₽'."""
    if not text:
        return 0.0
    # Ищем паттерн цены: 119.90 или 99,99
    m = re.search(r'(\d+)[.,](\d{1,2})', text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    # Целое число
    m2 = re.search(r'(\d+)', text)
    if m2:
        return float(m2.group(1))
    return 0.0


def _fetch_products_playwright(slug: str, max_scrolls: int = 3) -> list[dict]:
    """Загрузить товары через headless Playwright."""
    from playwright.sync_api import sync_playwright

    url = f"{BASE_URL}/catalog/{slug}/"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=20000)
            page.wait_for_timeout(3000)

            # Scroll to load more
            for _ in range(max_scrolls):
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                page.wait_for_timeout(1500)

            raw_cards = page.evaluate('''() => {
                const items = document.querySelectorAll('.card');
                const results = [];
                items.forEach(item => {
                    const title = item.querySelector('.card__title')?.textContent?.trim() || '';
                    const priceNum = item.querySelector('.card__price-num')?.textContent?.trim() || '';
                    const priceCrossed = item.querySelector('.card__price-crossed')?.textContent?.trim() || '';
                    const badges = [...item.querySelectorAll('.badge span')].map(b => b.textContent.trim());
                    const link = item.querySelector('a.card__link')?.getAttribute('href') || '';
                    if (!title) return;
                    results.push({name: title, price: priceNum, old_price: priceCrossed, badges, link});
                });
                return results;
            }''')

            browser.close()
            return raw_cards

    except Exception as e:
        print(f"Playwright error: {e}")
        return []


def search_discounts(promo_text: str) -> list[dict]:
    """Найти товары на dixy.ru по тексту акции.

    Returns: список dict:
        - name: название товара
        - price: текущая цена (число)
        - old_price: старая цена (число или "")
        - discount: скидка ("-16% по карте" или "по акции" или "")
        - date_to: дата окончания ("до 29.03.2026" или "")
    """
    slugs = _find_slugs(promo_text)
    if not slugs:
        return []

    # Собираем товары из всех подходящих категорий
    raw_cards = []
    seen_names = set()
    for slug in slugs:
        cards = _fetch_products_playwright(slug, max_scrolls=2)
        for card in cards:
            name = card.get("name", "")
            if name not in seen_names:
                seen_names.add(name)
                raw_cards.append(card)

    if not raw_cards:
        return []

    products = []
    for card in raw_cards:
        price = _parse_price(card.get("price", ""))
        old_price = _parse_price(card.get("old_price", ""))
        badges = card.get("badges", [])

        # Дата и пометка "по карте", текстовый бейдж из бейджей
        date_to = ""
        by_card = ""
        badge_text = ""
        has_badge = False
        for badge in badges:
            if "%" in badge or "акци" in badge.lower():
                has_badge = True
                badge_text = badge  # сохраняем текстовый бейдж ("по акции", "-16% по карте")
                if "карт" in badge.lower():
                    by_card = "по карте"
            if "до " in badge.lower():
                date_to = badge

        # Показываем только товары со скидкой (есть бейдж или перечёркнутая цена)
        if not has_badge and old_price <= 0:
            continue

        # Считаем скидку математически если есть старая цена
        discount = ""
        if old_price > 0 and price > 0 and price < old_price:
            pct = round((old_price - price) / old_price * 100)
            discount = f"-{pct}%"
            if by_card:
                discount += " по карте"
        elif badge_text:
            # Fallback: текстовый бейдж если нет старой цены для расчёта
            discount = badge_text

        # Ссылка на товар
        link = card.get("link", "")
        product_url = f"{BASE_URL}{link}" if link and link.startswith("/") else ""

        products.append({
            "name": card["name"],
            "price": price,
            "old_price": old_price if old_price > 0 else "",
            "discount": discount,
            "by_card": by_card,
            "date_to": date_to,
            "url": product_url,
        })

    # Сортируем по размеру скидки (больше скидка — выше)
    def _sort_key(x):
        m = re.search(r'-(\d+)%', x.get("discount", ""))
        return -(int(m.group(1)) if m else 0)
    products.sort(key=_sort_key)

    return products
