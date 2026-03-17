"""Модуль AI-генерации текстов push-уведомлений."""

import json
from config import AI_PROVIDER, ANTHROPIC_API_KEY, OPENAI_API_KEY


def build_prompt(promo: dict, rules: str, num_variants: int,
                 title_max_len: int, body_max_len: int,
                 schedule: list[dict]) -> str:
    """Собрать промпт для генерации push-текстов."""
    schedule_text = ""
    for i, s in enumerate(schedule, 1):
        schedule_text += f"  Push #{i}: дата {s['date']}, время {s['time']}\n"

    prompt = f"""Ты — копирайтер сети магазинов ДИКСИ. Сгенерируй тексты push-уведомлений для мобильного приложения.

ДАННЫЕ АКЦИИ:
- Номер: {promo.get('НОМЕР', '')}
- Название: {promo.get('Название промо', '')}
- Описание: {promo.get('Описание акции', '')}
- Сегмент: {promo.get('Сегмент', '')}
- Категория: {promo.get('Категория', '')}
- Механика: {promo.get('Механика', '')}
- Скидка: {promo.get('Скидка', '')}
- Бонусы: {promo.get('Бонусы', '')}
- Старт: {promo.get('Старт акции', '')}
- Окончание: {promo.get('Окончание акции', '')}
- Купон: {promo.get('Текст на информационном купоне / слип-чеке', '')}
- Кнопка: {promo.get('Кнопка', '')}
- Ограничения: {promo.get('Ограничения и комментарии', '')}

РАСПИСАНИЕ PUSH-УВЕДОМЛЕНИЙ:
{schedule_text}

ПРАВИЛА ГЕНЕРАЦИИ:
{rules}

ОГРАНИЧЕНИЯ:
- Заголовок push: СТРОГО до {title_max_len} символов (включая эмодзи)
- Текст push: СТРОГО до {body_max_len} символов (включая эмодзи)
- Учитывай дату отправки — если push отправляется ближе к концу акции, создай ощущение срочности
- Первый push в серии — информирующий, последующие — напоминающие

ЗАДАНИЕ:
Для каждого push из расписания сгенерируй {num_variants} вариантов (заголовок + текст).

Ответь СТРОГО в формате JSON (без markdown):
{{
  "pushes": [
    {{
      "push_number": 1,
      "date": "...",
      "time": "...",
      "variants": [
        {{
          "title": "заголовок push",
          "title_length": 25,
          "body": "текст push",
          "body_length": 95
        }}
      ]
    }}
  ]
}}"""
    return prompt


def generate_push_texts(promo: dict, rules: str, num_variants: int,
                        title_max_len: int, body_max_len: int,
                        schedule: list[dict], provider: str = None,
                        anthropic_key: str = None, openai_key: str = None) -> dict:
    """Вызвать AI для генерации push-текстов."""
    provider = provider or AI_PROVIDER
    prompt = build_prompt(promo, rules, num_variants, title_max_len, body_max_len, schedule)

    if provider == "anthropic":
        return _call_anthropic(prompt, anthropic_key or ANTHROPIC_API_KEY)
    elif provider == "openai":
        return _call_openai(prompt, openai_key or OPENAI_API_KEY)
    else:
        raise ValueError(f"Неизвестный AI провайдер: {provider}")


def _call_anthropic(prompt: str, api_key: str) -> dict:
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    text = message.content[0].text
    return _parse_response(text)


def _call_openai(prompt: str, api_key: str) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=4096,
        temperature=0.8,
    )
    text = response.choices[0].message.content
    return _parse_response(text)


def _parse_response(text: str) -> dict:
    """Извлечь JSON из ответа AI."""
    text = text.strip()
    # Убрать markdown обёртку если есть
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1])
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Попытка найти JSON в тексте
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
        raise ValueError(f"Не удалось разобрать ответ AI: {text[:200]}")
