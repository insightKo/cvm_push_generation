"""CVM Push Generation — Streamlit-приложение для генерации push-уведомлений."""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(page_title="CVM Push Generator", page_icon="📱", layout="wide")

# ─── Sidebar: настройки ──────────────────────────────────────────────────────

st.sidebar.title("⚙️ Настройки")

# AI Provider
ai_provider = st.sidebar.selectbox("AI провайдер", ["anthropic", "openai"])
ai_key = st.sidebar.text_input(
    "API ключ" + (" (Anthropic)" if ai_provider == "anthropic" else " (OpenAI)"),
    type="password",
    key="ai_key",
)

# Google Sheets — можно через service account или публичный доступ
st.sidebar.markdown("---")
st.sidebar.subheader("Google Sheets")
spreadsheet_id = st.sidebar.text_input(
    "Spreadsheet ID",
    value="1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY",
)
credentials_file = st.sidebar.file_uploader(
    "Service Account JSON", type=["json"], help="Загрузите файл credentials от Google Service Account"
)

# Сохранить credentials если загружен
if credentials_file is not None:
    import os
    os.makedirs("credentials", exist_ok=True)
    with open("credentials/service_account.json", "wb") as f:
        f.write(credentials_file.getvalue())
    st.sidebar.success("Credentials сохранены")

# ─── Загрузка данных ─────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def load_cvm_data(sid: str) -> pd.DataFrame:
    """Загрузить CVM offline через публичный CSV export."""
    import io, csv
    import urllib.request

    # Получаем gid для листа CVM offline
    # Пробуем загрузить через gviz API (работает для публичных таблиц)
    url = f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet=CVM%20offline"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки CVM offline: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_push_data(sid: str) -> pd.DataFrame:
    """Загрузить PUSH через публичный CSV export."""
    import io
    import urllib.request

    url = f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet=PUSH"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки PUSH: {e}")
        return pd.DataFrame()


def save_to_sheets(rows: list[dict], sid: str):
    """Сохранить строки в Google Sheets через gspread (нужен service account)."""
    import os
    cred_path = "credentials/service_account.json"
    if not os.path.exists(cred_path):
        st.error("Для сохранения нужен файл Service Account. Загрузите его в боковой панели.")
        return False

    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    client = gspread.authorize(creds)
    ss = client.open_by_key(sid)
    ws = ss.worksheet("PUSH")
    headers = ws.row_values(1)

    for row_data in rows:
        row_values = [str(row_data.get(h, "")) for h in headers]
        ws.append_row(row_values, value_input_option="USER_ENTERED")

    return True


# ─── Основной интерфейс ─────────────────────────────────────────────────────

st.title("📱 CVM Push Generator")
st.markdown("Генерация текстов push-уведомлений для акций ДИКСИ")

# Табы
tab_promos, tab_generate, tab_push, tab_rules = st.tabs(
    ["📋 Акции (CVM offline)", "✨ Генерация PUSH", "📨 PUSH (результат)", "📝 Правила генерации"]
)

# ─── Таб: Правила генерации ──────────────────────────────────────────────────

with tab_rules:
    st.subheader("Правила генерации текстов push-уведомлений")
    st.markdown("Эти правила будут использоваться AI при генерации текстов. Редактируйте под ваш стиль.")

    default_rules = """1. Тон: дружелюбный, энергичный, разговорный (как в мессенджере с другом)
2. Используй эмодзи в заголовке (1-2 шт.), в тексте — умеренно
3. Обязательно указывай размер выгоды (скидку, кешбэк, бонусы) в заголовке или первых словах текста
4. Указывай срок действия акции в тексте ("до DD.MM", "только сегодня", "три дня")
5. Призыв к действию: "Забегай", "Лови", "Активируй", "Бери" — не "Посетите" или "Приобретите"
6. Для сегмента "Отток/Спящие" — более щедрое предложение, упоминай что соскучились
7. Для повторных push — напоминающий тон: "Ещё можно успеть", "Не упусти", "Последний день"
8. Не используй слова: "уважаемый", "клиент", "предложение ограничено"
9. Если акция на категорию — перечисли 2-3 конкретных товара из неё
10. Заголовок должен быть самодостаточным — понятен без прочтения текста"""

    if "generation_rules" not in st.session_state:
        st.session_state.generation_rules = default_rules

    st.session_state.generation_rules = st.text_area(
        "Правила генерации",
        value=st.session_state.generation_rules,
        height=350,
        key="rules_editor",
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Сбросить к значениям по умолчанию"):
            st.session_state.generation_rules = default_rules
            st.rerun()
    with col2:
        st.info("Правила автоматически применяются при генерации")

# ─── Таб: Акции ──────────────────────────────────────────────────────────────

with tab_promos:
    st.subheader("Акции из вкладки CVM offline")

    if st.button("🔄 Обновить данные", key="refresh_cvm"):
        load_cvm_data.clear()
        load_push_data.clear()

    df_cvm = load_cvm_data(spreadsheet_id)

    if df_cvm.empty:
        st.warning("Нет данных. Проверьте доступ к таблице.")
    else:
        # Фильтры
        col1, col2 = st.columns(2)
        with col1:
            if "Каналы коммуникации" in df_cvm.columns:
                channels = df_cvm["Каналы коммуникации"].dropna().unique().tolist()
                selected_channel = st.multiselect("Канал", channels, default=["PUSH"] if "PUSH" in channels else [])
            else:
                selected_channel = []
        with col2:
            if "Месяц" in df_cvm.columns:
                months = sorted(df_cvm["Месяц"].dropna().unique().tolist())
                selected_months = st.multiselect("Месяц", months)
            else:
                selected_months = []

        df_filtered = df_cvm.copy()
        if selected_channel:
            df_filtered = df_filtered[df_filtered["Каналы коммуникации"].isin(selected_channel)]
        if selected_months:
            df_filtered = df_filtered[df_filtered["Месяц"].isin(selected_months)]

        # Показать ключевые колонки
        display_cols = [
            c for c in [
                "НОМЕР", "Название промо", "Сегмент", "Старт акции", "Окончание акции",
                "Каналы коммуникации", "Описание акции", "Категория", "Бонусы", "Скидка",
            ]
            if c in df_filtered.columns
        ]
        if display_cols:
            st.dataframe(df_filtered[display_cols], use_container_width=True, height=500)
        else:
            st.dataframe(df_filtered, use_container_width=True, height=500)

        st.caption(f"Всего акций: {len(df_filtered)}")

# ─── Таб: Генерация ──────────────────────────────────────────────────────────

with tab_generate:
    st.subheader("Генерация push-уведомлений")

    df_cvm = load_cvm_data(spreadsheet_id)

    if df_cvm.empty:
        st.warning("Сначала загрузите данные на вкладке 'Акции'")
    else:
        # Фильтр только PUSH-акций
        if "Каналы коммуникации" in df_cvm.columns:
            df_push_promos = df_cvm[df_cvm["Каналы коммуникации"].str.strip().str.upper() == "PUSH"].copy()
        else:
            df_push_promos = df_cvm.copy()

        if df_push_promos.empty:
            st.warning("Нет акций с каналом PUSH")
        else:
            # Выбор акции
            promo_options = {}
            for _, row in df_push_promos.iterrows():
                key = f"{row.get('НОМЕР', '?')} — {row.get('Название промо', '?')}"
                promo_options[key] = row.to_dict()

            selected_promo_key = st.selectbox("Выберите акцию", list(promo_options.keys()))
            selected_promo = promo_options[selected_promo_key]

            # Информация об акции
            with st.expander("📋 Детали акции", expanded=True):
                info_cols = st.columns(3)
                with info_cols[0]:
                    st.markdown(f"**Номер:** {selected_promo.get('НОМЕР', '')}")
                    st.markdown(f"**Сегмент:** {selected_promo.get('Сегмент', '')}")
                    st.markdown(f"**Категория:** {selected_promo.get('Категория', '')}")
                with info_cols[1]:
                    st.markdown(f"**Старт:** {selected_promo.get('Старт акции', '')}")
                    st.markdown(f"**Окончание:** {selected_promo.get('Окончание акции', '')}")
                    st.markdown(f"**Бонусы:** {selected_promo.get('Бонусы', '')}")
                with info_cols[2]:
                    st.markdown(f"**Описание:** {selected_promo.get('Описание акции', '')}")
                    st.markdown(f"**Механика:** {selected_promo.get('Механика', '')}")

            st.markdown("---")

            # Конфигурация генерации
            st.markdown("### ⚙️ Конфигурация")

            config_cols = st.columns(3)
            with config_cols[0]:
                num_pushes = st.number_input("Количество push-отправок", min_value=1, max_value=10, value=2)
            with config_cols[1]:
                num_variants = st.number_input("Вариантов на каждый push", min_value=1, max_value=5, value=3)
            with config_cols[2]:
                title_max = st.number_input("Макс. символов в заголовке", min_value=15, max_value=50, value=35)
                body_max = st.number_input("Макс. символов в тексте", min_value=50, max_value=200, value=120)

            # Расписание push-отправок
            st.markdown("### 📅 Расписание отправок")
            schedule = []
            for i in range(num_pushes):
                cols = st.columns([2, 1, 2])
                with cols[0]:
                    push_date = st.date_input(
                        f"Дата push #{i + 1}",
                        value=datetime.now().date() + timedelta(days=i),
                        key=f"date_{i}",
                    )
                with cols[1]:
                    push_time = st.time_input(
                        f"Время #{i + 1}",
                        value=datetime.strptime("12:00", "%H:%M").time(),
                        key=f"time_{i}",
                    )
                with cols[2]:
                    day_names = {0: "пн", 1: "вт", 2: "ср", 3: "чт", 4: "пт", 5: "сб", 6: "вс"}
                    st.markdown(f"**{day_names[push_date.weekday()]}**, push #{i + 1} из {num_pushes}")

                schedule.append({
                    "date": push_date.strftime("%d.%m.%Y"),
                    "time": push_time.strftime("%H:%M"),
                    "date_obj": push_date,
                    "time_obj": push_time,
                })

            st.markdown("---")

            # Кнопка генерации
            if st.button("🚀 Сгенерировать push-тексты", type="primary", use_container_width=True):
                if not ai_key:
                    st.error("Введите API ключ в боковой панели")
                else:
                    with st.spinner("AI генерирует тексты..."):
                        try:
                            from ai_generator import generate_push_texts

                            result = generate_push_texts(
                                promo=selected_promo,
                                rules=st.session_state.get("generation_rules", ""),
                                num_variants=num_variants,
                                title_max_len=title_max,
                                body_max_len=body_max,
                                schedule=schedule,
                                provider=ai_provider,
                                anthropic_key=ai_key if ai_provider == "anthropic" else None,
                                openai_key=ai_key if ai_provider == "openai" else None,
                            )

                            st.session_state.generated_result = result
                            st.session_state.selected_promo = selected_promo
                            st.session_state.schedule = schedule
                            st.success("Тексты сгенерированы!")
                        except Exception as e:
                            st.error(f"Ошибка генерации: {e}")

            # Показать результаты генерации
            if "generated_result" in st.session_state and st.session_state.get("selected_promo", {}).get("НОМЕР") == selected_promo.get("НОМЕР"):
                result = st.session_state.generated_result
                schedule_saved = st.session_state.schedule

                st.markdown("### 📝 Результаты генерации")

                # Инициализация выбранных вариантов
                if "selected_variants" not in st.session_state:
                    st.session_state.selected_variants = {}

                pushes = result.get("pushes", [])
                for push_data in pushes:
                    push_num = push_data.get("push_number", "?")
                    push_date = push_data.get("date", "")
                    push_time = push_data.get("time", "")

                    st.markdown(f"#### Push #{push_num} — {push_date} {push_time}")

                    variants = push_data.get("variants", [])
                    for v_idx, variant in enumerate(variants):
                        title = variant.get("title", "")
                        body = variant.get("body", "")

                        with st.container(border=True):
                            st.markdown(f"**Вариант {v_idx + 1}**")

                            edited_title = st.text_input(
                                f"Заголовок",
                                value=title,
                                key=f"title_{push_num}_{v_idx}",
                            )
                            title_len = len(edited_title)
                            title_color = "🟢" if title_len <= title_max else "🔴"
                            st.caption(f"{title_color} {title_len}/{title_max} символов")

                            edited_body = st.text_area(
                                f"Текст",
                                value=body,
                                key=f"body_{push_num}_{v_idx}",
                                height=80,
                            )
                            body_len = len(edited_body)
                            body_color = "🟢" if body_len <= body_max else "🔴"
                            st.caption(f"{body_color} {body_len}/{body_max} символов")

                            # Предпросмотр push
                            with st.expander("👁️ Предпросмотр"):
                                st.markdown(
                                    f"""<div style="background:#f0f2f6; border-radius:12px; padding:12px; max-width:350px;">
                                    <div style="font-weight:bold; font-size:14px;">ДИКСИ</div>
                                    <div style="font-weight:bold; font-size:13px; margin-top:4px;">{edited_title}</div>
                                    <div style="font-size:12px; color:#555; margin-top:2px;">{edited_body}</div>
                                    </div>""",
                                    unsafe_allow_html=True,
                                )

                            if st.button(f"✅ Выбрать вариант {v_idx + 1}", key=f"select_{push_num}_{v_idx}"):
                                st.session_state.selected_variants[push_num] = {
                                    "title": edited_title,
                                    "body": edited_body,
                                    "date": push_date,
                                    "time": push_time,
                                }
                                st.success(f"Выбран вариант {v_idx + 1} для push #{push_num}")

                # Сохранение в Google Sheets
                st.markdown("---")
                st.markdown("### 💾 Сохранение в Google Sheets")

                if st.session_state.get("selected_variants"):
                    st.markdown("**Выбранные варианты:**")
                    for pn, var in sorted(st.session_state.selected_variants.items()):
                        st.markdown(f"- Push #{pn}: **{var['title']}** | {var['body']}")

                    # Дополнительные поля для сохранения
                    deeplink = st.text_input(
                        "Экран - ссылка (deeplink)",
                        value=selected_promo.get("Кнопка", "").split(" ")[-1] if selected_promo.get("Кнопка") else "",
                    )
                    coupon_text = st.text_input(
                        "Текст купона",
                        value="купон" if selected_promo.get("Купон") == "да" else "",
                    )

                    if st.button("💾 Сохранить в Google Sheets", type="primary"):
                        rows_to_save = []
                        for pn, var in sorted(st.session_state.selected_variants.items()):
                            row = {
                                "Сегмент": selected_promo.get("Сегмент", ""),
                                "Канал": "",
                                "Название промо": selected_promo.get("Название промо", ""),
                                "Год": selected_promo.get("Год", ""),
                                "Месяц": selected_promo.get("Месяц", ""),
                                "Нед": selected_promo.get("Неделя", ""),
                                "День недели": "",
                                "Дата": var["date"],
                                "Время": var["time"],
                                "Клиентов": selected_promo.get("Примерное количество клиентов", ""),
                                "Доп настройки клиентов": "",
                                "Номер промо": selected_promo.get("НОМЕР", ""),
                                "Номер msg": str(pn),
                                "PUSH заголовок": var["title"],
                                "": str(len(var["title"])),
                                "текст PUSH": var["body"],
                                "Экран - ссылка": deeplink,
                                "Текст купона": coupon_text,
                                "Кнопка": "",
                            }
                            rows_to_save.append(row)

                        try:
                            if save_to_sheets(rows_to_save, spreadsheet_id):
                                st.success(f"Сохранено {len(rows_to_save)} push в Google Sheets!")
                                load_push_data.clear()
                        except Exception as e:
                            st.error(f"Ошибка сохранения: {e}")
                else:
                    st.info("Выберите варианты для каждого push, нажав кнопку '✅ Выбрать'")

# ─── Таб: PUSH результат ─────────────────────────────────────────────────────

with tab_push:
    st.subheader("Существующие push-уведомления")

    if st.button("🔄 Обновить", key="refresh_push"):
        load_push_data.clear()

    df_push = load_push_data(spreadsheet_id)

    if df_push.empty:
        st.warning("Нет данных")
    else:
        # Фильтры
        col1, col2 = st.columns(2)
        with col1:
            if "Номер промо" in df_push.columns:
                promos = sorted(df_push["Номер промо"].dropna().unique().tolist())
                selected_promos_filter = st.multiselect("Фильтр по номеру промо", promos)
            else:
                selected_promos_filter = []
        with col2:
            if "Месяц" in df_push.columns:
                months = sorted(df_push["Месяц"].dropna().unique().tolist())
                selected_months_filter = st.multiselect("Фильтр по месяцу", months, key="push_month_filter")
            else:
                selected_months_filter = []

        df_push_filtered = df_push.copy()
        if selected_promos_filter:
            df_push_filtered = df_push_filtered[df_push_filtered["Номер промо"].isin(selected_promos_filter)]
        if selected_months_filter:
            df_push_filtered = df_push_filtered[df_push_filtered["Месяц"].isin(selected_months_filter)]

        display_push_cols = [
            c for c in [
                "Номер промо", "Название промо", "Сегмент", "Дата", "Время",
                "Номер msg", "PUSH заголовок", "текст PUSH", "Клиентов",
            ]
            if c in df_push_filtered.columns
        ]
        if display_push_cols:
            st.dataframe(df_push_filtered[display_push_cols], use_container_width=True, height=600)
        else:
            st.dataframe(df_push_filtered, use_container_width=True, height=600)

        st.caption(f"Всего push: {len(df_push_filtered)}")
