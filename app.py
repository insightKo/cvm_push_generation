"""CVM Push Generation — Streamlit-приложение для генерации push-уведомлений."""

import ssl
import certifi
import os
import streamlit as st
import pandas as pd
from datetime import date as _date_cls, datetime, timedelta

# ─── SSL fix for macOS ───────────────────────────────────────────────────────
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
try:
    ssl._create_default_https_context = ssl.create_default_context
except Exception:
    pass

# ─── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="CVM генератор", page_icon="📱", layout="wide")

# ─── Modern CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Gradient metrics */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    border-radius: 12px;
    padding: 16px 20px;
    color: white !important;
}
div[data-testid="stMetric"] label {
    color: rgba(255,255,255,0.85) !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: white !important;
    font-size: 28px !important;
    font-weight: 700 !important;
}

/* Rounded buttons */
.stButton > button {
    border-radius: 12px;
    font-weight: 600;
}

/* Dark sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1e1e2f 0%, #2d2d44 100%);
}
section[data-testid="stSidebar"] * {
    color: #e0e0e0 !important;
}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stRadio label {
    color: #e0e0e0 !important;
}

/* Gradient progress bar */
.stProgress > div > div {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%) !important;
    border-radius: 8px;
}

/* Radio as tab-style buttons (no markers) */
div[data-testid="stRadio"] > div {
    flex-direction: row !important;
    gap: 4px;
}
div[data-testid="stRadio"] > div > label {
    background: rgba(255,255,255,0.05);
    border-radius: 8px;
    padding: 8px 16px;
    cursor: pointer;
    border: 1px solid rgba(255,255,255,0.1);
    transition: all 0.2s;
}
div[data-testid="stRadio"] > div > label:hover {
    background: rgba(255,255,255,0.12);
}
div[data-testid="stRadio"] > div > label[data-checked="true"] {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    border-color: transparent;
}
div[data-testid="stRadio"] > div > label > div:first-child {
    display: none !important;
}
</style>
""", unsafe_allow_html=True)

# ─── Sidebar ─────────────────────────────────────────────────────────────────
_nav_page = st.sidebar.radio(
    "Навигация",
    ["📅 План", "🔧 Условия акций", "✨ Генерация PUSH",
     "📝 Правила генерации", "🔗 Deeplinks"],
    label_visibility="collapsed",
)

with st.sidebar.expander("⚙️ Настройки", expanded=False):
    spreadsheet_id = st.text_input(
        "Spreadsheet ID",
        value="1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY",
        key="spreadsheet_id",
    )
    credentials_file = st.file_uploader(
        "Service Account JSON",
        type=["json"],
        help="Загрузите файл credentials от Google Service Account",
    )
    if credentials_file is not None:
        os.makedirs("credentials", exist_ok=True)
        with open("credentials/service_account.json", "wb") as f:
            f.write(credentials_file.getvalue())
        st.success("Credentials сохранены")

# AI defaults
if "ai_provider" not in st.session_state:
    st.session_state.ai_provider = "builtin"
if "ai_key" not in st.session_state:
    st.session_state.ai_key = ""

# ─── Data loading ────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_cvm_data(sid: str) -> pd.DataFrame:
    """Загрузить CVM offline через gspread с fallback на CSV."""
    cred_path = "credentials/service_account.json"
    # Попытка через gspread
    if os.path.exists(cred_path):
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
            client = gspread.authorize(creds)
            ss = client.open_by_key(sid)
            ws = ss.worksheet("CVM offline")
            data = ws.get_all_values()
            if not data:
                return pd.DataFrame()
            headers = data[0]
            # Дедупликация заголовков
            seen = {}
            unique_headers = []
            for h in headers:
                if h in seen:
                    seen[h] += 1
                    unique_headers.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 0
                    unique_headers.append(h)
            df = pd.DataFrame(data[1:], columns=unique_headers)
            # Конвертация НОМЕР в числовой
            if "НОМЕР" in df.columns:
                df["НОМЕР"] = pd.to_numeric(df["НОМЕР"], errors="coerce")
            return df
        except Exception:
            pass
    # Fallback: CSV
    try:
        import io
        import urllib.request
        url = f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet=CVM%20offline"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))
        if "НОМЕР" in df.columns:
            df["НОМЕР"] = pd.to_numeric(df["НОМЕР"], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки CVM offline: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_push_data(sid: str) -> pd.DataFrame:
    """Загрузить PUSH tab через gspread, fallback на CSV."""
    cred_path = "credentials/service_account.json"
    if os.path.exists(cred_path):
        try:
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
            data = ws.get_all_values()
            if not data:
                return pd.DataFrame()
            headers = data[0]
            seen = {}
            unique_headers = []
            for h in headers:
                if h in seen:
                    seen[h] += 1
                    unique_headers.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 0
                    unique_headers.append(h)
            df = pd.DataFrame(data[1:], columns=unique_headers)
            return df
        except Exception:
            pass
    # Fallback: CSV
    try:
        import io
        import urllib.request
        url = f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet=PUSH"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки PUSH: {e}")
        return pd.DataFrame()


def save_to_sheets(rows: list, sid: str):
    """Сохранить строки в PUSH sheet. Column 14 = __title_len, column 16 = __body_len."""
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
        # Special handling: column index 14 = title_len, column index 16 = body_len
        if len(row_values) > 14 and "__title_len" in row_data:
            row_values[14] = str(row_data["__title_len"])
        if len(row_values) > 16 and "__body_len" in row_data:
            row_values[16] = str(row_data["__body_len"])
        ws.append_row(row_values, value_input_option="USER_ENTERED")

    return True


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _parse_promo_date(date_str, year=2026):
    """Парсит 'DD.MM.' или 'DD.MM' в date."""
    if not date_str or str(date_str).strip() in ("", "nan", "NaT", "None"):
        return None
    s = str(date_str).strip().rstrip(".")
    # DD.MM.YYYY
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # DD.MM (без года)
    try:
        parsed = datetime.strptime(s, "%d.%m")
        return parsed.replace(year=int(year)).date()
    except (ValueError, TypeError):
        pass
    return None


def _norm_num(x):
    """Конвертирует float 101218.0 -> str '101218'."""
    if x is None:
        return ""
    if isinstance(x, float):
        if pd.isna(x):
            return ""
        return str(int(x))
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 📅 План (Gantt)
# ═════════════════════════════════════════════════════════════════════════════

if _nav_page == "📅 План":
    sid = st.session_state.get("spreadsheet_id",
                                "1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY")

    df_cvm = load_cvm_data(sid)

    if df_cvm.empty:
        st.warning("Нет данных. Проверьте доступ к таблице.")
    else:
        # Filters
        fcol1, fcol2 = st.columns(2)
        with fcol1:
            channel_options = ["Все"]
            if "Каналы коммуникации" in df_cvm.columns:
                ch_vals = sorted(df_cvm["Каналы коммуникации"].dropna().astype(str).unique().tolist())
                channel_options += ch_vals
            selected_channel = st.selectbox("Канал", channel_options, index=0, key="plan_channel")
        with fcol2:
            _MONTH_NAMES_PLAN = {
                1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
                5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
                9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
            }
            _plan_ym = []
            if "Год" in df_cvm.columns and "Месяц" in df_cvm.columns:
                for _, _r in df_cvm[["Год", "Месяц"]].drop_duplicates().iterrows():
                    _y, _m = _r["Год"], _r["Месяц"]
                    if str(_y).strip().isdigit() and str(_m).strip().isdigit() and int(_y) > 2000:
                        _plan_ym.append((int(_y), int(_m)))
                _plan_ym = sorted(set(_plan_ym))
            if not _plan_ym:
                _plan_ym = [(2026, 4)]
            month_options = [f"{_MONTH_NAMES_PLAN.get(m, str(m))} {y}" for y, m in _plan_ym]
            _plan_default = 0
            _today = _date_cls.today()
            for _i, (_y, _m) in enumerate(_plan_ym):
                if _y == _today.year and _m == _today.month:
                    _plan_default = _i
                    break
            selected_month_str = st.selectbox("Месяц", month_options, index=_plan_default, key="plan_month")

        # Parse selected month to date range
        _MONTH_MAP = {
            "Январь": 1, "Февраль": 2, "Март": 3, "Апрель": 4,
            "Май": 5, "Июнь": 6, "Июль": 7, "Август": 8,
            "Сентябрь": 9, "Октябрь": 10, "Ноябрь": 11, "Декабрь": 12,
        }
        parts = selected_month_str.split()
        sel_month_num = _MONTH_MAP.get(parts[0], 4)
        sel_year = int(parts[1]) if len(parts) > 1 else 2026
        month_start = _date_cls(sel_year, sel_month_num, 1)
        if sel_month_num == 12:
            month_end = _date_cls(sel_year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = _date_cls(sel_year, sel_month_num + 1, 1) - timedelta(days=1)

        # Filter promos by date intersection with selected month
        filtered_rows = []
        for _, row in df_cvm.iterrows():
            year_hint = row.get("Год", sel_year)
            try:
                year_hint = int(year_hint)
            except (ValueError, TypeError):
                year_hint = sel_year
            start_d = _parse_promo_date(row.get("Старт акции"), year_hint)
            end_d = _parse_promo_date(row.get("Окончание акции"), year_hint)
            if not start_d or not end_d:
                continue
            if end_d < start_d:
                start_d, end_d = end_d, start_d
            # Intersection check
            if start_d <= month_end and end_d >= month_start:
                if selected_channel != "Все":
                    ch = str(row.get("Каналы коммуникации", "")).strip()
                    if ch != selected_channel:
                        continue
                filtered_rows.append(row)

        # Load PUSH data
        df_push = load_push_data(sid)

        # Build push message map: (promo_num, date_str) -> [msg_numbers]
        _push_msg_map = {}
        if not df_push.empty and "Номер промо" in df_push.columns:
            for _, pr in df_push.iterrows():
                pnum = _norm_num(pr.get("Номер промо"))
                pdate = str(pr.get("Дата", "")).strip()
                msg = str(pr.get("Номер msg", "")).strip()
                key = (pnum, pdate)
                if key not in _push_msg_map:
                    _push_msg_map[key] = []
                if msg:
                    _push_msg_map[key].append(msg)

        # Build gantt_rows
        today = _date_cls.today()
        gantt_rows = []
        for row in filtered_rows:
            year_hint = row.get("Год", sel_year)
            try:
                year_hint = int(year_hint)
            except (ValueError, TypeError):
                year_hint = sel_year
            num = _norm_num(row.get("НОМЕР"))
            name = str(row.get("Название промо", "")).strip()
            segment = str(row.get("Сегмент", "")).strip()
            start_d = _parse_promo_date(row.get("Старт акции"), year_hint)
            end_d = _parse_promo_date(row.get("Окончание акции"), year_hint)
            channel = str(row.get("Каналы коммуникации", "")).strip()

            if not start_d or not end_d:
                continue
            if end_d < start_d:
                start_d, end_d = end_d, start_d

            # Status logic
            is_push_channel = channel.strip().upper() == "PUSH"
            if is_push_channel:
                # Check if push exists for this promo
                has_push = any(
                    k[0] == num for k in _push_msg_map.keys()
                )
                if has_push:
                    status = "done"
                else:
                    # Check if conditions filled
                    desc = str(row.get("Описание акции", "")).strip()
                    if desc:
                        status = "conditions"
                    else:
                        status = "empty"
            else:
                if end_d < today:
                    status = "done"
                else:
                    status = "conditions"

            gantt_rows.append({
                "num": num,
                "name": name,
                "segment": segment,
                "start": start_d,
                "end": end_d,
                "status": status,
                "channel": channel,
            })

        # Metrics
        total_promos = len(gantt_rows)
        promos_with_push = sum(1 for g in gantt_rows if g["status"] == "done" and
                               str(g.get("channel", "")).strip().upper() == "PUSH")
        total_msgs = sum(len(v) for v in _push_msg_map.values())

        mcol1, mcol2, mcol3 = st.columns(3)
        mcol1.metric("Акций", total_promos)
        mcol2.metric("С push", promos_with_push)
        mcol3.metric("Сообщений", total_msgs)

        if not gantt_rows:
            st.info("Нет акций для выбранного периода")
        else:
            # Build HTML Gantt table
            # Generate date columns for the month
            all_dates = []
            d = month_start
            while d <= month_end:
                all_dates.append(d)
                d += timedelta(days=1)

            # Group by segment
            segments_dict = {}
            for g in gantt_rows:
                seg = g["segment"] or "Без сегмента"
                if seg not in segments_dict:
                    segments_dict[seg] = []
                segments_dict[seg].append(g)

            show_channel = (selected_channel == "Все")

            html = '<div style="overflow-x:auto;">'
            html += '<table style="border-collapse:collapse; font-size:10px; width:100%;">'

            # Header row: dates
            html += '<tr>'
            html += '<th style="padding:2px 4px; border:1px solid #ddd; position:sticky; left:0; background:#f8f9fa; z-index:2; min-width:180px;">Акция</th>'
            if show_channel:
                html += '<th style="padding:2px 4px; border:1px solid #ddd; background:#f8f9fa; min-width:40px;">Канал</th>'
            for dt in all_dates:
                is_weekend = dt.weekday() >= 5
                bg = "#fff3cd" if is_weekend else "#f8f9fa"
                border_style = "2px solid #ffc107" if is_weekend else "1px solid #ddd"
                html += f'<th style="padding:1px 2px; border:{border_style}; background:{bg}; text-align:center; min-width:18px;">{dt.day}</th>'
            html += '</tr>'

            # Day of week header
            _DOW_SHORT = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
            html += '<tr>'
            html += '<td style="border:1px solid #ddd; background:#f8f9fa;"></td>'
            html += '<td style="border:1px solid #ddd; background:#f8f9fa;"></td>'
            if show_channel:
                html += '<td style="border:1px solid #ddd; background:#f8f9fa;"></td>'
            for dt in all_dates:
                is_weekend = dt.weekday() >= 5
                bg = "#fff3cd" if is_weekend else "#f8f9fa"
                border_style = "2px solid #ffc107" if is_weekend else "1px solid #ddd"
                html += f'<td style="padding:1px 2px; border:{border_style}; background:{bg}; text-align:center; font-size:10px; color:#888;">{_DOW_SHORT[dt.weekday()]}</td>'
            html += '</tr>'

            # Promo rows grouped by segment
            for seg_name, seg_rows in segments_dict.items():
                # Segment header
                col_span = 2 + (1 if show_channel else 0) + len(all_dates)
                html += f'<tr><td colspan="{col_span}" style="padding:6px 8px; border:1px solid #ddd; background:#e8eaf6; font-weight:bold;">{seg_name}</td></tr>'

                for g in seg_rows:
                    status_colors = {
                        "done": "#c8e6c9",
                        "conditions": "#fff9c4",
                        "empty": "#ffcdd2",
                    }
                    row_bg = status_colors.get(g["status"], "#ffffff")

                    html += '<tr>'
                    # Column N removed - num shown in name cell
                    html += f'<td style="padding:3px 6px; border:1px solid #ddd; background:{row_bg}; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:220px;" title="{g["name"]}">{g["name"]}</td>'
                    if show_channel:
                        html += f'<td style="padding:3px 6px; border:1px solid #ddd; background:{row_bg}; font-size:11px;">{g["channel"]}</td>'

                    for dt in all_dates:
                        is_weekend = dt.weekday() >= 5
                        border_style = "2px solid #ffc107" if is_weekend else "1px solid #ddd"

                        in_range = g["start"] <= dt <= g["end"]
                        date_str = dt.strftime("%d.%m.%Y")
                        push_key = (g["num"], date_str)
                        msgs_list = _push_msg_map.get(push_key, [])

                        if msgs_list:
                            # Blue cells with message numbers
                            msg_text = ",".join(msgs_list)
                            html += f'<td style="padding:1px; border:{border_style}; background:#bbdefb; text-align:center; font-size:10px; font-weight:bold; color:#1565c0;" title="Push {msg_text}">{msg_text}</td>'
                        elif in_range:
                            html += f'<td style="padding:1px; border:{border_style}; background:{row_bg};"></td>'
                        else:
                            html += f'<td style="padding:1px; border:{border_style};"></td>'
                    html += '</tr>'

            html += '</table></div>'

            st.markdown(html, unsafe_allow_html=True)

            # Legend
            st.markdown("""
            <div style="margin-top:12px; font-size:12px;">
                <span style="display:inline-block; width:16px; height:16px; background:#c8e6c9; border:1px solid #aaa; vertical-align:middle; border-radius:3px;"></span> Готово (push создан / акция завершена)&nbsp;&nbsp;
                <span style="display:inline-block; width:16px; height:16px; background:#fff9c4; border:1px solid #aaa; vertical-align:middle; border-radius:3px;"></span> Условия заполнены&nbsp;&nbsp;
                <span style="display:inline-block; width:16px; height:16px; background:#ffcdd2; border:1px solid #aaa; vertical-align:middle; border-radius:3px;"></span> Не заполнено&nbsp;&nbsp;
                <span style="display:inline-block; width:16px; height:16px; background:#bbdefb; border:1px solid #aaa; vertical-align:middle; border-radius:3px;"></span> PUSH (с номерами сообщений)
            </div>
            """, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 🔧 Условия акций
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "🔧 Условия акций":
    pass  # page loaded

    sid = st.session_state.get("spreadsheet_id",
                                "1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY")

    if st.button("🔄 Обновить данные", key="refresh_conditions"):
        load_cvm_data.clear()

    df_cvm = load_cvm_data(sid)

    if df_cvm.empty:
        st.warning("Нет данных. Проверьте доступ к таблице.")
    else:
        # Filter unfilled promos (no Описание акции)
        if "Описание акции" in df_cvm.columns:
            df_unfilled = df_cvm[
                df_cvm["Описание акции"].astype(str).str.strip().isin(["", "nan", "None", "NaT"])
            ].copy()
        else:
            df_unfilled = df_cvm.copy()

        st.caption(f"Акций без условий: {len(df_unfilled)}")

        if df_unfilled.empty:
            st.success("Все акции заполнены!")
        else:
            # Display unfilled promos
            display_cols = [c for c in ["НОМЕР", "Название промо", "Сегмент",
                                         "Старт акции", "Окончание акции",
                                         "Каналы коммуникации", "Описание акции"]
                            if c in df_unfilled.columns]
            if display_cols:
                st.dataframe(df_unfilled[display_cols], use_container_width=True, height=300)

            st.markdown("---")

            # Generate conditions button
            if st.button("🤖 Сгенерировать условия через AI", type="primary", key="gen_conditions"):
                try:
                    from ai_generator import generate_promo_conditions, get_similar_examples

                    all_promos = df_cvm.to_dict("records")
                    results = []
                    progress = st.progress(0)
                    total = len(df_unfilled)

                    for idx, (_, row) in enumerate(df_unfilled.iterrows()):
                        promo = row.to_dict()
                        examples = get_similar_examples(promo, all_promos, n=5)
                        try:
                            result = generate_promo_conditions(promo, examples)
                            result["__row_idx"] = row.name
                            result["__promo_num"] = _norm_num(promo.get("НОМЕР"))
                            result["__promo_name"] = str(promo.get("Название промо", ""))
                            results.append(result)
                        except Exception as e:
                            st.warning(f"Ошибка для акции {_norm_num(promo.get('НОМЕР'))}: {e}")
                        progress.progress((idx + 1) / total)

                    st.session_state.conditions_results = results
                    st.success(f"Сгенерировано условий: {len(results)}")
                except ImportError:
                    st.error("Модуль ai_generator не найден")
                except Exception as e:
                    st.error(f"Ошибка: {e}")

            # Show results with editable fields
            if "conditions_results" in st.session_state:
                results = st.session_state.conditions_results
                st.markdown("### Результаты генерации")

                edited_results = []
                for i, res in enumerate(results):
                    with st.container(border=True):
                        st.markdown(f"**{res.get('__promo_num', '')} — {res.get('__promo_name', '')}**")

                        desc = st.text_area(
                            "Описание акции",
                            value=res.get("Описание акции", res.get("description", "")),
                            key=f"cond_desc_{i}",
                            height=80,
                        )
                        coupon = st.text_input(
                            "Текст купона",
                            value=res.get("Текст на информационном купоне / слип-чеке",
                                         res.get("coupon_text", "")),
                            key=f"cond_coupon_{i}",
                        )
                        button_text = st.text_input(
                            "Кнопка",
                            value=res.get("Кнопка", res.get("button", "")),
                            key=f"cond_button_{i}",
                        )
                        edited_results.append({
                            "__row_idx": res.get("__row_idx"),
                            "Описание акции": desc,
                            "Текст на информационном купоне / слип-чеке": coupon,
                            "Кнопка": button_text,
                        })

                # Save to Google Sheets button
                if st.button("💾 Сохранить условия в Google Sheets", type="primary", key="save_conditions"):
                    try:
                        import gspread
                        from google.oauth2.service_account import Credentials
                        cred_path = "credentials/service_account.json"
                        if not os.path.exists(cred_path):
                            st.error("Нужен файл Service Account.")
                        else:
                            scopes = [
                                "https://www.googleapis.com/auth/spreadsheets",
                                "https://www.googleapis.com/auth/drive",
                            ]
                            creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
                            client = gspread.authorize(creds)
                            ss = client.open_by_key(sid)
                            ws = ss.worksheet("CVM offline")
                            headers = ws.row_values(1)

                            saved = 0
                            for er in edited_results:
                                row_idx = er.get("__row_idx")
                                if row_idx is None:
                                    continue
                                sheet_row = int(row_idx) + 2  # +1 header, +1 for 0-index
                                for field in ["Описание акции",
                                              "Текст на информационном купоне / слип-чеке",
                                              "Кнопка"]:
                                    if field in headers:
                                        col_idx = headers.index(field) + 1
                                        ws.update_cell(sheet_row, col_idx, er.get(field, ""))
                                saved += 1

                            load_cvm_data.clear()
                            st.success(f"Сохранено {saved} акций!")
                    except Exception as e:
                        st.error(f"Ошибка сохранения: {e}")


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: ✨ Генерация PUSH
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "✨ Генерация PUSH":
    pass  # page loaded

    sid = st.session_state.get("spreadsheet_id",
                                "1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY")

    # AI provider selectbox in top right
    top_col1, top_col2 = st.columns([3, 1])
    with top_col2:
        ai_provider = st.selectbox(
            "AI провайдер",
            ["builtin", "anthropic", "openai"],
            index=["builtin", "anthropic", "openai"].index(
                st.session_state.get("ai_provider", "builtin")
            ),
            key="gen_ai_provider",
        )
        st.session_state.ai_provider = ai_provider
        if ai_provider != "builtin":
            ai_key = st.text_input(
                "API ключ",
                type="password",
                key="gen_ai_key",
                value=st.session_state.get("ai_key", ""),
            )
            st.session_state.ai_key = ai_key
        else:
            ai_key = ""

    df_cvm = load_cvm_data(sid)
    df_push = load_push_data(sid)

    if df_cvm.empty:
        st.warning("Нет данных. Проверьте доступ к таблице.")
    else:
        # Filter PUSH-only promos (exclude slip)
        if "Каналы коммуникации" in df_cvm.columns:
            df_push_promos = df_cvm[
                df_cvm["Каналы коммуникации"].astype(str).str.strip().str.upper() == "PUSH"
            ].copy()
        else:
            df_push_promos = df_cvm.copy()

        # Exclude promos that already have push
        existing_push_nums = set()
        if not df_push.empty and "Номер промо" in df_push.columns:
            for val in df_push["Номер промо"].dropna():
                existing_push_nums.add(_norm_num(val))

        if "НОМЕР" in df_push_promos.columns:
            df_push_promos["_num_str"] = df_push_promos["НОМЕР"].apply(_norm_num)
            df_push_promos = df_push_promos[~df_push_promos["_num_str"].isin(existing_push_nums)].copy()

        # Month selectbox
        month_options_gen = ["Апрель 2026", "Май 2026", "Июнь 2026",
                             "Июль 2026", "Август 2026"]
        _MONTH_MAP_GEN = {
            "Январь": 1, "Февраль": 2, "Март": 3, "Апрель": 4,
            "Май": 5, "Июнь": 6, "Июль": 7, "Август": 8,
            "Сентябрь": 9, "Октябрь": 10, "Ноябрь": 11, "Декабрь": 12,
        }

        def _on_month_change():
            for k in ["mass_results", "single_result", "single_selected"]:
                if k in st.session_state:
                    del st.session_state[k]

        selected_gen_month = st.selectbox(
            "Месяц",
            month_options_gen,
            index=0,
            key="gen_month",
            on_change=_on_month_change,
        )

        # Filter by month intersection
        gen_parts = selected_gen_month.split()
        gen_month_num = _MONTH_MAP_GEN.get(gen_parts[0], 4)
        gen_year = int(gen_parts[1]) if len(gen_parts) > 1 else 2026
        gen_month_start = _date_cls(gen_year, gen_month_num, 1)
        if gen_month_num == 12:
            gen_month_end = _date_cls(gen_year + 1, 1, 1) - timedelta(days=1)
        else:
            gen_month_end = _date_cls(gen_year, gen_month_num + 1, 1) - timedelta(days=1)

        month_filtered = []
        for _, row in df_push_promos.iterrows():
            year_hint = row.get("Год", gen_year)
            try:
                year_hint = int(year_hint)
            except (ValueError, TypeError):
                year_hint = gen_year
            start_d = _parse_promo_date(row.get("Старт акции"), year_hint)
            end_d = _parse_promo_date(row.get("Окончание акции"), year_hint)
            if start_d and end_d:
                if end_d < start_d:
                    start_d, end_d = end_d, start_d
                if start_d <= gen_month_end and end_d >= gen_month_start:
                    month_filtered.append(row)

        df_gen = pd.DataFrame(month_filtered) if month_filtered else pd.DataFrame()

        if df_gen.empty:
            st.info("Нет акций без push-сообщений для выбранного месяца")
        else:
            st.caption(f"Акций без push-сообщений: {len(df_gen)}")

            # Вкладки массовая / по одной
            _tab_mass, _tab_single = st.tabs(["⚡ Массовая генерация", "🎯 По одной акции"])

            # ── МАССОВАЯ ─────────────────────────────────────────────
            with _tab_mass:
                if st.button("🚀 Сгенерировать все", type="primary", key="gen_mass_btn"):
                    from ai_generator import (
                        generate_push_texts, calculate_push_schedule,
                        _needs_activation, find_best_deeplink,
                    )

                    all_results = []
                    progress = st.progress(0)
                    total = len(df_gen)
                    rules = st.session_state.get("generation_rules", "")

                    for idx, (_, row) in enumerate(df_gen.iterrows()):
                        promo = row.to_dict()
                        schedule = calculate_push_schedule(promo)
                        try:
                            result = generate_push_texts(
                                promo=promo,
                                rules=rules,
                                num_variants=3,
                                title_max_len=35,
                                body_max_len=120,
                                schedule=schedule,
                                provider=ai_provider,
                                anthropic_key=ai_key if ai_provider == "anthropic" else None,
                                openai_key=ai_key if ai_provider == "openai" else None,
                            )
                            # Auto deeplink
                            needs_act = _needs_activation(promo)
                            if needs_act:
                                auto_deeplink = "купон"
                            else:
                                cat = str(promo.get("Название промо", ""))
                                dl = find_best_deeplink(cat)
                                auto_deeplink = dl["deeplink"] if dl else ""

                            all_results.append({
                                "promo": promo,
                                "result": result,
                                "schedule": schedule,
                                "auto_deeplink": auto_deeplink,
                            })
                        except Exception as e:
                            st.warning(f"Ошибка для {_norm_num(promo.get('НОМЕР'))}: {e}")
                        progress.progress((idx + 1) / total)

                    st.session_state.mass_results = all_results
                    st.success(f"Сгенерировано: {len(all_results)} акций")

                # Display mass results
                if "mass_results" in st.session_state:
                    results = st.session_state.mass_results

                    if "mass_checked" not in st.session_state:
                        st.session_state.mass_checked = {i: True for i in range(len(results))}

                    for i, item in enumerate(results):
                        promo = item["promo"]
                        result = item["result"]
                        auto_deeplink = item["auto_deeplink"]
                        promo_num = _norm_num(promo.get("НОМЕР"))
                        promo_name = str(promo.get("Название промо", ""))

                        with st.container(border=True):
                            ccol1, ccol2 = st.columns([0.05, 0.95])
                            with ccol1:
                                checked = st.checkbox(
                                    "",
                                    value=st.session_state.mass_checked.get(i, True),
                                    key=f"mass_check_{i}",
                                    label_visibility="collapsed",
                                )
                                st.session_state.mass_checked[i] = checked
                            with ccol2:
                                st.markdown(f"**{promo_num} — {promo_name}**")

                            pushes = result.get("pushes", [])
                            for p_idx, push_data in enumerate(pushes):
                                push_num = push_data.get("push_number", p_idx + 1)
                                variants = push_data.get("variants", [])
                                if not variants:
                                    continue
                                # Use first variant by default
                                var = variants[0]
                                title = var.get("title", "")
                                body = var.get("body", "")

                                st.caption(f"Push #{push_num}")

                                pcol1, pcol2, pcol3 = st.columns(3)
                                with pcol1:
                                    push_date_val = st.text_input(
                                        "Дата",
                                        value=push_data.get("date", ""),
                                        key=f"mass_date_{i}_{p_idx}",
                                        label_visibility="collapsed",
                                    )
                                with pcol2:
                                    push_time_val = st.text_input(
                                        "Время",
                                        value=push_data.get("time", "10:00"),
                                        key=f"mass_time_{i}_{p_idx}",
                                        label_visibility="collapsed",
                                    )
                                with pcol3:
                                    push_deeplink_val = st.text_input(
                                        "Deeplink",
                                        value=auto_deeplink,
                                        key=f"mass_dl_{i}_{p_idx}",
                                        label_visibility="collapsed",
                                    )

                                edited_title = st.text_input(
                                    "Заголовок",
                                    value=title,
                                    key=f"mass_title_{i}_{p_idx}",
                                    label_visibility="collapsed",
                                )
                                edited_body = st.text_area(
                                    "Текст",
                                    value=body,
                                    key=f"mass_body_{i}_{p_idx}",
                                    height=60,
                                    label_visibility="collapsed",
                                )
                                tlen = len(edited_title)
                                blen = len(edited_body)
                                t_ok = "🟢" if tlen <= 35 else "🔴"
                                b_ok = "🟢" if blen <= 120 else "🔴"
                                st.caption(f"Заголовок: {t_ok} {tlen}/35 | Текст: {b_ok} {blen}/120")

                    # Save button
                    if st.button("💾 Сохранить выбранные", type="primary", key="save_mass"):
                        rows_to_save = []
                        for i, item in enumerate(results):
                            if not st.session_state.mass_checked.get(i, False):
                                continue
                            promo = item["promo"]
                            result = item["result"]
                            pushes = result.get("pushes", [])

                            for p_idx, push_data in enumerate(pushes):
                                push_num = push_data.get("push_number", p_idx + 1)
                                title_val = st.session_state.get(f"mass_title_{i}_{p_idx}", "")
                                body_val = st.session_state.get(f"mass_body_{i}_{p_idx}", "")
                                date_val = st.session_state.get(f"mass_date_{i}_{p_idx}", "")
                                time_val = st.session_state.get(f"mass_time_{i}_{p_idx}", "10:00")
                                dl_val = st.session_state.get(f"mass_dl_{i}_{p_idx}", "")

                                # Day of week and week number from date
                                dow = ""
                                week_num = ""
                                parsed_d = _parse_promo_date(date_val)
                                if parsed_d:
                                    _DOW_NAMES = {0: "пн", 1: "вт", 2: "ср", 3: "чт",
                                                  4: "пт", 5: "сб", 6: "вс"}
                                    dow = _DOW_NAMES.get(parsed_d.weekday(), "")
                                    week_num = str(parsed_d.isocalendar()[1])

                                row = {
                                    "Сегмент": str(promo.get("Сегмент", "")),
                                    "Канал": "PUSH",
                                    "Название промо": str(promo.get("Название промо", "")),
                                    "Год": str(promo.get("Год", "")),
                                    "Месяц": str(promo.get("Месяц", "")),
                                    "Нед": week_num,
                                    "День недели": dow,
                                    "Дата": date_val,
                                    "Время": time_val,
                                    "Клиентов": str(promo.get("Примерное количество клиентов", "")),
                                    "Доп настройки клиентов": "минус фрод",
                                    "Номер промо": _norm_num(promo.get("НОМЕР")),
                                    "Номер msg": str(push_num),
                                    "PUSH заголовок": title_val,
                                    "__title_len": len(title_val),
                                    "текст PUSH": body_val,
                                    "__body_len": len(body_val),
                                    "Экран - ссылка": dl_val,
                                    "Текст купона": "",
                                    "Кнопка": "",
                                }
                                rows_to_save.append(row)

                        if rows_to_save:
                            try:
                                if save_to_sheets(rows_to_save, sid):
                                    st.success(f"Сохранено {len(rows_to_save)} push!")
                                    load_push_data.clear()
                            except Exception as e:
                                st.error(f"Ошибка сохранения: {e}")
                        else:
                            st.warning("Нет выбранных акций для сохранения")

            # ── ПО ОДНОЙ ─────────────────────────────────────────────
            with _tab_single:
                # Selectbox with promos
                promo_options = {}
                for _, row in df_gen.iterrows():
                    key = f"{_norm_num(row.get('НОМЕР'))} — {row.get('Название промо', '?')}"
                    promo_options[key] = row.to_dict()

                if not promo_options:
                    st.info("Нет доступных акций")
                else:
                    selected_promo_key = st.selectbox(
                        "Выберите акцию",
                        list(promo_options.keys()),
                        key="single_promo_select",
                    )
                    selected_promo = promo_options[selected_promo_key]

                    # Promo details
                    with st.expander("📋 Детали акции", expanded=False):
                        info_cols = st.columns(3)
                        with info_cols[0]:
                            st.markdown(f"**Номер:** {_norm_num(selected_promo.get('НОМЕР'))}")
                            st.markdown(f"**Сегмент:** {selected_promo.get('Сегмент', '')}")
                            st.markdown(f"**Категория:** {selected_promo.get('Категория', '')}")
                        with info_cols[1]:
                            st.markdown(f"**Старт:** {selected_promo.get('Старт акции', '')}")
                            st.markdown(f"**Окончание:** {selected_promo.get('Окончание акции', '')}")
                            st.markdown(f"**Бонусы:** {selected_promo.get('Бонусы', '')}")
                        with info_cols[2]:
                            st.markdown(f"**Описание:** {selected_promo.get('Описание акции', '')}")
                            st.markdown(f"**Механика:** {selected_promo.get('Механика', '')}")

                    # Search on dixy.ru button
                    if st.button("🔍 Найти акции на dixy.ru", key="search_dixy"):
                        try:
                            from dixy_parser import search_discounts
                            promo_name = str(selected_promo.get("Название промо", ""))
                            discounts = search_discounts(promo_name)
                            if discounts:
                                st.session_state.dixy_results = discounts
                                dixy_df = pd.DataFrame(discounts)
                                display_cols_dixy = [c for c in ["name", "price", "old_price", "discount"]
                                                     if c in dixy_df.columns]
                                if display_cols_dixy:
                                    st.dataframe(dixy_df[display_cols_dixy], use_container_width=True)
                                else:
                                    st.dataframe(dixy_df, use_container_width=True)
                            else:
                                st.info("Товары не найдены на dixy.ru")
                        except Exception as e:
                            st.warning(f"Ошибка поиска: {e}")

                    # Extra rules for AI providers
                    extra_rules = ""
                    if ai_provider != "builtin":
                        extra_rules = st.text_area(
                            "Дополнительные правила для AI",
                            value="",
                            key="single_extra_rules",
                            height=80,
                        )

                    # Generate button
                    if st.button("🚀 Сгенерировать push", type="primary", key="gen_single_btn"):
                        from ai_generator import (
                            generate_push_texts, calculate_push_schedule,
                            _needs_activation, find_best_deeplink,
                        )

                        schedule = calculate_push_schedule(selected_promo)
                        rules = st.session_state.get("generation_rules", "")
                        if extra_rules:
                            rules = rules + "\n\n" + extra_rules

                        with st.spinner("Генерация..."):
                            try:
                                result = generate_push_texts(
                                    promo=selected_promo,
                                    rules=rules,
                                    num_variants=3,
                                    title_max_len=35,
                                    body_max_len=120,
                                    schedule=schedule,
                                    provider=ai_provider,
                                    anthropic_key=ai_key if ai_provider == "anthropic" else None,
                                    openai_key=ai_key if ai_provider == "openai" else None,
                                )
                                needs_act = _needs_activation(selected_promo)
                                if needs_act:
                                    auto_dl = "купон"
                                else:
                                    cat = str(selected_promo.get("Название промо", ""))
                                    dl = find_best_deeplink(cat)
                                    auto_dl = dl["deeplink"] if dl else ""

                                st.session_state.single_result = {
                                    "result": result,
                                    "promo": selected_promo,
                                    "schedule": schedule,
                                    "auto_deeplink": auto_dl,
                                }
                                st.success("Тексты сгенерированы!")
                            except Exception as e:
                                st.error(f"Ошибка: {e}")

                    # Display single results
                    if "single_result" in st.session_state:
                        sr = st.session_state.single_result
                        result = sr["result"]
                        promo = sr["promo"]
                        auto_dl = sr["auto_deeplink"]

                        st.markdown("### Результаты")

                        pushes = result.get("pushes", [])
                        if "single_approved" not in st.session_state:
                            st.session_state.single_approved = {}

                        for p_idx, push_data in enumerate(pushes):
                            push_num = push_data.get("push_number", p_idx + 1)
                            st.markdown(f"#### Push #{push_num}")

                            # Date/time/deeplink inputs
                            scol1, scol2, scol3 = st.columns(3)
                            with scol1:
                                s_date = st.text_input(
                                    "Дата",
                                    value=push_data.get("date", ""),
                                    key=f"single_date_{p_idx}",
                                )
                            with scol2:
                                s_time = st.text_input(
                                    "Время",
                                    value=push_data.get("time", "10:00"),
                                    key=f"single_time_{p_idx}",
                                )
                            with scol3:
                                s_dl = st.text_input(
                                    "Deeplink",
                                    value=auto_dl,
                                    key=f"single_dl_{p_idx}",
                                )

                            variants = push_data.get("variants", [])
                            for v_idx, variant in enumerate(variants):
                                with st.container(border=True):
                                    vkey = f"{p_idx}_{v_idx}"
                                    is_approved = st.checkbox(
                                        f"Вариант {v_idx + 1}",
                                        value=st.session_state.single_approved.get(vkey, False),
                                        key=f"single_approve_{vkey}",
                                    )
                                    st.session_state.single_approved[vkey] = is_approved

                                    edited_title = st.text_input(
                                        "Заголовок",
                                        value=variant.get("title", ""),
                                        key=f"single_title_{vkey}",
                                    )
                                    edited_body = st.text_area(
                                        "Текст",
                                        value=variant.get("body", ""),
                                        key=f"single_body_{vkey}",
                                        height=60,
                                    )
                                    tlen = len(edited_title)
                                    blen = len(edited_body)
                                    t_ok = "🟢" if tlen <= 35 else "🔴"
                                    b_ok = "🟢" if blen <= 120 else "🔴"
                                    st.caption(f"Заголовок: {t_ok} {tlen}/35 | Текст: {b_ok} {blen}/120")

                        # Save approved variants
                        st.markdown("---")
                        if st.button("💾 Сохранить выбранные варианты", type="primary", key="save_single"):
                            rows_to_save = []
                            for p_idx, push_data in enumerate(pushes):
                                push_num = push_data.get("push_number", p_idx + 1)
                                variants = push_data.get("variants", [])
                                for v_idx, _ in enumerate(variants):
                                    vkey = f"{p_idx}_{v_idx}"
                                    if not st.session_state.single_approved.get(vkey, False):
                                        continue
                                    title_val = st.session_state.get(f"single_title_{vkey}", "")
                                    body_val = st.session_state.get(f"single_body_{vkey}", "")
                                    date_val = st.session_state.get(f"single_date_{p_idx}", "")
                                    time_val = st.session_state.get(f"single_time_{p_idx}", "10:00")
                                    dl_val = st.session_state.get(f"single_dl_{p_idx}", "")

                                    # Day of week and week number
                                    dow = ""
                                    week_num = ""
                                    parsed_d = _parse_promo_date(date_val)
                                    if parsed_d:
                                        _DOW_NAMES = {0: "пн", 1: "вт", 2: "ср", 3: "чт",
                                                      4: "пт", 5: "сб", 6: "вс"}
                                        dow = _DOW_NAMES.get(parsed_d.weekday(), "")
                                        week_num = str(parsed_d.isocalendar()[1])

                                    row = {
                                        "Сегмент": str(promo.get("Сегмент", "")),
                                        "Канал": "PUSH",
                                        "Название промо": str(promo.get("Название промо", "")),
                                        "Год": str(promo.get("Год", "")),
                                        "Месяц": str(promo.get("Месяц", "")),
                                        "Нед": week_num,
                                        "День недели": dow,
                                        "Дата": date_val,
                                        "Время": time_val,
                                        "Клиентов": str(promo.get("Примерное количество клиентов", "")),
                                        "Доп настройки клиентов": "минус фрод",
                                        "Номер промо": _norm_num(promo.get("НОМЕР")),
                                        "Номер msg": str(push_num),
                                        "PUSH заголовок": title_val,
                                        "__title_len": len(title_val),
                                        "текст PUSH": body_val,
                                        "__body_len": len(body_val),
                                        "Экран - ссылка": dl_val,
                                        "Текст купона": "",
                                        "Кнопка": "",
                                    }
                                    rows_to_save.append(row)

                            if rows_to_save:
                                try:
                                    if save_to_sheets(rows_to_save, sid):
                                        st.success(f"Сохранено {len(rows_to_save)} push!")
                                        load_push_data.clear()
                                except Exception as e:
                                    st.error(f"Ошибка сохранения: {e}")
                            else:
                                st.warning("Выберите хотя бы один вариант")


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 📝 Правила генерации
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "📝 Правила генерации":
    pass  # page loaded
    st.markdown("Эти правила используются AI при генерации текстов push-уведомлений.")

    default_rules = """1. Тон: дружелюбный, энергичный, разговорный (как в мессенджере с другом)
2. Используй эмодзи в заголовке (1-2 шт.), в тексте — умеренно
3. Обязательно указывай размер выгоды (скидку, кешбэк, бонусы) в заголовке или первых словах текста
4. Указывай срок действия акции в тексте ("до DD.MM", "только сегодня", "три дня")
5. Призыв к действию: "Забегай", "Лови", "Активируй", "Бери" — не "Посетите" или "Приобретите"
6. Для сегмента "Отток/Спящие" — более щедрое предложение, упоминай что соскучились
7. Для повторных push — напоминающий тон: "Ещё можно успеть", "Не упусти", "Последний день"
8. Не используй слова: "уважаемый", "клиент", "предложение ограничено"
9. Если акция на категорию — перечисли 2-3 конкретных товара из неё
10. Заголовок должен быть самодостаточным — понятен без прочтения текста
11. PUSH заголовок: макс 35 символов
12. Текст PUSH: макс 120 символов
13. Не используй кавычки-ёлочки и сложные конструкции
14. Deeplink = "купон" если в акции есть активация купона
15. Для каждого push — уникальный текст, не повторяй формулировки"""

    if "generation_rules" not in st.session_state:
        st.session_state.generation_rules = default_rules

    st.session_state.generation_rules = st.text_area(
        "Правила генерации",
        value=st.session_state.generation_rules,
        height=400,
        key="rules_editor",
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Сбросить к значениям по умолчанию", key="reset_rules"):
            st.session_state.generation_rules = default_rules
            st.rerun()
    with col2:
        if st.button("💾 Сохранить правила", key="save_rules"):
            st.success("Правила сохранены!")


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 🔗 Deeplinks
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "🔗 Deeplinks":
    pass  # page loaded

    try:
        from ai_generator import search_deeplinks, _load_deeplinks

        # Load all deeplinks
        df_dl = _load_deeplinks()

        if df_dl is not None and not df_dl.empty:
            # Search
            search_query = st.text_input(
                "🔍 Поиск по категории",
                value="",
                key="dl_search",
                placeholder="Введите название категории...",
            )

            if search_query:
                results = search_deeplinks(search_query, limit=50)
                if results:
                    st.dataframe(
                        pd.DataFrame(results),
                        use_container_width=True,
                        height=500,
                    )
                    st.caption(f"Найдено: {len(results)}")
                else:
                    st.info("Ничего не найдено")
            else:
                # Show all
                display_cols_dl = [c for c in ["Id", "КАТЕГОРИЯ", "deeplink"]
                                   if c in df_dl.columns]
                if display_cols_dl:
                    st.dataframe(
                        df_dl[display_cols_dl],
                        use_container_width=True,
                        height=600,
                    )
                else:
                    st.dataframe(df_dl, use_container_width=True, height=600)
                st.caption(f"Всего deeplinks: {len(df_dl)}")
        else:
            st.warning("Файл deeplink.xlsx не найден или пуст. Поместите его в data/deeplink.xlsx")
    except ImportError:
        st.error("Модуль ai_generator не найден")
    except Exception as e:
        st.error(f"Ошибка загрузки deeplinks: {e}")
