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
st.set_page_config(
    page_title="CVM генератор",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Brand design system ─────────────────────────────────────────────────────
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
/* ════════════════════════════════════════════════════════════════════
   DIXY · CVM STUDIO — Brand Design System
   Aligned with real Dixy brand palette: orange / purple / green
   ════════════════════════════════════════════════════════════════════ */
:root {
    /* Primary — Dixy Orange */
    --dx-orange:     #EF7C1A;
    --dx-orange-700: #C8620B;
    --dx-orange-500: #F2913C;
    --dx-orange-100: #FDE6CF;
    --dx-orange-50:  #FFF4E6;

    /* Secondary — Dixy Purple */
    --dx-purple:     #5E2D8A;
    --dx-purple-700: #4A2270;
    --dx-purple-500: #7E4DAE;
    --dx-purple-100: #E4D6F2;
    --dx-purple-50:  #F4ECFA;

    /* Tertiary — Dixy Green */
    --dx-green:      #8BC34A;
    --dx-green-700:  #6CA13A;
    --dx-green-500:  #A0D165;
    --dx-green-100:  #DCEEC4;
    --dx-green-50:   #F0F8E5;

    /* Cream tint used in Dixy materials */
    --dx-cream:      #FFF6E0;

    --ink-900: #1F1B2E;
    --ink-800: #2D2740;
    --ink-700: #3F3859;
    --ink-600: #524A6E;
    --ink-500: #7A7290;
    --ink-400: #A299B5;
    --ink-300: #C9C2D6;
    --ink-200: #E5E0EE;
    --ink-100: #F2EFF7;
    --ink-50:  #FAF8FD;

    --surface: #FFFFFF;
    --bg: #F7F5FB;

    --success-700: #6CA13A;
    --success-50:  #F0F8E5;
    --warning-700: #C8620B;
    --warning-50:  #FFF4E6;
    --info-700:    #4A2270;
    --info-50:     #F4ECFA;
    --danger-700:  #B91C1C;
    --danger-50:   #FEE2E2;

    --shadow-sm: 0 1px 2px rgba(31,27,46,.05), 0 1px 1px rgba(31,27,46,.03);
    --shadow-md: 0 4px 12px rgba(31,27,46,.07), 0 2px 4px rgba(31,27,46,.04);
    --shadow-lg: 0 18px 40px rgba(31,27,46,.10), 0 6px 16px rgba(31,27,46,.05);

    --radius-sm: 8px;
    --radius-md: 12px;
    --radius-lg: 16px;
}

/* ─── Global typography & layout ──────────────────────────────────── */
html, body, [class*="css"], .stApp, .main, section[data-testid="stSidebar"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    color: var(--ink-800);
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}
.stApp {
    background: var(--bg) !important;
}
.main .block-container {
    padding-top: 1.2rem !important;
    padding-bottom: 4rem !important;
    max-width: 1400px;
}
h1, h2, h3, h4, h5 {
    font-family: 'Inter', sans-serif !important;
    color: var(--ink-900) !important;
    letter-spacing: -0.015em !important;
    font-weight: 700 !important;
}
h1 { font-size: 28px !important; }
h2 { font-size: 22px !important; }
h3 { font-size: 18px !important; }
h4 { font-size: 15px !important; }
p, label, .stMarkdown { color: var(--ink-700); font-size: 14px; }
code, pre { font-family: 'JetBrains Mono', monospace !important; font-size: 12.5px; }

/* Hide Streamlit's default chrome */
#MainMenu, footer, header[data-testid="stHeader"] { visibility: hidden; height: 0; }

/* ─── Sidebar ─────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: #1F1B2E !important;
    border-right: 1px solid #2D2740;
}
section[data-testid="stSidebar"] > div:first-child {
    padding-top: 24px;
}
section[data-testid="stSidebar"] * { color: #E5E0EE !important; }
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 { color: #FFFFFF !important; }
section[data-testid="stSidebar"] .stMarkdown p { color: #A299B5 !important; font-size: 12px; }

/* Sidebar nav radio → vertical pill list with orange active */
section[data-testid="stSidebar"] div[data-testid="stRadio"] > div {
    flex-direction: column !important;
    gap: 2px !important;
}
section[data-testid="stSidebar"] div[data-testid="stRadio"] > div > label {
    background: transparent !important;
    border: 1px solid transparent !important;
    border-radius: 10px !important;
    padding: 10px 14px !important;
    margin: 0 !important;
    transition: all .15s ease;
    color: #C9C2D6 !important;
    font-weight: 500 !important;
    font-size: 13.5px !important;
    position: relative;
}
section[data-testid="stSidebar"] div[data-testid="stRadio"] > div > label:hover {
    background: rgba(255,255,255,.04) !important;
    color: #FFFFFF !important;
}
section[data-testid="stSidebar"] div[data-testid="stRadio"] > div > label[data-checked="true"] {
    background: rgba(239,124,26,.14) !important;
    color: #FFFFFF !important;
    border-color: rgba(239,124,26,.30) !important;
}
section[data-testid="stSidebar"] div[data-testid="stRadio"] > div > label[data-checked="true"]::before {
    content: "";
    position: absolute;
    left: -10px;
    top: 10px;
    bottom: 10px;
    width: 3px;
    border-radius: 0 3px 3px 0;
    background: var(--dx-orange);
}
section[data-testid="stSidebar"] div[data-testid="stRadio"] > div > label > div:first-child {
    display: none !important;
}

/* Sidebar button */
section[data-testid="stSidebar"] .stButton > button {
    background: rgba(255,255,255,.04) !important;
    color: #E5E7EB !important;
    border: 1px solid rgba(255,255,255,.10) !important;
    border-radius: 10px !important;
    font-weight: 500 !important;
    font-size: 12.5px !important;
    padding: 8px 12px !important;
    transition: all .15s ease;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(255,255,255,.08) !important;
    border-color: rgba(255,255,255,.18) !important;
}
section[data-testid="stSidebar"] .stExpander {
    background: rgba(255,255,255,.02) !important;
    border: 1px solid rgba(255,255,255,.06) !important;
    border-radius: 10px !important;
}
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea {
    background: rgba(255,255,255,.04) !important;
    color: #F8FAFC !important;
    border: 1px solid rgba(255,255,255,.10) !important;
    border-radius: 8px !important;
}

/* ─── Metric cards — McKinsey style ──────────────────────────────── */
div[data-testid="stMetric"] {
    background: var(--surface) !important;
    border: 1px solid var(--ink-200);
    border-left: 3px solid var(--dx-orange);
    border-radius: var(--radius-md);
    padding: 16px 20px !important;
    box-shadow: var(--shadow-sm);
    transition: all .18s ease;
}
div[data-testid="stMetric"]:hover {
    box-shadow: var(--shadow-md);
    transform: translateY(-1px);
}
div[data-testid="stMetric"] label {
    color: var(--ink-500) !important;
    font-size: 11.5px !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: var(--ink-900) !important;
    font-size: 30px !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em !important;
    line-height: 1.1 !important;
    margin-top: 4px !important;
}
div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
    font-size: 12px !important;
    font-weight: 500 !important;
}

/* ─── Buttons ─────────────────────────────────────────────────────── */
.stButton > button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-size: 13.5px !important;
    padding: 9px 16px !important;
    border: 1px solid var(--ink-200) !important;
    background: var(--surface) !important;
    color: var(--ink-800) !important;
    transition: all .15s ease;
    box-shadow: var(--shadow-sm);
}
.stButton > button:hover {
    background: var(--ink-50) !important;
    border-color: var(--ink-300) !important;
    transform: translateY(-1px);
    box-shadow: var(--shadow-md);
}
.stButton > button[kind="primary"],
.stButton > button[data-testid="baseButton-primary"] {
    background: var(--dx-orange) !important;
    color: #fff !important;
    border-color: var(--dx-orange) !important;
    box-shadow: 0 6px 14px rgba(239,124,26,.30);
}
.stButton > button[kind="primary"]:hover,
.stButton > button[data-testid="baseButton-primary"]:hover {
    background: var(--dx-orange-700) !important;
    border-color: var(--dx-orange-700) !important;
    box-shadow: 0 10px 20px rgba(239,124,26,.35);
}

/* ─── Inputs (text, select, textarea, date) ──────────────────────── */
.stTextInput input,
.stTextArea textarea,
.stNumberInput input,
.stDateInput input,
div[data-baseweb="select"] > div {
    background: var(--surface) !important;
    border: 1px solid var(--ink-200) !important;
    border-radius: 10px !important;
    color: var(--ink-900) !important;
    font-size: 13.5px !important;
    transition: all .15s ease;
}
.stTextInput input:focus,
.stTextArea textarea:focus,
.stNumberInput input:focus,
.stDateInput input:focus,
div[data-baseweb="select"]:focus-within > div {
    border-color: var(--dx-orange) !important;
    box-shadow: 0 0 0 3px rgba(239,124,26,.14) !important;
}
.stTextInput label, .stTextArea label, .stNumberInput label,
.stSelectbox label, .stDateInput label, .stMultiSelect label,
.stRadio > label, .stCheckbox > label {
    color: var(--ink-700) !important;
    font-size: 12.5px !important;
    font-weight: 600 !important;
}

/* ─── Radio as segmented control (main area, not sidebar) ────────── */
.main div[data-testid="stRadio"] > div {
    flex-direction: row !important;
    gap: 0 !important;
    background: var(--surface);
    padding: 5px;
    border-radius: 12px;
    border: 1px solid var(--ink-200);
    display: inline-flex !important;
    width: fit-content;
    box-shadow: var(--shadow-sm);
}
.main div[data-testid="stRadio"] > div > label {
    background: transparent !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 8px 18px !important;
    margin: 0 !important;
    color: var(--ink-600) !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    transition: all .18s ease;
    cursor: pointer;
    letter-spacing: -0.005em;
}
.main div[data-testid="stRadio"] > div > label:hover {
    color: var(--dx-orange-700) !important;
    background: var(--dx-orange-50) !important;
}
.main div[data-testid="stRadio"] > div > label[data-checked="true"] {
    background: var(--dx-orange) !important;
    color: #fff !important;
    box-shadow: 0 4px 10px rgba(239,124,26,.30);
}
.main div[data-testid="stRadio"] > div > label[data-checked="true"]:hover {
    background: var(--dx-orange-700) !important;
    color: #fff !important;
}
.main div[data-testid="stRadio"] > div > label > div:first-child {
    display: none !important;
}

/* ─── Tabs — underline style (McKinsey/Stripe) ────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: transparent;
    padding: 0 0 0 0;
    border-radius: 0;
    border: none;
    border-bottom: 1px solid var(--ink-200);
    margin-bottom: 18px;
}
.stTabs [data-baseweb="tab"] {
    height: 42px !important;
    padding: 0 18px !important;
    background: transparent !important;
    border-radius: 0 !important;
    color: var(--ink-500) !important;
    font-weight: 600 !important;
    font-size: 14px !important;
    border-bottom: 2px solid transparent !important;
    margin-bottom: -1px !important;
    transition: all .15s ease;
}
.stTabs [data-baseweb="tab"]:hover {
    color: var(--ink-900) !important;
}
.stTabs [aria-selected="true"] {
    background: transparent !important;
    color: var(--dx-orange-700) !important;
    border-bottom: 2px solid var(--dx-orange) !important;
}

/* ─── Multiselect chips ──────────────────────────────────────────── */
div[data-baseweb="select"] [data-baseweb="tag"] {
    background: var(--dx-purple-50) !important;
    border: 1px solid var(--dx-purple-100) !important;
    color: var(--dx-purple-700) !important;
    border-radius: 999px !important;
    font-size: 12px !important;
    font-weight: 500 !important;
}

/* ─── Toggle / switch ─────────────────────────────────────────────── */
label[data-baseweb="checkbox"] > div:first-child {
    border-radius: 5px !important;
}
input:checked + div[role="switch"],
[data-baseweb="checkbox"][aria-checked="true"] > div:first-child {
    background: var(--dx-orange) !important;
    border-color: var(--dx-orange) !important;
}

/* ─── Expander as card ────────────────────────────────────────────── */
.streamlit-expanderHeader, details > summary {
    background: var(--surface) !important;
    border: 1px solid var(--ink-200) !important;
    border-radius: var(--radius-md) !important;
    padding: 12px 16px !important;
    font-weight: 600 !important;
    color: var(--ink-800) !important;
    font-size: 14px !important;
    transition: all .15s ease;
}
.streamlit-expanderHeader:hover {
    border-color: var(--dx-orange) !important;
    box-shadow: var(--shadow-sm);
}
div[data-testid="stExpander"] {
    background: var(--surface);
    border-radius: var(--radius-md);
    overflow: hidden;
    margin-bottom: 10px;
}
div[data-testid="stExpander"] > details {
    border: 1px solid var(--ink-200);
    border-radius: var(--radius-md);
    background: var(--surface);
}
div[data-testid="stExpander"] > details[open] {
    box-shadow: var(--shadow-md);
}
div[data-testid="stExpander"] > details > summary {
    border: none !important;
    box-shadow: none !important;
}

/* Bordered containers as proper cards */
div[data-testid="stContainer"][class*="st-emotion"] {
    border-radius: var(--radius-md);
}
div[data-testid="stVerticalBlockBorderWrapper"] {
    border: 1px solid var(--ink-200) !important;
    border-radius: var(--radius-md) !important;
    background: var(--surface) !important;
    box-shadow: var(--shadow-sm) !important;
    padding: 6px !important;
}

/* ─── Progress bar — Dixy orange ──────────────────────────────────── */
.stProgress > div > div {
    background: linear-gradient(90deg, var(--dx-orange) 0%, var(--dx-orange-500) 100%) !important;
    border-radius: 999px !important;
    height: 8px !important;
}
.stProgress {
    background: var(--ink-100);
    border-radius: 999px;
}

/* ─── Alerts (info/warning/success/error) ────────────────────────── */
div[data-baseweb="notification"],
.stAlert {
    border-radius: var(--radius-md) !important;
    border: 1px solid transparent !important;
    box-shadow: var(--shadow-sm) !important;
    padding: 12px 16px !important;
}
div[data-testid="stAlert"][kind="info"],
.stAlert[data-baseweb="notification"][kind="info"] {
    background: var(--info-50) !important;
    border-color: rgba(30,64,175,.20) !important;
    color: var(--info-700) !important;
}
div[data-testid="stAlert"][kind="success"] {
    background: var(--success-50) !important;
    border-color: rgba(4,120,87,.20) !important;
    color: var(--success-700) !important;
}
div[data-testid="stAlert"][kind="warning"] {
    background: var(--warning-50) !important;
    border-color: rgba(146,64,14,.20) !important;
    color: var(--warning-700) !important;
}
div[data-testid="stAlert"][kind="error"] {
    background: var(--danger-50) !important;
    border-color: rgba(185,28,28,.20) !important;
    color: var(--danger-700) !important;
}

/* ─── DataFrame ───────────────────────────────────────────────────── */
.stDataFrame, div[data-testid="stDataFrame"] {
    border-radius: var(--radius-md) !important;
    border: 1px solid var(--ink-200) !important;
    overflow: hidden;
    box-shadow: var(--shadow-sm);
}

/* ─── Captions ────────────────────────────────────────────────────── */
.stCaption, p.caption, [data-testid="stCaptionContainer"] {
    color: var(--ink-500) !important;
    font-size: 12.5px !important;
}

/* ─── Section title (used on each page) ──────────────────────────── */
.dx-section {
    margin: 6px 0 20px 0;
}
.dx-section-eyebrow {
    text-transform: uppercase;
    letter-spacing: 0.10em;
    font-size: 11px;
    font-weight: 700;
    color: var(--dx-orange);
    margin-bottom: 6px;
}
.dx-section-title {
    font-size: 26px;
    font-weight: 700;
    color: var(--ink-900);
    letter-spacing: -0.02em;
    line-height: 1.15;
    margin: 0;
}
.dx-section-sub {
    color: var(--ink-500);
    font-size: 13.5px;
    margin-top: 6px;
    max-width: 720px;
}

/* ─── Phone preview (push mockup) ─────────────────────────────────── */
.dx-phone {
    width: 320px;
    background: linear-gradient(180deg, #1F2937 0%, #0F172A 100%);
    border-radius: 36px;
    padding: 14px;
    box-shadow: 0 24px 60px rgba(15,23,42,.35), 0 4px 12px rgba(15,23,42,.10);
    position: relative;
}
.dx-phone-notch {
    width: 96px;
    height: 22px;
    background: #000;
    border-radius: 0 0 16px 16px;
    margin: -14px auto 10px auto;
}
.dx-phone-screen {
    background: linear-gradient(160deg, #5B7FD7 0%, #C76FA8 50%, #E89B61 100%);
    border-radius: 24px;
    min-height: 380px;
    padding: 18px 10px 10px 10px;
    position: relative;
}
.dx-phone-time {
    color: #fff;
    font-weight: 700;
    font-size: 38px;
    text-align: center;
    letter-spacing: -0.02em;
    text-shadow: 0 2px 6px rgba(0,0,0,.25);
    margin-bottom: 4px;
}
.dx-phone-date {
    color: rgba(255,255,255,.92);
    font-weight: 500;
    font-size: 12px;
    text-align: center;
    margin-bottom: 18px;
    text-shadow: 0 1px 3px rgba(0,0,0,.20);
}
.dx-push {
    background: rgba(255,255,255,.78);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border-radius: 16px;
    padding: 10px 12px;
    margin: 8px 4px;
    box-shadow: 0 4px 14px rgba(0,0,0,.10);
    display: flex;
    gap: 10px;
    align-items: flex-start;
}
.dx-push-icon {
    width: 36px;
    height: 36px;
    border-radius: 9px;
    background: var(--dx-orange);
    color: #fff;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 800;
    font-size: 16px;
    flex: 0 0 36px;
    box-shadow: 0 4px 10px rgba(239,124,26,.30);
}
.dx-push-body { flex: 1; min-width: 0; }
.dx-push-app {
    font-size: 10.5px;
    font-weight: 600;
    color: #1F2937;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    display: flex;
    justify-content: space-between;
    margin-bottom: 2px;
}
.dx-push-time { color: #475569; font-weight: 500; }
.dx-push-title {
    font-size: 13.5px;
    font-weight: 600;
    color: #0B1220;
    line-height: 1.25;
    margin-bottom: 2px;
    word-wrap: break-word;
}
.dx-push-text {
    font-size: 12.5px;
    color: #1F2937;
    line-height: 1.35;
    word-wrap: break-word;
}

/* ─── Misc polish ─────────────────────────────────────────────────── */
hr { border-color: var(--ink-200) !important; margin: 18px 0 !important; }
.stCheckbox > label > div:first-child {
    border-radius: 5px !important;
}
/* Scrollbars */
*::-webkit-scrollbar { width: 10px; height: 10px; }
*::-webkit-scrollbar-thumb { background: var(--ink-300); border-radius: 999px; }
*::-webkit-scrollbar-thumb:hover { background: var(--ink-400); }
*::-webkit-scrollbar-track { background: transparent; }
</style>
""", unsafe_allow_html=True)


# ─── Sidebar ─────────────────────────────────────────────────────────────────
st.sidebar.markdown(
    '<div style="font-size:11px; font-weight:700; letter-spacing:0.12em; '
    'text-transform:uppercase; color:#A299B5; padding:8px 6px 10px 6px;">'
    'Навигация</div>',
    unsafe_allow_html=True,
)

_nav_page = st.sidebar.radio(
    "Навигация",
    ["План", "Условия акций", "Генерация PUSH",
     "Прогноз", "Идеи акций",
     "Типы акций", "Правила генерации", "Deeplinks"],
    label_visibility="collapsed",
)

st.sidebar.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)

if st.sidebar.button("Обновить данные из Google", key="global_refresh", use_container_width=True):
    load_cvm_data.clear()
    load_push_data.clear()
    st.rerun()

with st.sidebar.expander("Настройки", expanded=False):
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

def dx_page_header(eyebrow: str, title: str, sub: str = ""):
    """Brand-aligned page header used at the top of every page."""
    _sub = f'<div class="dx-section-sub">{sub}</div>' if sub else ""
    st.markdown(
        f"""
        <div class="dx-section">
          <div class="dx-section-eyebrow">{eyebrow}</div>
          <div class="dx-section-title">{title}</div>
          {_sub}
        </div>
        """,
        unsafe_allow_html=True,
    )


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

if _nav_page == "План":
    dx_page_header(
        "Календарь кампаний",
        "План акций месяца",
        "Диаграмма Ганта по сегментам с привязкой push-сообщений к датам отправки.",
    )
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
            # Дефолт — последний (самый поздний) месяц списка
            _plan_default = len(month_options) - 1 if month_options else 0
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

        # Build push message map: (promo_num, date_obj) -> [msg_numbers]
        _push_msg_map = {}
        if not df_push.empty and "Номер промо" in df_push.columns:
            for _, pr in df_push.iterrows():
                pnum = _norm_num(pr.get("Номер промо"))
                pdate_str = str(pr.get("Дата", "")).strip()
                msg = str(pr.get("Номер msg", "")).strip()
                if not pnum or not pdate_str or pdate_str.lower() in ("nan", "none", ""):
                    continue
                # Parse date to date object (supports DD.MM.YYYY, DD.MM., YYYY-MM-DD)
                _pyear = str(pr.get("Год", "")).strip()
                _push_yr = int(_pyear) if _pyear.isdigit() and int(_pyear) > 2000 else sel_year
                try:
                    if "-" in pdate_str and len(pdate_str) >= 10:
                        # ISO format: 2026-04-10
                        _iso = pdate_str.split("-")
                        _pdate_obj = _date_cls(int(_iso[0]), int(_iso[1]), int(_iso[2][:2]))
                    else:
                        _dp = pdate_str.rstrip(".").split(".")
                        if len(_dp) >= 3 and _dp[2].isdigit() and int(_dp[2]) > 2000:
                            _pdate_obj = _date_cls(int(_dp[2]), int(_dp[1]), int(_dp[0]))
                        elif len(_dp) >= 2:
                            _pdate_obj = _date_cls(_push_yr, int(_dp[1]), int(_dp[0]))
                        else:
                            continue
                except (ValueError, IndexError):
                    continue
                key = (pnum, _pdate_obj)
                if key not in _push_msg_map:
                    _push_msg_map[key] = []
                if msg and msg.lower() not in ("nan", "none", ""):
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
                has_push = any(k[0] == num and msgs for k, msgs in _push_msg_map.items())
                if has_push:
                    status = "done"
                elif end_d < today:
                    status = "empty"
                else:
                    status = "conditions"
            else:
                status = "done" if end_d < today else "conditions"

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

            # Brand-aligned palette for Gantt
            BG_HEADER = "#F8FAFC"
            BG_WKND   = "#FFFBEB"
            BORDER    = "#E2E8F0"
            BORDER_WK = "#FCD34D"
            INK_500   = "#64748B"
            INK_900   = "#0B1220"

            status_palette = {
                "done":       {"bg": "#ECFDF5", "fg": "#047857"},
                "conditions": {"bg": "#FFFBEB", "fg": "#92400E"},
                "empty":      {"bg": "#FEF2F2", "fg": "#991B1B"},
            }

            html  = '<div style="overflow-x:auto; border:1px solid #E2E8F0; border-radius:12px; background:#fff; box-shadow:0 1px 2px rgba(15,23,42,.04);">'
            html += '<table style="border-collapse:separate; border-spacing:0; font-size:11px; width:100%; font-family:Inter,-apple-system,sans-serif;">'

            # Header row: dates
            html += '<tr>'
            html += (
                f'<th style="padding:10px 12px; border-bottom:1px solid {BORDER}; '
                f'position:sticky; left:0; background:{BG_HEADER}; z-index:2; '
                f'min-width:200px; text-align:left; font-weight:700; font-size:11.5px; '
                f'color:{INK_900}; letter-spacing:-0.005em;">Акция</th>'
            )
            if show_channel:
                html += (
                    f'<th style="padding:10px 8px; border-bottom:1px solid {BORDER}; '
                    f'background:{BG_HEADER}; min-width:48px; font-weight:700; '
                    f'font-size:10.5px; color:{INK_500}; text-transform:uppercase; '
                    f'letter-spacing:0.06em;">Канал</th>'
                )
            for dt in all_dates:
                is_weekend = dt.weekday() >= 5
                bg = BG_WKND if is_weekend else BG_HEADER
                fg = "#92400E" if is_weekend else INK_900
                html += (
                    f'<th style="padding:6px 0; border-bottom:1px solid {BORDER}; '
                    f'background:{bg}; text-align:center; min-width:22px; '
                    f'font-weight:700; font-size:11px; color:{fg};">{dt.day}</th>'
                )
            html += '</tr>'

            # Day of week header
            _DOW_SHORT = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
            html += '<tr>'
            html += (
                f'<td style="border-bottom:1px solid {BORDER}; background:{BG_HEADER}; '
                f'position:sticky; left:0; z-index:2; padding:0 12px 8px 12px;"></td>'
            )
            if show_channel:
                html += f'<td style="border-bottom:1px solid {BORDER}; background:{BG_HEADER};"></td>'
            for dt in all_dates:
                is_weekend = dt.weekday() >= 5
                bg = BG_WKND if is_weekend else BG_HEADER
                fg = "#B45309" if is_weekend else "#94A3B8"
                html += (
                    f'<td style="padding:0 0 6px 0; border-bottom:1px solid {BORDER}; '
                    f'background:{bg}; text-align:center; font-size:9.5px; '
                    f'color:{fg}; font-weight:600; text-transform:uppercase; '
                    f'letter-spacing:0.05em;">{_DOW_SHORT[dt.weekday()]}</td>'
                )
            html += '</tr>'

            # Promo rows grouped by segment
            for seg_name, seg_rows in segments_dict.items():
                col_span = 1 + (1 if show_channel else 0) + len(all_dates)
                html += (
                    f'<tr><td colspan="{col_span}" style="padding:10px 14px; '
                    f'border-top:1px solid {BORDER}; border-bottom:1px solid {BORDER}; '
                    f'background:#FAFBFC; font-weight:700; font-size:11.5px; '
                    f'color:{INK_900}; text-transform:uppercase; '
                    f'letter-spacing:0.06em;">{seg_name}</td></tr>'
                )

                for g in seg_rows:
                    pal = status_palette.get(g["status"], {"bg": "#FFFFFF", "fg": INK_900})
                    row_bg = pal["bg"]
                    row_fg = pal["fg"]
                    name_cell = (
                        f'<td style="padding:8px 12px; border-bottom:1px solid {BORDER}; '
                        f'background:{row_bg}; white-space:nowrap; overflow:hidden; '
                        f'text-overflow:ellipsis; max-width:220px; font-weight:600; '
                        f'font-size:12px; color:{INK_900};" title="{g["name"]}">'
                        f'<span style="display:inline-block; width:6px; height:6px; '
                        f'border-radius:50%; background:{row_fg}; margin-right:8px; '
                        f'vertical-align:middle;"></span>{g["name"]}</td>'
                    )
                    html += '<tr>'
                    html += name_cell
                    if show_channel:
                        html += (
                            f'<td style="padding:8px; border-bottom:1px solid {BORDER}; '
                            f'background:{row_bg}; font-size:10.5px; color:{INK_500}; '
                            f'font-weight:600; text-transform:uppercase; '
                            f'letter-spacing:0.06em;">{g["channel"]}</td>'
                        )

                    for dt in all_dates:
                        is_weekend = dt.weekday() >= 5
                        cell_border = f'border-bottom:1px solid {BORDER};'
                        if is_weekend:
                            cell_border += f' background-image:linear-gradient(rgba(252,211,77,.10), rgba(252,211,77,.10));'

                        in_range = g["start"] <= dt <= g["end"]
                        push_key = (g["num"], dt)
                        msgs_list = _push_msg_map.get(push_key, []) if g.get("channel", "").upper() == "PUSH" else []

                        if msgs_list:
                            msg_text = ",".join(msgs_list)
                            html += (
                                f'<td style="padding:2px; {cell_border} background:#FDE6CF; '
                                f'text-align:center; font-size:10px; font-weight:700; '
                                f'color:#C8620B;" title="Push {msg_text}">{msg_text}</td>'
                            )
                        elif in_range:
                            html += (
                                f'<td style="padding:2px; {cell_border} background:{row_bg};">'
                                f'<div style="height:6px; background:{row_fg}; opacity:.55; '
                                f'border-radius:2px; margin:6px 1px;"></div></td>'
                            )
                        else:
                            html += f'<td style="padding:2px; {cell_border}"></td>'
                    html += '</tr>'

            html += '</table></div>'

            st.markdown(html, unsafe_allow_html=True)

            # Legend — pill style
            st.markdown("""
            <div style="margin-top:14px; display:flex; flex-wrap:wrap; gap:8px; align-items:center;">
                <span style="display:inline-flex; align-items:center; gap:6px; background:#ECFDF5; color:#047857; border:1px solid rgba(4,120,87,.20); padding:5px 10px; border-radius:999px; font-size:11.5px; font-weight:600;">
                    <span style="width:7px; height:7px; border-radius:50%; background:#047857;"></span> Готово
                </span>
                <span style="display:inline-flex; align-items:center; gap:6px; background:#FFFBEB; color:#92400E; border:1px solid rgba(146,64,14,.20); padding:5px 10px; border-radius:999px; font-size:11.5px; font-weight:600;">
                    <span style="width:7px; height:7px; border-radius:50%; background:#92400E;"></span> Условия заполнены
                </span>
                <span style="display:inline-flex; align-items:center; gap:6px; background:#FEF2F2; color:#991B1B; border:1px solid rgba(153,27,27,.20); padding:5px 10px; border-radius:999px; font-size:11.5px; font-weight:600;">
                    <span style="width:7px; height:7px; border-radius:50%; background:#991B1B;"></span> Не запущена
                </span>
                <span style="display:inline-flex; align-items:center; gap:6px; background:#FDE6CF; color:#C8620B; border:1px solid rgba(200,98,11,.20); padding:5px 10px; border-radius:999px; font-size:11.5px; font-weight:600;">
                    <span style="width:7px; height:7px; border-radius:50%; background:#EF7C1A;"></span> PUSH с номерами msg
                </span>
            </div>
            """, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 🔧 Условия акций
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "Условия акций":
    dx_page_header(
        "Подготовка кампаний",
        "Условия акций",
        "Генерация описаний, скидок, бонусов и купонов для незаполненных акций. Все правки согласовываются и сохраняются в Google Sheets.",
    )

    sid = st.session_state.get("spreadsheet_id",
                                "1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY")

    if st.button("Обновить данные", key="refresh_conditions"):
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

        # Фильтр по месяцу
        _MONTH_NAMES_COND = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
        }
        if "Год" in df_unfilled.columns and "Месяц" in df_unfilled.columns and not df_unfilled.empty:
            _ym_pairs = []
            for _, _r in df_unfilled[["Год", "Месяц"]].drop_duplicates().iterrows():
                _y, _m = str(_r["Год"]).strip(), str(_r["Месяц"]).strip()
                if _y.isdigit() and _m.isdigit():
                    _ym_pairs.append((int(_y), int(_m)))
            _ym_pairs = sorted(set(_ym_pairs))
            if _ym_pairs:
                _ym_labels = ["Все"] + [f"{_MONTH_NAMES_COND.get(m, m)} {y}" for y, m in _ym_pairs]
                # Дефолт — последний (самый поздний) месяц списка
                _sel_ym = st.selectbox(
                    "Месяц", _ym_labels,
                    index=len(_ym_labels) - 1,
                    key="cond_month_filter",
                )
                if _sel_ym != "Все":
                    _idx = _ym_labels.index(_sel_ym) - 1
                    _y, _m = _ym_pairs[_idx]
                    df_unfilled = df_unfilled[
                        (df_unfilled["Год"].astype(str).str.strip() == str(_y)) &
                        (df_unfilled["Месяц"].astype(str).str.strip() == str(_m))
                    ].copy()

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

            # Генерация по одной акции
            _promo_options_cond = {}
            for _, _row in df_unfilled.iterrows():
                _key = f"{_norm_num(_row.get('НОМЕР'))} — {_row.get('Название промо', '')}"
                _promo_options_cond[_key] = _row.to_dict()

            _col_one, _col_all = st.columns([2, 1])
            with _col_one:
                _selected_cond_key = st.selectbox(
                    "Выберите акцию для генерации условий",
                    list(_promo_options_cond.keys()),
                    key="cond_single_select",
                )
            with _col_all:
                st.markdown("&nbsp;", unsafe_allow_html=True)
                _gen_one = st.button("Сгенерировать одну", key="gen_cond_one", use_container_width=True)

            if _gen_one and _selected_cond_key:
                try:
                    from ai_generator import generate_promo_conditions, get_similar_examples
                    _selected_promo = _promo_options_cond[_selected_cond_key]
                    _all_promos = df_cvm.to_dict("records")
                    _examples = get_similar_examples(_selected_promo, _all_promos, n=5)
                    with st.spinner(f"Генерирую условия для {_norm_num(_selected_promo.get('НОМЕР'))}..."):
                        _res = generate_promo_conditions(_selected_promo, _examples)
                        _res["__row_idx"] = list(df_cvm[df_cvm["НОМЕР"].astype(str).str.strip() == str(_selected_promo.get("НОМЕР", "")).strip()].index)
                        _res["__row_idx"] = _res["__row_idx"][0] if _res["__row_idx"] else None
                        _res["__promo_num"] = _norm_num(_selected_promo.get("НОМЕР"))
                        _res["__promo_name"] = str(_selected_promo.get("Название промо", ""))
                    # Заменяем в conditions_results если уже есть, иначе добавляем
                    _existing = st.session_state.get("conditions_results", [])
                    _existing = [r for r in _existing if r.get("__promo_num") != _res["__promo_num"]]
                    _existing.append(_res)
                    st.session_state.conditions_results = _existing
                    st.success(f"Сгенерированы условия для {_res['__promo_num']}")
                except Exception as e:
                    st.error(f"Ошибка: {e}")

            st.markdown("---")

            # Generate conditions button
            if st.button("Сгенерировать условия для всех акций", type="primary", key="gen_conditions"):
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
                        _check_col, _name_col = st.columns([0.05, 0.95])
                        with _check_col:
                            _approved = st.checkbox(
                                "Согласовано",
                                value=False,
                                key=f"cond_approved_{i}",
                                label_visibility="collapsed",
                            )
                        with _name_col:
                            st.markdown(f"**{res.get('__promo_num', '')} — {res.get('__promo_name', '')}**")

                        desc = st.text_area(
                            "Описание акции",
                            value=res.get("Описание акции", ""),
                            key=f"cond_desc_{i}",
                            height=80,
                        )
                        _c1, _c2, _c3 = st.columns(3)
                        with _c1:
                            discount = st.text_input(
                                "Скидка",
                                value=res.get("Скидка", ""),
                                key=f"cond_discount_{i}",
                            )
                        with _c2:
                            bonus = st.text_input(
                                "Бонусы",
                                value=res.get("Бонусы", ""),
                                key=f"cond_bonus_{i}",
                            )
                        with _c3:
                            mechanic = st.text_input(
                                "Механика",
                                value=res.get("Механика", ""),
                                key=f"cond_mech_{i}",
                            )
                        _c4, _c5 = st.columns(2)
                        with _c4:
                            category = st.text_input(
                                "Категория",
                                value=res.get("Категория", ""),
                                key=f"cond_category_{i}",
                            )
                        with _c5:
                            expiry = st.text_input(
                                "Срок сгорания бонусов",
                                value=res.get("Срок сгорания бонусов", ""),
                                key=f"cond_expiry_{i}",
                            )
                        coupon_name = st.text_input(
                            "Название информационного купона для МП",
                            value=res.get("Название информационного купона для МП", ""),
                            key=f"cond_coupon_name_{i}",
                        )
                        coupon = st.text_area(
                            "Текст купона",
                            value=res.get("Текст на информационном купоне / слип-чеке", ""),
                            key=f"cond_coupon_{i}",
                            height=120,
                        )
                        button_text = st.text_input(
                            "Кнопка",
                            value=res.get("Кнопка", ""),
                            key=f"cond_button_{i}",
                        )
                        edited_results.append({
                            "__row_idx": res.get("__row_idx"),
                            "__promo_num": res.get("__promo_num"),
                            "__approved": _approved,
                            "Описание акции": desc,
                            "Скидка": discount,
                            "Бонусы": bonus,
                            "Механика": mechanic,
                            "Категория": category,
                            "Срок сгорания бонусов": expiry,
                            "Название информационного купона для МП": coupon_name,
                            "Текст на информационном купоне / слип-чеке": coupon,
                            "Кнопка": button_text,
                        })

                _approved_count = sum(1 for er in edited_results if er.get("__approved"))
                if _approved_count:
                    st.info(f"Согласовано к сохранению: {_approved_count} из {len(edited_results)}")
                else:
                    st.warning("Отметьте галочкой акции, которые хотите сохранить в Google Sheets")

                # Save to Google Sheets button
                if st.button("Сохранить согласованные условия", type="primary", key="save_conditions"):
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
                                if not er.get("__approved"):
                                    continue
                                row_idx = er.get("__row_idx")
                                if row_idx is None:
                                    continue
                                sheet_row = int(row_idx) + 2  # +1 header, +1 for 0-index
                                for field in ["Описание акции",
                                              "Скидка",
                                              "Бонусы",
                                              "Механика",
                                              "Категория",
                                              "Срок сгорания бонусов",
                                              "Название информационного купона для МП",
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

elif _nav_page == "Генерация PUSH":
    dx_page_header(
        "AI генерация",
        "Push-сообщения",
        "Генерация заголовков и текстов push-уведомлений с учётом сегмента, периода и шаблонов. Поддерживает массовый и точечный режим.",
    )

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
        ai_key = ""  # ключи берутся из .env автоматически

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

        # Count existing push messages per promo (not exclude!)
        _existing_push_count = {}  # promo_num -> count of existing messages
        if not df_push.empty and "Номер промо" in df_push.columns:
            for val in df_push["Номер промо"].dropna():
                _pn = _norm_num(val)
                _existing_push_count[_pn] = _existing_push_count.get(_pn, 0) + 1

        if "НОМЕР" in df_push_promos.columns:
            df_push_promos["_num_str"] = df_push_promos["НОМЕР"].apply(_norm_num)
            df_push_promos["_existing_msgs"] = df_push_promos["_num_str"].map(lambda x: _existing_push_count.get(x, 0))

        # Month selectbox
        month_options_gen = ["Апрель 2026", "Май 2026", "Июнь 2026",
                             "Июль 2026", "Август 2026"]
        _MONTH_MAP_GEN = {
            "Январь": 1, "Февраль": 2, "Март": 3, "Апрель": 4,
            "Май": 5, "Июнь": 6, "Июль": 7, "Август": 8,
            "Сентябрь": 9, "Октябрь": 10, "Ноябрь": 11, "Декабрь": 12,
        }

        def _on_month_change():
            for k in ["mass_results", "single_result", "single_selected",
                      "tmpl_results", "tmpl_checked", "tmpl_targets"]:
                if k in st.session_state:
                    del st.session_state[k]

        # Дефолт месяца — последний месяц СПИСКА, в котором есть акции (по полю «Месяц»)
        _gen_months_with_data = set()
        if "Месяц" in df_push_promos.columns:
            for _m in df_push_promos["Месяц"].dropna():
                _ms = str(_m).strip()
                if _ms.isdigit():
                    _gen_months_with_data.add(int(_ms))
        _gen_default_idx = 0
        for _i in range(len(month_options_gen) - 1, -1, -1):
            _mn = _MONTH_MAP_GEN.get(month_options_gen[_i].split()[0])
            if _mn and _mn in _gen_months_with_data:
                _gen_default_idx = _i
                break

        selected_gen_month = st.selectbox(
            "Месяц",
            month_options_gen,
            index=_gen_default_idx,
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
                if start_d <= gen_month_end and end_d >= gen_month_start and str(row.get("Месяц", "")).strip() == str(gen_month_num):
                    month_filtered.append(row)

        df_gen = pd.DataFrame(month_filtered) if month_filtered else pd.DataFrame()

        if df_gen.empty:
            st.info("Нет акций без push-сообщений для выбранного месяца")
        else:
            _no_push = len(df_gen[df_gen.get("_existing_msgs", 0) == 0]) if "_existing_msgs" in df_gen.columns else len(df_gen)
            _has_push = len(df_gen) - _no_push
            _caption = f"Акций: {len(df_gen)}"
            if _no_push:
                _caption += f" (без push: {_no_push})"
            if _has_push:
                _caption += f" (с push: {_has_push})"
            st.caption(_caption)

            # Вкладки массовая / по одной / по шаблону
            gen_tab = st.radio(
                "",
                ["По одной акции", "Массовая генерация", "По шаблону"],
                index=1,
                key="gen_tab_radio",
                label_visibility="collapsed",
            )

            # ── МАССОВАЯ ─────────────────────────────────────────────
            if gen_tab == "Массовая генерация":
                if st.button("Сгенерировать все", type="primary", key="gen_mass_btn"):
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
                        _promo_num = _norm_num(promo.get("НОМЕР"))
                        _existing_count = _existing_push_count.get(_promo_num, 0)
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
                            # Сдвигаем нумерацию push если уже есть сообщения
                            if _existing_count > 0 and "pushes" in result:
                                for _p in result["pushes"]:
                                    _p["push_number"] = _p.get("push_number", 1) + _existing_count
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
                        st.session_state.mass_checked = {i: False for i in range(len(results))}

                    for i, item in enumerate(results):
                        promo = item["promo"]
                        result = item["result"]
                        auto_deeplink = item["auto_deeplink"]
                        promo_num = _norm_num(promo.get("НОМЕР"))
                        promo_name = str(promo.get("Название промо", ""))
                        _ec = _existing_push_count.get(promo_num, 0)
                        _existing_badge = f" (уже {_ec} msg)" if _ec > 0 else ""

                        _check_col, _exp_col = st.columns([0.05, 0.95])
                        with _check_col:
                            checked = st.checkbox(
                                "",
                                value=st.session_state.mass_checked.get(i, False),
                                key=f"mass_check_{i}",
                                label_visibility="collapsed",
                            )
                            st.session_state.mass_checked[i] = checked
                        with _exp_col:
                          with st.expander(f"**{promo_num} — {promo_name}{_existing_badge}**", expanded=False):
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
                                    # Парсим дату из строки в date объект
                                    _date_str = push_data.get("date", "")
                                    _date_default = None
                                    if _date_str:
                                        from datetime import datetime as _dt_cls
                                        for _fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d", "%d.%m."):
                                            try:
                                                _date_default = _dt_cls.strptime(_date_str, _fmt).date()
                                                break
                                            except ValueError:
                                                continue
                                    if not _date_default:
                                        from datetime import date as _d_cls
                                        _date_default = _d_cls.today()
                                    push_date_obj = st.date_input(
                                        "Дата",
                                        value=_date_default,
                                        key=f"mass_date_{i}_{p_idx}",
                                        label_visibility="collapsed",
                                        format="DD.MM.YYYY",
                                    )
                                    push_date_val = push_date_obj.strftime("%d.%m.%Y")
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
                                    height=68,
                                    label_visibility="collapsed",
                                )
                                tlen = len(edited_title)
                                blen = len(edited_body)
                                t_ok = "ok" if tlen <= 35 else "над лимитом"
                                b_ok = "ok" if blen <= 120 else "над лимитом"
                                st.caption(f"Заголовок: {t_ok} {tlen}/35 | Текст: {b_ok} {blen}/120")

                    # Save button
                    if st.button("Сохранить выбранные", type="primary", key="save_mass"):
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
            elif gen_tab == "По одной акции":
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

                    # Очищаем данные поиска при смене акции
                    _prev_promo = st.session_state.get("_single_prev_promo", "")
                    if selected_promo_key != _prev_promo:
                        st.session_state["_single_prev_promo"] = selected_promo_key
                        st.session_state.pop("dixy_results", None)
                        st.session_state.pop("dixy_selected_products", None)
                        st.session_state.pop("dixy_chips_select", None)
                        st.session_state.pop("single_generated_result", None)
                        st.session_state.pop("single_result", None)
                        st.session_state.pop("single_approved", None)

                    # Показать количество уже существующих push
                    _sel_num = _norm_num(selected_promo.get("НОМЕР"))
                    _sel_existing = _existing_push_count.get(_sel_num, 0)
                    if _sel_existing > 0:
                        st.info(f"У этой акции уже есть {_sel_existing} push-сообщений. Новые будут нумероваться с #{_sel_existing + 1}")

                    # Promo details
                    with st.expander("Детали акции", expanded=False):
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
                    if st.button("Найти акции на dixy.ru", key="search_dixy"):
                        try:
                            from dixy_parser import search_discounts
                            promo_name = str(selected_promo.get("Название промо", ""))
                            with st.spinner("Ищу товары на dixy.ru..."):
                                discounts = search_discounts(promo_name)
                            if discounts:
                                st.session_state.dixy_results = discounts
                            else:
                                st.session_state.dixy_results = []
                                st.info("Товары не найдены на dixy.ru")
                        except Exception as e:
                            st.warning(f"Ошибка поиска: {e}")

                    # Показываем результаты поиска (сохранённые в session_state)
                    if st.session_state.get("dixy_results"):
                        _dixy_data = st.session_state.dixy_results
                        st.caption(f"Найдено товаров на dixy.ru: {len(_dixy_data)}")

                        dixy_df = pd.DataFrame(_dixy_data)
                        dixy_df.index = range(1, len(dixy_df) + 1)
                        dixy_df.index.name = "№"

                        # Числовая скидка для сортировки
                        import re as _re
                        dixy_df["discount_num"] = dixy_df["discount"].apply(
                            lambda x: int(m.group(1)) if (m := _re.search(r'-(\d+)%', str(x))) else 0
                        )

                        # Колонки для отображения
                        dixy_df["url"] = dixy_df.get("url", "")
                        display_cols = ["name", "price", "old_price", "discount_num", "discount", "by_card", "date_to", "url"]
                        display_cols = [c for c in display_cols if c in dixy_df.columns]

                        col_config = {
                            "name": st.column_config.TextColumn("Товар", width="large"),
                            "price": st.column_config.NumberColumn("Цена ₽", format="%.2f", width="small"),
                            "old_price": st.column_config.NumberColumn("Была ₽", format="%.2f", width="small"),
                            "discount_num": st.column_config.NumberColumn("Скидка %", width="small"),
                            "discount": st.column_config.TextColumn("Тип", width="small"),
                            "by_card": st.column_config.TextColumn("Карта", width="small"),
                            "date_to": st.column_config.TextColumn("Срок", width="small"),
                            "url": st.column_config.LinkColumn("Ссылка", width="small", display_text="открыть"),
                        }

                        # Чекбокс прямо в таблице — тап на строку добавляет в промпт
                        dixy_df["Выбор"] = False
                        edit_cols = ["Выбор"] + display_cols
                        col_config["Выбор"] = st.column_config.CheckboxColumn("Выбор", width="small", default=False)

                        _edited = st.data_editor(
                            dixy_df[edit_cols],
                            use_container_width=True,
                            column_config=col_config,
                            height=min(400, 35 * len(dixy_df) + 38),
                            key="dixy_table_editor",
                            disabled=[c for c in display_cols],
                        )

                        # Собираем выбранные
                        _sel_mask = _edited["Выбор"] == True
                        if _sel_mask.any():
                            _sel_items = []
                            for _, _r in _edited[_sel_mask].iterrows():
                                _lbl = str(_r.get("name", ""))
                                _d = str(_r.get("discount", ""))
                                _bc = str(_r.get("by_card", ""))
                                if _d:
                                    _lbl += f" {_d}"
                                if _bc:
                                    _lbl += f" {_bc}"
                                _sel_items.append(_lbl)
                            st.session_state["dixy_selected_products"] = _sel_items
                            st.caption(f"Выбрано для промпта: {len(_sel_items)}")
                        else:
                            st.session_state["dixy_selected_products"] = []

                    # Extra rules for AI providers (сохраняются между переключениями акций)
                    extra_rules = ""
                    if ai_provider != "builtin":
                        if "single_extra_rules" not in st.session_state:
                            st.session_state["single_extra_rules"] = ""
                        extra_rules = st.text_area(
                            "Дополнительные правила для AI",
                            key="single_extra_rules",
                            height=80,
                        )

                    # Generate button
                    if st.button("Сгенерировать push", type="primary", key="gen_single_btn"):
                        from ai_generator import (
                            generate_push_texts, calculate_push_schedule,
                            _needs_activation, find_best_deeplink,
                        )

                        schedule = calculate_push_schedule(selected_promo)
                        rules = st.session_state.get("generation_rules", "")
                        if extra_rules:
                            rules = rules + "\n\n" + extra_rules
                        # Добавляем выбранные товары с dixy.ru в контекст
                        _sel_prods = st.session_state.get("dixy_selected_products", [])
                        if _sel_prods:
                            rules += "\n\nАктуальные товары со скидками на dixy.ru:\n"
                            rules += "\n".join(f"- {p}" for p in _sel_prods)

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
                                # Сдвигаем нумерацию если уже есть push
                                if _sel_existing > 0 and "pushes" in result:
                                    for _p in result["pushes"]:
                                        _p["push_number"] = _p.get("push_number", 1) + _sel_existing
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
                                _sd_str = push_data.get("date", "")
                                _sd_default = None
                                if _sd_str:
                                    from datetime import datetime as _dt_cls
                                    for _fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d", "%d.%m."):
                                        try:
                                            _sd_default = _dt_cls.strptime(_sd_str, _fmt).date()
                                            break
                                        except ValueError:
                                            continue
                                if not _sd_default:
                                    from datetime import date as _d_cls
                                    _sd_default = _d_cls.today()
                                s_date_obj = st.date_input(
                                    "Дата",
                                    value=_sd_default,
                                    key=f"single_date_{p_idx}",
                                    format="DD.MM.YYYY",
                                )
                                s_date = s_date_obj.strftime("%d.%m.%Y")
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
                                        height=68,
                                    )
                                    tlen = len(edited_title)
                                    blen = len(edited_body)
                                    t_ok = "ok" if tlen <= 35 else "над лимитом"
                                    b_ok = "ok" if blen <= 120 else "над лимитом"
                                    st.caption(f"Заголовок: {t_ok} {tlen}/35 | Текст: {b_ok} {blen}/120")

                        # Save approved variants
                        st.markdown("---")
                        if st.button("Сохранить выбранные варианты", type="primary", key="save_single"):
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

            # ── ПО ШАБЛОНУ ───────────────────────────────────────────
            elif gen_tab == "По шаблону":
                st.caption("Берём акцию-шаблон с уже согласованными push, и для целевых акций "
                           "генерируем тексты по аналогии — заменяя только условия (суммы, проценты, чек, даты, товары).")

                # Все акции с ≥1 push-сообщением (потенциальные шаблоны)
                df_with_msgs = df_push_promos[df_push_promos.get("_existing_msgs", 0) > 0].copy() \
                    if "_existing_msgs" in df_push_promos.columns else pd.DataFrame()

                if df_with_msgs.empty:
                    st.info("Нет акций с уже согласованными push-сообщениями. "
                            "Сначала согласуйте хотя бы одну акцию-шаблон.")
                else:
                    # ── Фильтр по месяцу для шаблона ──
                    tmpl_month_options = ["Все месяцы"] + month_options_gen
                    # Дефолт — последний месяц, в котором есть шаблоны (с push)
                    _tmpl_months_with_data = set()
                    if "Месяц" in df_with_msgs.columns:
                        for _m in df_with_msgs["Месяц"].dropna():
                            _ms = str(_m).strip()
                            if _ms.isdigit():
                                _tmpl_months_with_data.add(int(_ms))
                    _tmpl_default_idx = 0
                    for _i in range(len(tmpl_month_options) - 1, 0, -1):
                        _mn = _MONTH_MAP_GEN.get(tmpl_month_options[_i].split()[0])
                        if _mn and _mn in _tmpl_months_with_data:
                            _tmpl_default_idx = _i
                            break

                    tmpl_month = st.selectbox(
                        "Месяц шаблона",
                        tmpl_month_options,
                        index=_tmpl_default_idx,
                        key="tmpl_month",
                    )

                    # Применяем фильтр по месяцу к шаблонам (по пересечению дат акции)
                    if tmpl_month != "Все месяцы":
                        _tm_parts = tmpl_month.split()
                        _tm_num = _MONTH_MAP_GEN.get(_tm_parts[0], 4)
                        _tm_year = int(_tm_parts[1]) if len(_tm_parts) > 1 else 2026
                        _tm_start = _date_cls(_tm_year, _tm_num, 1)
                        if _tm_num == 12:
                            _tm_end = _date_cls(_tm_year + 1, 1, 1) - timedelta(days=1)
                        else:
                            _tm_end = _date_cls(_tm_year, _tm_num + 1, 1) - timedelta(days=1)

                        _tmpl_filtered = []
                        for _, _r in df_with_msgs.iterrows():
                            _yh = _r.get("Год", _tm_year)
                            try:
                                _yh = int(_yh)
                            except (ValueError, TypeError):
                                _yh = _tm_year
                            _sd = _parse_promo_date(_r.get("Старт акции"), _yh)
                            _ed = _parse_promo_date(_r.get("Окончание акции"), _yh)
                            if _sd and _ed:
                                if _ed < _sd:
                                    _sd, _ed = _ed, _sd
                                if _sd <= _tm_end and _ed >= _tm_start \
                                        and str(_r.get("Месяц", "")).strip() == str(_tm_num):
                                    _tmpl_filtered.append(_r)
                        df_with_msgs = pd.DataFrame(_tmpl_filtered) if _tmpl_filtered else pd.DataFrame()

                    if df_with_msgs.empty:
                        st.info("Нет шаблонов в выбранном месяце.")
                        st.stop()

                    # ── Шаблон ──
                    tmpl_options = {}
                    for _, row in df_with_msgs.iterrows():
                        n = _norm_num(row.get("НОМЕР"))
                        nm = row.get("Название промо", "?")
                        ec = int(row.get("_existing_msgs", 0))
                        tmpl_options[f"{n} — {nm} ({ec} msg)"] = row.to_dict()

                    tmpl_key = st.selectbox(
                        "Акция-шаблон (с согласованными push)",
                        list(tmpl_options.keys()),
                        key="tmpl_select",
                    )
                    tmpl_promo = tmpl_options[tmpl_key]
                    tmpl_num = _norm_num(tmpl_promo.get("НОМЕР"))

                    # Загружаем шаблонные сообщения из PUSH sheet
                    tmpl_messages = []
                    if not df_push.empty and "Номер промо" in df_push.columns:
                        _df_push_copy = df_push.copy()
                        _df_push_copy["_num_str"] = _df_push_copy["Номер промо"].apply(_norm_num)
                        _rows = _df_push_copy[_df_push_copy["_num_str"] == tmpl_num]
                        for _, r in _rows.iterrows():
                            tmpl_messages.append({
                                "push_number": str(r.get("Номер msg", "")),
                                "title": str(r.get("PUSH заголовок", "")),
                                "body": str(r.get("текст PUSH", "")),
                                "date": str(r.get("Дата", "")),
                                "time": str(r.get("Время", "")),
                            })

                    # Превью шаблонных сообщений
                    with st.expander(f"Шаблонные push ({len(tmpl_messages)})", expanded=True):
                        if not tmpl_messages:
                            st.warning("Не удалось загрузить шаблонные сообщения")
                        for m in tmpl_messages:
                            st.markdown(f"**Push #{m['push_number']}** ({m['date']} {m['time']})")
                            st.markdown(f"- **Заголовок:** {m['title']}")
                            st.markdown(f"- **Текст:** {m['body']}")

                    # ── Целевые акции ──
                    # Берём из df_gen (текущий месяц), исключая саму шаблонную.
                    # Сортировка: сначала акции БЕЗ push (приоритет), потом — с push (с пометкой).
                    _no_push_options = {}
                    _with_push_options = {}
                    for _, row in df_gen.iterrows():
                        n = _norm_num(row.get("НОМЕР"))
                        if n == tmpl_num:
                            continue
                        ec = int(row.get("_existing_msgs", 0)) if "_existing_msgs" in row else 0
                        nm = row.get("Название промо", "?")
                        if ec > 0:
                            _with_push_options[f"{n} — {nm} (уже {ec} msg)"] = row.to_dict()
                        else:
                            _no_push_options[f"{n} — {nm}"] = row.to_dict()

                    target_options = {**_no_push_options, **_with_push_options}

                    _cnt_caption_parts = []
                    if _no_push_options:
                        _cnt_caption_parts.append(f"без push: {len(_no_push_options)}")
                    if _with_push_options:
                        _cnt_caption_parts.append(f"с push (внизу списка): {len(_with_push_options)}")
                    if _cnt_caption_parts:
                        st.caption("Доступно — " + ", ".join(_cnt_caption_parts))

                    if not target_options:
                        st.info("Нет целевых акций для генерации в выбранном месяце")
                    else:
                        selected_targets = st.multiselect(
                            "Целевые акции (для них сгенерируем по аналогии)",
                            list(target_options.keys()),
                            key="tmpl_targets",
                        )

                        # Доп. правила
                        tmpl_extra_rules = st.text_area(
                            "Дополнительные правила (опционально)",
                            key="tmpl_extra_rules",
                            height=68,
                        )

                        if st.button("Сгенерировать по шаблону",
                                     type="primary", key="tmpl_gen_btn",
                                     disabled=(not selected_targets or not tmpl_messages)):
                            from ai_generator import (
                                generate_push_from_template, calculate_push_schedule,
                                _needs_activation, find_best_deeplink,
                            )

                            base_rules = st.session_state.get("generation_rules", "")
                            full_rules = base_rules
                            if tmpl_extra_rules:
                                full_rules = (full_rules + "\n\n" + tmpl_extra_rules).strip()

                            tmpl_provider = ai_provider

                            results = []
                            progress = st.progress(0)
                            total = len(selected_targets)
                            for idx, tk in enumerate(selected_targets):
                                tgt_promo = target_options[tk]
                                tgt_num = _norm_num(tgt_promo.get("НОМЕР"))
                                tgt_existing = _existing_push_count.get(tgt_num, 0)
                                schedule = calculate_push_schedule(tgt_promo)
                                try:
                                    result = generate_push_from_template(
                                        target_promo=tgt_promo,
                                        template_promo=tmpl_promo,
                                        template_messages=tmpl_messages,
                                        schedule=schedule,
                                        title_max_len=35,
                                        body_max_len=120,
                                        rules=full_rules,
                                        provider=tmpl_provider,
                                        anthropic_key=ai_key if tmpl_provider == "anthropic" else None,
                                        openai_key=ai_key if tmpl_provider == "openai" else None,
                                    )
                                    if tgt_existing > 0 and "pushes" in result:
                                        for _p in result["pushes"]:
                                            _p["push_number"] = _p.get("push_number", 1) + tgt_existing

                                    if _needs_activation(tgt_promo):
                                        auto_dl = "купон"
                                    else:
                                        cat = str(tgt_promo.get("Название промо", ""))
                                        dl = find_best_deeplink(cat)
                                        auto_dl = dl["deeplink"] if dl else ""

                                    results.append({
                                        "promo": tgt_promo,
                                        "result": result,
                                        "schedule": schedule,
                                        "auto_deeplink": auto_dl,
                                    })
                                except Exception as e:
                                    st.warning(f"Ошибка для {tgt_num}: {e}")
                                progress.progress((idx + 1) / total)

                            # Очищаем старые значения виджетов (date_input/text_input
                            # с key запоминают значение в session_state и игнорируют value=
                            # при повторном рендере, из-за чего «прилипают» старые даты).
                            for _k in list(st.session_state.keys()):
                                if _k.startswith(("tmpl_date_", "tmpl_time_",
                                                  "tmpl_title_", "tmpl_body_",
                                                  "tmpl_dl_", "tmpl_check_")):
                                    del st.session_state[_k]

                            st.session_state.tmpl_results = results
                            st.success(f"Сгенерировано: {len(results)} акций")

                        # Display template results
                        if "tmpl_results" in st.session_state and st.session_state.tmpl_results:
                            results = st.session_state.tmpl_results

                            if "tmpl_checked" not in st.session_state \
                                    or len(st.session_state.tmpl_checked) != len(results):
                                st.session_state.tmpl_checked = {i: True for i in range(len(results))}

                            for i, item in enumerate(results):
                                promo = item["promo"]
                                result = item["result"]
                                auto_dl = item["auto_deeplink"]
                                tgt_schedule = item.get("schedule", [])
                                promo_num = _norm_num(promo.get("НОМЕР"))
                                promo_name = str(promo.get("Название промо", ""))
                                _ec = _existing_push_count.get(promo_num, 0)
                                _existing_badge = f" (уже {_ec} msg)" if _ec > 0 else ""

                                _check_col, _exp_col = st.columns([0.05, 0.95])
                                with _check_col:
                                    checked = st.checkbox(
                                        "",
                                        value=st.session_state.tmpl_checked.get(i, True),
                                        key=f"tmpl_check_{i}",
                                        label_visibility="collapsed",
                                    )
                                    st.session_state.tmpl_checked[i] = checked
                                with _exp_col:
                                  with st.expander(f"**{promo_num} — {promo_name}{_existing_badge}**", expanded=True):
                                    pushes = result.get("pushes", [])
                                    for p_idx, push_data in enumerate(pushes):
                                        push_num = push_data.get("push_number", p_idx + 1)
                                        variants = push_data.get("variants", [])
                                        if not variants:
                                            continue
                                        var = variants[0]
                                        title = var.get("title", "")
                                        body = var.get("body", "")

                                        # Дата/время — ИЗ РАСПИСАНИЯ ЦЕЛЕВОЙ АКЦИИ,
                                        # а не то, что вернул AI (он мог скопировать дату шаблона).
                                        sched_idx = min(p_idx, len(tgt_schedule) - 1) if tgt_schedule else -1
                                        if sched_idx >= 0:
                                            _date_default = tgt_schedule[sched_idx].get("date_obj")
                                            _time_default = tgt_schedule[sched_idx].get("time", "10:00")
                                        else:
                                            _date_default = None
                                            _time_default = push_data.get("time", "10:00")
                                        if not _date_default:
                                            from datetime import date as _d_cls
                                            _date_default = _d_cls.today()

                                        st.caption(f"Push #{push_num}")
                                        pcol1, pcol2, pcol3 = st.columns(3)
                                        with pcol1:
                                            push_date_obj = st.date_input(
                                                "Дата",
                                                value=_date_default,
                                                key=f"tmpl_date_{i}_{p_idx}",
                                                label_visibility="collapsed",
                                                format="DD.MM.YYYY",
                                            )
                                        with pcol2:
                                            st.text_input(
                                                "Время",
                                                value=_time_default,
                                                key=f"tmpl_time_{i}_{p_idx}",
                                                label_visibility="collapsed",
                                            )
                                        with pcol3:
                                            st.text_input(
                                                "Deeplink",
                                                value=auto_dl,
                                                key=f"tmpl_dl_{i}_{p_idx}",
                                                label_visibility="collapsed",
                                            )

                                        edited_title = st.text_input(
                                            "Заголовок",
                                            value=title,
                                            key=f"tmpl_title_{i}_{p_idx}",
                                            label_visibility="collapsed",
                                        )
                                        edited_body = st.text_area(
                                            "Текст",
                                            value=body,
                                            key=f"tmpl_body_{i}_{p_idx}",
                                            height=68,
                                            label_visibility="collapsed",
                                        )
                                        tlen = len(edited_title)
                                        blen = len(edited_body)
                                        t_ok = "ok" if tlen <= 35 else "над лимитом"
                                        b_ok = "ok" if blen <= 120 else "над лимитом"
                                        st.caption(f"Заголовок: {t_ok} {tlen}/35 | Текст: {b_ok} {blen}/120")

                            if st.button("Сохранить выбранные", type="primary", key="save_tmpl"):
                                rows_to_save = []
                                for i, item in enumerate(results):
                                    if not st.session_state.tmpl_checked.get(i, False):
                                        continue
                                    promo = item["promo"]
                                    result = item["result"]
                                    pushes = result.get("pushes", [])
                                    for p_idx, push_data in enumerate(pushes):
                                        push_num = push_data.get("push_number", p_idx + 1)
                                        title_val = st.session_state.get(f"tmpl_title_{i}_{p_idx}", "")
                                        body_val = st.session_state.get(f"tmpl_body_{i}_{p_idx}", "")
                                        date_obj = st.session_state.get(f"tmpl_date_{i}_{p_idx}")
                                        if hasattr(date_obj, "strftime"):
                                            date_val = date_obj.strftime("%d.%m.%Y")
                                        else:
                                            date_val = str(date_obj) if date_obj else ""
                                        time_val = st.session_state.get(f"tmpl_time_{i}_{p_idx}", "10:00")
                                        dl_val = st.session_state.get(f"tmpl_dl_{i}_{p_idx}", "")

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
                                        import traceback as _tb
                                        st.error(f"Ошибка сохранения: {e}")
                                        with st.expander("Подробности ошибки", expanded=False):
                                            st.code(_tb.format_exc())
                                else:
                                    st.warning("Нет выбранных акций для сохранения")


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 📋 Типы акций
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "Типы акций":
    dx_page_header(
        "Каталог механик",
        "Типы акций",
        "Классификация акций по 6 осям: активация, тип выгоды, охват, чек, период, контекст. Шаблоны заголовков и тел push-сообщений.",
    )
    from ai_generator import classify_promo, _PUSH_PAIRS, _CONTEXT_BODY_PHRASES

    _types_tab1, _types_tab2, _types_tab3 = st.tabs(["Классификация акций", "Шаблоны", "Оси"])

    # ── TAB 1: Классификация акций ──
    with _types_tab1:
        _sid_types = st.session_state.get("sheet_session_id", None) or spreadsheet_id
        try:
            df_cvm_types = load_cvm_data(_sid_types)
            if not df_cvm_types.empty:
                _type_rows = []
                for _, row in df_cvm_types.iterrows():
                    promo_dict = row.to_dict()
                    num = str(promo_dict.get("НОМЕР", "")).strip()
                    name = str(promo_dict.get("Название промо", "")).strip()
                    if not num or not name or num == "nan":
                        continue
                    cl = classify_promo(promo_dict)
                    _type_rows.append({
                        "№": num,
                        "Акция": name[:45],
                        "Акт": "Да" if cl["activation"] == "yes" else "",
                        "Выгода": cl["benefit"],
                        "Охват": cl["scope"],
                        "Чек": cl["check_amount"] if cl["check"] == "check_from" else "",
                        "Период": cl["period"],
                        "Значение": cl["value"],
                        "Контекст": ", ".join(cl["context"][:2]),
                        "Онлайн": "Да" if cl["is_online"] else "",
                    })
                if _type_rows:
                    df_types = pd.DataFrame(_type_rows)
                    # Фильтр по типу выгоды
                    _benefit_filter = st.multiselect(
                        "Фильтр по типу выгоды",
                        df_types["Выгода"].unique().tolist(),
                        default=df_types["Выгода"].unique().tolist(),
                        key="types_benefit_filter",
                    )
                    df_types_filtered = df_types[df_types["Выгода"].isin(_benefit_filter)]
                    st.dataframe(df_types_filtered, use_container_width=True, hide_index=True, height=600)
                    st.caption(f"Показано: {len(df_types_filtered)} из {len(_type_rows)}")
        except Exception as e:
            st.warning(f"Ошибка загрузки: {e}")

    # ── TAB 2: Шаблоны ──
    with _types_tab2:
        st.caption("Готовые пары (заголовок + body) — подбираются по комбинации осей")

        _benefit_names = {
            "cashback_pct": "Кешбэк %", "cashback_rub": "Кешбэк ₽",
            "discount_pct": "Скидка %", "discount_rub": "Скидка ₽",
            "gift": "Предначисление", "communication": "Коммуникация",
            "present": "Подарок",
        }
        for benefit_code, benefit_label in _benefit_names.items():
            with st.expander(benefit_label, expanded=False):
                _found_any = False
                for key, pairs in _PUSH_PAIRS.items():
                    if key[1] == benefit_code or key[1] == "*":
                        _act_label = "активация" if key[0] == "yes" else ""
                        _scope_label = f"{key[2]}" if key[2] != "*" else ""
                        _check_label = f"{key[3]}" if key[3] != "*" else ""
                        _per_label = f"{key[4]}" if key[4] != "*" else ""
                        _tags = " ".join(filter(None, [_act_label, _scope_label, _check_label, _per_label]))
                        if _tags:
                            st.markdown(f"**{_tags}**")
                        for i, (title, body) in enumerate(pairs, 1):
                            st.markdown(f"{i}. **`{title}`**")
                            st.markdown(f"   `{body}`")
                        st.markdown("---")
                        _found_any = True
                if not _found_any:
                    st.caption("Нет шаблонов")

        with st.expander("Напоминания (reminder)", expanded=False):
            for key, pairs in _PUSH_PAIRS.items():
                if key[4] == "reminder":
                    for i, (title, body) in enumerate(pairs, 1):
                        st.markdown(f"{i}. **`{title}`**")
                        st.markdown(f"   `{body}`")

        with st.expander("Контекстные фразы (праздники/сезон)", expanded=False):
            for tag, phrases in _CONTEXT_BODY_PHRASES.items():
                st.markdown(f"**{tag}:** {' | '.join(phrases[:2])}")

    # ── TAB 3: Оси ──
    with _types_tab3:
        st.markdown("#### Ось 1: Активация")
        st.dataframe(pd.DataFrame([
            {"Код": "no", "Значение": "Без активации", "Определение": "По умолчанию — акция работает автоматически"},
            {"Код": "yes", "Значение": "Нужна активация", "Определение": "Ключевые слова: «активируй», «акцептн»"},
        ]), use_container_width=True, hide_index=True)

        st.markdown("#### Ось 2: Тип выгоды")
        st.dataframe(pd.DataFrame([
            {"Код": "cashback_pct", "Тип": "Кешбэк X%", "Ключевые слова": "кешбэк / кэшбэк / вернём / возвращаем / обратно на счет + %", "Пример": "20% кешбэк на овощи"},
            {"Код": "cashback_rub", "Тип": "Кешбэк X₽", "Ключевые слова": "кешбэк / вернём / возвращаем + ₽/р.", "Пример": "Вернём 300₽ с чека"},
            {"Код": "discount_pct", "Тип": "Скидка X%", "Ключевые слова": "скидка / скидки + %", "Пример": "-10% на зубную пасту"},
            {"Код": "discount_rub", "Тип": "Скидка X₽", "Ключевые слова": "скидка / купон на скидку + ₽/р.", "Пример": "Скидка 50₽ по купону"},
            {"Код": "gift", "Тип": "Предначисление", "Ключевые слова": "«дарим» + число, или число + «монет» без маркеров кешбэка/скидки", "Пример": "Дарим 150₽ монетами"},
            {"Код": "communication", "Тип": "Коммуникация", "Ключевые слова": "коммуникац / тематик / подборка / рассылка", "Пример": "Детские товары — цены снижены"},
            {"Код": "present", "Тип": "Подарок", "Ключевые слова": "подарок / подарки за", "Пример": "Подарки за покупку"},
        ]), use_container_width=True, hide_index=True)

        st.markdown("#### Ось 3: Товарный охват")
        st.dataframe(pd.DataFrame([
            {"Код": "all", "Значение": "Все товары", "Определение": "Нет категории / «все товары» / «на покупки»"},
            {"Код": "category", "Значение": "Категория/список", "Определение": "Конкретная категория: овощи, химия, алкоголь и т.д."},
        ]), use_container_width=True, hide_index=True)

        st.markdown("#### Ось 4: Условие чека")
        st.dataframe(pd.DataFrame([
            {"Код": "no_check", "Значение": "Без ограничения", "Определение": "Нет минимальной суммы чека"},
            {"Код": "check_from", "Значение": "От X₽", "Определение": "«от 1000р» / «чек от» / «с 1500р»"},
        ]), use_container_width=True, hide_index=True)

        st.markdown("#### Ось 5: Период")
        st.dataframe(pd.DataFrame([
            {"Код": "one_day", "Значение": "1 день", "Push": "1 push в день акции"},
            {"Код": "week", "Значение": "2-7 дней", "Push": "Старт + напоминание через 1 день после старта"},
            {"Код": "month", "Значение": "8+ дней", "Push": "Каждую неделю в тот же день недели"},
        ]), use_container_width=True, hide_index=True)

        st.markdown("#### Ось 6: Контекст (автоматически по дате)")
        st.dataframe(pd.DataFrame([
            {"Тег": "зима / весна / лето / осень", "Описание": "Сезон определяется по месяцу старта акции"},
            {"Тег": "пасха", "Описание": "За 7 дней до Пасхи (2026: 12 апреля)"},
            {"Тег": "новый_год", "Описание": "За 7 дней до 31 декабря"},
            {"Тег": "8_марта", "Описание": "За 7 дней до 8 марта"},
            {"Тег": "23_февраля", "Описание": "За 7 дней до 23 февраля"},
            {"Тег": "шашлыки", "Описание": "Май — сентябрь"},
            {"Тег": "уборка", "Описание": "Апрель — май (весенняя уборка)"},
            {"Тег": "жара", "Описание": "Июнь — август"},
            {"Тег": "тепло_уют", "Описание": "Декабрь — февраль"},
        ]), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("""
**Синонимы кешбэка:** кешбэк = кэшбэк = вернём = возвращаем = обратно на счет

**Определение gift:** «дарим» + число, или число + «монет» без маркеров кешбэка/скидки

**Расписание push:**
- 1 день → 1 push
- 2-7 дней → старт + напоминание на 2-й день
- 8+ дней → каждую неделю в тот же день недели
""")


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 📝 Правила генерации
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "Правила генерации":
    dx_page_header(
        "Настройки AI",
        "Правила генерации",
        "Системные инструкции для модели — тон, лимиты, призывы к действию, обращения по сегментам.",
    )

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
        if st.button("Сбросить к значениям по умолчанию", key="reset_rules"):
            st.session_state.generation_rules = default_rules
            st.rerun()
    with col2:
        if st.button("Сохранить правила", key="save_rules"):
            st.success("Правила сохранены!")


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 🔗 Deeplinks
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "Deeplinks":
    dx_page_header(
        "Каталог ссылок",
        "Deeplinks",
        "Поиск deeplink-ссылок мобильного приложения по категории. Используется при генерации PUSH для автоподстановки ссылки.",
    )

    try:
        from ai_generator import search_deeplinks, _load_deeplinks

        # Load all deeplinks
        df_dl = _load_deeplinks()

        if df_dl is not None and not df_dl.empty:
            # Search
            search_query = st.text_input(
                "Поиск по категории",
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


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 📊 Прогноз
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "Прогноз":
    import forecast as _fc

    dx_page_header(
        "Финансовый прогноз",
        "Прогноз по акциям месяца",
        "Расчёт PL по формуле CVM offline: PL = Доп ТО × 0.30 − Скидка. Прогноз по каждой акции и сводный итог.",
    )
    st.caption(
        "Расчёт по формуле CVM offline: **PL = Доп ТО × 0.30 − Скидка**. "
        "Если в строке акции уже есть отклик и Доп ТО — берётся как план; "
        "иначе считаем по медиане исторических аналогов (сегмент + механика + категория)."
    )

    sid = st.session_state.get(
        "spreadsheet_id", "1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY"
    )
    df_cvm = load_cvm_data(sid)

    if df_cvm.empty:
        st.warning("Нет данных. Проверьте доступ к таблице.")
    else:
        _MONTH_NAMES_F = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
        }
        _MONTH_MAP_F = {v: k for k, v in _MONTH_NAMES_F.items()}

        ymf = []
        if "Год" in df_cvm.columns and "Месяц" in df_cvm.columns:
            for _, _r in df_cvm[["Год", "Месяц"]].drop_duplicates().iterrows():
                y, m = str(_r["Год"]).strip(), str(_r["Месяц"]).strip()
                if y.isdigit() and m.isdigit() and int(y) > 2000:
                    ymf.append((int(y), int(m)))
            ymf = sorted(set(ymf))
        if not ymf:
            ymf = [(2026, 5)]
        month_opts = [f"{_MONTH_NAMES_F.get(m, str(m))} {y}" for y, m in ymf]

        fc_col1, fc_col2 = st.columns([1, 1])
        with fc_col1:
            sel_str = st.selectbox(
                "Месяц", month_opts,
                index=len(month_opts) - 1,
                key="forecast_month",
            )
        with fc_col2:
            channels = ["Все"]
            if "Каналы коммуникации" in df_cvm.columns:
                channels += sorted(
                    df_cvm["Каналы коммуникации"].dropna().astype(str).unique().tolist()
                )
            sel_ch = st.selectbox("Канал", channels, index=0, key="forecast_channel")

        parts = sel_str.split()
        sel_m = _MONTH_MAP_F.get(parts[0], 5)
        sel_y = int(parts[1]) if len(parts) > 1 else 2026
        m_start = _date_cls(sel_y, sel_m, 1)
        m_end = (_date_cls(sel_y + 1, 1, 1) - timedelta(days=1)
                 if sel_m == 12
                 else _date_cls(sel_y, sel_m + 1, 1) - timedelta(days=1))

        # Promos in selected month
        month_rows, history_rows = [], []
        for _, row in df_cvm.iterrows():
            yh = row.get("Год", sel_y)
            try:
                yh = int(yh)
            except (ValueError, TypeError):
                yh = sel_y
            sd = _parse_promo_date(row.get("Старт акции"), yh)
            ed = _parse_promo_date(row.get("Окончание акции"), yh)
            if not sd or not ed:
                continue
            if ed < sd:
                sd, ed = ed, sd
            # history = всё, что началось ДО выбранного месяца
            if ed < m_start:
                history_rows.append(row)
                continue
            # промо месяца — пересечение с месяцем
            if sd <= m_end and ed >= m_start:
                if sel_ch != "Все":
                    if str(row.get("Каналы коммуникации", "")).strip() != sel_ch:
                        continue
                month_rows.append(row)

        if not month_rows:
            st.info("В выбранном месяце акций не найдено.")
        else:
            month_df = pd.DataFrame(month_rows)
            hist_df = pd.DataFrame(history_rows) if history_rows else df_cvm.iloc[0:0]

            out_df = _fc.forecast_dataframe(month_df, hist_df)

            # Сводка
            sum_dt = out_df["Доп ТО, ₽"].dropna().sum()
            sum_sk = out_df["Скидка, ₽"].dropna().sum()
            sum_pl = out_df["PL, ₽"].dropna().sum()
            sum_cl = out_df["Кол-во клиентов"].dropna().sum()

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Акций", f"{len(out_df)}")
            m2.metric("Σ клиентов", f"{int(sum_cl):,}".replace(",", " "))
            m3.metric("Σ Доп ТО, ₽", f"{int(sum_dt):,}".replace(",", " "))
            m4.metric("Σ PL, ₽", f"{int(sum_pl):,}".replace(",", " "),
                      delta=f"скидка: {int(sum_sk):,}".replace(",", " "))

            display_df = out_df.copy()
            for col in ("Доп ТО, ₽", "Скидка, ₽", "PL, ₽", "Кол-во клиентов"):
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(
                        lambda x: f"{int(x):,}".replace(",", " ") if pd.notna(x) else ""
                    )
            st.dataframe(display_df, use_container_width=True, height=520)

            # XLSX
            import io
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as wr:
                out_df.to_excel(wr, sheet_name=f"{_MONTH_NAMES_F.get(sel_m,'')}", index=False)
            st.download_button(
                "Скачать прогноз (XLSX)",
                data=buf.getvalue(),
                file_name=f"прогноз_{sel_y}-{sel_m:02d}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            with st.expander("Методика расчёта", expanded=False):
                st.markdown(
                    "- **Формула PL:** `PL = Доп ТО × 0.30 − Скидка` "
                    "(маржа 30% — выведена из 10+ заполненных акций октября 2025)\n"
                    "- **Если акция уже заполнена** в CVM offline (есть отклик и Доп ТО) — "
                    "значения берутся как план без перерасчёта.\n"
                    "- **Иначе** прогноз = медиана `per-client` метрик аналогов "
                    "× количество клиентов:\n"
                    "  1. Сначала ищем точные аналоги — *сегмент + механика + категория*.\n"
                    "  2. Если <2 — расширяем до *сегмент + механика*.\n"
                    "  3. Дальше — *только механика*, затем *только сегмент*, "
                    "в крайнем случае глобальная медиана.\n"
                    "- **Классификация механик:** BONUS_GIFT (Дарим N монет), "
                    "BONUS_THRESHOLD (Дарим N монет на чек от), CASHBACK_PCT, "
                    "DISCOUNT_PCT, DISCOUNT_THRESHOLD, COUPON, OTHER.\n"
                    "- **Сегментные группы:** ACTIVE_NEW, CHURN_SLEEP, ALL_SEG, NICHE."
                )


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE: 💡 Идеи акций
# ═════════════════════════════════════════════════════════════════════════════

elif _nav_page == "Идеи акций":
    import forecast as _fc

    dx_page_header(
        "Аналитика и инсайты",
        "Идеи акций",
        "Подсказки по пробелам в плане, сезонным окнам и топ-исторических акциям. Источник идей для следующих кампаний.",
    )

    sid = st.session_state.get(
        "spreadsheet_id", "1-jsqs-YChB9uN56PcQ2aWqR01MW3O-uaTunNKYZJ7IY"
    )
    df_cvm = load_cvm_data(sid)

    if df_cvm.empty:
        st.warning("Нет данных.")
    else:
        _MONTH_NAMES_I = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
        }
        _MONTH_MAP_I = {v: k for k, v in _MONTH_NAMES_I.items()}

        ymi = []
        if "Год" in df_cvm.columns and "Месяц" in df_cvm.columns:
            for _, _r in df_cvm[["Год", "Месяц"]].drop_duplicates().iterrows():
                y, m = str(_r["Год"]).strip(), str(_r["Месяц"]).strip()
                if y.isdigit() and m.isdigit() and int(y) > 2000:
                    ymi.append((int(y), int(m)))
            ymi = sorted(set(ymi))
        if not ymi:
            ymi = [(2026, 5)]
        opts = [f"{_MONTH_NAMES_I.get(m, str(m))} {y}" for y, m in ymi]
        sel_str = st.selectbox(
            "Месяц для идей", opts,
            index=len(opts) - 1, key="ideas_month",
        )
        parts = sel_str.split()
        sel_m = _MONTH_MAP_I.get(parts[0], 5)
        sel_y = int(parts[1]) if len(parts) > 1 else 2026
        m_start = _date_cls(sel_y, sel_m, 1)
        m_end = (_date_cls(sel_y + 1, 1, 1) - timedelta(days=1)
                 if sel_m == 12
                 else _date_cls(sel_y, sel_m + 1, 1) - timedelta(days=1))

        # Промо месяца и история
        month_rows, history_rows = [], []
        for _, row in df_cvm.iterrows():
            yh = row.get("Год", sel_y)
            try:
                yh = int(yh)
            except (ValueError, TypeError):
                yh = sel_y
            sd = _parse_promo_date(row.get("Старт акции"), yh)
            ed = _parse_promo_date(row.get("Окончание акции"), yh)
            if not sd or not ed:
                continue
            if ed < sd:
                sd, ed = ed, sd
            if ed < m_start:
                history_rows.append(row)
            elif sd <= m_end and ed >= m_start:
                month_rows.append(row)

        month_df = pd.DataFrame(month_rows) if month_rows else df_cvm.iloc[0:0]
        hist_df = pd.DataFrame(history_rows) if history_rows else df_cvm.iloc[0:0]

        # 1) Покрытие сегментов
        st.markdown("### Покрытие сегментов в месяце")
        cov = _fc.segment_coverage(month_df)
        if cov:
            cov_df = pd.DataFrame(
                [{"Сегмент": k, "Акций": v} for k, v in sorted(cov.items(), key=lambda x: -x[1])]
            )
            st.dataframe(cov_df, use_container_width=True, hide_index=True)
        else:
            st.info("Нет акций в выбранном месяце.")

        # 2) Эвристические идеи: пробелы + сезонность
        st.markdown("### Идеи на основе пробелов и сезонности")
        ideas = _fc.gap_ideas(month_df, sel_m)
        if ideas:
            for i in ideas:
                st.markdown(f"- {i}")
        else:
            st.success("Пробелов не найдено — план месяца сбалансирован по сегментам/сезону.")

        # 3) Топ исторических аналогов (что копировать)
        st.markdown("### Топ исторических акций по PL/клиент")
        st.caption(
            "Лучшие шаблоны для копирования — высокая прибыльность на клиента. "
            "Берите как образец механики/категории."
        )
        top_df = _fc.top_historical_by_pl_per_client(hist_df, n=15)
        if top_df.empty:
            st.info("Недостаточно заполненных исторических данных для рейтинга.")
        else:
            disp = top_df.copy()
            for col in ("Клиенты", "PL, ₽"):
                if col in disp.columns:
                    disp[col] = disp[col].apply(
                        lambda x: f"{int(x):,}".replace(",", " ") if pd.notna(x) else ""
                    )
            st.dataframe(disp, use_container_width=True, hide_index=True, height=420)

