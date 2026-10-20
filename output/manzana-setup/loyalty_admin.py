#!/usr/bin/env python3
"""Админка ДИКСИ для инфокупонов в мобильном приложении (https://loyalty-admin.dixy.ru, ActiveAdmin / Rails) — наша
реализация по разбору программы ДИКСИ 20.10.2026 (lib/loyalty_admin/{client,coupon_buttons}.py, extension/src/worker/
24c_coupon_data_csv_lookup.js, 20_sessions.js). Чужой код не копируется.

Что делает: вход робота (форма Devise /admin/login: admin_user[email], admin_user[password], authenticity_token),
проверка сессии (GET /admin/coupon_data без редиректа на /admin/login), поиск кнопки инфокупона по GUID (полный CSV
/admin/coupon_data.csv; фильтр Ransack по GUID у ДИКСИ отвечал 500, поэтому не используется), создание кнопки одним
POST /admin/coupon_data (форма с authenticity_token), чтение карточки /admin/coupon_data/<id> и сверка полей.
Повторный POST после неясного ответа запрещён: результат ищется по GUID.
"""
from __future__ import annotations

import csv
import io
import re
import time
import uuid
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Callable, Mapping
from urllib.parse import urlparse

INDEX_PATH = "/admin/coupon_data"
NEW_PATH = "/admin/coupon_data/new"
LOGIN_PATH = "/admin/login"
CSV_COLUMNS = {"id": ("id",), "guid": ("id купона в манзане",), "title": ("текст кнопки",), "deeplink": ("диплинк для кнопки",),
               "emission": ("id выпуска в манзане",)}


class LoyaltyAdminError(RuntimeError):
    def __init__(self, message: str, code: str = "LOYALTY_ADMIN") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ButtonRecord:
    record_id: int
    guid: str
    title: str
    deeplink: str
    emission_id: str = ""


def normalize_guid(value: str) -> str:
    try:
        return str(uuid.UUID(str(value).strip()))
    except (ValueError, AttributeError) as exc:
        raise LoyaltyAdminError(f"Некорректный GUID купона: {value!r}", code="GUID_INVALID") from exc


# ---------- разбор HTML ----------

class _CsrfParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hidden, self.meta = "", ""

    def handle_starttag(self, tag, attrs):
        d = {k: v for k, v in attrs if v is not None}
        if tag == "input" and d.get("name") == "authenticity_token" and d.get("value", "").strip():
            self.hidden = d["value"].strip()
        elif tag == "meta" and d.get("name") == "csrf-token" and d.get("content", "").strip():
            self.meta = d["content"].strip()


def csrf_token(html: str) -> str:
    p = _CsrfParser()
    p.feed(html)
    token = p.hidden or p.meta
    if not token:
        raise LoyaltyAdminError("В HTML нет authenticity_token / csrf-token.", code="CSRF_MISSING")
    return token


def looks_like_login(html: str) -> bool:
    low = html.lower()
    return 'id="admin_user_email"' in low or 'name="admin_user[email]"' in low


class _DetailParser(HTMLParser):
    FIELDS = {"row-coupon_external_id": "guid", "row-coupon_emission_id": "emission_id", "row-button_title": "title",
              "row-button_deeplink": "deeplink"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.values: dict[str, str] = {}
        self._field, self._in_td, self._buf = None, False, []

    def handle_starttag(self, tag, attrs):
        d = {k: v for k, v in attrs if v is not None}
        if tag == "tr":
            classes = set(d.get("class", "").split())
            self._field = next((f for c, f in self.FIELDS.items() if c in classes), None)
        elif tag == "td" and self._field:
            self._in_td, self._buf = True, []

    def handle_data(self, data):
        if self._in_td and self._field:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if tag == "td" and self._in_td and self._field:
            value = "".join(self._buf).strip()
            self.values[self._field] = "" if value.casefold() == "пусто" else value
            self._in_td = False
        elif tag == "tr":
            self._field = None


def parse_detail(html: str, record_id: int) -> ButtonRecord:
    p = _DetailParser()
    p.feed(html)
    v = p.values
    return ButtonRecord(record_id, v.get("guid", ""), v.get("title", ""), v.get("deeplink", ""), v.get("emission_id", ""))


def parse_csv(text: str) -> list[ButtonRecord]:
    """Полный экспорт кнопок — строго, как у ДИКСИ (24c_coupon_data_csv_lookup.js): метка BOM снимается; без обязательных
    колонок, без строк данных, со строкой другой длины или нечисловым Id отсутствие GUID не доказано — остановка."""
    if text.startswith("\ufeff"):
        text = text[1:]
    try:
        rows = [r for r in csv.reader(io.StringIO(text, newline=""), strict=True) if r]
    except csv.Error as exc:
        raise LoyaltyAdminError(f"Экспорт CSV повреждён: {exc}.", code="CSV_INCOMPLETE") from exc
    if not rows:
        raise LoyaltyAdminError("Экспорт CSV пуст.", code="CSV_INCOMPLETE")
    header = [h.strip().casefold() for h in rows[0]]
    idx = {}
    for key, names in CSV_COLUMNS.items():
        pos = next((i for i, h in enumerate(header) if h in names), None)
        if pos is None:
            raise LoyaltyAdminError(f"В экспорте CSV нет колонки «{names[0]}».", code="CSV_INCOMPLETE")
        idx[key] = pos
    out = []
    for row in rows[1:]:
        if len(row) != len(header) or not re.fullmatch(r"[1-9][0-9]*", row[idx["id"]].strip()):
            raise LoyaltyAdminError("Экспорт CSV: неполная строка или недопустимый Id; отсутствие GUID не доказано.", code="CSV_INCOMPLETE")
        out.append(ButtonRecord(int(row[idx["id"]]), row[idx["guid"]].strip(), row[idx["title"]].strip(),
                                row[idx["deeplink"]].strip(), row[idx["emission"]].strip()))
    if not out:
        raise LoyaltyAdminError("Экспорт CSV без строк данных; отсутствие GUID не доказано.", code="CSV_INCOMPLETE")
    return out


# ---------- сессия и операции ----------

class LoyaltyAdmin:
    """HTTP к админке через APIRequestContext Playwright того же контекста, что у робота (cookies сессии Devise)."""

    def __init__(self, request: Any, *, base_url: str, login: str = "", password: str = "", timeout_ms: int = 60_000,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.request = request
        self.sleep = sleep
        self.base_url = base_url.rstrip("/")
        self.login, self.password = login, password
        self.timeout_ms = int(timeout_ms)

    def _url(self, path: str) -> str:
        return path if path.startswith("http") else self.base_url + path

    def _get(self, path: str, *, params: Mapping[str, str] | None = None, max_redirects: int | None = None,
             headers: Mapping[str, str] | None = None) -> tuple[int, str, str, dict[str, str]]:
        kwargs: dict[str, Any] = {"method": "GET", "params": dict(params or {}), "timeout": self.timeout_ms,
                                  "fail_on_status_code": False, "headers": dict(headers or {})}
        if max_redirects is not None:
            kwargs["max_redirects"] = max_redirects
        r = self.request.fetch(self._url(path), **kwargs)
        try:
            return r.status, r.url, r.text(), {k.lower(): v for k, v in r.headers.items()}
        finally:
            r.dispose()

    def _post_form(self, path: str, form: Mapping[str, str], *, max_redirects: int = 0) -> tuple[int, str, str, dict[str, str]]:
        r = self.request.fetch(self._url(path), method="POST", form=dict(form), timeout=self.timeout_ms, fail_on_status_code=False,
                               max_redirects=max_redirects)
        try:
            return r.status, r.url, r.text(), {k.lower(): v for k, v in r.headers.items()}
        finally:
            r.dispose()

    def is_authenticated(self) -> bool:
        status, url, body, headers = self._get(INDEX_PATH, max_redirects=0)
        if status in (301, 302, 303, 307, 308, 401, 403):  # 401/403 — сессия истекла, нужен вход (как 20_sessions.js у ДИКСИ)
            return False
        if status != 200:
            raise LoyaltyAdminError(f"Проверка сессии админки: HTTP {status}.", code="ADMIN_HTTP")
        return LOGIN_PATH not in str(url).lower() and not looks_like_login(body)

    def sign_in(self) -> None:
        if not (self.login and self.password):
            raise LoyaltyAdminError("Админка просит вход, а LOYALTY_ADMIN_LOGIN / LOYALTY_ADMIN_PASSWORD не заданы.", code="AUTH_REQUIRED")
        status, _, html, _ = self._get(LOGIN_PATH)
        if status != 200:
            raise LoyaltyAdminError(f"Страница входа админки: HTTP {status}.", code="ADMIN_HTTP")
        token = csrf_token(html)
        status, url, body, headers = self._post_form(LOGIN_PATH, {"authenticity_token": token, "admin_user[email]": self.login,
                                                                   "admin_user[password]": self.password, "commit": "Войти"})
        if status in (301, 302, 303, 307, 308) and LOGIN_PATH in headers.get("location", "").lower():
            raise LoyaltyAdminError("Вход в админку не удался (возврат на страницу входа).", code="AUTH_FAILED")
        if status not in (200, 301, 302, 303, 307, 308):
            raise LoyaltyAdminError(f"Вход в админку: HTTP {status}.", code="AUTH_FAILED")
        if not self.is_authenticated():
            raise LoyaltyAdminError("Вход в админку не подтверждён проверкой сессии.", code="AUTH_FAILED")

    def ensure(self) -> None:
        if not self.is_authenticated():
            self.sign_in()

    # --- кнопки ---

    def find_button(self, guid: str) -> ButtonRecord | None:
        """Поиск только по полному CSV; запасного постраничного обхода нет (у ДИКСИ — «never fall back to pages»):
        недоступный или урезанный экспорт не доказывает, что кнопки нет."""
        guid = normalize_guid(guid)
        status, url, text, _ = self._get(INDEX_PATH + ".csv", headers={"Accept": "text/csv"})
        if status != 200 or LOGIN_PATH in str(url).lower():
            raise LoyaltyAdminError(f"Экспорт CSV кнопок недоступен (HTTP {status}); отсутствие GUID не доказано.", code="CSV_INCOMPLETE")
        records = parse_csv(text)
        matches = [r for r in records if r.guid.strip().casefold() == guid]
        if len(matches) > 1:
            raise LoyaltyAdminError(f"В админке несколько кнопок для GUID {guid}: {[m.record_id for m in matches]}.", code="BUTTON_AMBIGUOUS")
        return matches[0] if matches else None

    def read_button(self, record_id: int) -> ButtonRecord:
        status, _, html, _ = self._get(f"{INDEX_PATH}/{record_id}")
        if status != 200:
            raise LoyaltyAdminError(f"Карточка кнопки {record_id}: HTTP {status}.", code="ADMIN_HTTP")
        return parse_detail(html, record_id)

    @staticmethod
    def verify_button(record: ButtonRecord, *, guid: str, title: str, deeplink: str) -> None:
        problems = []
        if record.guid.casefold() != guid.casefold():
            problems.append("GUID")
        if record.title != title:
            problems.append("текст")
        if record.deeplink != deeplink:
            problems.append("ссылка")
        if record.emission_id:
            problems.append("выпуск должен быть пустым")
        if problems:
            raise LoyaltyAdminError("Кнопка в админке не совпадает: " + ", ".join(problems), code="BUTTON_MISMATCH")

    def ensure_button(self, *, guid: str, title: str, deeplink: str, before_create=lambda: None,
                      attempted: bool = False) -> tuple[str, ButtonRecord]:
        """Существующая кнопка проверяется, новая создаётся одним POST; после неясного ответа — только поиск по GUID.
        attempted — POST уже отправлялся в прошлом прогоне (отметка реестра): кнопки нет в экспорте — остановка без POST."""
        guid = normalize_guid(guid)
        title, deeplink = title.strip(), deeplink.strip()
        if not title or not deeplink:
            raise LoyaltyAdminError("Текст и ссылка кнопки обязательны.", code="BUTTON_INVALID")
        existing = self.find_button(guid)
        if existing is not None:
            record = self.read_button(existing.record_id)
            self.verify_button(record, guid=guid, title=title, deeplink=deeplink)
            return "exists", record
        if attempted:
            raise LoyaltyAdminError("Прошлое создание кнопки не подтверждено, в экспорте её нет; повторный POST запрещён — "
                                    "проверить в админке.", code="UNKNOWN_OUTCOME")
        status, _, html, _ = self._get(NEW_PATH)
        if status != 200:
            raise LoyaltyAdminError(f"Форма новой кнопки: HTTP {status}.", code="ADMIN_HTTP")
        token = csrf_token(html)
        before_create()
        status, url, body, headers = self._post_form(INDEX_PATH, {
            "authenticity_token": token, "coupon_data[coupon_external_id]": guid, "coupon_data[coupon_emission_id]": "",
            "coupon_data[button_title]": title, "coupon_data[button_deeplink]": deeplink, "coupon_data[push_request]": "0",
            "commit": "Cоздать Данные купона"})
        record_id = None
        target = headers.get("location", "") if status in (301, 302, 303, 307, 308) else (str(url) if status == 200 else "")
        if status not in (200, 301, 302, 303, 307, 308):
            raise LoyaltyAdminError(f"Создание кнопки: HTTP {status}; повтор запрещён до проверки по GUID.", code="UNKNOWN_OUTCOME")
        m = re.search(r"/admin/coupon_data/(\d+)$", urlparse(target).path)
        if m:
            record_id = int(m.group(1))
        else:
            # Успешное создание может уйти редиректом на /admin/welcome без ID; запись появляется в экспорте не сразу —
            # до 4 поисков с паузой 0,6 с (как у ДИКСИ). Сбой экспорта — сразу остановка, повторный POST запрещён.
            found = None
            for attempt in range(4):
                found = self.find_button(guid)
                if found is not None:
                    break
                if attempt < 3:
                    self.sleep(0.6)
            if found is None:
                raise LoyaltyAdminError("После создания кнопка не найдена по GUID; повторный POST запрещён.", code="UNKNOWN_OUTCOME")
            record_id = found.record_id
        record = self.read_button(record_id)
        self.verify_button(record, guid=guid, title=title, deeplink=deeplink)
        return "created", record
