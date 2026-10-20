#!/usr/bin/env python3
"""Сессия робота в Manzana Campaign и клиент API (наша реализация по разбору программы ДИКСИ 20.10.2026).

Как устроен доступ к Manzana Campaign (по коду расширения ДИКСИ, extension/src/worker/00_config.js, 19_session_capture.js,
33_http_transport.js, и его Python-клиенту lib/manzana/client.py):
  * веб-приложение https://campaign.manzanagroup.ru (вход пользователя) само вызывает API
    https://apicampaign.manzanagroup.ru/v1/dixy с заголовками auth / authorization (в разделе Loyalty ещё groupingid);
  * программа ДИКСИ берёт эти заголовки у собственных запросов приложения в открытой вкладке и выполняет вызовы API
    вместе с cookies браузера; сессия годна 4 часа; при 401 заголовки берутся заново;
  * профиль manzana_campaign подтверждается запросом приложения к …/msgtemplates*, профиль manzana_loyalty — запросом
    к …/mlp/ml/{entity,relationship,metadata,action} с groupingid.
Здесь вместо вкладки менеджера — Playwright с постоянным профилем нашей учётки робота: вход по логину и паролю
(селекторы формы — в настройках, уточняются при первом входе), заголовки запросов приложения читаются через
page.on("request"), затем вызовы API идут через APIRequestContext того же контекста (те же cookies).
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any, Mapping
from urllib.parse import urlparse

APP_ORIGIN_DEFAULT = "https://campaign.manzanagroup.ru"
API_PREFIX_DEFAULT = "https://apicampaign.manzanagroup.ru/v1/dixy"
REUSABLE_HEADER_NAMES = frozenset({"accept", "auth", "authorization", "entityfornodeid", "entityfornodetype",
                                   "groupingid", "x-requested-with", "x-csrf-token", "csrf-token"})
LOYALTY_CONTEXT_PREFIXES = ("/mlp/ml/entity", "/mlp/ml/relationship", "/mlp/ml/metadata", "/mlp/ml/action")
BOOTSTRAP_PATHS = {"manzana_campaign": "/templates/push", "manzana_loyalty": "/loyalty/contacts/clients"}


class ManzanaSessionError(RuntimeError):
    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self.code = code


class ManzanaApiError(RuntimeError):
    def __init__(self, method: str, url: str, status: int, body: str) -> None:
        super().__init__(f"{method} {url}: HTTP {status} {body[:200]}")
        self.status, self.url = status, url


def classify_api_request(url: str, headers: Mapping[str, str], api_prefix: str = API_PREFIX_DEFAULT) -> str | None:
    """Какой профиль подтверждает запрос приложения: manzana_campaign (…/msgtemplates*, есть auth/authorization),
    manzana_loyalty (/mlp/ml/*, есть groupingid). Иначе None."""
    if not url.startswith(api_prefix):
        return None
    lower = {str(k).lower(): str(v) for k, v in headers.items()}
    if not (lower.get("auth") or lower.get("authorization")):
        return None
    path = urlparse(url).path
    root = urlparse(api_prefix).path.rstrip("/")
    relative = path[len(root):] if path.startswith(root) else path
    if lower.get("groupingid") and any(relative == p or relative.startswith(p + "/") for p in LOYALTY_CONTEXT_PREFIXES):
        return "manzana_loyalty"
    if "/msgtemplates" in path.lower():
        return "manzana_campaign"
    return None


def reusable_headers(headers: Mapping[str, str], profile: str) -> dict[str, str]:
    """Переиспользуемые заголовки приложения; campaign — без groupingid и entityfornode*, loyalty — без entityfornode*."""
    out = {str(k).lower(): str(v) for k, v in headers.items() if str(k).lower() in REUSABLE_HEADER_NAMES and v}
    drop = ("groupingid", "entityfornodeid", "entityfornodetype") if profile == "manzana_campaign" else ("entityfornodeid", "entityfornodetype")
    for key in drop:
        out.pop(key, None)
    return out


class ManzanaApi:
    """Вызовы API Campaign через APIRequestContext Playwright (cookies робота) с заголовками приложения.
    Методы по смыслу те же, что у клиента ДИКСИ: get_json_or_text, post_json, put_json."""

    def __init__(self, api_request: Any, headers: Mapping[str, str], *, api_prefix: str = API_PREFIX_DEFAULT,
                 timeout_ms: int = 120_000, logger: logging.Logger | None = None) -> None:
        self.request = api_request
        self.headers = {str(k).lower(): str(v) for k, v in headers.items()}
        self.api_prefix = api_prefix.rstrip("/")
        self.timeout_ms = int(timeout_ms)
        self.logger = logger or logging.getLogger("cvm.manzana.api")

    def _url(self, path: str) -> str:
        return path if path.startswith("http") else self.api_prefix + path

    def _call(self, method: str, path: str, *, params: Mapping[str, Any] | None = None, data: Any = None) -> Any:
        url = self._url(path)
        response = self.request.fetch(url, method=method, headers=self.headers,
                                      params={k: v for k, v in (params or {}).items() if v is not None},
                                      data=data, timeout=self.timeout_ms, fail_on_status_code=False)
        try:
            status, body = response.status, response.text()
        finally:
            response.dispose()
        if not 200 <= status < 300:
            raise ManzanaApiError(method, url, status, body)
        try:
            return json.loads(body)
        except ValueError:
            return body

    def get_json_or_text(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        return self._call("GET", path, params=params)

    def post_json(self, path: str, payload: dict[str, Any], *, params: Mapping[str, Any] | None = None) -> Any:
        return self._call("POST", path, params=params, data=payload)

    def put_json(self, path: str, payload: dict[str, Any], *, params: Mapping[str, Any] | None = None) -> Any:
        return self._call("PUT", path, params=params, data=payload)


class CampaignSession:
    """Вход робота и получение заголовков API для профилей manzana_campaign / manzana_loyalty."""

    def __init__(self, context: Any, *, login: str, password: str, totp_secret: str = "", app_origin: str = APP_ORIGIN_DEFAULT,
                 api_prefix: str = API_PREFIX_DEFAULT, selectors: Mapping[str, str] | None = None, timeout_ms: int = 60_000,
                 logger: logging.Logger | None = None) -> None:
        self.context = context
        self.login, self.password, self.totp_secret = login, password, totp_secret
        self.app_origin, self.api_prefix = app_origin.rstrip("/"), api_prefix.rstrip("/")
        self.selectors = {"login": 'input[name="login"], input[name="username"], input[type="email"]',
                          "password": 'input[type="password"]', "submit": 'button[type="submit"], input[type="submit"]',
                          "code": 'input[name*="code" i], input[name*="otp" i]', **(selectors or {})}
        self.timeout_ms = int(timeout_ms)
        self.logger = logger or logging.getLogger("cvm.manzana.session")
        self.captured: dict[str, dict[str, str]] = {}
        self._lock = threading.Lock()
        self._page: Any = None

    def _on_request(self, request: Any) -> None:
        try:
            profile = classify_api_request(request.url, request.headers, self.api_prefix)
        except Exception:  # noqa: BLE001 — наблюдение за запросами не должно ронять навигацию
            return
        if profile:
            with self._lock:
                self.captured[profile] = reusable_headers(request.headers, profile)

    def page(self) -> Any:
        if self._page is None:
            self._page = self.context.pages[0] if self.context.pages else self.context.new_page()
            self._page.on("request", self._on_request)
        return self._page

    @staticmethod
    def is_login_url(url: str) -> bool:
        low = str(url).lower()
        return "/login" in low or "login?" in low

    def sign_in(self, page: Any) -> None:
        if not (self.login and self.password):
            raise ManzanaSessionError("Manzana Campaign просит вход, а MC_LOGIN / MC_PASSWORD не заданы.", code="AUTH_REQUIRED")
        page.fill(self.selectors["login"], self.login)
        page.fill(self.selectors["password"], self.password)
        page.click(self.selectors["submit"])
        self._settle(page)
        if self.totp_secret and page.locator(self.selectors["code"]).count():
            import pyotp
            page.fill(self.selectors["code"], pyotp.TOTP(self.totp_secret).now())
            page.click(self.selectors["submit"])
            self._settle(page)
        if self._wait_leave_login(page):
            raise ManzanaSessionError("Вход в Manzana Campaign не удался: страница входа осталась (логин, пароль, второй фактор, "
                                      "CAPTCHA или селекторы формы — проверить при первом входе).", code="AUTH_FAILED")
        self.logger.info("Manzana Campaign: вход робота выполнен.")

    def _settle(self, page: Any) -> None:
        """Приложение Manzana держит фоновые соединения — ждать networkidle нельзя; ждём загрузку документа."""
        try:
            page.wait_for_load_state("load", timeout=self.timeout_ms)
        except Exception:  # noqa: BLE001 — таймаут загрузки не отменяет уже выполненный переход
            pass

    def _wait_leave_login(self, page: Any, step_ms: int = 500) -> bool:
        """True, если страница входа так и осталась (поздний редирект SPA учитывается)."""
        waited = 0
        while self.is_login_url(page.url) and waited < self.timeout_ms:
            page.wait_for_timeout(step_ms)
            waited += step_ms
        return self.is_login_url(page.url)

    def ensure(self, profile: str) -> dict[str, str]:
        """Открывает раздел приложения, при необходимости входит, ждёт запрос приложения к API, отдаёт заголовки."""
        if profile not in BOOTSTRAP_PATHS:
            raise ManzanaSessionError(f"Неизвестный профиль сессии: {profile}", code="UNKNOWN_SESSION_PROFILE")
        page = self.page()
        target = self.app_origin + BOOTSTRAP_PATHS[profile]
        page.goto(target, wait_until="load", timeout=self.timeout_ms)
        waited, signed_in = 0, False
        # Редирект на вход бывает поздним (SPA): проверяем адрес в том же цикле, что ждёт запрос приложения к API.
        while profile not in self.captured and waited < self.timeout_ms:
            if self.is_login_url(page.url):
                if signed_in:
                    raise ManzanaSessionError("Manzana Campaign снова показала страницу входа после входа робота.", code="AUTH_FAILED")
                self.sign_in(page)
                signed_in = True
                page.goto(target, wait_until="load", timeout=self.timeout_ms)
                continue
            page.wait_for_timeout(500)
            waited += 500
        headers = self.captured.get(profile)
        if not headers:
            raise ManzanaSessionError("Приложение Manzana не выполнило запрос к API, по которому берутся заголовки профиля "
                                      f"{profile} (auth/authorization" + (", groupingid" if profile == "manzana_loyalty" else "") + ").",
                                      code="MANZANA_AUTH_HEADER_MISSING")
        return headers

    def api(self, profile: str = "manzana_campaign") -> ManzanaApi:
        return ManzanaApi(self.context.request, self.ensure(profile), api_prefix=self.api_prefix, logger=self.logger)

    def forget(self, profile: str) -> None:
        with self._lock:
            self.captured.pop(profile, None)
