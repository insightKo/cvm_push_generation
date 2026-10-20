"""Тесты loyalty_admin.py без сети: подставной APIRequestContext с ответами админки (ActiveAdmin)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import loyalty_admin as la  # noqa: E402

BASE = "https://admin.test"
GUID = "0f9a1c2e-1b3d-4c5e-8f7a-9b0c1d2e3f4a"
LOGIN_HTML = '<html><head><meta name="csrf-token" content="meta-token"></head><body><form action="/admin/login">' \
             '<input type="hidden" name="authenticity_token" value="form-token"><input id="admin_user_email" name="admin_user[email]">' \
             '<input name="admin_user[password]" type="password"></form></body></html>'
NEW_HTML = '<html><body><form><input type="hidden" name="authenticity_token" value="new-token"></form></body></html>'
INDEX_HTML = '<html><body><table><tr id="coupon_data_7"><td class="col-id">7</td><td class="col-coupon_external_id">' + GUID + \
             '</td><td class="col-coupon_emission_id"></td><td class="col-button_title">В КАТАЛОГ</td>' \
             '<td class="col-button_deeplink">dixyapp://app/catalog</td></tr></table></body></html>'
CSV = "Id,Id купона в манзане,Id выпуска в манзане,Текст кнопки,Диплинк для кнопки\r\n7," + GUID + ",,В КАТАЛОГ,dixyapp://app/catalog\r\n"


def detail(record_id, guid=GUID, title="В КАТАЛОГ", link="dixyapp://app/catalog", emission="Пусто"):
    return (f'<html><body><table><tr class="row row-coupon_external_id"><th>GUID</th><td>{guid}</td></tr>'
            f'<tr class="row row-coupon_emission_id"><th>Выпуск</th><td>{emission}</td></tr>'
            f'<tr class="row row-button_title"><th>Текст</th><td>{title}</td></tr>'
            f'<tr class="row row-button_deeplink"><th>Ссылка</th><td>{link}</td></tr></table></body></html>')


class Resp:
    def __init__(self, status, url, text, headers=None):
        self.status, self.url, self._text, self.headers = status, url, text, headers or {}

    def text(self):
        return self._text

    def dispose(self):
        pass


class FakeRequest:
    """Маршруты: (method, path) → функция(form, params) → Resp. Ведёт журнал вызовов."""

    def __init__(self, routes, authenticated=True):
        self.routes, self.authenticated, self.calls = routes, authenticated, []

    def fetch(self, url, method="GET", params=None, headers=None, form=None, timeout=None, fail_on_status_code=False, max_redirects=None):
        path = url[len(BASE):] if url.startswith(BASE) else url
        self.calls.append((method, path, dict(params or {}), dict(form or {})))
        handler = self.routes.get((method, path))
        if handler is None:
            return Resp(404, url, "nope")
        return handler(self, dict(form or {}), dict(params or {}))


def index_handler(req, form, params):
    if not req.authenticated:
        return Resp(302, BASE + la.INDEX_PATH, "", {"Location": BASE + la.LOGIN_PATH})
    return Resp(200, BASE + la.INDEX_PATH, INDEX_HTML)


def csv_handler(req, form, params):
    if not req.authenticated:
        return Resp(302, BASE + la.INDEX_PATH + ".csv", "", {"Location": BASE + la.LOGIN_PATH})
    return Resp(200, BASE + la.INDEX_PATH + ".csv", CSV)


def admin(routes, authenticated=True, **kw):
    req = FakeRequest(routes, authenticated)
    return la.LoyaltyAdmin(req, base_url=BASE, login=kw.get("login", "robot@test"), password=kw.get("password", "pw")), req


def test_csrf_and_login_detection():
    assert la.csrf_token(LOGIN_HTML) == "form-token"
    assert la.csrf_token('<meta name="csrf-token" content="m">') == "m"
    with pytest.raises(la.LoyaltyAdminError) as e:
        la.csrf_token("<html></html>")
    assert e.value.code == "CSRF_MISSING"
    assert la.looks_like_login(LOGIN_HTML) and not la.looks_like_login(INDEX_HTML)


def test_parse_csv_and_detail():
    rows = la.parse_csv(CSV)
    assert rows == [la.ButtonRecord(7, GUID, "В КАТАЛОГ", "dixyapp://app/catalog", "")]
    with pytest.raises(la.LoyaltyAdminError) as e:
        la.parse_csv("Id,Текст кнопки\r\n1,x\r\n")
    assert e.value.code == "CSV_INCOMPLETE"
    d = la.parse_detail(detail(7), 7)
    assert d.guid == GUID and d.title == "В КАТАЛОГ" and d.deeplink == "dixyapp://app/catalog" and d.emission_id == ""


def test_sign_in_when_session_expired():
    state = {"ok": False}

    def login_get(req, form, params):
        return Resp(200, BASE + la.LOGIN_PATH, LOGIN_HTML)

    def login_post(req, form, params):
        assert form["authenticity_token"] == "form-token" and form["admin_user[email]"] == "robot@test"
        req.authenticated = form["admin_user[password]"] == "pw"
        return Resp(302, BASE + la.LOGIN_PATH, "", {"Location": BASE + ("/admin" if req.authenticated else la.LOGIN_PATH)})

    a, req = admin({("GET", la.INDEX_PATH): index_handler, ("GET", la.LOGIN_PATH): login_get, ("POST", la.LOGIN_PATH): login_post},
                   authenticated=False)
    assert a.is_authenticated() is False
    a.ensure()
    assert req.authenticated and a.is_authenticated()
    bad, _ = admin({("GET", la.INDEX_PATH): index_handler, ("GET", la.LOGIN_PATH): login_get, ("POST", la.LOGIN_PATH): login_post},
                   authenticated=False, password="wrong")
    with pytest.raises(la.LoyaltyAdminError) as e:
        bad.ensure()
    assert e.value.code == "AUTH_FAILED"
    nocreds, _ = admin({("GET", la.INDEX_PATH): index_handler}, authenticated=False, login="", password="")
    with pytest.raises(la.LoyaltyAdminError) as e:
        nocreds.ensure()
    assert e.value.code == "AUTH_REQUIRED"


def test_existing_button_is_verified_not_recreated():
    a, req = admin({("GET", la.INDEX_PATH + ".csv"): csv_handler, ("GET", la.INDEX_PATH): index_handler,
                    ("GET", f"{la.INDEX_PATH}/7"): lambda r, f, p: Resp(200, BASE, detail(7))})
    state, rec = a.ensure_button(guid=GUID.upper(), title="В КАТАЛОГ", deeplink="dixyapp://app/catalog")
    assert state == "exists" and rec.record_id == 7
    assert not [c for c in req.calls if c[0] == "POST"]
    with pytest.raises(la.LoyaltyAdminError) as e:
        a.ensure_button(guid=GUID, title="ВЫБРАТЬ", deeplink="dixyapp://app/catalog")
    assert e.value.code == "BUTTON_MISMATCH"


def test_create_button_once_with_redirect_and_verify():
    created = {}

    def post(req, form, params):
        assert form["coupon_data[coupon_external_id]"] == GUID and form["coupon_data[coupon_emission_id]"] == ""
        assert form["authenticity_token"] == "new-token" and form["coupon_data[push_request]"] == "0"
        created["form"] = form
        return Resp(302, BASE + la.INDEX_PATH, "", {"Location": BASE + f"{la.INDEX_PATH}/9"})

    a, req = admin({("GET", la.INDEX_PATH + ".csv"): lambda r, f, p: Resp(200, BASE, CSV.splitlines()[0] + "\r\n"),
                    ("GET", la.NEW_PATH): lambda r, f, p: Resp(200, BASE, NEW_HTML), ("POST", la.INDEX_PATH): post,
                    ("GET", f"{la.INDEX_PATH}/9"): lambda r, f, p: Resp(200, BASE, detail(9, title="ВЫБРАТЬ", link="dixyapp://app/x"))})
    state, rec = a.ensure_button(guid=GUID, title="ВЫБРАТЬ", deeplink="dixyapp://app/x")
    assert state == "created" and rec.record_id == 9 and created["form"]["coupon_data[button_title]"] == "ВЫБРАТЬ"
    assert len([c for c in req.calls if c[0] == "POST"]) == 1


def test_unclear_create_response_searches_by_guid_and_never_reposts():
    seen = {"posts": 0}

    def post(req, form, params):
        seen["posts"] += 1
        return Resp(200, BASE + la.INDEX_PATH, "<html>ok</html>")  # без редиректа и ID

    csv_after = CSV.replace("7,", "12,")
    a, req = admin({("GET", la.INDEX_PATH + ".csv"): lambda r, f, p: Resp(200, BASE, csv_after if seen["posts"] else CSV.splitlines()[0] + "\r\n"),
                    ("GET", la.NEW_PATH): lambda r, f, p: Resp(200, BASE, NEW_HTML), ("POST", la.INDEX_PATH): post,
                    ("GET", f"{la.INDEX_PATH}/12"): lambda r, f, p: Resp(200, BASE, detail(12))})
    state, rec = a.ensure_button(guid=GUID, title="В КАТАЛОГ", deeplink="dixyapp://app/catalog")
    assert state == "created" and rec.record_id == 12 and seen["posts"] == 1
    # ответ 500 — исход неясен, повторный POST запрещён
    b, req2 = admin({("GET", la.INDEX_PATH + ".csv"): lambda r, f, p: Resp(200, BASE, CSV.splitlines()[0] + "\r\n"),
                     ("GET", la.NEW_PATH): lambda r, f, p: Resp(200, BASE, NEW_HTML),
                     ("POST", la.INDEX_PATH): lambda r, f, p: Resp(500, BASE, "error")})
    with pytest.raises(la.LoyaltyAdminError) as e:
        b.ensure_button(guid=GUID, title="В КАТАЛОГ", deeplink="dixyapp://app/catalog")
    assert e.value.code == "UNKNOWN_OUTCOME" and len([c for c in req2.calls if c[0] == "POST"]) == 1


def test_csv_unavailable_falls_back_to_pages_and_proves_completeness():
    pages = {1: INDEX_HTML, 2: "<html><body><table></table></body></html>"}

    def index(req, form, params):
        page = int(params.get("page", "1"))
        return Resp(200, BASE + la.INDEX_PATH, pages[page])

    a, req = admin({("GET", la.INDEX_PATH + ".csv"): lambda r, f, p: Resp(302, BASE, "", {"Location": BASE + la.LOGIN_PATH}),
                    ("GET", la.INDEX_PATH): index})
    assert a.find_button(GUID).record_id == 7
    assert a.find_button("11111111-2222-3333-4444-555555555555") is None
    stuck, _ = admin({("GET", la.INDEX_PATH + ".csv"): lambda r, f, p: Resp(500, BASE, ""),
                      ("GET", la.INDEX_PATH): lambda r, f, p: Resp(200, BASE + la.INDEX_PATH, INDEX_HTML)})
    with pytest.raises(la.LoyaltyAdminError) as e:
        stuck.find_button("11111111-2222-3333-4444-555555555555")
    assert e.value.code == "CSV_INCOMPLETE"
