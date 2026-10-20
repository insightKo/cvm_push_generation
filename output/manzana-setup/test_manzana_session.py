"""Тесты manzana_session.py без браузера: разбор запросов приложения, выбор заголовков, клиент API на подставном контексте."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import manzana_session as ms  # noqa: E402

API = ms.API_PREFIX_DEFAULT


def test_classify_campaign_and_loyalty_requests():
    assert ms.classify_api_request(API + "/msgtemplates/new/5", {"Auth": "t"}) == "manzana_campaign"
    assert ms.classify_api_request(API + "/mlp/ml/entity/x", {"authorization": "Bearer t", "GroupingId": "g"}) == "manzana_loyalty"
    assert ms.classify_api_request(API + "/mlp/ml/entity/x", {"authorization": "Bearer t"}) is None
    assert ms.classify_api_request(API + "/mlp/ml/maintenance/ping", {"auth": "t", "groupingid": "g"}) is None
    assert ms.classify_api_request(API + "/camps/1", {"auth": "t"}) is None
    assert ms.classify_api_request("https://evil.example/v1/dixy/msgtemplates", {"auth": "t"}) is None
    assert ms.classify_api_request(API + "/msgtemplates", {"cookie": "x"}) is None


def test_reusable_headers_per_profile():
    raw = {"Auth": "t", "GroupingId": "g", "EntityForNodeId": "e", "Cookie": "c", "User-Agent": "ua", "Accept": "application/json"}
    assert ms.reusable_headers(raw, "manzana_campaign") == {"auth": "t", "accept": "application/json"}
    assert ms.reusable_headers(raw, "manzana_loyalty") == {"auth": "t", "groupingid": "g", "accept": "application/json"}


class Resp:
    def __init__(self, status, body):
        self.status, self._body = status, body
    def text(self):
        return self._body
    def dispose(self):
        pass


class Ctx:
    def __init__(self, status=200, body='{"exist": false}'):
        self.status, self.body, self.calls = status, body, []
    def fetch(self, url, **kw):
        self.calls.append((url, kw))
        return Resp(self.status, self.body)


def test_api_client_adds_headers_and_parses_json():
    ctx = Ctx()
    api = ms.ManzanaApi(ctx, {"auth": "t"})
    assert api.post_json("/mlist/exist", {"Name": "x"}) == {"exist": False}
    url, kw = ctx.calls[0]
    assert url == API + "/mlist/exist" and kw["method"] == "POST" and kw["headers"] == {"auth": "t"} and kw["data"] == {"Name": "x"}
    ctx2 = Ctx(body="https://host/mlists/x.zip?X-Amz-Signature=1")
    assert ms.ManzanaApi(ctx2, {"auth": "t"}).get_json_or_text("/import/s3url", params={"key": "x.zip"}).startswith("https://")
    assert ctx2.calls[0][1]["params"] == {"key": "x.zip"}


def test_api_client_raises_on_http_error():
    api = ms.ManzanaApi(Ctx(status=401, body="unauthorized"), {"auth": "t"})
    with pytest.raises(ms.ManzanaApiError) as e:
        api.put_json("/import/s3import", {})
    assert e.value.status == 401


class FakeRequest:
    def __init__(self, url, headers):
        self.url, self.headers = url, headers


class FakePage:
    def __init__(self, session, login_first=True):
        self.session, self.login_first, self.url, self.handlers, self.filled, self.clicks = session, login_first, "", [], [], 0
    def on(self, event, fn):
        self.handlers.append(fn)
    def goto(self, url, **kw):
        if self.login_first and not self.filled:
            self.url = self.session.app_origin + "/login?return=" + url
            return
        self.url = url
        for fn in self.handlers:
            fn(FakeRequest(API + "/msgtemplates/list", {"auth": "token-1", "accept": "application/json", "cookie": "c"}))
    def fill(self, selector, value):
        self.filled.append((selector, value))
    def click(self, selector):
        self.clicks += 1
    def wait_for_load_state(self, *a, **k):
        self.url = self.session.app_origin + "/templates/push"
        assert a and a[0] == "load"  # networkidle не ждём: приложение держит фоновые соединения
    def locator(self, selector):
        class L:
            def count(self_inner):
                return 0
        return L()
    def wait_for_timeout(self, ms):
        pass


class FakeContext:
    def __init__(self):
        self.pages, self.request = [], Ctx()


def test_session_signs_in_and_captures_campaign_headers():
    ctx = FakeContext()
    s = ms.CampaignSession(ctx, login="robot", password="pw")
    page = FakePage(s)
    ctx.pages.append(page)
    headers = s.ensure("manzana_campaign")
    assert headers == {"auth": "token-1", "accept": "application/json"} and page.clicks == 1
    assert [v for _, v in page.filled] == ["robot", "pw"]
    api = s.api("manzana_campaign")
    assert api.headers["auth"] == "token-1"


def test_session_without_credentials_fails_closed():
    ctx = FakeContext()
    s = ms.CampaignSession(ctx, login="", password="")
    ctx.pages.append(FakePage(s))
    with pytest.raises(ms.ManzanaSessionError) as e:
        s.ensure("manzana_campaign")
    assert e.value.code == "AUTH_REQUIRED"
