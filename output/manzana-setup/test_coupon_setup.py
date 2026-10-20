"""Тесты coupon_setup.py без сети: подставной MLP API с хранилищем сущностей и связей, подставная админка."""
import datetime as dt
import json
import uuid
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import coupon_setup as cs  # noqa: E402
from loyalty_admin import ButtonRecord  # noqa: E402

TODAY = dt.date(2026, 10, 20)
GUID = "0f9a1c2e-1b3d-4c5e-8f7a-9b0c1d2e3f4a"


class FakeMlp:
    """Сущности: entity → {id: {"id", "attributes", businessUnitId, ownerId, createdBy}}; связи: (entity, id, target, mark) → set."""

    def __init__(self):
        self.entities = {"MarketingList": {}, "ChequeFilter": {}, "Partner": {}, "Campaign": {}, cs.RULE: {}}
        self.links = {}
        self.calls = []
        self.next_id = 1000
        self.put_raises_after_apply = False
        self.post_hides_id = False

    def add(self, entity, ident, **attributes):
        self.entities[entity][ident] = {"id": ident, "businessUnitId": "d1", "ownerId": "u1", "createdBy": "u1", "attributes": dict(attributes)}
        return self.entities[entity][ident]

    def _list(self, entity, params):
        q = ""
        if params.get("fullTextSearch"):
            q = json.loads(params["fullTextSearch"])["text"].rstrip("*").casefold()
        items = [v for v in self.entities[entity].values()
                 if not q or any(str(v["attributes"].get(k) or "").casefold().startswith(q) for k in ("Name", "ExternalId"))]
        skip, take = int(params.get("skip", 0)), int(params.get("take", 100))
        return items[skip:skip + take]

    def get_json_or_text(self, path, *, params=None):
        params = params or {}
        self.calls.append(("GET", path))
        if path == "/accessv2/getaccessids":
            return {"UserID": "u1", "DepartmentID": "d1"}
        m = re.fullmatch(r"/mlp/ml/entity/(\w+)/(\d+)", path)
        if m:
            e = self.entities[m.group(1)].get(int(m.group(2)))
            if e is None:
                raise RuntimeError("404")
            return json.loads(json.dumps(e))
        m = re.fullmatch(r"/mlp/ml/entity/(\w+)", path)
        if m:
            return json.loads(json.dumps(self._list(m.group(1), params)))
        m = re.fullmatch(r"/mlp/ml/relationship/(\w+)/(\d+)/(\w+)/(\w+)", path)
        if m:
            ids = self.links.get((m.group(1), int(m.group(2)), m.group(3), m.group(4)), set())
            return [json.loads(json.dumps(self.entities[m.group(3)][i])) for i in sorted(ids)]
        raise AssertionError(path)

    def put_json(self, path, payload, *, params=None):
        self.calls.append(("PUT", path, json.loads(json.dumps(payload))))
        assert path == "/mlp/ml/entity"
        e = self.entities[payload["name"]][payload["id"]]
        e["attributes"].update(payload["attributes"])
        if self.put_raises_after_apply:
            raise RuntimeError("timeout after apply")
        return {"isSuccess": True}

    def post_json(self, path, payload, *, params=None):
        self.calls.append(("POST", path, json.loads(json.dumps(payload))))
        if path == "/mlp/ml/relationship/associate":
            key = (payload["entityName"], payload["id"], payload["TargetEntityName"], payload["mark"])
            self.links.setdefault(key, set()).update(payload["references"])
            return {"isSuccess": True}
        if path == "/mlp/ml/entity":
            self.next_id += 1
            self.add(payload["name"], self.next_id, **payload["attributes"])
            return {"isSuccess": True} if self.post_hides_id else {"id": self.next_id}
        raise AssertionError(path)


class FakeAdmin:
    def __init__(self):
        self.calls = []

    def ensure_button(self, *, guid, title, deeplink, before_create=lambda: None, attempted=False):
        before_create()
        self.calls.append((guid, title, deeplink))
        return "created", ButtonRecord(9, guid, title, deeplink)


def M(api):
    return cs.Mlp(api, sleep=lambda s: None)


def world():
    api = FakeMlp()
    api.add("MarketingList", 11, Name=cs.TEST_LIST, ExternalId="TEST_GROUP", PartnerId={"id": 31}, MemberType="2", IsDynamic="false")
    api.add("ChequeFilter", 21, Name=cs.CHEQUE_FILTER, IsActive="true", ExternalId="ALL")
    api.add("Partner", 31, Name=cs.PARTNER, IsActive="true")
    api.add("Campaign", 1025, Name="Информационные купоны", IsActive="true", ActualStartOn="2020-01-01T00:00:00", ActualEndOn="2030-12-31T23:59:00")
    api.add(cs.RULE, 101, Name="Инфокупон 1", ExternalId="Инфокупон 1", IsForCoupons="false", ResultType="1", Bonus="0", IsActive="false",
            CampaignId={"name": "Campaign", "id": 1025, "label": "Информационные купоны"}, Description="", DateFrom="", DateTo="",
            PrivateOfficeLogoUrl="", CanDisplayInPrivateOffice="false", PrivateOfficeDisplayType="1", FilterByContactListsType="4",
            FilterByChequesType="3")
    return api


def promo(**kw):
    base = {"number": 100001, "title": "Кешбэк 20% на кофе", "coupon_name": "20% кешбэка на кофе", "slip_text": "Купи кофе — вернём 20% монетами.",
            "button": "В КАТАЛОГ dixyapp://app/products_list?id=777", "start_date": dt.date(2026, 10, 23), "end_date": dt.date(2026, 10, 29)}
    base.update(kw)
    return base


def drafts(tmp_path, *entries):
    p = tmp_path / "coupon_drafts.json"
    p.write_text(json.dumps(list(entries) or [{"rule_id": 101, "url": "https://x/edit/101", "guid": GUID.upper(), "used_by": ""}]), encoding="utf-8")
    return cs.DraftRegistry(p)


def test_spec_from_promo_and_button_parsing():
    s = cs.spec_from_promo(promo())
    assert s.title == "20% кешбэка на кофе" and s.external_id == "20% кешбэка на кофе" and not s.accept
    assert (s.button_title, s.button_url) == ("В КАТАЛОГ", "dixyapp://app/products_list?id=777")
    assert cs.parse_button("") == ("В КАТАЛОГ", "dixyapp://app/catalog") and cs.parse_button("В каталог\n") == ("В КАТАЛОГ", "dixyapp://app/catalog")
    # значения колонки «Кнопка» с прода: ссылка с пробелами в имени, перенос строки, ссылка без текста
    assert cs.parse_button("Выбрать dixyapp://app/products_list?id=729&name=Бытовая химия") == (
        "Выбрать", "dixyapp://app/products_list?id=729&name=Бытовая химия")
    assert cs.parse_button("Продукты по акции\n\ndixyapp://app/products_list?id=3476&name=Готовим сморреброд") == (
        "Продукты по акции", "dixyapp://app/products_list?id=3476&name=Готовим сморреброд")
    assert cs.parse_button("dixyapp://app/catalog") == ("В КАТАЛОГ", "dixyapp://app/catalog")
    for after in ("dixyapp://app/catalog В КАТАЛОГ", "Выбрать dixyapp://app/products_list?id=1 (проверить)"):
        with pytest.raises(cs.CouponError) as e:
            cs.parse_button(after)
        assert e.value.code == "BUTTON_INVALID"
    for no_link in ("подборка", "подборка  с горячим кофе", "нужен диплинк", "ВЫБРАТЬ", "ШОКОЛАД И КОНФЕТЫ\nдиплинк в категорию"):
        with pytest.raises(cs.CouponError) as e:
            cs.parse_button(no_link)
        assert e.value.code == "BUTTON_NO_LINK"
    assert cs.spec_from_promo(promo(coupon_name="Активируй 50р. монетами")).accept
    assert cs.spec_from_promo(promo(coupon_name="x" * 47 + "yyy")).external_id == "x" * 47
    for bad in ({"coupon_name": ""}, {"coupon_name": "--"}, {"slip_text": ""}, {"coupon_name": "a" * 101}, {"end_date": None}):
        with pytest.raises(cs.CouponError) as e:
            cs.spec_from_promo(promo(**bad))
        assert e.value.code == "SPEC_INVALID"
    date_from, date_to = s.period(TODAY)
    assert date_from.startswith("2026-10-20T00:00:00+03:00") and date_to.startswith("2026-10-29T23:59:00+03:00")


def test_image_field_sent_only_with_link_and_null_draft_value_passes(tmp_path):
    """Без ссылки на картинку PrivateOfficeLogoUrl в PUT не идёт: у черновика это поле может быть null, и пустая строка
    никогда не прошла бы проверку чтением. Со ссылкой поле записывается."""
    api, admin = world(), FakeAdmin()
    api.entities[cs.RULE][101]["attributes"]["PrivateOfficeLogoUrl"] = None
    spec = cs.spec_from_promo(promo())
    assert "PrivateOfficeLogoUrl" not in spec.attributes(campaign_ref={"id": 1025}, visible=False, active=False, today=TODAY)
    r = cs.setup_coupon_test(M(api), admin, spec, key="100001_20261023", promo=100001, drafts=drafts(tmp_path),
                             ledger=cs.CouponLedger(tmp_path / "coupons.sqlite3"), today=TODAY)
    assert r["status"] == "test_active" and api.entities[cs.RULE][101]["attributes"]["PrivateOfficeLogoUrl"] is None
    with_image = cs.spec_from_promo(promo(coupon_layout_url="https://img.example/coupon.png"))
    assert with_image.attributes(campaign_ref={"id": 1025}, visible=False, active=False, today=TODAY)["PrivateOfficeLogoUrl"] == "https://img.example/coupon.png"
    assert cs.spec_from_promo(promo(coupon_layout_url="http://insecure/x.png")).image_url == ""


def test_draft_registry_reserve_and_free_count(tmp_path):
    reg = drafts(tmp_path, {"rule_id": 101, "guid": GUID, "used_by": ""}, {"rule_id": 102, "guid": "", "used_by": ""},
                 {"rule_id": 103, "guid": str(uuid.uuid4()), "used_by": "100000_20261001"})
    assert reg.free_count() == 1
    d = reg.reserve("100001_20261023")
    assert d["rule_id"] == 101 and reg.reserve("100001_20261023")["rule_id"] == 101 and reg.free_count() == 0
    with pytest.raises(cs.CouponError) as e:
        reg.reserve("100002_20261023")
    assert e.value.code == "DRAFTS_EMPTY"
    assert json.loads((tmp_path / "coupon_drafts.json").read_text(encoding="utf-8"))[0]["used_by"] == "100001_20261023"
    for dup in ({"rule_id": 101, "guid": str(uuid.uuid4()), "used_by": ""}, {"rule_id": 104, "guid": GUID.lower(), "used_by": ""}):
        with pytest.raises(cs.CouponError) as e:
            drafts(tmp_path, {"rule_id": 101, "guid": GUID, "used_by": ""}, dup).reserve("100003_20261023")
        assert e.value.code == "DRAFTS_INVALID"


def test_draft_already_owned_in_ledger_is_not_given_to_second_promo(tmp_path):
    """Отметку used_by потеряли правкой файла: правило 101 уже у купона первой акции — вторая его не получает."""
    api, admin = world(), FakeAdmin()
    ledger = cs.CouponLedger(tmp_path / "c.sqlite3")
    cs.setup_coupon_test(M(api), admin, cs.spec_from_promo(promo()), key="100001_20261023", promo=100001, drafts=drafts(tmp_path),
                         ledger=ledger, today=TODAY)
    calls = len(api.calls)
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api), admin, cs.spec_from_promo(promo(number=100002, coupon_name="Другой купон")), key="100002_20261023",
                             promo=100002, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert e.value.code == "CONFLICT" and not [c for c in api.calls[calls:] if c[0] in ("PUT", "POST")]


def test_own_lists_recognized_by_name_when_links_lack_external_id(tmp_path):
    """Повтор после частичного прогона: связь _ex уже стоит, ответ связей без ExternalId — свой список узнаётся по имени."""
    api, admin = world(), FakeAdmin()
    spec = cs.spec_from_promo(promo())
    api.add("MarketingList", 77, Name=spec.external_id + " Исключаемый", PartnerId={"id": 31}, MemberType="2", IsDynamic="false")
    api.links[(cs.RULE, 101, "MarketingList", "Exclude")] = {77}
    real = api.get_json_or_text

    def no_external_in_links(path, *, params=None):
        out = real(path, params=params)
        if path.startswith("/mlp/ml/relationship/"):
            for i in out:
                i["attributes"].pop("ExternalId", None)
        return out
    api.get_json_or_text = no_external_in_links
    with pytest.raises(cs.CouponError) as e:  # до «чужих связей» проходит; дальше список без ExternalId — CONFLICT ensure_list
        cs.setup_coupon_test(M(api), admin, spec, key="k", promo=100001, drafts=drafts(tmp_path),
                             ledger=cs.CouponLedger(tmp_path / "c.sqlite3"), today=TODAY)
    assert "чужие связи" not in str(e.value)


def test_foreign_marketing_list_on_draft_stops_before_any_write(tmp_path):
    api, admin = world(), FakeAdmin()
    api.add("MarketingList", 66, Name="Реальные клиенты", ExternalId="REAL", PartnerId={"id": 31}, MemberType="2", IsDynamic="false")
    api.links[(cs.RULE, 101, "MarketingList", "Include")] = {66}
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api), admin, cs.spec_from_promo(promo()), key="k", promo=100001, drafts=drafts(tmp_path),
                             ledger=cs.CouponLedger(tmp_path / "c.sqlite3"), today=TODAY)
    assert e.value.code == "CONFLICT" and not [c for c in api.calls if c[0] in ("PUT", "POST")] and admin.calls == []


def test_error_text_hides_playwright_call_log_and_headers():
    exc = RuntimeError("APIRequestContext.fetch: Timeout 60000ms exceeded.\nCall log:\n  - → GET https://x\n    - auth: SECRET1\n    - cookie: s=SECRET2")
    assert "SECRET" not in cs.error_text(exc) and "Timeout" in cs.error_text(exc)
    assert cs.error_text(RuntimeError("bad authorization: Bearer abc.def")) == "RuntimeError: bad authorization: <скрыто>"
    for leak in ("cookie: a=1; _session=SECRET3; remember=SECRET4", "{'auth': 'SECRET5'}", '{"Authorization": "Bearer SECRET6"}',
                 "x-api-key: SECRET7", "url ?token=SECRET8&x=1", "password=SECRET9"):
        assert "SECRET" not in cs.error_text(RuntimeError(leak)), leak


def test_full_test_display_flow_and_idempotent_rerun(tmp_path):
    api, admin = world(), FakeAdmin()
    ledger = cs.CouponLedger(tmp_path / "coupons.sqlite3")
    spec = cs.spec_from_promo(promo())
    r = cs.setup_coupon_test(M(api), admin, spec, key="100001_20261023", promo=100001, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert r["status"] == "test_active" and r["rule_id"] == 101 and r["guid"] == GUID and r["button"] == "created" and r["campaign_id"] == 1025
    rule = api.entities[cs.RULE][101]["attributes"]
    assert rule["Name"] == spec.title and rule["ExternalId"] == spec.external_id and rule["Description"] == spec.description
    assert rule["PrivateOfficeDisplayType"] == 3 and rule["IsActive"] is True and rule["CanDisplayInPrivateOffice"] is False
    assert rule["FilterByContactListsType"] == 4 and rule["FilterByChequesType"] == 3 and rule["CampaignId"]["id"] == 1025
    assert rule["DateFrom"].startswith("2026-10-20T00:00") and rule["DateTo"].startswith("2026-10-29T23:59")
    ex = api.entities["MarketingList"][r["excluded_list_id"]]["attributes"]
    assert ex["Name"] == spec.external_id + " Исключаемый" and ex["ExternalId"] == spec.external_id + "_ex" and ex["PartnerId"]["id"] == 31
    assert api.links[(cs.RULE, 101, "MarketingList", "Include")] == {11}
    assert api.links[(cs.RULE, 101, "MarketingList", "Exclude")] == {r["excluded_list_id"]}
    assert api.links[(cs.RULE, 101, "ChequeFilter", "Exclude")] == {21}
    assert admin.calls == [(GUID, "В КАТАЛОГ", "dixyapp://app/products_list?id=777")] and r["included_list_id"] is None
    # порядок записей: скрытое сохранение → связи → кнопка → показ → активация; активация — отдельный PUT одного поля
    puts = [c[2]["attributes"] for c in api.calls if c[0] == "PUT"]
    assert puts[0]["PrivateOfficeDisplayType"] == 1 and puts[0]["IsActive"] is False
    assert puts[-1] == {"IsActive": True} and puts[-2]["PrivateOfficeDisplayType"] == 3
    assert ledger.get("100001_20261023")["status"] == "test_active"
    before = len(api.calls)
    again = cs.setup_coupon_test(M(api), admin, spec, key="100001_20261023", promo=100001, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert again["already_done"] and len(api.calls) == before and len(admin.calls) == 1


def test_accept_coupon_gets_inclusion_list_and_display_flag(tmp_path):
    api, admin = world(), FakeAdmin()
    spec = cs.spec_from_promo(promo(coupon_name="Активируй 50р. монетами", button=""))
    r = cs.setup_coupon_test(M(api), admin, spec, key="100001_20261023", promo=100001, drafts=drafts(tmp_path),
                             ledger=cs.CouponLedger(tmp_path / "c.sqlite3"), today=TODAY)
    inc = api.entities["MarketingList"][r["included_list_id"]]["attributes"]
    assert inc["ExternalId"] == "Активируй 50р. монетами_in" and api.entities[cs.RULE][101]["attributes"]["CanDisplayInPrivateOffice"] is True
    assert api.links[(cs.RULE, 101, "MarketingList", "Include")] == {11, r["included_list_id"]}
    assert admin.calls[0][1:] == ("В КАТАЛОГ", "dixyapp://app/catalog")


def test_name_conflict_and_wrong_draft_change_nothing(tmp_path):
    api, admin = world(), FakeAdmin()
    api.add(cs.RULE, 555, Name="20% кешбэка на кофе", ExternalId="другой")
    spec = cs.spec_from_promo(promo())
    ledger = cs.CouponLedger(tmp_path / "c.sqlite3")
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api), admin, spec, key="k1", promo=100001, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert e.value.code == "NAME_CONFLICT" and not [c for c in api.calls if c[0] in ("PUT", "POST")] and admin.calls == []
    assert ledger.get("k1")["error"].startswith("скрытое правило")
    api2 = world()
    api2.entities[cs.RULE][101]["attributes"]["IsForCoupons"] = "true"
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api2), admin, spec, key="k2", promo=100002, drafts=drafts(tmp_path), ledger=cs.CouponLedger(tmp_path / "c2.sqlite3"), today=TODAY)
    assert e.value.code == "CONFLICT" and not [c for c in api2.calls if c[0] in ("PUT", "POST")]
    # ответ без обязательного поля — тип черновика не подтверждён
    api3 = world()
    del api3.entities[cs.RULE][101]["attributes"]["IsForCoupons"]
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api3), admin, spec, key="k3", promo=100003, drafts=drafts(tmp_path), ledger=cs.CouponLedger(tmp_path / "c3.sqlite3"), today=TODAY)
    assert e.value.code == "CONFLICT" and "IsForCoupons" in str(e.value) and not [c for c in api3.calls if c[0] in ("PUT", "POST")]


def test_moved_start_stops_without_taking_new_draft(tmp_path):
    api, admin = world(), FakeAdmin()
    ledger = cs.CouponLedger(tmp_path / "c.sqlite3")
    reg = drafts(tmp_path, {"rule_id": 101, "url": "u", "guid": GUID.upper(), "used_by": ""},
                 {"rule_id": 102, "url": "u2", "guid": str(uuid.uuid4()), "used_by": ""})
    cs.setup_coupon_test(M(api), admin, cs.spec_from_promo(promo()), key="100001_20261023", promo=100001, drafts=reg, ledger=ledger, today=TODAY)
    calls = len(api.calls)
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api), admin, cs.spec_from_promo(promo(start_date=dt.date(2026, 10, 25))), key="100001_20261025",
                             promo=100001, drafts=reg, ledger=ledger, today=TODAY)
    assert e.value.code == "PROMO_MOVED" and reg.free_count() == 1 and not [c for c in api.calls[calls:] if c[0] in ("PUT", "POST")]
    # и на следующем прогоне таймера — тоже (строка нового ключа без правила не снимает остановку)
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api), admin, cs.spec_from_promo(promo(start_date=dt.date(2026, 10, 25))), key="100001_20261025",
                             promo=100001, drafts=reg, ledger=ledger, today=TODAY)
    assert e.value.code == "PROMO_MOVED" and reg.free_count() == 1


def test_early_failure_under_old_key_does_not_block_moved_start(tmp_path):
    """Старый ключ упал до черновика (кончились черновики) — купон не заводился, перенос старта не блокируется."""
    api, admin, ledger = world(), FakeAdmin(), cs.CouponLedger(tmp_path / "c.sqlite3")
    empty = drafts(tmp_path, {"rule_id": 101, "url": "u", "guid": GUID, "used_by": "чужая"})
    with pytest.raises(cs.CouponError):
        cs.setup_coupon_test(M(api), admin, cs.spec_from_promo(promo()), key="100001_20261023", promo=100001, drafts=empty,
                             ledger=ledger, today=TODAY)
    r = cs.setup_coupon_test(M(api), admin, cs.spec_from_promo(promo(start_date=dt.date(2026, 10, 25))), key="100001_20261025",
                             promo=100001, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert r["status"] == "test_active"


def test_button_post_sent_in_previous_run_is_not_repeated(tmp_path):
    api, ledger = world(), cs.CouponLedger(tmp_path / "c.sqlite3")
    seen = []

    class LostAdmin:
        def ensure_button(self, *, guid, title, deeplink, before_create=lambda: None, attempted=False):
            seen.append(attempted)
            if not attempted:
                before_create()
                raise RuntimeError("timeout after POST")
            raise RuntimeError("UNKNOWN_OUTCOME")
    for _ in range(2):
        with pytest.raises(RuntimeError):
            cs.setup_coupon_test(M(api), LostAdmin(), cs.spec_from_promo(promo()), key="k", promo=100001, drafts=drafts(tmp_path),
                                 ledger=ledger, today=TODAY)
    assert seen == [False, True] and "button" in ledger.marks("k")


def test_unclear_list_create_is_not_reposted_on_next_run(tmp_path):
    """POST списка _ex дошёл, но ответ без ID и полнотекстовый индекс его не видит: прогон падает, следующий находит список
    полным перебором и второй POST не шлёт."""
    api, admin = world(), FakeAdmin()
    api.post_hides_id = True
    real_list = api._list
    api._list = lambda entity, params: [v for v in real_list(entity, params)
                                        if not (params.get("fullTextSearch") and str(v["attributes"].get("ExternalId", "")).endswith("_ex"))]
    spec, ledger = cs.spec_from_promo(promo()), cs.CouponLedger(tmp_path / "c.sqlite3")
    r = cs.setup_coupon_test(M(api), admin, spec, key="k", promo=100001, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert r["status"] == "test_active"  # после POST — полный перебор нашёл список
    creates = [c for c in api.calls if c[0] == "POST" and c[1] == "/mlp/ml/entity"]
    assert len(creates) == 1 and ledger.marks("k") == {"_ex", "button"}
    # отметка есть, а списка нет совсем (POST не дошёл или удалён) — остановка, POST не повторяется
    api2 = world()
    ledger.upsert("k", promo=100001, status="hidden_saved")
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api2), admin, spec, key="k", promo=100001, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert e.value.code == "UNKNOWN_OUTCOME" and not [c for c in api2.calls if c[0] == "POST" and c[1] == "/mlp/ml/entity"]


def test_confirm_read_survives_single_get_failure_and_admin_error_is_recorded(tmp_path):
    api, ledger = world(), cs.CouponLedger(tmp_path / "c.sqlite3")
    real_get, state = api.get_json_or_text, {"fail": 0}

    def flaky(path, *, params=None):
        if state["fail"] and path == f"/mlp/ml/entity/{cs.RULE}/101":
            state["fail"] -= 1
            raise RuntimeError("HTTP 502")
        return real_get(path, params=params)
    api.get_json_or_text = flaky
    real_put = api.put_json

    def put(path, payload, *, params=None):
        out = real_put(path, payload, params=params)
        state["fail"] = 1  # первое чтение после записи падает
        return out
    api.put_json = put

    class BrokenAdmin:
        def ensure_button(self, **kw):
            raise RuntimeError("admin timeout")
    with pytest.raises(RuntimeError):
        cs.setup_coupon_test(M(api), BrokenAdmin(), cs.spec_from_promo(promo()), key="k", promo=100001, drafts=drafts(tmp_path),
                             ledger=ledger, today=TODAY)
    row = ledger.get("k")
    assert row["status"] == "links_done" and "admin timeout" in row["error"]  # сохранение пережило сбой чтения
    r = cs.setup_coupon_test(M(api), FakeAdmin(), cs.spec_from_promo(promo()), key="k", promo=100001, drafts=drafts(tmp_path),
                             ledger=ledger, today=TODAY)
    assert r["status"] == "test_active"


def test_existing_lists_reused_and_foreign_cheque_filter_stops(tmp_path):
    api, admin = world(), FakeAdmin()
    spec = cs.spec_from_promo(promo())
    api.add("MarketingList", 77, Name=spec.external_id + " Исключаемый", ExternalId=spec.external_id + "_ex", PartnerId={"id": 31},
            MemberType="2", IsDynamic="false")
    r = cs.setup_coupon_test(M(api), admin, spec, key="k", promo=100001, drafts=drafts(tmp_path), ledger=cs.CouponLedger(tmp_path / "c.sqlite3"), today=TODAY)
    assert r["excluded_list_id"] == 77 and not [c for c in api.calls if c[0] == "POST" and c[1] == "/mlp/ml/entity"]
    api2 = world()
    api2.add("ChequeFilter", 22, Name="Другой", IsActive="true", ExternalId="X")
    api2.links[(cs.RULE, 101, "ChequeFilter", "Exclude")] = {22}
    with pytest.raises(cs.CouponError) as e:
        cs.setup_coupon_test(M(api2), admin, spec, key="k2", promo=100001, drafts=drafts(tmp_path), ledger=cs.CouponLedger(tmp_path / "d.sqlite3"), today=TODAY)
    assert e.value.code == "CONFLICT" and not [c for c in api2.calls if c[0] in ("PUT", "POST")]


def test_write_failures_after_apply_are_settled_by_reading(tmp_path):
    api, admin = world(), FakeAdmin()
    api.put_raises_after_apply, api.post_hides_id = True, True
    spec = cs.spec_from_promo(promo())
    r = cs.setup_coupon_test(M(api), admin, spec, key="k", promo=100001, drafts=drafts(tmp_path), ledger=cs.CouponLedger(tmp_path / "c.sqlite3"), today=TODAY)
    assert r["status"] == "test_active" and len([c for c in api.calls if c[0] == "POST" and c[1] == "/mlp/ml/entity"]) == 1


def test_resume_after_partial_failure_does_not_repeat_done_steps(tmp_path):
    api = world()
    spec = cs.spec_from_promo(promo())
    ledger = cs.CouponLedger(tmp_path / "c.sqlite3")

    class FailingAdmin(FakeAdmin):
        def ensure_button(self, **kw):
            raise cs.CouponError("админка недоступна", code="ADMIN")

    with pytest.raises(cs.CouponError):
        cs.setup_coupon_test(M(api), FailingAdmin(), spec, key="k", promo=100001, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert ledger.get("k")["status"] == "links_done" and api.entities[cs.RULE][101]["attributes"]["IsActive"] is False
    posts_before = len([c for c in api.calls if c[0] == "POST"])
    r = cs.setup_coupon_test(M(api), FakeAdmin(), spec, key="k", promo=100001, drafts=drafts(tmp_path), ledger=ledger, today=TODAY)
    assert r["status"] == "test_active" and len([c for c in api.calls if c[0] == "POST"]) == posts_before  # списки и связи уже есть
    assert api.entities[cs.RULE][101]["attributes"]["PrivateOfficeDisplayType"] == 3


def test_publish_coupon_sets_start_audience_and_activates():
    api = world()
    rule = api.entities[cs.RULE][101]["attributes"]
    rule.update(IsActive="true", DateFrom="2026-10-20T00:00:00+03:00", FilterByContactListsType="4", Name="x", ExternalId="x")
    api.add("MarketingList", 88, Name="100001_Кешбэк на кофе", ExternalId="100001_Кешбэк на кофе", PartnerId={"id": 31}, MemberType="2", IsDynamic="false")
    r = cs.publish_coupon(M(api), rule_id=101, start_date=dt.date(2026, 10, 23), list_name="100001_Кешбэк на кофе")
    assert r["status"] == "live" and r["modified"] and rule["IsActive"] is True and rule["DateFrom"].startswith("2026-10-23T00:00")
    assert api.links[(cs.RULE, 101, "MarketingList", "Include")] == {88}
    puts = [c[2]["attributes"] for c in api.calls if c[0] == "PUT"]
    assert puts[0] == {"IsActive": False} and puts[-1] == {"IsActive": True}
    before = len(api.calls)
    r2 = cs.publish_coupon(M(api), rule_id=101, start_date=dt.date(2026, 10, 23), list_name="100001_Кешбэк на кофе")
    assert not r2["modified"] and not [c for c in api.calls[before:] if c[0] in ("PUT", "POST")]
    r3 = cs.publish_coupon(M(api), rule_id=101, start_date=dt.date(2026, 10, 23), list_name="все")
    assert r3["audience"] == "все" and rule["FilterByContactListsType"] == 1
    with pytest.raises(cs.CouponError) as e:
        cs.publish_coupon(M(api), rule_id=101, start_date=dt.date(2026, 10, 23), list_name="нет такого")
    assert e.value.code == "NOT_FOUND"
