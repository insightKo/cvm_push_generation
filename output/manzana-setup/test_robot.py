"""Тесты robot.py без баз и браузера: подставные база портала и база списков, состояние во временном каталоге.

Запуск на ферме: python3 -m pytest -q tools/manzana-setup/test_robot.py
"""
import datetime as dt
import json
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import robot as rb  # noqa: E402

NOW = dt.datetime(2026, 10, 20, 12, 0, tzinfo=rb.MSK)
START = dt.date(2026, 10, 23)
BASE_ENV = {"PG_DB": "portal", "PG_PASSWORD": "x", "LISTS_PG_PASSWORD": "y", "LISTS_STATE": ""}


def env(tmp_path, **extra):
    e = dict(BASE_ENV, LISTS_STATE=str(tmp_path / "state"), BROWSER_PROFILE_DIR=str(tmp_path / "profile"))
    e.update(extra)
    return e


class Portal:
    def __init__(self, promos, pushes=None):
        self.promos, self.pushes = promos, pushes or {}

    def promos_to_setup(self, start_from):
        return [p for p in self.promos if p["start_date"] >= start_from]

    def pushes_of(self, number):
        return self.pushes.get(number, [])


class Lists:
    """База списков: lists.promo (номер, старт, имя кампании, guids_send), участники без КГ, итог lists.manzana."""

    def __init__(self, promos=(), members=None):
        self.promos = list(promos)
        self.members = members or {}
        self.records = {}

    def lists_to_load(self, start_from):
        return [p for p in self.promos if p["campaign_name"] and p["start_date"] >= start_from
                and (self.records.get((p["id_promo"], p["start_date"], "list", "")) or {}).get("status") != "done"]

    def list_of(self, number):
        return next((p for p in self.promos if p["id_promo"] == number), None)

    def guids(self, number):
        # как sql/list_guids.sql: различные, без пустых (клиент без CRM_GUID), в верхнем регистре
        yield from sorted({g.upper() for g in self.members.get(number, []) if g is not None})

    def done(self, number, start_date, kind, item=""):
        r = self.records.get((number, start_date, kind, item))
        return r if r and r["status"] == "done" else None

    def record(self, number, start_date, kind, item="", **fields):
        old = self.records.get((number, start_date, kind, item))
        if old and old["status"] == "done" and fields["status"] == "error":
            return
        self.records[(number, start_date, kind, item)] = {"name": "", "external_id": "", "guid": None, "rows": None,
                                                          "detail": "", **fields}


def lst(number, start=START, name=None, count=3):
    return {"id_promo": number, "start_date": start, "campaign_name": f"{number}_Кешбэк" if name is None else name,
            "guids_send": count}


def members(n=3):
    return [str(uuid.uuid4()) for _ in range(n)]


def promo(number, setup="да", start=START, title="Активируй 20% кешбэка на кофе"):
    return {"number": number, "title": title, "setup": setup, "start_date": start, "end_date": start, "segments_raw": "Активные"}


def make_storage(tmp_path):
    return rb.Storage(tmp_path / "state")


def journal(st):
    return [json.loads(line) for line in st.journal_path.read_text(encoding="utf-8").splitlines()]


# ---------- настройки ----------

def test_config_defaults_and_systems(tmp_path):
    cfg = rb.Config.from_env(env(tmp_path))
    assert cfg.enabled is False and cfg.systems == rb.SYSTEMS and cfg.headless is True
    # LISTS_PROMOS пуст — робот сам не грузит ни одного списка (решение «2б»: списки грузит ДИКСИ)
    assert cfg.lists_promos == () and not rb.promo_allowed(500001, cfg.lists_promos)
    assert rb.parse_ranges("all") is None and rb.promo_allowed(1, None)
    assert rb.parse_ranges("500120; 500000-500009") == ((500120, 500120), (500000, 500009))
    assert cfg.lists_pg == {"host": "cvm-lists-db", "port": "5432", "dbname": "cvm_lists", "user": "lists_manzana", "password": "y"}
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_SYSTEMS="lists, campaign"))
    assert cfg.systems == ("campaign", "lists")
    with pytest.raises(rb.ConfigError):
        rb.Config.from_env(env(tmp_path, MANZANA_SETUP_SYSTEMS="online,sms"))


def test_problems_only_when_enabled(tmp_path):
    assert rb.Config.from_env(env(tmp_path)).problems() == []
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MC_URL="http://mc"))
    p = cfg.problems()
    assert any("LOYALTY_ADMIN_LOGIN" in x for x in p) and any("MC_PASSWORD" in x for x in p) and any("черновиков" in x for x in p)
    assert any("только https" in x for x in p)
    assert rb.Config.from_env(env(tmp_path, PG_PASSWORD="")).problems() == ["база портала: PG_DB / PG_PASSWORD пусты — запустить set-secrets.sh"]
    assert any("LISTS_PG_PASSWORD" in x for x in rb.Config.from_env(env(tmp_path, LISTS_PG_PASSWORD="")).problems())


# ---------- журнал и отметки ----------

def test_journal_redacts_guid_and_keeps_record(tmp_path):
    st = make_storage(tmp_path)
    g = str(uuid.uuid4()).upper()
    st.journal("x", error=f"element {g} not found")
    line = st.journal_path.read_text(encoding="utf-8")
    assert g not in line and "<guid>" in line and '"redacted": true' in line
    st.mark_done("101430_20261023", "campaign", note=g)
    assert g not in st.done_mark("101430_20261023", "campaign").read_text(encoding="utf-8")
    assert rb.safe_text(f"row {g} bad") == "row <guid> bad"


def test_base_of_and_cleanup(tmp_path):
    st = make_storage(tmp_path)
    assert rb.base_of(promo(101430)) == "101430_20261023"
    st.mark_done("101400_20260901", "online"); st.mark_done("101430_20261023", "online"); (st.state_dir / "x.done").write_text("")
    assert rb.cleanup(st, 14, dt.date(2026, 10, 20)) == 1
    assert st.is_done("101430_20261023", "online") and not st.is_done("101400_20260901", "online")


# ---------- очередь и прогон ----------

def test_pending_skips_online_without_setup_and_done(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path))
    st.mark_done("2_20261023", "campaign")
    q = rb.pending(cfg, st, [promo(1, setup="нет"), promo(2, setup="Да ")])
    assert [(p["number"], s) for p, s in q] == [(1, "campaign"), (2, "online")]


def test_dry_run_writes_plan_and_touches_nothing(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path))
    lists = Lists([lst(500001), lst(500002, start=dt.date(2026, 10, 1)), lst(500003, name=None) | {"campaign_name": None}])
    portal = Portal([promo(500001), promo(500004, start=dt.date(2026, 10, 1))], pushes={500001: [{"id": 1, "title": "t", "body": "b"}]})
    assert rb.run(cfg, portal, st, lists_db=lists, now=NOW, dry_run=True) == 0
    events = journal(st)
    assert events[0]["event"] == "run" and events[0]["promos"] == 1 and events[0]["tasks"] == 2 and events[0]["lists_db"]
    assert [e["event"] for e in events[1:]] == ["lists_scan", "dry_run", "dry_run"]
    # 500001 не из LISTS_PROMOS — его грузит ДИКСИ; 500002 уже начался; 500003 (SLIP) в Campaign не нужен
    assert events[1]["lists"] == [] and events[1]["not_ours"] == ["500001_20261023"]
    assert events[3]["system"] == "campaign" and events[3]["list"] == "500001_Кешбэк"
    cfg_all = rb.Config.from_env(env(tmp_path, LISTS_PROMOS="all"))
    assert rb.run(cfg_all, portal, st, lists_db=lists, now=NOW, dry_run=True) == 0
    plan = [e for e in journal(st)[-4:] if e["event"] == "dry_run" and e.get("system") == "lists"]
    assert len(plan) == 1 and plan[0]["campaign"] == "500001_Кешбэк" and plan[0]["count"] == 3
    assert not list(st.state_dir.iterdir()) and lists.records == {}


def test_disabled_run_never_opens_browser(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path))
    opened = []
    assert rb.run(cfg, Portal([promo(101436)]), st, lists_db=Lists(), now=NOW, browser_factory=lambda c: opened.append(c)) == 0
    assert opened == []


class NoBrowser:
    def __init__(self, c): pass
    def __enter__(self): return self
    def __exit__(self, *a): pass


def test_enabled_lists_cycle_loads_from_lists_db_and_records_result(tmp_path, monkeypatch):
    import campaign_lists as cl
    from test_campaign_lists import Api, fake_uploader
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="lists", LISTS_PROMOS="500001-500002"))
    good = members(3) + [None]  # клиент без CRM_GUID в пачке — не отдаётся
    lists = Lists([lst(500001), lst(500002, count=5), lst(500003)], members={500001: good, 500002: members(3), 500003: members(3)})
    monkeypatch.setattr(cl, "upload_to_storage", fake_uploader)
    api = Api(loaded=3)
    code = rb.run(cfg, Portal([]), st, lists_db=lists, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: api)
    assert code == 1
    events = journal(st)
    assert [e["event"] for e in events] == ["run", "lists_scan", "list_loaded", "list_failed"]
    assert events[1]["lists"] == ["500001_20261023", "500002_20261023"] and events[1]["not_ours"] == ["500003_20261023"]
    assert events[2]["campaign"] == "500001_Кешбэк" and events[2]["loaded"] == 3 and events[3]["code"] == "SOURCE_MISMATCH"
    assert sorted(api.uploaded) == sorted(g.upper() for g in good if g)
    ok, bad = lists.records[(500001, START, "list", "")], lists.records[(500002, START, "list", "")]
    assert ok["status"] == "done" and ok["rows"] == 3 and ok["name"] == "500001_Кешбэк" and ok["external_id"] == "0123456789abcdef01234567"
    assert bad["status"] == "error" and "SOURCE_MISMATCH" in bad["detail"]
    assert len([c for c in api.calls if c[0] == "PUT"]) == 1
    # второй прогон: 500001 уже done — в Campaign не идём; 500002 снова не сходится
    before = len(api.calls)
    assert rb.run(cfg, Portal([]), st, lists_db=lists, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: api) == 1
    assert len(api.calls) == before and lists.records[(500001, START, "list", "")]["status"] == "done"


def test_lists_need_lists_db(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="lists", LISTS_PROMOS="all"))
    assert rb.run(cfg, Portal([]), st, lists_db=None, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: object()) == 1
    assert journal(st)[-1]["event"] == "lists_db_missing"
    assert rb.run(cfg, Portal([]), st, lists_db=None, now=NOW, dry_run=True) == 0


def test_online_task_runs_coupon_test_display_and_marks_done(tmp_path, monkeypatch):
    import coupon_setup as cs
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="online"))
    seen = []

    def fake_setup(mlp, admin, spec, *, key, promo, drafts, ledger, today):
        seen.append((mlp, admin, spec.title, key, promo, drafts.path, today))
        return {"rule_id": 101, "guid": str(uuid.uuid4()), "status": "test_active", "button": "created"}

    monkeypatch.setattr(cs, "setup_coupon_test", fake_setup)
    p = dict(promo(101437), coupon_name="20% кешбэка на кофе", slip_text="Купи кофе — вернём 20% монетами.", button="")
    online = ("mlp", "admin")
    lists = Lists()
    code = rb.run(cfg, Portal([p]), st, lists_db=lists, now=NOW, browser_factory=NoBrowser, online_factory=lambda c, b: online)
    assert code == 0 and seen[0][:2] == online and seen[0][2:] == ("20% кешбэка на кофе", "101437_20261023", 101437, cfg.coupon_drafts, NOW.date())
    events = [json.loads(l) for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    assert events[-1]["event"] == "task_done" and events[-1]["rule_id"] == 101 and events[-1]["guid"] == "<guid>"
    assert st.is_done("101437_20261023", "online")
    rec = lists.records[(101437, START, "coupon", "")]
    assert rec["status"] == "done" and rec["external_id"] == "101" and rec["guid"] and rec["name"] == "20% кешбэка на кофе"
    # повторный прогон: задача сделана, клиенты Loyalty не создаются
    created = []
    assert rb.run(cfg, Portal([p]), st, lists_db=lists, now=NOW, browser_factory=NoBrowser, online_factory=lambda c, b: created.append(1)) == 0
    assert created == [] and len(seen) == 1


def test_online_task_failure_keeps_queue_and_hides_guid(tmp_path, monkeypatch):
    import coupon_setup as cs
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="online"))
    g = str(uuid.uuid4())
    monkeypatch.setattr(cs, "setup_coupon_test", lambda *a, **k: (_ for _ in ()).throw(cs.CouponError(f"правило {g} занято", code="CONFLICT")))
    p = dict(promo(101437), coupon_name="20% кешбэка на кофе", slip_text="Текст", button="")
    lists = Lists()
    assert rb.run(cfg, Portal([p]), st, lists_db=lists, now=NOW, browser_factory=NoBrowser, online_factory=lambda c, b, **k: ("mlp", "admin")) == 1
    text = st.journal_path.read_text(encoding="utf-8")
    assert '"task_failed"' in text and g not in text and not st.is_done("101437_20261023", "online")
    rec = lists.records[(101437, START, "coupon", "")]
    assert rec["status"] == "error" and rec["detail"].startswith("CONFLICT") and g not in rec["detail"] and "<guid>" in rec["detail"]
    # пустое название купона — задача падает до обращения к Manzana
    bad = dict(promo(101438), coupon_name="", slip_text="Текст")
    assert rb.run(cfg, Portal([bad]), st, now=NOW, browser_factory=NoBrowser, online_factory=lambda c, b, **k: ("mlp", "admin")) == 1
    failed = [e for e in journal(st) if e["event"] == "task_failed"][-1]
    assert failed["promo"] == 101438 and ("SPEC_INVALID" in failed["error"] or "Название" in failed["error"])


def test_publish_coupon_command_requires_test_display(tmp_path, monkeypatch):
    import coupon_setup as cs
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="online"))
    portal = Portal([promo(101439)])
    assert rb.publish_main(cfg, portal, st, "101439_20261023", "все", browser_factory=NoBrowser, online_factory=lambda c, b, **k: ("mlp", "admin")) == 1
    assert [e["event"] for e in journal(st)][-2:] == ["coupon_publish_failed", "record_skipped"]
    cs.CouponLedger(cfg.state / "coupons.sqlite3").upsert("101439_20261023", promo=101439, status="test_active", rule_id=101)
    calls = []
    monkeypatch.setattr(cs, "publish_coupon", lambda mlp, *, rule_id, start_date, list_name: calls.append((mlp, rule_id, start_date, list_name)) or {"status": "live", "rule_id": rule_id})
    lists = Lists()
    assert rb.publish_main(cfg, portal, st, "101439_20261023", "101439_Кешбэк", lists_db=lists, browser_factory=NoBrowser, online_factory=lambda c, b, **k: ("mlp", "admin")) == 0
    assert lists.records[(101439, START, "coupon", "бой")]["status"] == "done"
    assert calls == [("mlp", 101, dt.date(2026, 10, 23), "101439_Кешбэк")]
    assert json.loads(st.journal_path.read_text(encoding="utf-8").splitlines()[-1])["event"] == "coupon_published"
    assert cs.CouponLedger(cfg.state / "coupons.sqlite3").get("101439_20261023")["status"] == "live"
    assert rb.publish_main(rb.Config.from_env(env(tmp_path)), portal, st, "101439_20261023", "все", browser_factory=NoBrowser) == 1


def test_publish_needs_no_admin_and_interrupted_publish_is_recorded(tmp_path, monkeypatch):
    import coupon_setup as cs
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="online"))
    ledger = cs.CouponLedger(cfg.state / "coupons.sqlite3")
    ledger.upsert("101439_20261023", promo=101439, status="test_active", rule_id=101)
    asked = []
    monkeypatch.setattr(cs, "publish_coupon", lambda *a, **k: (_ for _ in ()).throw(
        cs.CouponError("купон ВЫКЛЮЧЕН; auth: TOKEN1\nCall log:\n - cookie: TOKEN2", code="PUBLISH_INTERRUPTED")))
    code = rb.publish_main(cfg, Portal([promo(101439)]), st, "101439_20261023", "все", browser_factory=NoBrowser,
                           online_factory=lambda c, b, **k: asked.append(k) or ("mlp", None))
    assert code == 1 and asked == [{"with_admin": False}]
    row = ledger.get("101439_20261023")
    assert row["status"] == "publish_failed" and "TOKEN" not in row["error"] and "TOKEN" not in st.journal_path.read_text(encoding="utf-8")
    # таймер не трогает купон, перевод которого в бой начат
    assert cs.setup_coupon_test(None, None, None, key="101439_20261023", promo=101439, drafts=None, ledger=ledger)["already_done"]


def test_failed_login_is_not_repeated_for_every_online_task(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="online"))
    tries = []

    def login(c, b, **k):
        tries.append(1)
        raise RuntimeError("AUTH_FAILED")
    ps = [dict(promo(n), coupon_name="Купон", slip_text="Текст", button="") for n in (101441, 101442, 101443)]
    assert rb.run(cfg, Portal(ps), st, now=NOW, browser_factory=NoBrowser, online_factory=login) == 1
    assert tries == [1] and st.journal_path.read_text(encoding="utf-8").count('"task_failed"') == 3


def test_campaign_waits_for_pushes_and_list_in_lists_db(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_SYSTEMS="campaign", LISTS_PROMOS="500010"))
    pushes = {n: [{"id": 1}] for n in (500010, 500011, 500012, 500013, 500014)}

    def last_event(promos, lists):
        rb.run(cfg, Portal(promos, pushes=pushes), st, lists_db=lists, now=NOW, dry_run=True)
        return journal(st)[-1]

    assert last_event([promo(500099)], Lists())["event"] == "waiting_pushes"
    assert last_event([promo(500011)], Lists())["event"] == "waiting_list"           # списка акции нет в базе списков
    e = last_event([promo(500012, start=dt.date(2026, 10, 30))], Lists([lst(500012)]))
    assert e["event"] == "list_date_mismatch" and e["found"] == "2026-10-23"           # акцию перенесли, список на старую дату
    assert last_event([promo(500013)], Lists([lst(500013) | {"campaign_name": None}]))["event"] == "campaign_not_needed"
    # список робота (LISTS_PROMOS) ещё не загружен — кампании ждут; загружен — аудитория под его именем
    lists = Lists([lst(500010)])
    assert last_event([promo(500010)], lists)["event"] == "waiting_list_import"
    lists.record(500010, START, "list", status="done", name="500010_Кешбэк_т", rows=3)
    e = last_event([promo(500010)], lists)
    assert e["event"] == "dry_run" and e["list"] == "500010_Кешбэк_т"
    # список ДИКСИ (не из LISTS_PROMOS) — кампании ставятся сразу на имя кампании акции
    e = last_event([promo(500014)], Lists([lst(500014)]))
    assert e["event"] == "dry_run" and e["list"] == "500014_Кешбэк"
    assert last_event([promo(500011)], None)["why"] == "базы списков нет"


def test_enabled_campaign_records_each_message(tmp_path, monkeypatch):
    import campaign_push as cp
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="campaign", PUSH_TEST_RECIPIENTS="+79990000000"))
    seen = []

    def fake_push(api, *, key, include_lists, expected_count, **kw):
        seen.append((key, include_lists, expected_count))
        if key.startswith("500020:2:"):
            raise cp.PushSetupError("шаблон не сохранён", code="TEMPLATE_FAILED")
        return {"campaign_sid": "c1", "template_sid": "t1", "audience": 3, "status": "saved"}

    monkeypatch.setattr(cp, "setup_push", fake_push)
    lists = Lists([lst(500020)])
    pushes = {500020: [{"id": 1, "msg_no": 1, "send_date": START, "title": "t", "body": "b"},
                       {"id": 2, "msg_no": 2, "send_date": START, "title": "t", "body": "b"}]}
    assert rb.run(cfg, Portal([promo(500020)], pushes=pushes), st, lists_db=lists, now=NOW, browser_factory=NoBrowser,
                  api_factory=lambda c, b: object()) == 1
    assert seen[0] == ("500020:1:2026-10-23", ["500020_Кешбэк"], 3)
    ok, bad = lists.records[(500020, START, "push", "1:2026-10-23")], lists.records[(500020, START, "push", "2:2026-10-23")]
    assert ok["status"] == "done" and ok["external_id"] == "c1" and bad["status"] == "error" and "TEMPLATE_FAILED" in bad["detail"]
    assert not st.is_done("500020_20261023", "campaign")


def test_failed_list_with_guid_is_redacted_and_queue_continues(tmp_path, monkeypatch):
    import campaign_lists as cl
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="lists", LISTS_PROMOS="all"))
    g = str(uuid.uuid4()).upper()
    monkeypatch.setattr(cl, "import_guids", lambda *a, **k: (_ for _ in ()).throw(RuntimeError(f"row {g} rejected")))
    lists = Lists([lst(500031), lst(500032)], members={500031: members(), 500032: members()})
    assert rb.run(cfg, Portal([]), st, lists_db=lists, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: object()) == 1
    text = st.journal_path.read_text(encoding="utf-8")
    assert g not in text and text.count('"list_failed"') == 2 and "<guid>" in text
    assert all(g not in r["detail"] and r["status"] == "error" for r in lists.records.values())


def test_record_failure_is_logged_and_counted(tmp_path, monkeypatch):
    import campaign_lists as cl
    from test_campaign_lists import Api, fake_uploader
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="lists", LISTS_PROMOS="all"))
    lists = Lists([lst(500041)], members={500041: members()})
    lists.record = lambda *a, **k: (_ for _ in ()).throw(ConnectionError("lists db down"))
    monkeypatch.setattr(cl, "upload_to_storage", fake_uploader)
    assert rb.run(cfg, Portal([]), st, lists_db=lists, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: Api()) == 1
    assert [e["event"] for e in journal(st)][-2:] == ["list_loaded", "record_failed"]


def test_setup_sql_selects_every_column_the_coupon_reads():
    """Поля акции, которые читает spec_from_promo, должны выбираться запросом очереди — иначе (как было с картинкой)
    значение молча не доходит до Manzana."""
    import ast
    import re
    import coupon_setup as cs
    tree = ast.parse(Path(cs.__file__).read_text(encoding="utf-8"))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "spec_from_promo")
    read = {c.args[0].value for c in ast.walk(fn) if isinstance(c, ast.Call) and getattr(c.func, "attr", "") == "get"
            and c.args and isinstance(c.args[0], ast.Constant)}
    sql = rb.read_sql("promos_to_setup.sql")
    select = re.sub(r"--[^\n]*", "", sql).split("from cvm.promo")[0]
    selected = set(re.findall(r"\b([a-z_]+)\b", select))
    assert read and read <= selected, read - selected


def test_queue_sql_reads_only_columns_granted_to_robot_role():
    """Роль cvm_manzana_ro видит колонки по списку set-secrets.sh: колонка в запросе без права роняет весь прогон."""
    import re
    script = (Path(rb.__file__).parent / "set-secrets.sh").read_text(encoding="utf-8")
    for table, sql_name in (("promo", "promos_to_setup.sql"), ("push", "pushes_of_promo.sql")):
        block = script.split(f"table_name = '{table}' and column_name in", 1)[1].split(");", 1)[0]
        granted = set(re.findall(r"'([a-z_]+)'", block))
        sql = re.sub(r"--[^\n]*", "", rb.read_sql(sql_name)).lower()
        words = set(re.findall(r"\b([a-z_]+)\b", re.sub(r"'[^']*'|%\([a-z_]+\)s", " ", sql)))
        keywords = {"select", "from", "where", "and", "or", "order", "by", "as", "is", "null", "not", "coalesce", "nullif",
                    "upper", "cvm", "promo", "push", "approved_by"}  # approved_by — псевдоним
        assert words - keywords <= granted, (table, sorted(words - keywords - granted))


def test_lists_sql_reads_only_columns_granted_to_lists_role():
    """Роль lists_manzana видит в lists.promo только колонки из 002_manzana.sql: лишняя колонка в запросе роняет прогон."""
    import re
    migration = (Path(rb.__file__).parent / "lists-db-patch" / "002_manzana.sql").read_text(encoding="utf-8")
    granted = set(re.search(r"grant select \(([^)]*)\) on lists\.promo", migration).group(1).replace(" ", "").split(","))
    for name in ("lists_to_load.sql", "list_of_promo.sql"):
        sql = re.sub(r"--[^\n]*", "", rb.read_sql(name)).lower()
        promo_cols = set(re.findall(r"\bp\.([a-z_]+)", sql)) if " p\n" in sql or "lists.promo p" in sql else \
            set(re.findall(r"\b([a-z_]+)\b", sql.split("from lists.promo")[0].replace("select", "")))
        assert promo_cols and promo_cols <= granted, (name, sorted(promo_cols - granted))


def test_list_stream_is_closed_before_error_is_recorded(tmp_path):
    """Поток GUID читается в транзакции серверного курсора: при ошибке сборки он закрывается до записи итога."""
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="lists", LISTS_PROMOS="all"))
    order = []

    class Stream:
        def __init__(self): self.items = iter(["not-a-guid"])
        def __iter__(self): return self
        def __next__(self): return next(self.items)
        def close(self): order.append("closed")

    lists = Lists([lst(500051)])
    lists.guids = lambda n: Stream()
    real_record = lists.record
    lists.record = lambda *a, **k: order.append("record") or real_record(*a, **k)
    assert rb.run(cfg, Portal([]), st, lists_db=lists, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: object()) == 1
    assert order == ["closed", "record"] and lists.records[(500051, START, "list", "")]["detail"].startswith("SOURCE_INVALID")


def test_publish_returns_error_when_result_not_recorded(tmp_path, monkeypatch):
    import coupon_setup as cs
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="online"))
    cs.CouponLedger(cfg.state / "coupons.sqlite3").upsert("101439_20261023", promo=101439, status="test_active", rule_id=101)
    monkeypatch.setattr(cs, "publish_coupon", lambda *a, **k: {"status": "live"})
    assert rb.publish_main(cfg, Portal([promo(101439)]), st, "101439_20261023", "все", lists_db=None,
                           browser_factory=NoBrowser, online_factory=lambda c, b, **k: ("mlp", None)) == 1
