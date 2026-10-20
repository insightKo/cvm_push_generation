"""Тесты robot.py без баз и браузера: подставной источник портала, хранилище во временном каталоге.

Запуск на ферме: python3 -m pytest -q tools/manzana-setup/test_robot.py
"""
import datetime as dt
import hashlib
import json
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import robot as rb  # noqa: E402

NOW = dt.datetime(2026, 10, 20, 12, 0, tzinfo=rb.MSK)
BASE_ENV = {"PG_DB": "portal", "PG_PASSWORD": "x", "LISTS_STORAGE": "", "LISTS_STATE": ""}


def env(tmp_path, **extra):
    e = dict(BASE_ENV, LISTS_STORAGE=str(tmp_path / "lists"), LISTS_STATE=str(tmp_path / "state"),
             CRM_TOOLS_DIR=str(tmp_path / "ext"), BROWSER_PROFILE_DIR=str(tmp_path / "profile"))
    e.update(extra)
    return e


class Portal:
    def __init__(self, promos, pushes=None):
        self.promos, self.pushes = promos, pushes or {}

    def promos_to_setup(self, start_from):
        return [p for p in self.promos if p["start_date"] >= start_from]

    def pushes_of(self, number):
        return self.pushes.get(number, [])


def promo(number, setup="да", start=dt.date(2026, 10, 23), title="Активируй 20% кешбэка на кофе"):
    return {"number": number, "title": title, "setup": setup, "start_date": start, "end_date": start, "segments_raw": "Активные"}


def make_storage(tmp_path):
    root = tmp_path / "lists"
    for d in rb.Storage.DIRS:
        (root / d).mkdir(parents=True, exist_ok=True)
    return rb.Storage(root, tmp_path / "state")


def put_list(storage, number, start="20261023", rows=3, title="Кешбэк на кофе", bad_sha=False, bad_count=False):
    lines = [str(uuid.uuid4()).upper() + "\r\n" for _ in range(rows)]
    body = "".join(lines).encode("utf-8")
    name = f"{number}_{start}.csv"
    (storage.path("inbox") / name).write_bytes(body)
    manifest = {"promo": number, "title": title, "start_date": "2026-10-23", "approved_by": "elena",
                "count": rows + (1 if bad_count else 0),
                "sha256": ("0" * 64) if bad_sha else hashlib.sha256(body).hexdigest(), "file": name,
                "exported_at": "2026-10-20T10:00:00+03:00"}
    (storage.path("inbox") / f"{number}_{start}.manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return storage.path("inbox") / name


# ---------- настройки ----------

def test_config_defaults_and_systems(tmp_path):
    cfg = rb.Config.from_env(env(tmp_path))
    assert cfg.enabled is False and cfg.systems == rb.SYSTEMS and cfg.headless is True
    assert cfg.lists_promos == ((100000, 100019),) and rb.promo_allowed(100005, cfg.lists_promos) and not rb.promo_allowed(101430, cfg.lists_promos)
    assert rb.parse_ranges("all") == () and rb.promo_allowed(1, ()) and rb.parse_ranges("101450; 100000-100019") == ((101450, 101450), (100000, 100019))
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_SYSTEMS="lists, campaign"))
    assert cfg.systems == ("campaign", "lists")
    with pytest.raises(rb.ConfigError):
        rb.Config.from_env(env(tmp_path, MANZANA_SETUP_SYSTEMS="online,sms"))


def test_problems_only_when_enabled(tmp_path):
    assert rb.Config.from_env(env(tmp_path)).problems() == []
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MC_URL="http://mc"))
    p = cfg.problems()
    assert any("LOYALTY_ADMIN_LOGIN" in x for x in p) and any("MC_PASSWORD" in x for x in p) and any("черновиков" in x for x in p)
    assert any("только https" in x for x in p) and not any("manifest.json" in x for x in p)
    assert rb.Config.from_env(env(tmp_path, PG_PASSWORD="")).problems() == ["база портала: PG_DB / PG_PASSWORD пусты — запустить set-secrets.sh"]


# ---------- списки и квитанции ----------

def test_check_list_ok_and_campaign_name(tmp_path):
    st = make_storage(tmp_path)
    path = put_list(st, 101430, rows=5, title="Кешбэк на кофе")
    assert rb.list_file_for(st, "101430_20261023") == path
    m = rb.check_list(path)
    assert m["count"] == 5 and rb.campaign_name(m) == "101430_Кешбэк на кофе"


def test_list_without_manifest_is_not_taken(tmp_path):
    st = make_storage(tmp_path)
    (st.path("inbox") / "101431_20261023.csv").write_bytes(b"")
    assert rb.list_file_for(st, "101431_20261023") is None


@pytest.mark.parametrize("kw,msg", [({"bad_sha": True}, "контрольная сумма"), ({"bad_count": True}, "строк")])
def test_check_list_rejects_mismatch(tmp_path, kw, msg):
    st = make_storage(tmp_path)
    path = put_list(st, 101432, **kw)
    with pytest.raises(ValueError, match=msg):
        rb.check_list(path)


def test_receipt_format_matches_tz(tmp_path):
    st = make_storage(tmp_path)
    path = put_list(st, 101433)
    ok = rb.write_receipt(st, path, True, campaign_name="101433_Кешбэк", loaded=3)
    assert ok.name == "101433_20261023.ok.json" and not list(st.path("receipts").glob("*.part"))
    body = json.loads(ok.read_text(encoding="utf-8"))
    assert set(body) == {"file", "campaign_name", "loaded", "loaded_at"} and body["loaded"] == 3
    err = rb.write_receipt(st, path, False, error="нет кампании")
    assert err.name == "101433_20261023.error.json"
    assert set(json.loads(err.read_text(encoding="utf-8"))) == {"file", "error", "at"}


def test_journal_redacts_guid_and_keeps_record(tmp_path):
    st = make_storage(tmp_path)
    g = str(uuid.uuid4()).upper()
    st.journal("x", error=f"element {g} not found")
    line = st.journal_path.read_text(encoding="utf-8")
    assert g not in line and "<guid>" in line and '"redacted": true' in line
    st.mark_done("101430_20261023", "lists", note=g)
    assert g not in st.done_mark("101430_20261023", "lists").read_text(encoding="utf-8")


def test_base_of_and_cleanup(tmp_path):
    st = make_storage(tmp_path)
    assert rb.base_of(promo(101430)) == "101430_20261023"
    st.mark_done("101400_20260901", "lists"); st.mark_done("101430_20261023", "lists"); (st.state_dir / "x.done").write_text("")
    assert rb.cleanup(st, 14, dt.date(2026, 10, 20)) == 1
    assert st.is_done("101430_20261023", "lists") and not st.is_done("101400_20260901", "lists")


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
    put_list(st, 101434, rows=2)
    portal = Portal([promo(101434), promo(101435, start=dt.date(2026, 10, 1))],
                    pushes={101434: [{"id": 1, "title": "t", "body": "b"}]})
    assert rb.run(cfg, portal, st, now=NOW, dry_run=True) == 0
    events = [json.loads(l) for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    assert events[0]["event"] == "run" and events[0]["promos"] == 1 and events[0]["tasks"] == 2
    assert [e["event"] for e in events[1:]] == ["lists_scan", "dry_run", "dry_run"]
    assert events[1]["files"] == [] and events[1]["not_ours"] == ["101434_20261023.csv"]  # 101434 не из LISTS_PROMOS
    cfg_all = rb.Config.from_env(env(tmp_path, LISTS_PROMOS="all"))
    assert rb.run(cfg_all, portal, st, now=NOW, dry_run=True) == 0
    events = [json.loads(l) for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    lists_plan = [e for e in events[-5:] if e["event"] == "dry_run" and e.get("system") == "lists"]
    assert len(lists_plan) == 1 and lists_plan[0]["campaign"] == "101434_Кешбэк на кофе"
    assert not list(st.path("receipts").iterdir()) and not list(st.state_dir.iterdir())


def test_disabled_run_never_opens_browser(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path))
    opened = []
    assert rb.run(cfg, Portal([promo(101436)]), st, now=NOW, browser_factory=lambda c: opened.append(c)) == 0
    assert opened == []


class NoBrowser:
    def __init__(self, c): pass
    def __enter__(self): return self
    def __exit__(self, *a): pass


def test_enabled_lists_cycle_imports_files_and_writes_receipts(tmp_path, monkeypatch):
    import campaign_lists as cl
    from test_campaign_lists import Api, fake_uploader
    st = make_storage(tmp_path)
    (tmp_path / "ext").mkdir(); (tmp_path / "ext" / "manifest.json").write_text("{}")
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="lists", LISTS_PROMOS="all"))
    put_list(st, 101437, rows=3); put_list(st, 101438, rows=3, bad_sha=True)
    monkeypatch.setattr(cl, "upload_to_storage", fake_uploader)
    api = Api(loaded=3)
    code = rb.run(cfg, Portal([]), st, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: api)
    assert code == 1
    events = [json.loads(l) for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    kinds = [e["event"] for e in events]
    assert kinds == ["run", "lists_scan", "list_loaded", "list_failed"]
    assert events[2]["campaign"] == "101437_Кешбэк на кофе" and events[2]["loaded"] == 3 and events[2]["receipt_written"]
    assert events[3]["code"] == "SOURCE_INTEGRITY"
    assert (st.path("receipts") / "101437_20261023.ok.json").is_file() and (st.path("receipts") / "101438_20261023.error.json").is_file()
    assert len([c for c in api.calls if c[0] == "PUT"]) == 1
    # второй прогон: оба файла уже с квитанциями — к Campaign не ходим
    before = len(api.calls)
    assert rb.run(cfg, Portal([]), st, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: api) == 0
    assert len(api.calls) == before


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
    code = rb.run(cfg, Portal([p]), st, now=NOW, browser_factory=NoBrowser, online_factory=lambda c, b: online)
    assert code == 0 and seen[0][:2] == online and seen[0][2:] == ("20% кешбэка на кофе", "101437_20261023", 101437, cfg.coupon_drafts, NOW.date())
    events = [json.loads(l) for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    assert events[-1]["event"] == "task_done" and events[-1]["rule_id"] == 101 and events[-1]["guid"] == "<guid>"
    assert st.is_done("101437_20261023", "online")
    # повторный прогон: задача сделана, клиенты Loyalty не создаются
    created = []
    assert rb.run(cfg, Portal([p]), st, now=NOW, browser_factory=NoBrowser, online_factory=lambda c, b: created.append(1)) == 0
    assert created == [] and len(seen) == 1


def test_online_task_failure_keeps_queue_and_hides_guid(tmp_path, monkeypatch):
    import coupon_setup as cs
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="online"))
    g = str(uuid.uuid4())
    monkeypatch.setattr(cs, "setup_coupon_test", lambda *a, **k: (_ for _ in ()).throw(cs.CouponError(f"правило {g} занято", code="CONFLICT")))
    p = dict(promo(101437), coupon_name="20% кешбэка на кофе", slip_text="Текст", button="")
    assert rb.run(cfg, Portal([p]), st, now=NOW, browser_factory=NoBrowser, online_factory=lambda c, b: ("mlp", "admin")) == 1
    text = st.journal_path.read_text(encoding="utf-8")
    assert '"task_failed"' in text and g not in text and not st.is_done("101437_20261023", "online")
    # пустое название купона — задача падает до обращения к Manzana
    bad = dict(promo(101438), coupon_name="", slip_text="Текст")
    assert rb.run(cfg, Portal([bad]), st, now=NOW, browser_factory=NoBrowser, online_factory=lambda c, b: ("mlp", "admin")) == 1
    assert "SPEC_INVALID" in st.journal_path.read_text(encoding="utf-8").splitlines()[-1] or "Название" in st.journal_path.read_text(encoding="utf-8").splitlines()[-1]


def test_publish_coupon_command_requires_test_display(tmp_path, monkeypatch):
    import coupon_setup as cs
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="online"))
    portal = Portal([promo(101439)])
    assert rb.publish_main(cfg, portal, st, "101439_20261023", "все", browser_factory=NoBrowser, online_factory=lambda c, b: ("mlp", "admin")) == 1
    assert json.loads(st.journal_path.read_text(encoding="utf-8").splitlines()[-1])["event"] == "coupon_publish_failed"
    cs.CouponLedger(cfg.state / "coupons.sqlite3").upsert("101439_20261023", promo=101439, status="test_active", rule_id=101)
    calls = []
    monkeypatch.setattr(cs, "publish_coupon", lambda mlp, *, rule_id, start_date, list_name: calls.append((mlp, rule_id, start_date, list_name)) or {"status": "live", "rule_id": rule_id})
    assert rb.publish_main(cfg, portal, st, "101439_20261023", "101439_Кешбэк", browser_factory=NoBrowser, online_factory=lambda c, b: ("mlp", "admin")) == 0
    assert calls == [("mlp", 101, dt.date(2026, 10, 23), "101439_Кешбэк")]
    assert json.loads(st.journal_path.read_text(encoding="utf-8").splitlines()[-1])["event"] == "coupon_published"
    assert rb.publish_main(rb.Config.from_env(env(tmp_path)), portal, st, "101439_20261023", "все", browser_factory=NoBrowser) == 1


def test_lists_waits_for_file_and_campaign(tmp_path):
    st = make_storage(tmp_path)
    (tmp_path / "ext").mkdir(); (tmp_path / "ext" / "manifest.json").write_text("{}")
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="campaign", PUSH_TEST_RECIPIENTS="+79990000000"))
    rb.run(cfg, Portal([promo(101438)]), st, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: object())
    events = [json.loads(l)["event"] for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    assert events[1:] == ["waiting_pushes"]
    rb.run(cfg, Portal([promo(101438)], pushes={101438: [{"id": 1}]}), st, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: object())
    events = [json.loads(l)["event"] for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    assert events[-1] == "waiting_list"


def test_campaign_waits_for_list_and_name_comes_from_manifest(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_SYSTEMS="campaign"))
    portal = Portal([promo(101439, title="Очень длинное название акции")], pushes={101439: [{"id": 1}]})
    rb.run(cfg, portal, st, now=NOW, dry_run=True)
    events = [json.loads(l) for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    assert events[-1]["event"] == "waiting_list"
    put_list(st, 101439, title="Кешбэк")
    rb.run(cfg, portal, st, now=NOW, dry_run=True)
    events = [json.loads(l) for l in st.journal_path.read_text(encoding="utf-8").splitlines()]
    assert events[-1]["event"] == "dry_run" and events[-1]["list"] == "101439_Кешбэк"


def test_moved_promo_sees_old_file_as_mismatch(tmp_path):
    st = make_storage(tmp_path)
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_SYSTEMS="campaign"))
    put_list(st, 101440, start="20261016")
    rb.run(cfg, Portal([promo(101440, start=dt.date(2026, 10, 30))], pushes={101440: [{"id": 1}]}), st, now=NOW, dry_run=True)
    last = json.loads(st.journal_path.read_text(encoding="utf-8").splitlines()[-1])
    assert last["event"] == "list_date_mismatch" and last["found"] == ["101440_20261016.csv"]


def test_failed_list_with_guid_is_redacted_and_queue_continues(tmp_path, monkeypatch):
    import campaign_lists as cl
    st = make_storage(tmp_path)
    (tmp_path / "ext").mkdir(); (tmp_path / "ext" / "manifest.json").write_text("{}")
    cfg = rb.Config.from_env(env(tmp_path, MANZANA_SETUP_ENABLED="1", MANZANA_SETUP_SYSTEMS="lists", LISTS_PROMOS="all"))
    g = str(uuid.uuid4()).upper()
    monkeypatch.setattr(cl, "import_list", lambda *a, **k: (_ for _ in ()).throw(RuntimeError(f"row {g} rejected")))
    put_list(st, 101441); put_list(st, 101442)
    assert rb.run(cfg, Portal([]), st, now=NOW, browser_factory=NoBrowser, api_factory=lambda c, b: object()) == 1
    text = st.journal_path.read_text(encoding="utf-8")
    assert g not in text and text.count('"list_failed"') == 2 and "<guid>" in text
