"""Тесты campaign_push.py без сети: подставной API Campaign с состоянием (шаблоны, копии заготовок, деревья сегментов,
расчёт, сохранение, активация)."""
import copy
import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import campaign_push as cp  # noqa: E402

BP1, BP2 = cp.BLUEPRINTS_DEFAULT[1], cp.BLUEPRINTS_DEFAULT[2]


def blueprint(sid, blocks):
    steps = [{"StepID": 1, "StepType": 1, "StepProps": {}}, {"StepID": 2, "StepType": 0, "Name": "", "StepProps": {"SegmentID": 0}}]
    for i in range(blocks):
        steps.append({"StepID": 7 + i, "StepType": 12, "StepProps": {}, "Text": ""})
    return {"SID": sid, "Name": "Заготовка", "Status": 1, "IsTemplate": True, "Deleted": False,
            "StartInfo": {"Text": "", "ScheduleRule": {"EffectiveFrom": 0, "EffectiveTo": 0, "StartTime": 0, "UseLocalTime": False}},
            "ProfileUIInfo": {"SegmentsTreeSID": ""}, "PersonalRule": [{"Activated": False, "BranchID": 0, "TaskID": "", "Steps": steps}]}


def tree(sid, camp):
    return {"SID": sid, "CampSID": camp, "Segments": [
        {"ID": 0, "Parent": -1, "Total": "10 000 000", "TotalInt": 10_000_000, "TotalPercent": "100%"},
        {"ID": 1, "Parent": 0, "SelectedToUse": False, "Name": "Сегмент №1",
         "Expression": {"Connective": 0, "Expressions": [], "Filters": []}, "ParamsText": []}]}


class Api:
    """Подставной Campaign: хранит созданные объекты, отвечает как backend по разбору протокола ДИКСИ."""

    def __init__(self, *, lists=("101430_Кешбэк на кофе",), audience=120_000, lose_save=False, reject_activation=False):
        self.lists, self.audience, self.lose_save, self.reject_activation = list(lists), audience, lose_save, reject_activation
        self.calls, self.templates, self.campaigns, self.trees = [], {}, {}, {}
        self.n = 0
        self.blueprints = {BP1: blueprint(BP1, 1), BP2: blueprint(BP2, 2)}

    def _sid(self):
        self.n += 1
        return f"{self.n:024x}"

    def get_json_or_text(self, path, params=None):
        self.calls.append(("GET", path))
        if path.startswith("/msgtemplates/new/"):
            sid = self._sid()
            self.templates[sid] = {"SID": sid, "TemplateName": "", "MessageTypeID": 5, "PushProps": {"IOS": {}, "Android": {}, "Huawei": {}}}
            return copy.deepcopy(self.templates[sid])
        if path.startswith("/msgtemplates/"):
            return copy.deepcopy(self.templates[path.rsplit("/", 1)[1]])
        if path.startswith("/camps/templates/copy/"):
            src = path.rsplit("/", 1)[1]
            sid, tsid = self._sid(), self._sid()
            c = copy.deepcopy(self.blueprints[src])
            c["SID"], c["IsTemplate"] = sid, False
            c["ProfileUIInfo"]["SegmentsTreeSID"] = tsid
            self.campaigns[sid], self.trees[tsid] = c, tree(tsid, sid)
            return {"SID": sid}
        if path.startswith("/camps/"):
            return copy.deepcopy(self.campaigns[path.rsplit("/", 1)[1]])
        if path.startswith("/segments/get/"):
            return copy.deepcopy(self.trees[path.rsplit("/", 1)[1]])
        raise AssertionError(path)

    def post_json(self, path, body, params=None):
        self.calls.append(("POST", path, copy.deepcopy(body)))
        if path == "/segments/reflist":
            field = body["Fields"][0]
            return [{field: v} for v in self.lists if v.casefold().startswith(body["Filter"].casefold())]
        if path == "/msgtemplates/save":
            self.templates[body["SID"]] = copy.deepcopy(body)
            return "success"
        if path == "/segments":
            self.trees[body["SID"]] = copy.deepcopy(body)
            return "success"
        if path == "/segments/count":
            t = copy.deepcopy(body)
            for s in t["Segments"]:
                if s.get("ID") == 1:
                    s["Total"], s["TotalInt"] = f"{self.audience}", self.audience
            return t
        if path == "/camps/count":
            push = [s for s in body["PersonalRule"][0]["Steps"] if s["StepType"] == 12]
            share = self.audience // len(push)
            return {"Total": str(self.audience), "StepsInfo": [{"StepID": s["StepID"], "Total": str(share), "TotalInt": share} for s in push]}
        if path == "/camps":
            if self.lose_save:
                return ""
            self.campaigns[body["SID"]] = copy.deepcopy(body)
            return "success"
        if path == "/camps/status":
            if self.reject_activation:
                return "error"
            c = self.campaigns[body["SID"]]
            c.update({"Status": 2, "ActivatedOn": 1700000000, "ActivatedBy": "robot"})
            c["PersonalRule"][0].update({"Activated": True, "TaskID": "abcdefabcdefabcdefabcdef"})
            return "success"
        raise AssertionError(path)

    def put_json(self, path, body, params=None):
        raise AssertionError(path)


def run(api, ledger, **kw):
    args = dict(key="101430:1:2026-10-23", promo=101430, msg_no=1, name="Активируй 50% кешбэка на кофе", title="☕-50% кешбэка",
                text="Активируй кешбэк в приложении.", url="dixyapp://app/catalog", send_date=dt.date(2026, 10, 23), send_time="11:00",
                include_lists=["101430_Кешбэк на кофе"], exclude_lists=[], expected_count=120_000, test_recipients="+79990000000",
                ledger=ledger, sleep=lambda s: None)
    args.update(kw)
    return cp.setup_push(api, **args)


@pytest.fixture
def ledger(tmp_path):
    return cp.PushLedger(tmp_path / "pushes.sqlite3")


def test_full_setup_creates_template_and_inactive_campaign(ledger):
    api = Api()
    r = run(api, ledger)
    assert r["status"] == "saved" and r["audience"] == 120_000 and r["blocks"] == 1
    saved = api.campaigns[r["campaign_sid"]]
    assert saved["Name"] == "Активируй 50% кешбэка на кофе" and saved["Status"] == 1 and saved["PersonalRule"][0]["Activated"] is False
    rule = saved["StartInfo"]["ScheduleRule"]
    assert rule["EffectiveFrom"] == rule["StartTime"] == int(dt.datetime(2026, 10, 23, 11, 0, tzinfo=cp.MSK).timestamp())
    assert rule["EffectiveTo"] == int(dt.datetime(2026, 10, 24, 0, 1, tzinfo=cp.MSK).timestamp())
    push = [s for s in saved["PersonalRule"][0]["Steps"] if s["StepType"] == 12]
    assert len(push) == 1 and push[0]["StepProps"]["SourceTemplateSID"] == r["template_sid"] and push[0]["Text"] == "120 000"
    assert push[0]["StepProps"]["IOS"]["Title"] == "☕-50% кешбэка" and push[0]["StepProps"]["IOS"]["OpenAction"] == "deeplink"
    seg = next(s for s in saved["PersonalRule"][0]["Steps"] if s["StepType"] == 0)
    assert seg["StepProps"]["SegmentID"] == 1
    t = api.trees[saved["ProfileUIInfo"]["SegmentsTreeSID"]]
    root_filters = t["Segments"][1]["Expression"]["Filters"]
    assert any(f["Field"] == cp.INCLUDE_MLIST_FIELD and f["Operands"] == ["101430_Кешбэк на кофе"] for f in root_filters)
    assert not any(f["Field"] == cp.EXCLUDE_MLIST_FIELD for f in root_filters)
    order = [c[1] for c in api.calls]
    assert order.index("/segments/reflist") < order.index("/msgtemplates/new/5") < order.index(f"/camps/templates/copy/{BP1}")
    assert order.count("/camps") == 1 and "/camps/status" not in order
    assert ledger.get("101430:1:2026-10-23")["status"] == "saved"
    before = len(api.calls)
    r2 = run(api, ledger)
    assert r2["already_done"] and len(api.calls) == before


def test_big_audience_uses_two_block_blueprint(ledger):
    api = Api(audience=1_500_000)
    r = run(api, ledger, expected_count=1_500_000)
    assert r["blocks"] == 2 and f"/camps/templates/copy/{BP2}" in [c[1] for c in api.calls]
    push = [s for s in api.campaigns[r["campaign_sid"]]["PersonalRule"][0]["Steps"] if s["StepType"] == 12]
    assert len(push) == 2 and all(s["StepProps"]["SourceTemplateSID"] == r["template_sid"] for s in push)


def test_unknown_list_stops_before_any_creation(ledger):
    api = Api(lists=["другой список"])
    with pytest.raises(cp.PushSetupError) as e:
        run(api, ledger)
    assert e.value.code == "LIST_NOT_FOUND" and not api.templates and not api.campaigns
    assert ledger.get("101430:1:2026-10-23")["status"] == "failed"


def test_lost_save_response_blocks_repeat(ledger):
    api = Api(lose_save=True)
    with pytest.raises(cp.PushSetupError) as e:
        run(api, ledger)
    assert e.value.code == "UNKNOWN_OUTCOME" and ledger.get("101430:1:2026-10-23")["status"] == "uncertain"
    with pytest.raises(cp.PushSetupError) as e2:
        run(Api(), ledger)
    assert e2.value.code == "UNKNOWN_OUTCOME"


def test_activation_confirmed_by_two_reads(ledger):
    api = Api()
    r = run(api, ledger, activate=True)
    assert r["status"] == "activated" and api.campaigns[r["campaign_sid"]]["Status"] == 2
    assert ledger.get("101430:1:2026-10-23")["status"] == "activated"


def test_activation_rejected_is_not_repeated(ledger):
    api = Api(reject_activation=True)
    with pytest.raises(cp.PushSetupError) as e:
        run(api, ledger, activate=True)
    assert e.value.code == "UNKNOWN_OUTCOME" and [c[1] for c in api.calls].count("/camps/status") == 1
    assert ledger.get("101430:1:2026-10-23")["status"] == "saved"


def test_missing_test_recipients_fails_before_requests(ledger):
    api = Api()
    with pytest.raises(cp.PushSetupError) as e:
        run(api, ledger, test_recipients="")
    assert e.value.code == "TEST_RECIPIENTS_MISSING" and api.calls == []


def test_schedule_and_utm_deeplink():
    assert cp.schedule_epochs(dt.date(2026, 10, 23), None)[0] == int(dt.datetime(2026, 10, 23, 10, 0, tzinfo=cp.MSK).timestamp())
    utm = {"isActive": True, "utm_source": "manzana", "utm_medium": "push", "utm_campaign": "101430", "utm_content": "", "utm_term": ""}
    assert cp._utm_deeplink("dixyapp://app/catalog?x=1", utm) == "dixyapp:/catalog?x=1&utm_source=manzana&utm_medium=push&utm_campaign=101430"
    assert cp._utm_deeplink("https://dixy.ru", utm) == "https://dixy.ru"
