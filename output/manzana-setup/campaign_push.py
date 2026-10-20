#!/usr/bin/env python3
"""Настройка PUSH-рассылки в Manzana Campaign — тот же протокол, что у программы ДИКСИ (Dixy CRM Tools 0.74.21,
сценарий setup_push: lib/scenarios/push_setup.py, lib/manzana/{push_payload,push_node_api,segment_tree,segments_api,
promotions_api,campaign_assembly}.py, lib/push/waves.py), написанный заново под сервер ru и учётку робота по разбору
20.10.2026. Чужой код сюда не копируется.

Порядок одной рассылки (одно сообщение вкладки PUSH = один шаблон + одна кампания):
  1. реестр: по ключу (акция, сообщение, дата) уже есть кампания → ничего не создавать (у ДИКСИ такой защиты для PUSH
     нет: повторный запуск делает второй шаблон и вторую кампанию — здесь это закрыто);
  2. шаблон: GET /msgtemplates/new/5 (MessageTypeID 5 = PUSH) → POST /msgtemplates/save с TemplateName = название акции,
     PushProps для IOS/Android/Huawei (Title, Template, ButtonURL; deeplink dixyapp:// → OpenAction 'deeplink',
     TransitionType 1), SendForAllTypes; затем GET /msgtemplates/<sid> до совпадения полей (≤ 15 с);
  3. кампания: GET /camps/templates/copy/<заготовка> — заготовки ДИКСИ без контрольной группы на 1 / 2 / 3 блока
     отправки (аудитория ≤ 1 млн / ≤ 2,2 млн / больше); копия должна быть неактивной (Status 1), со своим деревом
     сегмента ProfileUIInfo.SegmentsTreeSID; GET /camps/<sid>, GET /segments/get/<дерево>;
  4. аудитория: имена МС проверяются POST /segments/reflist (точное совпадение или единственный префикс); дерево
     сегмента — фильтры «разрешение PUSH», «не удалён», тип токена IOS/ANDROID/HUAWEI, «исключить ГКГ», плюс
     list.list_name.false = включаемые МС и list_exclude.list_name_exclude.false = исключаемые; POST /segments →
     POST /segments/count → POST /segments (посчитанное) → GET /segments/get — сверка фильтров после каждого шага;
  5. кампания в памяти: Name, StartInfo.ScheduleRule (EffectiveFrom = StartTime = старт по МСК, EffectiveTo = следующий
     день 00:01), StepID 2 → SegmentID, во все блоки StepType 12 — PushProps шаблона, TestRecipients;
  6. POST /camps/count → численность блоков (TotalInt), сверка с выбранной схемой; Text блоков; один POST /camps,
     ответ строго "success"; GET /camps/<sid> до совпадения (≤ 10 с); кампания остаётся неактивной — как у ДИКСИ;
  7. по настройке — активация: POST /camps/status {SID, Status: 2} → "success" → GET /camps/<sid>: Status 2,
     ActivatedOn > 0, ActivatedBy, PersonalRule[0].Activated, TaskID (24 hex) — два одинаковых чтения подряд.
Контрольная группа в Manzana не задаётся (КГ% = 0): она уже выделена в списке MCI. Реестр — SQLite без клиентских данных.
"""
from __future__ import annotations

import copy
import datetime as dt
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
SID_RE = re.compile(r"^[0-9a-fA-F]{24}$")
PUSH_MESSAGE_TYPE_ID = 5
PUSH_STEP_TYPE = 12
PUSH_PROVIDER_ID = "12"
SEGMENT_STEP_ID, SEGMENT_STEP_TYPE = 2, 0
TEMPLATE_PLATFORMS = ("IOS", "Android", "Huawei")
PUSH_TOKEN_TYPES = ["IOS", "ANDROID", "HUAWEI"]
# Поля фильтров сегмента — как их называет backend Campaign (сняты с кода ДИКСИ, lib/manzana/segment_contract.py).
PUSH_PERMISSION_FIELD = "contacts_info.d_contact_push_send_contacts.false"
CONTACT_NOT_DELETED_FIELD = "contacts_info.is_del_contacts_info.false"
PUSH_TOKEN_TYPE_FIELD = "authorization_tokens_for_contacts_info.tokens_type_authorization_tokens_for_contacts_info.false"
GLOBAL_CONTROL_GROUP_EXCLUDE_FIELD = "contacts_info.global_control_group_exlude_contacts.false"
INCLUDE_MLIST_FIELD = "list.list_name.false"
EXCLUDE_MLIST_FIELD = "list_exclude.list_name_exclude.false"
# Заготовки кампаний ДИКСИ без контрольной группы: 1, 2, 3 блока отправки (lib/push/waves.py PUSH_BLUEPRINTS).
BLUEPRINTS_DEFAULT = {1: "68ece43ec1c4a64e15a374b0", 2: "6aa97c65cd1570a97ad36bcf", 3: "6aa97d2ccd1570a97ad36f1e"}
BLOCK_LIMITS = (1_000_000, 2_200_000)


class PushSetupError(RuntimeError):
    def __init__(self, message: str, code: str = "PUSH_SETUP_FAILED") -> None:
        super().__init__(message)
        self.code = code


# ---------- входные данные ----------

def blocks_for(total: int) -> int:
    return 1 if total <= BLOCK_LIMITS[0] else 2 if total <= BLOCK_LIMITS[1] else 3


def schedule_epochs(send_date: dt.date, send_time: str | None) -> tuple[int, int]:
    """Старт = дата и время по МСК (нет времени → 10:00, как у ДИКСИ); конец = следующий день 00:01."""
    m = re.search(r"(?<!\d)([01]?\d|2[0-3]):([0-5]\d)(?!\d)", str(send_time or ""))
    hour, minute = (int(m.group(1)), int(m.group(2))) if m else (10, 0)
    start = dt.datetime(send_date.year, send_date.month, send_date.day, hour, minute, tzinfo=MSK)
    end = dt.datetime.combine(send_date + dt.timedelta(days=1), dt.time(0, 1), tzinfo=MSK)
    return int(start.timestamp()), int(end.timestamp())


def _sid(value: Any, what: str) -> str:
    s = str(value or "").strip()
    if not SID_RE.fullmatch(s):
        raise PushSetupError(f"{what}: нет SID (24 hex) в ответе Campaign.", code="SID_MISSING")
    return s


def _steps(campaign: dict[str, Any]) -> list[dict[str, Any]]:
    rules = campaign.get("PersonalRule")
    if not isinstance(rules, list) or not rules or not isinstance(rules[0], dict) or not isinstance(rules[0].get("Steps"), list):
        raise PushSetupError("Кампания без PersonalRule[0].Steps.", code="CAMPAIGN_SHAPE")
    return [s for s in rules[0]["Steps"] if isinstance(s, dict)]


def _find_step(campaign: dict[str, Any], *, step_id: int, step_type: int) -> dict[str, Any]:
    steps = _steps(campaign)
    for s in steps:
        if s.get("StepID") == step_id and s.get("StepType") == step_type:
            return s
    same = [s for s in steps if s.get("StepType") == step_type]
    if len(same) == 1:
        return same[0]
    raise PushSetupError(f"В кампании нет ноды StepID={step_id}/StepType={step_type}.", code="CAMPAIGN_SHAPE")


# ---------- шаблон сообщения ----------

def build_template_payload(base: dict[str, Any], *, name: str, title: str, text: str, url: str, utm_campaign: str = "") -> dict[str, Any]:
    payload = copy.deepcopy(base)
    _sid(payload.get("SID"), "Шаблон")
    payload["TemplateName"] = name
    payload["MessageTypeID"] = PUSH_MESSAGE_TYPE_ID
    payload["UpdatedOn"] = int(time.time())
    props = payload.get("PushProps")
    if not isinstance(props, dict):
        props = payload["PushProps"] = {}
    url = str(url or "").replace(" ", "%20")
    for platform in TEMPLATE_PLATFORMS:
        p = props.get(platform)
        if not isinstance(p, dict):
            p = props[platform] = {}
        p.update({"Title": title, "Send": True, "Template": text, "Expression": {}, "ImageURL": "", "ButtonURL": url,
                  "isEscapeValidationImageURL": False, "useStandardImageURL": True,
                  "isEscapeValidationButtonURL": False, "useStandardButtonURL": True})
        if "dixyapp://" in url.lower():
            p["OpenAction"] = "deeplink"
            p["TransitionType"] = 1
    props["SendForAllTypes"] = True
    props.setdefault("MetadataML", "")
    props.setdefault("PushCategoryCode", "")
    if utm_campaign:
        utm = payload.get("UTM") if isinstance(payload.get("UTM"), dict) else {}
        utm.update({"changeType": "manual", "utm_source": "manzana", "utm_medium": "push", "utm_campaign": utm_campaign,
                    "utm_content": "", "utm_term": "", "isActive": True})
        payload["UTM"] = utm
    return payload


def template_mismatches(template: dict[str, Any], *, name: str, title: str, text: str, url: str) -> list[str]:
    out = []
    if str(template.get("TemplateName") or "") != name:
        out.append("TemplateName")
    props = template.get("PushProps")
    if not isinstance(props, dict):
        return out + ["PushProps"]
    url = str(url or "").replace(" ", "%20")
    for platform in TEMPLATE_PLATFORMS:
        p = props.get(platform)
        if not isinstance(p, dict):
            out.append(f"PushProps.{platform}")
            continue
        for key, expected in (("Title", title), ("Template", text), ("ButtonURL", url), ("Send", True)):
            if p.get(key) != expected:
                out.append(f"PushProps.{platform}.{key}")
        if "dixyapp://" in url.lower() and (str(p.get("OpenAction") or "").lower() != "deeplink" or p.get("TransitionType") != 1):
            out.append(f"PushProps.{platform}.deeplink")
    if props.get("SendForAllTypes") is not True:
        out.append("SendForAllTypes")
    return out


# ---------- дерево сегмента ----------

def _filter(field: str, operator: int, operand_type: int, operands: list[Any]) -> dict[str, Any]:
    return {"Field": field, "Operator": operator, "OperandType": operand_type, "Operands": operands, "OperandsForLabels": []}


def _segments(tree: dict[str, Any]) -> list[dict[str, Any]]:
    items = tree.get("Segments")
    if not isinstance(items, list):
        raise PushSetupError("В дереве сегмента нет Segments.", code="SEGMENT_SHAPE")
    return [s for s in items if isinstance(s, dict)]


def target_segment(tree: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """Рабочий дочерний сегмент: ID 1, иначе единственный выбранный / единственный дочерний; только корень — новый."""
    items = _segments(tree)
    for s in items:
        if s.get("ID") == 1 and s.get("Parent") != -1:
            return s, 1
    children = [s for s in items if s.get("Parent") == 0 and s.get("ID") != 0]
    selected = [s for s in children if s.get("SelectedToUse") is True]
    for pool in (selected, children):
        if len(pool) == 1 and isinstance(pool[0].get("ID"), int):
            return pool[0], int(pool[0]["ID"])
    if not children:
        root = next((s for s in items if s.get("ID") == 0 or s.get("Parent") == -1), None)
        if root is None:
            raise PushSetupError("В дереве сегмента нет корня.", code="SEGMENT_SHAPE")
        used = {s.get("ID") for s in items if isinstance(s.get("ID"), int)}
        new_id = 1 if 1 not in used else max(used) + 1
        child = {"ID": new_id, "Activated": False, "Parent": 0, "SelectedToUse": False,
                 "Expression": {"Connective": 0, "Expressions": [], "Filters": [], "HTML": "", "Columns": None},
                 "TotalInt": int(root.get("TotalInt") or 0), "Name": "", "SqlQuery": "", "ParamsText": [],
                 "Total": str(root.get("Total") or ""), "TotalPercent": str(root.get("TotalPercent") or "100%"),
                 "UICoordinate": {"X": 0, "Y": 0}}
        tree["Segments"].append(child)
        return child, new_id
    raise PushSetupError("В дереве сегмента несколько дочерних сегментов — нельзя выбрать рабочий.", code="SEGMENT_SHAPE")


def build_segment_tree(tree: dict[str, Any], *, include: Iterable[str], exclude: Iterable[str]) -> tuple[dict[str, Any], int]:
    updated = copy.deepcopy(tree)
    segment, segment_id = target_segment(updated)
    include, exclude = [str(x) for x in include if x], [str(x) for x in exclude if x]
    base = [_filter(PUSH_PERMISSION_FIELD, 0, 2, [True]), _filter(CONTACT_NOT_DELETED_FIELD, 0, 2, [False])]
    root = [_filter(PUSH_TOKEN_TYPE_FIELD, 2, 0, list(PUSH_TOKEN_TYPES)), _filter(GLOBAL_CONTROL_GROUP_EXCLUDE_FIELD, 0, 2, [True])]
    params = ["Тип PUSH-токена", "Исключить участника ГКГ"]
    if include:
        root.append(_filter(INCLUDE_MLIST_FIELD, 2, 0, include))
        params.append("Наименование МС")
    if exclude:
        root.append(_filter(EXCLUDE_MLIST_FIELD, 2, 0, exclude))
        params.append("Исключить по наименованию МС")
    segment["SelectedToUse"] = True
    segment["Expression"] = {"Connective": 0, "Expressions": [{"Connective": 0, "Expressions": [], "Filters": base, "HTML": "", "Columns": None}],
                             "Filters": root, "HTML": "", "Columns": None}
    segment["ParamsText"] = params
    return updated, segment_id


def segment_mismatches(tree: dict[str, Any], *, segment_id: int, include: list[str], exclude: list[str]) -> list[str]:
    seg = next((s for s in _segments(tree) if s.get("ID") == segment_id), None)
    if seg is None:
        return [f"нет сегмента ID={segment_id}"]
    out = []
    if seg.get("SelectedToUse") is not True:
        out.append("SelectedToUse")
    expr = seg.get("Expression") if isinstance(seg.get("Expression"), dict) else {}
    inner = expr.get("Expressions") if isinstance(expr.get("Expressions"), list) else []
    base = inner[0].get("Filters", []) if inner and isinstance(inner[0], dict) else []
    root = expr.get("Filters", []) if isinstance(expr.get("Filters"), list) else []

    def operands(filters: list[Any], field: str) -> Any:
        for f in filters:
            if isinstance(f, dict) and f.get("Field") == field:
                return f.get("Operands")
        return None

    checks = [(base, PUSH_PERMISSION_FIELD, [True]), (base, CONTACT_NOT_DELETED_FIELD, [False]),
              (root, PUSH_TOKEN_TYPE_FIELD, list(PUSH_TOKEN_TYPES)), (root, GLOBAL_CONTROL_GROUP_EXCLUDE_FIELD, [True]),
              (root, INCLUDE_MLIST_FIELD, include or None), (root, EXCLUDE_MLIST_FIELD, exclude or None)]
    for filters, field, expected in checks:
        if operands(filters, field) != expected:
            out.append(field)
    return out


# ---------- API-вызовы ----------

def resolve_list(api: Any, name: str, *, exclude: bool = False) -> str:
    """Имя МС в справочнике сегментов: точное совпадение (без регистра) или единственный префикс; иначе ошибка."""
    list_name, field = ("list_exclude", "list_name_exclude") if exclude else ("list", "list_name")
    rows = api.post_json("/segments/reflist", {"ListName": list_name, "PageSize": 100, "Page": 0, "Filter": name,
                                               "Fields": [field], "SortType": -1})
    values = []
    for row in rows if isinstance(rows, list) else []:
        value = row.get(field) if isinstance(row, dict) else row
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    exact = [v for v in values if v.casefold() == name.casefold()]
    if len(exact) == 1:
        return exact[0]
    prefix = [v for v in values if v.casefold().startswith(name.casefold())]
    if len(prefix) == 1 and not exact:
        return prefix[0]
    raise PushSetupError(f"МС «{name}» в справочнике сегментов Campaign не найден однозначно (совпадений: {len(prefix)}).",
                         code="LIST_NOT_FOUND")


def _wait_until(fn: Callable[[], Any], *, timeout: float, sleep: Callable[[float], None], clock: Callable[[], float], step: float = 0.5) -> Any:
    deadline = clock() + timeout
    last: Exception | None = None
    while True:
        try:
            result = fn()
            if result is not None:
                return result
        except PushSetupError as exc:
            last = exc
        if clock() >= deadline:
            raise last or PushSetupError("Backend не подтвердил состояние вовремя.", code="VERIFY_TIMEOUT")
        sleep(step)


# ---------- реестр ----------

class PushLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("""create table if not exists pushes (
                key text primary key, promo integer not null, msg_no integer, send_date text not null, name text not null,
                status text not null, template_sid text not null default '', campaign_sid text not null default '',
                audience integer, blocks integer, error text not null default '', created_at text not null, updated_at text not null)""")

    def _db(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _now() -> str:
        return dt.datetime.now(MSK).isoformat(timespec="seconds")

    def get(self, key: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute("select * from pushes where key = ?", (key,)).fetchone()
            return dict(row) if row else None

    def start(self, key: str, *, promo: int, msg_no: int | None, send_date: str, name: str) -> None:
        now = self._now()
        with self._db() as db:
            db.execute("""insert into pushes (key, promo, msg_no, send_date, name, status, created_at, updated_at)
                          values (?, ?, ?, ?, ?, 'template_requested', ?, ?)
                          on conflict(key) do update set status = 'template_requested', error = '', updated_at = excluded.updated_at
                          where pushes.status in ('template_requested', 'failed')""",
                       (key, promo, msg_no, send_date, name, now, now))

    def update(self, key: str, status: str, **fields: Any) -> None:
        sets, values = ["status = ?", "updated_at = ?"], [status, self._now()]
        for col in ("template_sid", "campaign_sid", "audience", "blocks", "error"):
            if col in fields and fields[col] is not None:
                sets.append(f"{col} = ?")
                values.append(fields[col] if col in ("audience", "blocks") else str(fields[col])[:300])
        values.append(key)
        with self._db() as db:
            db.execute(f"update pushes set {', '.join(sets)} where key = ?", values)


# ---------- основной сценарий ----------

def setup_push(api: Any, *, key: str, promo: int, msg_no: int | None, name: str, title: str, text: str, url: str,
               send_date: dt.date, send_time: str | None, include_lists: list[str], exclude_lists: list[str],
               expected_count: int, test_recipients: str, ledger: PushLedger, blueprints: dict[int, str] | None = None,
               utm_campaign: str = "", activate: bool = False, sleep: Callable[[float], None] = time.sleep,
               clock: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    """Одно сообщение → шаблон + кампания. Повторный вызов с тем же ключом ничего не создаёт заново."""
    blueprints = blueprints or BLUEPRINTS_DEFAULT
    if not test_recipients.strip():
        raise PushSetupError("Не заданы тестовые телефоны PUSH (PUSH_TEST_RECIPIENTS): без них ДИКСИ кампанию не настраивает.",
                             code="TEST_RECIPIENTS_MISSING")
    if not include_lists:
        raise PushSetupError("Не задан маркетинговый список для аудитории.", code="LIST_MISSING")
    saved = ledger.get(key)
    if saved and saved["status"] in ("saved", "activated") and SID_RE.fullmatch(saved["campaign_sid"] or ""):
        return {"key": key, "template_sid": saved["template_sid"], "campaign_sid": saved["campaign_sid"], "audience": saved["audience"],
                "already_done": True, "status": saved["status"]}
    if saved and saved["status"] not in ("template_requested", "failed"):
        raise PushSetupError(f"{key}: в реестре состояние «{saved['status']}» (шаблон {saved['template_sid'] or '—'}, "
                             f"кампания {saved['campaign_sid'] or '—'}); повтор без проверки в Campaign запрещён.", code="UNKNOWN_OUTCOME")
    ledger.start(key, promo=promo, msg_no=msg_no, send_date=send_date.isoformat(), name=name)
    try:
        # 1. Имена МС должны существовать до любого создания.
        include = [resolve_list(api, n) for n in include_lists]
        exclude = [resolve_list(api, n, exclude=True) for n in exclude_lists]
        # 2. Шаблон.
        created = api.get_json_or_text(f"/msgtemplates/new/{PUSH_MESSAGE_TYPE_ID}")
        template_sid = _sid(created.get("SID") if isinstance(created, dict) else "", "Шаблон")
        base = created if isinstance(created, dict) and created.get("SID") == template_sid and isinstance(created.get("PushProps"), dict) \
            else api.get_json_or_text(f"/msgtemplates/{template_sid}")
        ledger.update(key, "template_created", template_sid=template_sid)
        payload = build_template_payload(base, name=name, title=title, text=text, url=url, utm_campaign=utm_campaign)
        api.post_json("/msgtemplates/save", payload)

        def template_ok():
            t = api.get_json_or_text(f"/msgtemplates/{template_sid}")
            if isinstance(t, dict) and not template_mismatches(t, name=name, title=title, text=text, url=url):
                return t
            raise PushSetupError("Шаблон после сохранения не совпал с отправленным.", code="TEMPLATE_VERIFY")
        template = _wait_until(template_ok, timeout=15, sleep=sleep, clock=clock)
        # 3. Копия заготовки.
        blocks = blocks_for(int(expected_count or 0))
        source_sid = blueprints[blocks]
        copied = api.get_json_or_text(f"/camps/templates/copy/{source_sid}")
        campaign_sid = _sid(copied.get("SID") if isinstance(copied, dict) else _first_sid(copied, exclude=source_sid), "Копия кампании")
        if campaign_sid == source_sid:
            raise PushSetupError("Copy вернул SID заготовки вместо копии.", code="COPY_FAILED")
        ledger.update(key, "campaign_copied", campaign_sid=campaign_sid, blocks=blocks)
        campaign = _wait_until(lambda: _campaign_or_none(api, campaign_sid), timeout=10, sleep=sleep, clock=clock)
        if campaign.get("Status") != 1 or campaign.get("IsTemplate") or campaign.get("Deleted") or any(
                r.get("Activated") for r in campaign.get("PersonalRule", []) if isinstance(r, dict)):
            raise PushSetupError("Копия кампании не является новой неактивной кампанией.", code="COPY_STATE")
        tree_sid = _sid((campaign.get("ProfileUIInfo") or {}).get("SegmentsTreeSID"), "Дерево сегмента копии")

        def tree_ok():
            t = api.get_json_or_text(f"/segments/get/{tree_sid}")
            if isinstance(t, dict) and str(t.get("CampSID") or "") in ("", campaign_sid):
                return t
            raise PushSetupError("Дерево сегмента привязано к другой кампании.", code="SEGMENT_SHAPE")
        tree = _wait_until(tree_ok, timeout=10, sleep=sleep, clock=clock)
        # 4. Сегмент: save → count → save counted → get.
        segment_payload, segment_id = build_segment_tree(tree, include=include, exclude=exclude)
        api.post_json("/segments", segment_payload)
        counted = api.post_json("/segments/count", segment_payload)
        if not isinstance(counted, dict) or segment_mismatches(counted, segment_id=segment_id, include=include, exclude=exclude):
            raise PushSetupError("/segments/count вернул другую конфигурацию сегмента.", code="SEGMENT_VERIFY")
        api.post_json("/segments", counted)

        def segment_ok():
            t = api.get_json_or_text(f"/segments/get/{tree_sid}")
            if isinstance(t, dict) and not segment_mismatches(t, segment_id=segment_id, include=include, exclude=exclude):
                return t
            raise PushSetupError("Backend после сохранения сегмента вернул другие фильтры.", code="SEGMENT_VERIFY")
        confirmed_tree = _wait_until(segment_ok, timeout=10, sleep=sleep, clock=clock)
        segment_name = str(next((s.get("Name") for s in _segments(confirmed_tree) if s.get("ID") == segment_id), "") or "")
        # 5. Кампания в памяти.
        start_epoch, end_epoch = schedule_epochs(send_date, send_time)
        campaign["Name"] = name
        rule = ((campaign.get("StartInfo") or {}).get("ScheduleRule"))
        if not isinstance(rule, dict):
            raise PushSetupError("У копии нет StartInfo.ScheduleRule.", code="CAMPAIGN_SHAPE")
        rule.update({"EffectiveFrom": start_epoch, "EffectiveTo": end_epoch, "StartTime": start_epoch})
        seg_step = _find_step(campaign, step_id=SEGMENT_STEP_ID, step_type=SEGMENT_STEP_TYPE)
        seg_step["Name"] = segment_name
        seg_step.setdefault("StepProps", {})
        if not isinstance(seg_step["StepProps"], dict):
            seg_step["StepProps"] = {}
        seg_step["StepProps"]["SegmentID"] = segment_id
        push_steps = apply_push_nodes(campaign, template, test_recipients=test_recipients)
        if len(push_steps) != blocks:
            raise PushSetupError(f"В заготовке на {blocks} блок(а) найдено PUSH-блоков: {len(push_steps)}.", code="BLUEPRINT_SHAPE")
        campaign["UpdatedOn"] = int(time.time())
        # 6. Расчёт, один POST, проверка.
        count = api.post_json("/camps/count", campaign)
        if not isinstance(count, dict) or not isinstance(count.get("StepsInfo"), list):
            raise PushSetupError("/camps/count не вернул StepsInfo.", code="COUNT_FAILED")
        totals = {}
        for info in count["StepsInfo"]:
            if isinstance(info, dict) and info.get("StepID") in {s["StepID"] for s in push_steps}:
                value = info.get("TotalInt")
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise PushSetupError("Некорректный TotalInt PUSH-блока в /camps/count.", code="COUNT_FAILED")
                totals[info["StepID"]] = value
        if set(totals) != {s["StepID"] for s in push_steps}:
            raise PushSetupError("/camps/count не вернул численность всех PUSH-блоков.", code="COUNT_FAILED")
        audience = sum(totals.values())
        if blocks_for(audience) > blocks:
            raise PushSetupError(f"Аудитория {audience} больше, чем допускает заготовка на {blocks} блок(а); "
                                 "нужна заготовка с большим числом блоков.", code="AUDIENCE_TOO_BIG")
        total_text = str(count.get("Total") or "").strip()
        if total_text and isinstance(campaign.get("StartInfo"), dict):
            campaign["StartInfo"]["Text"] = total_text
        texts = {i.get("StepID"): str(i.get("Total") or "") for i in count["StepsInfo"] if isinstance(i, dict)}
        for step in _steps(campaign):
            if step.get("StepType") != SEGMENT_STEP_TYPE and texts.get(step.get("StepID")):
                step["Text"] = texts[step["StepID"]]
        for step in push_steps:
            step["Text"] = f"{totals[step['StepID']]:,}".replace(",", " ")
        campaign["UpdatedOn"] = int(time.time())
        ledger.update(key, "save_requested", audience=audience)
        receipt = api.post_json("/camps", campaign)
        if receipt != "success":
            ledger.update(key, "uncertain", error="сохранение кампании не подтверждено")
            raise PushSetupError("Сохранение кампании не подтверждено (ответ не 'success'); повтор запрещён.", code="UNKNOWN_OUTCOME")
        expected_texts = {s["StepID"]: s["Text"] for s in push_steps}

        def saved_ok():
            c = _campaign_or_none(api, campaign_sid)
            if c is None:
                raise PushSetupError("Кампания не читается.", code="VERIFY")
            problems = campaign_mismatches(c, name=name, start_epoch=start_epoch, end_epoch=end_epoch, segment_id=segment_id,
                                           template=template, test_recipients=test_recipients, expected_texts=expected_texts, tree_sid=tree_sid)
            if problems:
                raise PushSetupError("Backend GET после POST /camps не подтвердил: " + "; ".join(problems), code="VERIFY")
            return c
        _wait_until(saved_ok, timeout=10, sleep=sleep, clock=clock)
        ledger.update(key, "saved", template_sid=template_sid, campaign_sid=campaign_sid, audience=audience)
        result = {"key": key, "template_sid": template_sid, "campaign_sid": campaign_sid, "audience": audience, "blocks": blocks,
                  "segment_id": segment_id, "include": include, "exclude": exclude, "status": "saved",
                  "template_url": f"https://campaign.manzanagroup.ru/templates/push/edit/{template_sid}",
                  "campaign_url": f"https://campaign.manzanagroup.ru/promotions/edit?id={campaign_sid}&partner=dixy"}
        if activate:
            activate_campaign(api, campaign_sid, sleep=sleep, clock=clock)
            ledger.update(key, "activated")
            result["status"] = "activated"
        return result
    except PushSetupError as exc:
        current = ledger.get(key) or {}
        if current.get("status") not in ("uncertain", "saved", "activated"):
            ledger.update(key, "failed", error=str(exc))
        raise


def _first_sid(value: Any, *, exclude: str) -> str:
    for m in SID_RE.finditer(str(value or "")) if False else re.finditer(r"[0-9a-fA-F]{24}", str(value or "")):
        if m.group(0).lower() != exclude.lower():
            return m.group(0)
    return ""


def _campaign_or_none(api: Any, sid: str) -> dict[str, Any] | None:
    c = api.get_json_or_text(f"/camps/{sid}")
    return c if isinstance(c, dict) and str(c.get("SID") or "") == sid else None


def _utm_deeplink(url: str, utm: dict[str, Any]) -> str:
    """Повтор преобразования приложения Manzana для IOS/Android: dixyapp://app/<путь>?… → dixyapp:/<путь>?…&utm_*."""
    if not url or utm.get("isActive") is not True:
        return url
    parts = urlsplit(url)
    if parts.scheme.casefold() != "dixyapp" or parts.netloc.casefold() != "app":
        return url
    keys = ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term")
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in keys]
    query += [(k, str(utm.get(k) or "")) for k in keys if str(utm.get(k) or "")]
    return f"{parts.scheme}:{parts.path}" + (f"?{urlencode(query)}" if query else "")


def apply_push_nodes(campaign: dict[str, Any], template: dict[str, Any], *, test_recipients: str) -> list[dict[str, Any]]:
    props = template.get("PushProps")
    if not isinstance(props, dict):
        raise PushSetupError("Шаблон без PushProps.", code="TEMPLATE_SHAPE")
    utm = copy.deepcopy(template["UTM"]) if isinstance(template.get("UTM"), dict) else \
        {"changeType": "", "utm_source": "", "utm_medium": "", "utm_campaign": "", "utm_content": "", "utm_term": "", "isActive": False}
    platforms = {}
    for platform in ("Android", "IOS", "Huawei"):
        source = props.get(platform)
        if not isinstance(source, dict):
            raise PushSetupError(f"Шаблон без PushProps.{platform}.", code="TEMPLATE_SHAPE")
        p = copy.deepcopy(source)
        for k, v in (("TemplateName", ""), ("Expression", {}), ("PreviewURL", ""), ("DispatchType", 0), ("TransitionType", 0), ("Duplicate", False)):
            p.setdefault(k, v)
        p.update({"isEscapeValidationImageURL": False, "isEscapeValidationButtonURL": False, "useStandardImageURL": True, "useStandardButtonURL": True})
        if platform in ("Android", "IOS"):
            p["ButtonURL"] = _utm_deeplink(str(p.get("ButtonURL") or ""), utm)
        platforms[platform] = p
    steps = [s for s in _steps(campaign) if s.get("StepType") == PUSH_STEP_TYPE]
    if not steps:
        raise PushSetupError("В кампании нет PUSH-блоков.", code="CAMPAIGN_SHAPE")
    for step in steps:
        step["ProviderID"] = PUSH_PROVIDER_ID
        step["UIState"] = 1
        sp = step.get("StepProps") if isinstance(step.get("StepProps"), dict) else {}
        sp.update({"UsePref": False, "PrefHour": 20, "PrefMinute": 0, "TemplateName": template["TemplateName"],
                   "MailoutName": template["TemplateName"], "SourceTemplateSID": template["SID"], "RecipientsLimit": 0, "MoneyLimit": 0,
                   "TestRecipients": test_recipients, "ExtRecipients": "", "Android": copy.deepcopy(platforms["Android"]),
                   "IOS": copy.deepcopy(platforms["IOS"]), "Huawei": copy.deepcopy(platforms["Huawei"]),
                   "MetadataML": str(props.get("MetadataML") or ""), "MessageStoreTTL": None, "IsPollConnected": False,
                   "UTM": copy.deepcopy(utm), "SendForAllTypes": bool(props.get("SendForAllTypes")),
                   "PushCategoryCode": str(props.get("PushCategoryCode") or "")})
        step["StepProps"] = sp
    return steps


def campaign_mismatches(c: dict[str, Any], *, name: str, start_epoch: int, end_epoch: int, segment_id: int, template: dict[str, Any],
                        test_recipients: str, expected_texts: dict[int, str], tree_sid: str) -> list[str]:
    out = []
    if str(c.get("Name") or "") != name:
        out.append("Name")
    rule = ((c.get("StartInfo") or {}).get("ScheduleRule")) or {}
    if (rule.get("EffectiveFrom"), rule.get("EffectiveTo"), rule.get("StartTime")) != (start_epoch, end_epoch, start_epoch):
        out.append("ScheduleRule")
    try:
        seg = _find_step(c, step_id=SEGMENT_STEP_ID, step_type=SEGMENT_STEP_TYPE)
        if (seg.get("StepProps") or {}).get("SegmentID") != segment_id:
            out.append("SegmentID")
    except PushSetupError:
        out.append("SegmentStep")
    if str((c.get("ProfileUIInfo") or {}).get("SegmentsTreeSID") or "") != tree_sid:
        out.append("SegmentsTreeSID")
    if c.get("IsTemplate") or c.get("Deleted") or c.get("Status") != 1 or any(r.get("Activated") for r in c.get("PersonalRule", []) if isinstance(r, dict)):
        out.append("inactive_state")
    steps = {s.get("StepID"): s for s in _steps(c) if s.get("StepType") == PUSH_STEP_TYPE}
    if set(steps) != set(expected_texts):
        out.append("push_steps")
    for sid, step in steps.items():
        sp = step.get("StepProps") if isinstance(step.get("StepProps"), dict) else {}
        if sp.get("SourceTemplateSID") != template["SID"] or sp.get("TestRecipients") != test_recipients or step.get("ProviderID") != PUSH_PROVIDER_ID:
            out.append(f"push_step_{sid}")
        if str(step.get("Text") or "") != expected_texts.get(sid, ""):
            out.append(f"push_text_{sid}")
        for platform in ("Android", "IOS", "Huawei"):
            want = (template["PushProps"].get(platform) or {})
            have = sp.get(platform) if isinstance(sp.get(platform), dict) else {}
            if have.get("Title") != want.get("Title") or have.get("Template") != want.get("Template"):
                out.append(f"push_platform_{sid}_{platform}")
    return out


def activate_campaign(api: Any, sid: str, *, sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    """POST /camps/status {SID, Status: 2} → "success"; подтверждение двумя одинаковыми чтениями."""
    if api.post_json("/camps/status", {"SID": sid, "Status": 2}) != "success":
        raise PushSetupError("Backend не подтвердил активацию; повторный запрос запрещён до проверки в Campaign.", code="UNKNOWN_OUTCOME")
    previous, streak, last = None, 0, {}
    for attempt in range(6):
        c = _campaign_or_none(api, sid) or {}
        rules = c.get("PersonalRule") if isinstance(c.get("PersonalRule"), list) else []
        branch = rules[0] if rules and isinstance(rules[0], dict) else {}
        task = str(branch.get("TaskID") or "")
        try:
            activated_on = int(c.get("ActivatedOn") or 0)
        except (TypeError, ValueError):
            activated_on = 0
        last = {"status": c.get("Status"), "activated_on": activated_on, "activated_by": str(c.get("ActivatedBy") or "").strip(),
                "branch_activated": branch.get("Activated") is True, "task_id": task}
        valid = last["status"] == 2 and activated_on > 0 and bool(last["activated_by"]) and last["branch_activated"] and bool(SID_RE.fullmatch(task))
        signature = (last["status"], activated_on, last["activated_by"], task)
        streak = streak + 1 if valid and signature == previous else (1 if valid else 0)
        previous = signature if valid else None
        if streak >= 2:
            return last
        if attempt < 5:
            sleep(2)
    raise PushSetupError(f"Активация не подтверждена чтением: {last}. Повторный запрос запрещён.", code="ACTIVATION_UNCONFIRMED")
