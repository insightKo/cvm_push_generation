#!/usr/bin/env python3
"""Инфокупон в Manzana Online (раздел Loyalty приложения Campaign, API /mlp/ml/*) — тестовый показ и перевод в бой.
Наша реализация по разбору программы ДИКСИ 20.10.2026 (Dixy CRM Tools 0.74.21, сценарии setup_coupon,
setup_coupon_button, launch_coupon_assets: lib/coupons/{steps,rules,relationships,entities,marketing_lists,models}.py,
lib/manzana/mlp_mutations.py, lib/coupon_launch/coupon.py). Чужой код не копируется.

Решение Елены 20.10.2026: нужен тестовый показ; акцию (кампанию лояльности, механику) в Manzana заводит MCI сама.

Тестовый показ одной акции (setup_coupon_test):
  1. справочники по точному имени: MarketingList «Тестовая группа запуски», ChequeFilter «DIXY_ВСЕ МАГАЗИНЫ» (активен),
     Partner «DIXY PARTNER»;
  2. черновик — заранее созданное в Manzana пустое правило «Инфокупон N» с известным CRM GUID (реестр черновиков
     coupon_drafts.json ведёт Елена: ID правила, ссылка, GUID); проверка типа: IsForCoupons=false, ResultType=1, Bonus=0;
  3. кампания: по названию из настроек акции — существующая Campaign (активна, период покрывает купон); пусто —
     остаётся CampaignId черновика («Информационные купоны»); решение Елены 20.10.2026 (1а): переносим исходную
     логику ДИКСИ как есть, название кампании робот сейчас не передаёт;
  4. скрытое правило: один PUT полного набора атрибутов (Name, ExternalId ≤ 47, Description, DateFrom = min(сегодня,
     старт) 00:00 МСК, DateTo = окончание 23:59, PrivateOfficeLogoUrl (только при ссылке на картинку; у портала
     её пока нет — картинку в Manzana добавляет человек), CanDisplayInPrivateOffice = акцептный,
     PrivateOfficeDisplayType 1, FilterByContactListsType 4, FilterByChequesType 3, CampaignId, IsActive false);
     занятое другим правилом Name/ExternalId — остановка (автопереименование не делаем);
  5. связи: MarketingList/Include «Тестовая группа запуски», ChequeFilter/Exclude «DIXY_ВСЕ МАГАЗИНЫ»,
     MarketingList/Exclude «<ExternalId> Исключаемый» (_ex; создаётся один раз), для акцептного — Include
     «<ExternalId> Включаемый» (_in); сторонние связи не трогаем;
  6. кнопка в Loyalty Admin (loyalty_admin.py): GUID lower, текст, deeplink;
  7. проверка полей и связей → показ с фильтрацией (PrivateOfficeDisplayType 3) → активация (IsActive true) → проверка.
Перевод в бой (publish_coupon): DateFrom = старт 00:00, FilterByContactListsType 4 (или 1 = всем), Include = наш МС
по точному имени, IsActive; тестовая группа и _ex-список не снимаются (как у ДИКСИ).
Все записи — один раз и затем чтение до подтверждения; HTTP 200 с isSuccess=false — ошибка; GUID клиентов здесь нет.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
RULE = "ChargeRuleBaseView"
TEST_LIST, CHEQUE_FILTER, PARTNER = "Тестовая группа запуски", "DIXY_ВСЕ МАГАЗИНЫ", "DIXY PARTNER"
EXTERNAL_ID_MAX, COUPON_EXTERNAL_ID_MAX = 50, 47
LIST_COLUMNS = "Name,ExternalId,PartnerId,MemberType,IsDynamic,IncludeInApiResponse"
FIND_COLUMNS = {"MarketingList": LIST_COLUMNS, "ChequeFilter": "Name,IsActive,ExternalId", "Partner": "Name,IsActive",
                "Campaign": "Name,ExternalId,IsActive,ActualStartOn,ActualEndOn,MechanicType", RULE: "Name,ExternalId"}


class CouponError(RuntimeError):
    def __init__(self, message: str, code: str = "COUPON_FAILED") -> None:
        super().__init__(message)
        self.code = code


# ---------- значения API ----------

def attrs(entity: Any) -> dict[str, Any]:
    if not isinstance(entity, dict) or not isinstance(entity.get("attributes"), dict):
        raise CouponError("Manzana вернула сущность без attributes.", code="API_SHAPE")
    return entity["attributes"]


def entity_id(entity: Any) -> int:
    value = entity.get("id", entity.get("attributes", {}).get("Id")) if isinstance(entity, dict) else entity
    if isinstance(value, bool) or not str(value).isdigit() or int(value) < 1:
        raise CouponError("API не вернул корректный ID сущности.", code="API_SHAPE")
    return int(value)


def check_success(payload: Any) -> None:
    if isinstance(payload, dict) and (payload.get("isSuccess") is False or payload.get("isBad") is True or payload.get("errorCode") or payload.get("errors")):
        raise CouponError("Manzana отклонила операцию: " + json.dumps(payload, ensure_ascii=False)[:600], code="API_REJECTED")


def api_bool(value: Any) -> bool:
    if value in (True, 1, "true", "True", "1"):
        return True
    if value in (False, None, 0, "false", "False", "0", ""):
        return False
    raise CouponError(f"Некорректное логическое значение API: {value!r}", code="API_SHAPE")


def local_datetime(value: Any) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(str(value or "").strip().replace("Z", "+00:00"))
    return parsed.replace(tzinfo=MSK) if parsed.tzinfo is None else parsed.astimezone(MSK)


def field_equal(key: str, actual: Any, expected: Any) -> bool:
    try:
        if key in ("DateFrom", "DateTo"):
            return local_datetime(actual) == local_datetime(expected)
        if key == "CampaignId":
            return entity_id(actual) == entity_id(expected)
        if isinstance(expected, bool):
            return actual is not None and api_bool(actual) == expected
        if isinstance(expected, int):
            return str(actual) == str(expected)
    except (CouponError, ValueError):
        return False
    return actual == expected


def external_id_of(title: str) -> str:
    base = str(title or "").strip()
    if not base:
        raise CouponError("Пустой заголовок купона.", code="SPEC_INVALID")
    return base[:COUPON_EXTERNAL_ID_MAX]


# ---------- MLP API ----------

class Mlp:
    """Операции над сущностями раздела Loyalty: чтение, постраничный поиск, один PUT/POST с подтверждением чтением."""

    def __init__(self, api: Any, *, identity_api: Any | None = None, sleep: Callable[[float], None] = time.sleep) -> None:
        self.api = api
        self.identity_api = identity_api or api
        self.sleep = sleep

    def read(self, entity: str, ident: int) -> dict[str, Any]:
        result = self.api.get_json_or_text(f"/mlp/ml/entity/{entity}/{ident}")
        check_success(result)
        if entity_id(result) != ident:
            raise CouponError("GET вернул другую сущность.", code="API_SHAPE")
        attrs(result)
        return result

    def paged(self, path: str, columns: str, *, query: str | None = None) -> list[dict[str, Any]]:
        out, seen = [], set()
        for page in range(100):
            params = {"take": "100", "skip": str(page * 100), "orderBy": "Name", "withDesc": "false", "attributes": columns, "filter": "{}"}
            if query is not None:
                params["fullTextSearch"] = json.dumps({"ispattern": True, "text": query + "*"}, ensure_ascii=False, separators=(",", ":"))
            items = self.api.get_json_or_text(path, params=params)
            if not isinstance(items, list):
                check_success(items)
                raise CouponError("Неподдерживаемый ответ списка API.", code="API_SHAPE")
            for item in items:
                ident = entity_id(item)
                if ident in seen:
                    raise CouponError("Повтор страницы API; полнота списка не подтверждена.", code="API_SHAPE")
                seen.add(ident)
                out.append(item)
            if len(items) < 100:
                return out
        raise CouponError("Слишком много результатов API.", code="API_SHAPE")

    def find(self, entity: str, value: str, *, attribute: str = "Name", required: bool = True) -> dict[str, Any] | None:
        items = self.paged(f"/mlp/ml/entity/{entity}", FIND_COLUMNS[entity], query=value)
        matches = [v for v in items if str(attrs(v).get(attribute) or "").strip().casefold() == value.casefold()]
        if len(matches) > 1:
            raise CouponError(f"Несколько {entity} с {attribute} = «{value}».", code="CONFLICT")
        if not matches and required:
            raise CouponError(f"Не найден {entity} «{value}».", code="NOT_FOUND")
        return matches[0] if matches else None

    def references(self, entity: str, ident: int, target: str, mark: str) -> set[int]:
        return {entity_id(v) for v in self.paged(f"/mlp/ml/relationship/{entity}/{ident}/{target}/{mark}", "Name,ExternalId")}

    def associate(self, entity: str, ident: int, target: str, mark: str, ref_id: int, *, attempts: int = 4) -> None:
        if ref_id in self.references(entity, ident, target, mark):
            return
        failure = None
        try:
            check_success(self.api.post_json("/mlp/ml/relationship/associate", {"TargetEntityName": target, "entityName": entity,
                                                                                   "id": ident, "mark": mark, "references": [ref_id]}))
        except Exception as exc:  # noqa: BLE001 — запрос мог дойти; итог решает чтение
            failure = exc
        for attempt in range(attempts):
            if ref_id in self.references(entity, ident, target, mark):
                return
            if attempt < attempts - 1:
                self.sleep(0.4)
        raise CouponError(f"Связь {target}/{mark} с ID={ref_id} не подтверждена.", code="UNKNOWN_OUTCOME") from failure

    def save(self, ident: int, expected: dict[str, Any], *, attempts: int = 4) -> dict[str, Any]:
        """PUT полного набора атрибутов (живые + ожидаемые) один раз; без изменений — без записи."""
        current = self.read(RULE, ident)
        before = attrs(current)
        if all(k in before and field_equal(k, before.get(k), v) for k, v in expected.items()):
            return current
        payload = {"id": ident, "name": RULE, "attributes": {**before, **expected}}
        for k in ("businessUnitId", "ownerId", "createdBy"):
            if k in current:
                payload[k] = current[k]
        failure = None
        try:
            check_success(self.api.put_json("/mlp/ml/entity", payload))
        except Exception as exc:  # noqa: BLE001
            failure = exc
        last: list[str] = []
        for attempt in range(attempts):
            current = self.read(RULE, ident)
            last = [k for k, v in expected.items() if k not in attrs(current) or not field_equal(k, attrs(current).get(k), v)]
            if not last:
                return current
            if attempt < attempts - 1:
                self.sleep(0.4)
        raise CouponError("Поля купона не прошли проверку чтением: " + ", ".join(last), code="UNKNOWN_OUTCOME") from failure

    def set_active(self, ident: int, active: bool, *, attempts: int = 8) -> dict[str, Any]:
        current = self.read(RULE, ident)
        if api_bool(attrs(current).get("IsActive")) == active:
            return current
        failure = None
        try:
            check_success(self.api.put_json("/mlp/ml/entity", {"id": ident, "name": RULE, "attributes": {"IsActive": bool(active)}}))
        except Exception as exc:  # noqa: BLE001
            failure = exc
        for attempt in range(attempts):
            current = self.read(RULE, ident)
            if api_bool(attrs(current).get("IsActive")) == active:
                return current
            if attempt < attempts - 1:
                self.sleep(0.5)
        raise CouponError(f"{'Активация' if active else 'Деактивация'} купона {ident} не подтверждена; повтор запрещён.", code="UNKNOWN_OUTCOME") from failure

    def access_ids(self) -> tuple[str, str]:
        data = self.identity_api.get_json_or_text("/accessv2/getaccessids")
        user, dept = (str(data.get("UserID") or "").strip(), str(data.get("DepartmentID") or "").strip()) if isinstance(data, dict) else ("", "")
        if not user or not dept:
            raise CouponError("Manzana не вернула UserID/DepartmentID учётки робота.", code="API_SHAPE")
        return user, dept

    def ensure_list(self, external_base: str, partner: dict[str, Any], *, name_suffix: str, external_suffix: str,
                    before_create: Callable[[], None] = lambda: None) -> dict[str, Any]:
        """МС «<ExternalId> Исключаемый|Включаемый» / «<ExternalId>_ex|_in»: найти по ExternalId и Name, иначе создать один раз."""
        external, name = external_base + external_suffix, external_base + name_suffix
        if len(external) > EXTERNAL_ID_MAX:
            raise CouponError("ExternalId списка длиннее 50 знаков.", code="SPEC_INVALID")
        by_id = self.find("MarketingList", external, attribute="ExternalId", required=False)
        by_name = self.find("MarketingList", name, required=False)
        if by_id is not None and by_name is not None and entity_id(by_id) != entity_id(by_name):
            raise CouponError(f"Имя и ExternalId списка «{name}» указывают на разные списки.", code="CONFLICT")
        result = by_id or by_name
        if result is None:
            before_create()
            user, dept = self.access_ids()
            payload = {"name": "MarketingList", "businessUnitId": dept, "ownerId": user, "createdBy": user, "attributes": {
                "Name": name, "ExternalId": external,
                "PartnerId": {"id": entity_id(partner), "label": attrs(partner)["Name"], "name": "Partner"},
                "CountOfMembers": "0", "NeedExcludeMembersWhoOptOut": "true", "NeedIgnoreInactiveListMembers": "true",
                "MemberType": "2", "CreatedFromType": "2", "IsLocked": "false", "ExchangeRate": "1", "IsDynamic": "false",
                "IncludeInApiResponse": False}}
            created_id, failure = None, None
            try:
                response = self.api.post_json("/mlp/ml/entity", payload)
                check_success(response)
                try:
                    created_id = entity_id(response)
                except CouponError:
                    created_id = None
            except Exception as exc:  # noqa: BLE001
                failure = exc
            if created_id is not None:
                for attempt in range(20):
                    try:
                        result = self.read("MarketingList", created_id)
                        break
                    except Exception:  # noqa: BLE001 — сущность может появиться с задержкой
                        if attempt < 19:
                            self.sleep(0.5)
            if result is None:
                for attempt in range(40):
                    result = self.find("MarketingList", external, attribute="ExternalId", required=False)
                    if result:
                        break
                    if attempt < 39:
                        self.sleep(0.5)
            if result is None:
                raise CouponError(f"Создание списка «{name}» не подтверждено чтением; повторный POST запрещён.", code="UNKNOWN_OUTCOME") from failure
        actual = attrs(self.read("MarketingList", entity_id(result)))
        if str(actual.get("Name") or "").strip() != name or str(actual.get("ExternalId") or "").strip() != external:
            raise CouponError(f"У существующего списка «{name}» другое имя или ExternalId.", code="CONFLICT")
        if entity_id(actual.get("PartnerId")) != entity_id(partner) or str(actual.get("MemberType")) != "2" or api_bool(actual.get("IsDynamic")):
            raise CouponError(f"Список «{name}»: другой партнёр, тип участников или динамический.", code="CONFLICT")
        return result


# ---------- данные акции ----------

@dataclass(frozen=True)
class CouponSpec:
    title: str
    description: str
    start_date: dt.date
    end_date: dt.date
    accept: bool
    campaign_name: str = ""
    button_title: str = "В КАТАЛОГ"
    button_url: str = "dixyapp://app/catalog"
    image_url: str = ""

    @property
    def external_id(self) -> str:
        return external_id_of(self.title)

    def period(self, today: dt.date) -> tuple[str, str]:
        start = min(today, self.start_date)
        if self.end_date < start:
            raise CouponError("Окончание купона раньше старта.", code="SPEC_INVALID")
        return (dt.datetime.combine(start, dt.time(0, 0), tzinfo=MSK).isoformat(),
                dt.datetime.combine(self.end_date, dt.time(23, 59), tzinfo=MSK).isoformat())

    def attributes(self, *, campaign_ref: Any, visible: bool, active: bool, today: dt.date) -> dict[str, Any]:
        date_from, date_to = self.period(today)
        out = {"Name": self.title, "ExternalId": self.external_id, "Description": self.description, "DateFrom": date_from,
               "DateTo": date_to, "CanDisplayInPrivateOffice": self.accept, "PrivateOfficeDisplayType": 3 if visible else 1,
               "FilterByContactListsType": 4, "FilterByChequesType": 3, "CampaignId": campaign_ref, "IsActive": bool(active)}
        # Картинка — только если ссылка есть: у портала колонки картинки пока нет, а пустая строка в PUT могла бы
        # вернуться из API как null и навсегда «не пройти проверку чтением». Без ссылки поле черновика не трогаем.
        if self.image_url:
            out["PrivateOfficeLogoUrl"] = self.image_url
        return out


def parse_button(value: str, default_title: str = "В КАТАЛОГ", default_url: str = "dixyapp://app/catalog") -> tuple[str, str]:
    """Колонка «Кнопка» вкладки: «В КАТАЛОГ dixyapp://app/products_list?id=123», «подборка», пусто."""
    text = " ".join(str(value or "").split())
    if not text:
        return default_title, default_url
    parts = text.split(" ")
    url = next((p for p in parts if p.lower().startswith("dixyapp://") or p.lower().startswith("https://")), "")
    title = " ".join(p for p in parts if p != url).strip() or default_title
    return title, (url or default_url)


def spec_from_promo(promo: dict[str, Any], *, campaign_name: str = "") -> CouponSpec:
    title = " ".join(str(promo.get("coupon_name") or "").split())
    if not title or title.startswith("--"):
        raise CouponError("Не заполнено «Название информационного купона для МП».", code="SPEC_INVALID")
    if len(title) > 100:
        raise CouponError("Название купона длиннее 100 знаков (лимит Manzana).", code="SPEC_INVALID")
    description = str(promo.get("slip_text") or "").strip()
    if not description:
        raise CouponError("Не заполнен «Текст на информационном купоне / слип-чеке».", code="SPEC_INVALID")
    start, end = promo.get("start_date"), promo.get("end_date")
    if not isinstance(start, dt.date) or not isinstance(end, dt.date):
        raise CouponError("У акции нет дат старта и окончания.", code="SPEC_INVALID")
    button_title, button_url = parse_button(str(promo.get("button") or ""))
    image = str(promo.get("coupon_layout_url") or "").strip()
    if image and not image.lower().startswith("https://"):
        image = ""
    # Акцептный купон — тот, что клиент активирует («Активируй …» в названии или тексте); решение Елены 20.10.2026 (2а):
    # пока по слову, дальше — признак из справочника механик (поле придёт из портала вместе с акцией).
    accept = "активируй" in (title + " " + description).lower()
    return CouponSpec(title, description, start, end, accept, campaign_name, button_title, button_url, image)


# ---------- реестр черновиков и журнал ----------

class DraftRegistry:
    """coupon_drafts.json: [{"rule_id": 123, "url": "…/ChargeRuleBaseView/edit/123", "guid": "…", "used_by": ""}]; ведёт Елена."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def _load(self) -> list[dict[str, Any]]:
        if not self.path.is_file():
            return []
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise CouponError("coupon_drafts.json должен быть списком.", code="DRAFTS_INVALID")
        return data

    def _save(self, items: list[dict[str, Any]]) -> None:
        tmp = self.path.with_name(self.path.name + ".part")
        tmp.write_text(json.dumps(items, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, self.path)

    def free_count(self) -> int:
        return sum(1 for d in self._load() if not str(d.get("used_by") or "").strip()
                   and str(d.get("guid") or "").strip() and str(d.get("rule_id") or "").strip())

    def reserve(self, key: str) -> dict[str, Any]:
        items = self._load()
        mine = next((d for d in items if str(d.get("used_by") or "") == key), None)
        if mine:
            return mine
        for d in items:
            if not str(d.get("used_by") or "").strip() and str(d.get("guid") or "").strip() and str(d.get("rule_id") or "").strip():
                d["used_by"] = key
                self._save(items)
                return d
        raise CouponError("Нет свободного черновика инфокупона с GUID в coupon_drafts.json.", code="DRAFTS_EMPTY")


class CouponLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("""create table if not exists coupons (key text primary key, promo integer not null, rule_id integer, guid text,
                status text not null, stage text not null default '', campaign_id integer, excluded_list_id integer, included_list_id integer,
                button_id integer, name text, error text not null default '', created_at text not null, updated_at text not null)""")

    def _db(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _now() -> str:
        return dt.datetime.now(MSK).isoformat(timespec="seconds")

    def get(self, key: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute("select * from coupons where key = ?", (key,)).fetchone()
            return dict(row) if row else None

    def upsert(self, key: str, *, promo: int, status: str, **fields: Any) -> None:
        now = self._now()
        cols = {k: v for k, v in fields.items() if k in ("rule_id", "guid", "stage", "campaign_id", "excluded_list_id", "included_list_id", "button_id", "name", "error")}
        with self._db() as db:
            db.execute("insert into coupons (key, promo, status, created_at, updated_at) values (?, ?, ?, ?, ?) on conflict(key) do nothing",
                       (key, promo, status, now, now))
            sets = ["status = ?", "updated_at = ?"] + [f"{k} = ?" for k in cols]
            db.execute(f"update coupons set {', '.join(sets)} where key = ?", [status, now, *[str(v)[:300] if k == "error" else v for k, v in cols.items()], key])


# ---------- тестовый показ ----------

def setup_coupon_test(mlp: Mlp, admin: Any, spec: CouponSpec, *, key: str, promo: int, drafts: DraftRegistry, ledger: CouponLedger,
                      today: dt.date | None = None) -> dict[str, Any]:
    today = today or dt.datetime.now(MSK).date()
    saved = ledger.get(key)
    if saved and saved["status"] == "test_active":
        return {"key": key, "rule_id": saved["rule_id"], "guid": saved["guid"], "status": "test_active", "already_done": True}
    stage = "start"
    try:
        stage = "справочники"
        tester = mlp.find("MarketingList", TEST_LIST)
        cheque = mlp.find("ChequeFilter", CHEQUE_FILTER)
        partner = mlp.find("Partner", PARTNER)
        if not api_bool(attrs(cheque).get("IsActive")):
            raise CouponError("Фильтр чеков «DIXY_ВСЕ МАГАЗИНЫ» неактивен.", code="CONFLICT")
        stage = "черновик"
        draft = drafts.reserve(key)
        rule_id, guid = int(draft["rule_id"]), str(draft["guid"]).strip().lower()
        ledger.upsert(key, promo=promo, status="draft_reserved", rule_id=rule_id, guid=guid, name=spec.title, stage=stage)
        original = attrs(mlp.read(RULE, rule_id))
        if api_bool(original.get("IsForCoupons")) or str(original.get("ResultType")) != "1" or str(original.get("Bonus")) not in ("0", "0.0"):
            raise CouponError(f"Правило {rule_id} — не пустой инфокупон (IsForCoupons/ResultType/Bonus); ничего не изменено.", code="CONFLICT")
        initially_active = api_bool(original.get("IsActive"))
        campaign_ref = original.get("CampaignId")
        entity_id(campaign_ref)
        stage = "кампания"
        campaign_id = entity_id(campaign_ref)
        if spec.campaign_name:
            campaign = mlp.find("Campaign", spec.campaign_name)
            c = attrs(campaign)
            date_from, date_to = spec.period(today)
            if not api_bool(c.get("IsActive")):
                raise CouponError(f"Кампания «{spec.campaign_name}» неактивна; робот её не активирует.", code="CONFLICT")
            if local_datetime(c.get("ActualStartOn")) > local_datetime(date_from) or local_datetime(c.get("ActualEndOn")) < local_datetime(date_to):
                raise CouponError(f"Период кампании «{spec.campaign_name}» не покрывает купон.", code="CONFLICT")
            campaign_id = entity_id(campaign)
            campaign_ref = {"name": "Campaign", "id": campaign_id, "label": c["Name"]}
        ledger.upsert(key, promo=promo, status="draft_reserved", campaign_id=campaign_id, stage=stage)
        stage = "фильтр чеков"
        extra = mlp.references(RULE, rule_id, "ChequeFilter", "Exclude") - {entity_id(cheque)}
        if extra:
            raise CouponError(f"У правила {rule_id} лишние ChequeFilter/Exclude {sorted(extra)}; не удаляем.", code="CONFLICT")
        stage = "скрытое правило"
        hidden = spec.attributes(campaign_ref=campaign_ref, visible=False, active=False, today=today)
        conflicts = [entity_id(i) for q in {spec.title, spec.external_id}
                     for i in mlp.paged(f"/mlp/ml/entity/{RULE}", FIND_COLUMNS[RULE], query=q)
                     if entity_id(i) != rule_id and (str(attrs(i).get("Name") or "").strip().casefold() == spec.title.casefold()
                                                      or str(attrs(i).get("ExternalId") or "").strip().casefold() == spec.external_id.casefold())]
        if conflicts:
            raise CouponError(f"Name/ExternalId «{spec.title}» уже у правила {sorted(set(conflicts))}; переименование — решение человека.", code="NAME_CONFLICT")
        if initially_active:
            mlp.set_active(rule_id, False)
        mlp.save(rule_id, hidden)
        ledger.upsert(key, promo=promo, status="hidden_saved", stage=stage)
        stage = "связи"
        mlp.associate(RULE, rule_id, "MarketingList", "Include", entity_id(tester))
        mlp.associate(RULE, rule_id, "ChequeFilter", "Exclude", entity_id(cheque))
        excluded = mlp.ensure_list(spec.external_id, partner, name_suffix=" Исключаемый", external_suffix="_ex",
                                   before_create=lambda: ledger.upsert(key, promo=promo, status="hidden_saved", stage="создание _ex"))
        mlp.associate(RULE, rule_id, "MarketingList", "Exclude", entity_id(excluded))
        included = None
        if spec.accept:
            included = mlp.ensure_list(spec.external_id, partner, name_suffix=" Включаемый", external_suffix="_in",
                                       before_create=lambda: ledger.upsert(key, promo=promo, status="hidden_saved", stage="создание _in"))
            mlp.associate(RULE, rule_id, "MarketingList", "Include", entity_id(included))
        ledger.upsert(key, promo=promo, status="links_done", excluded_list_id=entity_id(excluded),
                      included_list_id=entity_id(included) if included else None, stage=stage)
        stage = "кнопка"
        state, record = admin.ensure_button(guid=guid, title=spec.button_title, deeplink=spec.button_url,
                                            before_create=lambda: ledger.upsert(key, promo=promo, status="links_done", stage="создание кнопки"))
        ledger.upsert(key, promo=promo, status="button_done", button_id=record.record_id, stage=stage)
        stage = "проверка перед показом"
        _verify(mlp, rule_id, hidden, tester, cheque, excluded, included)
        stage = "показ с фильтрацией"
        visible = spec.attributes(campaign_ref=campaign_ref, visible=True, active=False, today=today)
        mlp.save(rule_id, visible)
        _verify(mlp, rule_id, visible, tester, cheque, excluded, included)
        stage = "активация"
        mlp.set_active(rule_id, True)
        final = spec.attributes(campaign_ref=campaign_ref, visible=True, active=True, today=today)
        _verify(mlp, rule_id, final, tester, cheque, excluded, included)
        ledger.upsert(key, promo=promo, status="test_active", stage=stage)
        return {"key": key, "rule_id": rule_id, "guid": guid, "campaign_id": campaign_id, "button_id": record.record_id,
                "button": state, "excluded_list_id": entity_id(excluded), "included_list_id": entity_id(included) if included else None,
                "accept": spec.accept, "status": "test_active",
                "rule_url": f"https://campaign.manzanagroup.ru/loyalty/marketing/rules/ChargeRuleBaseView/edit/{rule_id}",
                "deeplink": f"dixyapp://app/coupons?id={guid}&type=activated"}
    except CouponError as exc:
        current = ledger.get(key)
        ledger.upsert(key, promo=promo, status=(current or {}).get("status") or "failed", stage=stage, error=f"{stage}: {exc}")
        raise


def _verify(mlp: Mlp, rule_id: int, expected: dict[str, Any], tester: dict, cheque: dict, excluded: dict, included: dict | None) -> None:
    live = attrs(mlp.read(RULE, rule_id))
    diff = [k for k, v in expected.items() if k not in live or not field_equal(k, live.get(k), v)]
    if diff:
        raise CouponError("Проверка полей купона: " + ", ".join(diff), code="VERIFY")
    include = mlp.references(RULE, rule_id, "MarketingList", "Include")
    need = {entity_id(tester)} | ({entity_id(included)} if included else set())
    if need - include:
        raise CouponError(f"Нет связей Include {sorted(need - include)}.", code="VERIFY")
    if entity_id(excluded) not in mlp.references(RULE, rule_id, "MarketingList", "Exclude"):
        raise CouponError("Нет связи Exclude с исключаемым списком.", code="VERIFY")
    if mlp.references(RULE, rule_id, "ChequeFilter", "Exclude") != {entity_id(cheque)}:
        raise CouponError("ChequeFilter/Exclude не равен «DIXY_ВСЕ МАГАЗИНЫ».", code="VERIFY")


# ---------- перевод в бой ----------

def publish_coupon(mlp: Mlp, *, rule_id: int, start_date: dt.date, list_name: str | None) -> dict[str, Any]:
    """Бой: DateFrom = старт 00:00 МСК, FilterByContactListsType 4 (список) или 1 (всем), Include = наш МС, IsActive true.
    Тексты и картинка не читаются и не меняются; тестовая группа и _ex-список остаются (как у ДИКСИ)."""
    live = attrs(mlp.read(RULE, rule_id))
    for k in ("IsActive", "DateFrom", "FilterByContactListsType"):
        if k not in live:
            raise CouponError(f"В правиле нет поля {k}.", code="API_SHAPE")
    changes: dict[str, Any] = {}
    date_from = dt.datetime.combine(start_date, dt.time(0, 0), tzinfo=MSK).isoformat()
    if not field_equal("DateFrom", live.get("DateFrom"), date_from):
        changes["DateFrom"] = date_from
    audience_type = 1 if not list_name or list_name.strip().casefold() == "все" else 4
    if str(live.get("FilterByContactListsType")) != str(audience_type):
        changes["FilterByContactListsType"] = audience_type
    ms = mlp.find("MarketingList", list_name) if audience_type == 4 else None
    need_link = ms is not None and entity_id(ms) not in mlp.references(RULE, rule_id, "MarketingList", "Include")
    if changes or need_link:
        if api_bool(live.get("IsActive")):
            mlp.set_active(rule_id, False)
        if changes:
            mlp.save(rule_id, changes)
        if ms is not None:
            mlp.associate(RULE, rule_id, "MarketingList", "Include", entity_id(ms))
    mlp.set_active(rule_id, True)
    live = attrs(mlp.read(RULE, rule_id))
    if not api_bool(live.get("IsActive")) or not field_equal("DateFrom", live.get("DateFrom"), date_from) \
            or str(live.get("FilterByContactListsType")) != str(audience_type) \
            or (ms is not None and entity_id(ms) not in mlp.references(RULE, rule_id, "MarketingList", "Include")):
        raise CouponError("Боевое состояние купона не подтверждено чтением.", code="VERIFY")
    return {"rule_id": rule_id, "modified": bool(changes or need_link), "audience": list_name if audience_type == 4 else "все", "status": "live"}
