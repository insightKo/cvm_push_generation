#!/usr/bin/env python3
"""Обходной путь «Заведение в Manzana» на нашей стороне: условия акции и купон / слип в Manzana Online, кампания
и сообщения в Manzana Campaign, загрузка списка CRM_GUID в кампанию — той же программой, которой это делает ДИКСИ
(расширение Chrome «Dixy CRM Tools» из архива Dixy CRM Tools.zip), под нашей отдельной учёткой робота.

Задание на ru (cvm-manzana-setup.timer, каждые 15 минут) в одноразовом контейнере. Ничего из работающего не трогает:
  * акции и условия — из базы портала (cvm.promo: заливка «CVM offline», решение «Согласовано» ставит человек);
  * сообщения — из cvm.push (вкладка PUSH, тексты согласованы в портале);
  * списки — из того же хранилища, которое ДИКСИ читает по SFTP (inbox/ задания cvm-campaign-lists); квитанции
    кладёт в receipts/ по ТЗ обмена (tools/campaign-lists/ТЗ-обмен-списками.md, раздел 7) — как партнёр.
Один прогон: очередь → по каждой акции до трёх задач (online, campaign, lists) → отметка выполненной задачи в state/.
Без MANZANA_SETUP_ENABLED=1 задание в Manzana ничего не делает — только журнал «что бы сделал».

Шаги задач повторяют шаги экрана «Заведение» портала (cvm_portal/src/api/mock/handlers/deployments.ts, TASK_STEPS),
чтобы позже портал показывал их состояние. Тело шагов переносится из хендовера ДИКСИ
(MCI_DIXY_CVM_Integration_Handover_2026-10-12) и кода расширения: пока шаг не перенесён, он останавливается
с HandoverNeeded, задача остаётся в очереди, прогон завершается с кодом 1.

CRM_GUID есть только в файлах списков. В журнал, вывод службы и отметки состояния — номера акций, числа, имена файлов.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
SQL_DIR = Path(__file__).resolve().parent / "sql"
SYSTEMS = ("online", "campaign", "lists")
GUID_RE = re.compile(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}")
LIST_NAME_RE = re.compile(r"^(\d{6})_(\d{8})\.csv$")
YES = {"да", "yes", "1", "true"}

# Шаги задач — те же, что на экране «Заведение» портала (deployments.ts, TASK_STEPS); lists — по ТЗ обмена.
STEPS: dict[str, list[str]] = {
    # online — инфокупон для тестового показа (решение Елены 20.10.2026: акцию и механику MCI заводит сама).
    "online": ["Подключение к Manzana Online", "Черновик инфокупона из реестра (ID правила, GUID)",
               "Скрытое сохранение полей купона", "Тестовая группа, фильтр чеков, исключаемый список",
               "Кнопка купона в админке ДИКСИ", "Показ с фильтрацией и активация", "Проверка чтением"],
    "campaign": ["Подключение к Manzana Campaign", "Проверка списка в справочнике сегментов", "Шаблон PUSH",
                 "Копия заготовки кампании", "Дерево сегмента", "Расчёт аудитории и сохранение кампании",
                 "Активация кампании (по настройке)"],
    "lists": ["Проверка файла списка по паспорту", "Проверка имени МС в Campaign", "Файл в хранилище Campaign",
              "Запуск импорта (один раз)", "Ожидание результата импорта", "Квитанция в receipts/"],
}


class HandoverNeeded(RuntimeError):
    """Шаг ещё не перенесён из хендовера ДИКСИ. Текст — что именно взять из архива."""


class ConfigError(RuntimeError):
    pass


# ---------- настройки ----------

def parse_ranges(value: str) -> tuple[tuple[int, int], ...]:
    """«100000-100019; 101450» → ((100000, 100019), (101450, 101450)); «all» или «*» — без ограничения."""
    out = []
    if str(value or "").strip().lower() in ("all", "*"):
        return ()
    for part in str(value or "").replace(",", ";").split(";"):
        part = part.strip()
        if not part:
            continue
        a, _, b = part.partition("-")
        lo, hi = int(a), int(b or a)
        if hi < lo:
            raise ConfigError(f"LISTS_PROMOS: диапазон {part} задан наоборот")
        out.append((lo, hi))
    return tuple(out)


def promo_allowed(number: int, ranges: tuple[tuple[int, int], ...]) -> bool:
    return not ranges or any(lo <= number <= hi for lo, hi in ranges)


def _bool(v: str | None, default: bool) -> bool:
    if v is None or not v.strip():
        return default
    return v.strip().lower() in YES


@dataclass(frozen=True)
class Config:
    enabled: bool
    systems: tuple[str, ...]
    mc_url: str
    mc_api_url: str
    mc_login: str
    mc_password: str
    mc_totp: str
    mc_partner: str
    mc_selectors: dict[str, str]
    lists_postfix: str
    lists_send_to_mo: bool
    s3_upload_host: str
    workdir: Path
    lists_promos: tuple[tuple[int, int], ...]
    push_test_recipients: str
    push_blueprints: dict[int, str]
    push_activate: bool
    push_exclude_lists: tuple[str, ...]
    loyalty_admin_url: str
    loyalty_admin_login: str
    loyalty_admin_password: str
    coupon_drafts: Path
    profile_dir: Path
    headless: bool
    timeout_sec: int
    screenshot_on_error: bool
    storage: Path
    state: Path
    retention_days: int
    pg: dict[str, str]
    notify_to: tuple[str, ...]

    @classmethod
    def from_env(cls, env: dict[str, str] | os._Environ[str] = os.environ) -> "Config":
        g = lambda k, d="": (env.get(k) or d).strip()  # noqa: E731
        systems_raw = [s.strip().lower() for s in g("MANZANA_SETUP_SYSTEMS").split(",") if s.strip()]
        unknown = [s for s in systems_raw if s not in SYSTEMS]
        if unknown:
            raise ConfigError(f"MANZANA_SETUP_SYSTEMS: неизвестно {unknown}; допустимо {list(SYSTEMS)}")
        return cls(
            enabled=_bool(g("MANZANA_SETUP_ENABLED"), False),
            systems=tuple(s for s in SYSTEMS if s in systems_raw) or SYSTEMS,
            mc_url=g("MC_URL", "https://campaign.manzanagroup.ru"), mc_api_url=g("MC_API_URL", "https://apicampaign.manzanagroup.ru/v1/dixy"),
            mc_login=g("MC_LOGIN"), mc_password=g("MC_PASSWORD"), mc_totp=g("MC_TOTP_SECRET"), mc_partner=g("MC_PARTNER"),
            mc_selectors={k: v for k, v in (("login", g("MC_LOGIN_SELECTOR")), ("password", g("MC_PASSWORD_SELECTOR")),
                                            ("submit", g("MC_SUBMIT_SELECTOR")), ("code", g("MC_CODE_SELECTOR"))) if v},
            lists_postfix=g("LISTS_POSTFIX"), lists_send_to_mo=_bool(g("LISTS_SEND_TO_MO"), False),
            s3_upload_host=g("S3_UPLOAD_HOST", "dixy-mlistimport.hb.bizmrg.com"),
            workdir=Path(g("LISTS_WORKDIR") or (g("LISTS_STATE", "/state") + "/work")),
            lists_promos=parse_ranges(g("LISTS_PROMOS", "100000-100019")),
            push_test_recipients=g("PUSH_TEST_RECIPIENTS"),
            push_blueprints={1: g("PUSH_BLUEPRINT_1", "68ece43ec1c4a64e15a374b0"), 2: g("PUSH_BLUEPRINT_2", "6aa97c65cd1570a97ad36bcf"),
                             3: g("PUSH_BLUEPRINT_3", "6aa97d2ccd1570a97ad36f1e")},
            push_activate=_bool(g("PUSH_ACTIVATE"), False),
            push_exclude_lists=tuple(x.strip() for x in g("PUSH_EXCLUDE_LISTS").split(";") if x.strip()),
            # Manzana Online = раздел Loyalty того же приложения Campaign, та же учётка робота (Елена 20.10.2026);
            # отдельная учётка нужна только админке купонов ДИКСИ.
            loyalty_admin_url=g("LOYALTY_ADMIN_URL", "https://loyalty-admin.dixy.ru"), loyalty_admin_login=g("LOYALTY_ADMIN_LOGIN"),
            loyalty_admin_password=g("LOYALTY_ADMIN_PASSWORD"),
            coupon_drafts=Path(g("COUPON_DRAFTS") or (g("LISTS_STATE", "/state") + "/coupon_drafts.json")),
            profile_dir=Path(g("BROWSER_PROFILE_DIR", "/profile")), headless=_bool(g("BROWSER_HEADLESS"), True),
            timeout_sec=int(g("BROWSER_TIMEOUT_SEC", "120")), screenshot_on_error=_bool(g("BROWSER_SCREENSHOT_ON_ERROR"), True),
            storage=Path(g("LISTS_STORAGE", "/data")), state=Path(g("LISTS_STATE", "/state")),
            retention_days=int(g("RETENTION_DAYS", "14")),
            pg={"host": g("PG_HOST", "cvm-db"), "port": g("PG_PORT", "5432"), "dbname": g("PG_DB"),
                "user": g("PG_USER", "cvm_manzana_ro"), "password": g("PG_PASSWORD")},
            notify_to=tuple(a.strip() for a in g("NOTIFY_TO").split(",") if a.strip()),
        )

    def problems(self) -> list[str]:
        """Чего не хватает, чтобы задание могло работать по-настоящему. Пустые доступы — не ошибка, пока ENABLED=0."""
        out: list[str] = []
        if not self.pg["dbname"] or not self.pg["password"]:
            out.append("база портала: PG_DB / PG_PASSWORD пусты — запустить set-secrets.sh")
        if not self.enabled:
            return out
        need = {"campaign": [("MC_URL", self.mc_url), ("MC_LOGIN", self.mc_login), ("MC_PASSWORD", self.mc_password)],
                "lists": [("MC_URL", self.mc_url), ("MC_LOGIN", self.mc_login), ("MC_PASSWORD", self.mc_password)],
                "online": [("MC_URL", self.mc_url), ("MC_LOGIN", self.mc_login), ("MC_PASSWORD", self.mc_password),
                           ("LOYALTY_ADMIN_URL", self.loyalty_admin_url), ("LOYALTY_ADMIN_LOGIN", self.loyalty_admin_login),
                           ("LOYALTY_ADMIN_PASSWORD", self.loyalty_admin_password)]}
        for system in self.systems:
            for name, value in need[system]:
                if not value:
                    out.append(f"{system}: {name} пуст")
        if "campaign" in self.systems and not self.push_test_recipients:
            out.append("campaign: PUSH_TEST_RECIPIENTS пуст")
        if "online" in self.systems and not self.coupon_drafts.is_file():
            out.append(f"online: нет реестра черновиков инфокупонов {self.coupon_drafts}")
        for name, url in (("MC_URL", self.mc_url), ("MC_API_URL", self.mc_api_url), ("LOYALTY_ADMIN_URL", self.loyalty_admin_url)):
            if url and not url.startswith("https://"):
                out.append(f"{name}: только https://")
        return sorted(set(out))


# ---------- хранилище, журнал, отметки ----------

def redact(fields: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Ни одного CRM_GUID в журнале, отметках и квитанциях: ошибки браузера цитируют страницу Manzana, а там могут быть
    строки списка и GUID объектов. Совпадения заменяются на «<guid>», запись помечается; исключение здесь не бросается —
    иначе текст с GUID ушёл бы в вывод службы трассировкой."""
    blob = json.dumps(fields, ensure_ascii=False, default=str)
    if not GUID_RE.search(blob):
        return fields, False
    return json.loads(GUID_RE.sub("<guid>", blob)), True


def base_of(promo: dict[str, Any]) -> str:
    """Ключ акции в обмене и в отметках — «<номер>_<ГГГГММДД старта>» (как имя файла списка): после переноса старта
    задание списков выгружает новый файл, и у робота это новая работа, а не сделанная."""
    start = promo["start_date"]
    start = start if isinstance(start, dt.date) else dt.date.fromisoformat(str(start))
    return f"{int(promo['number'])}_{start:%Y%m%d}"


class Storage:
    DIRS = ("inbox", "processing", "sent", "failed", "receipts")

    def __init__(self, root: Path, state_root: Path):
        self.root = root
        self.state_dir = state_root / "state"
        self.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.journal_path = state_root / "journal.jsonl"

    def path(self, name: str) -> Path:
        return self.root / name

    def journal(self, event: str, **fields: Any) -> None:
        fields, redacted = redact(fields)
        if redacted:
            fields["redacted"] = True
        row = {"at": dt.datetime.now(MSK).isoformat(timespec="seconds"), "event": event, **fields}
        line = json.dumps(row, ensure_ascii=False, default=str)
        with open(self.journal_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        print(line, flush=True)

    def done_mark(self, base: str, system: str) -> Path:
        return self.state_dir / f"{base}.{system}.done"

    def is_done(self, base: str, system: str) -> bool:
        return self.done_mark(base, system).is_file()

    def mark_done(self, base: str, system: str, **fields: Any) -> None:
        fields, _ = redact(fields)
        self.done_mark(base, system).write_text(json.dumps({"at": dt.datetime.now(MSK).isoformat(timespec="seconds"),
                                                            **fields}, ensure_ascii=False, default=str), encoding="utf-8")


MARK_RE = re.compile(r"^(\d+)_(\d{8})\.(\w+)\.done$")


def cleanup(storage: Storage, retention_days: int, today: dt.date) -> int:
    """Отметки живут до старта акции + RETENTION_DAYS (как в задании списков): дальше они только копятся."""
    removed = 0
    for p in storage.state_dir.glob("*.done"):
        m = MARK_RE.match(p.name)
        if not m:
            continue
        start = dt.datetime.strptime(m.group(2), "%Y%m%d").date()
        if start + dt.timedelta(days=retention_days) < today:
            p.unlink(missing_ok=True)
            removed += 1
    return removed


# ---------- списки и квитанции (ТЗ обмена, разделы 5–7) ----------

def list_file_for(storage: Storage, base: str) -> Path | None:
    """Файл списка «<номер>_<старт>.csv» в inbox/ — только вместе с паспортом (без паспорта файл ещё пишется)."""
    p = storage.path("inbox") / f"{base}.csv"
    return p if p.is_file() and manifest_path(p).is_file() else None


def other_list_files(storage: Storage, base: str) -> list[str]:
    """Файлы той же акции с другой датой старта: акцию перенесли, а старый файл ещё лежит, или наоборот."""
    number = base.split("_", 1)[0]
    return sorted(p.name for p in storage.path("inbox").glob(f"{number}_*.csv")
                  if LIST_NAME_RE.match(p.name) and p.name != f"{base}.csv")


def manifest_path(list_path: Path) -> Path:
    return list_path.with_suffix("").with_suffix(".manifest.json")


def check_list(list_path: Path) -> dict[str, Any]:
    """Сверка файла с паспортом (ТЗ, раздел 5): число строк = count, SHA-256 = sha256. Возвращает паспорт."""
    manifest = json.loads(manifest_path(list_path).read_text(encoding="utf-8"))
    m = LIST_NAME_RE.match(list_path.name)
    if not m:
        raise ValueError(f"имя файла не по ТЗ: {list_path.name}")
    if int(manifest.get("promo", -1)) != int(m.group(1)) or manifest.get("file") != list_path.name:
        raise ValueError(f"паспорт не от этого файла: {list_path.name}")
    digest, count = hashlib.sha256(), 0
    with open(list_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    with open(list_path, "r", encoding="utf-8", newline="") as f:
        for _ in f:
            count += 1
    if count != int(manifest["count"]):
        raise ValueError(f"строк {count}, в паспорте {manifest['count']}")
    if digest.hexdigest() != str(manifest["sha256"]).lower():
        raise ValueError("контрольная сумма не совпала с паспортом")
    return manifest


def campaign_name(manifest: dict[str, Any]) -> str:
    """Название кампании в Campaign — «<номер>_<title из паспорта>» (ТЗ, раздел 6): title уже ≤ 50 с номером
    и не совпадает с именем в Manzana Online — сокращать и менять нельзя."""
    return f"{int(manifest['promo'])}_{manifest['title']}"


def write_receipt(storage: Storage, list_path: Path, ok: bool, **fields: Any) -> Path:
    """Квитанция по ТЗ (раздел 7): сначала .part, потом переименование — сервер читает только готовые."""
    fields, _ = redact(fields)
    now = dt.datetime.now(MSK).isoformat(timespec="seconds")
    base = list_path.with_suffix("").name
    if ok:
        body = {"file": list_path.name, "campaign_name": fields["campaign_name"], "loaded": int(fields["loaded"]),
                "loaded_at": now}
        target = storage.path("receipts") / f"{base}.ok.json"
    else:
        body = {"file": list_path.name, "error": str(fields["error"])[:500], "at": now}
        target = storage.path("receipts") / f"{base}.error.json"
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    part = target.with_name(target.name + ".part")
    part.write_text(json.dumps(body, ensure_ascii=False, indent=1), encoding="utf-8")
    os.chmod(part, 0o600)
    part.replace(target)
    return target


# ---------- база портала (только чтение) ----------

def read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


class PortalDb:
    """Роль cvm_manzana_ro: SELECT только по нужным колонкам cvm.promo и cvm.push (set-secrets.sh), сессия только чтение."""

    def __init__(self, pg: dict[str, str]):
        import psycopg  # в контейнере; тесты подставляют свой источник
        self.conn = psycopg.connect(**pg, connect_timeout=10, application_name="cvm-manzana-setup",
                                    options="-c default_transaction_read_only=on")

    def promos_to_setup(self, start_from: dt.date) -> list[dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(read_sql("promos_to_setup.sql"), {"start_from": start_from})
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def pushes_of(self, number: int) -> list[dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(read_sql("pushes_of_promo.sql"), {"number": number})
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------- браузер с расширением ДИКСИ ----------

class Browser:
    """Chromium в постоянном профиле робота (вход в Manzana хранится между прогонами). Внутри контейнера без
    SUID-песочницы Chromium запускается с --no-sandbox под uid 10010 (как у остальных заданий на ru)."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._pw = None
        self.ctx = None

    def __enter__(self) -> "Browser":
        from playwright.sync_api import sync_playwright
        # Расширение ДИКСИ в браузер робота не грузится: робот повторяет его протокол своим кодом, а чужой код
        # с правами на сессию Manzana в профиле робота не нужен (ревью 20.10.2026).
        args = ["--no-sandbox", "--disable-dev-shm-usage", "--disable-extensions"]
        self.cfg.profile_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Прогон, убитый по таймауту, оставляет замок профиля с чужим именем хоста — Chromium не стартует, пока его не
        # убрать. Прогон здесь один (flock в main), поэтому старый замок всегда мёртвый.
        for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
            try:
                (self.cfg.profile_dir / name).unlink()
            except FileNotFoundError:
                pass
        self._pw = sync_playwright().start()
        self.ctx = self._pw.chromium.launch_persistent_context(
            str(self.cfg.profile_dir), headless=self.cfg.headless, channel="chromium", args=args,
            locale="ru-RU", timezone_id="Europe/Moscow", accept_downloads=False)
        self.ctx.set_default_timeout(self.cfg.timeout_sec * 1000)
        return self

    def __exit__(self, *exc: Any) -> None:
        if self.ctx:
            self.ctx.close()
        if self._pw:
            self._pw.stop()

    def page(self):
        assert self.ctx is not None
        return self.ctx.pages[0] if self.ctx.pages else self.ctx.new_page()

    @staticmethod
    def totp(secret: str) -> str:
        import pyotp
        return pyotp.TOTP(secret).now()


# ---------- Manzana Campaign: сессия робота и загрузка списков ----------

def campaign_api(cfg: Config, browser: "Browser"):
    """Клиент API Campaign над сессией робота (manzana_session.py): вход, заголовки приложения, cookies контекста."""
    from manzana_session import CampaignSession
    session = CampaignSession(browser.ctx, login=cfg.mc_login, password=cfg.mc_password, totp_secret=cfg.mc_totp,
                              app_origin=cfg.mc_url, api_prefix=cfg.mc_api_url, selectors=cfg.mc_selectors,
                              timeout_ms=cfg.timeout_sec * 1000)
    return session.api("manzana_campaign")


def lists_cycle(cfg: Config, storage: Storage, api: Any | None, dry_run: bool) -> int:
    """Все файлы с паспортом из inbox/, по которым нет квитанции, — в Campaign тем же протоколом, что у ДИКСИ
    (campaign_lists.py). Один файл — одна запись реестра; повтор безопасен. Возвращает число ошибок."""
    import campaign_lists as cl
    inbox, receipts = storage.path("inbox"), storage.path("receipts")
    found = cl.pending_files(inbox, receipts)
    # Решение Елены 20.10.2026 («2б»): списки грузит ДИКСИ, робот — только акции из LISTS_PROMOS (по умолчанию тестовые
    # 100000–100019), пока не договорились о передаче загрузки.
    files = [p for p in found if promo_allowed(int(p.name.split("_", 1)[0]), cfg.lists_promos)]
    storage.journal("lists_scan", files=[p.name for p in files], not_ours=[p.name for p in found if p not in files],
                    dry_run=dry_run or not cfg.enabled)
    if dry_run or not cfg.enabled or api is None:
        for p in files:
            try:
                m = cl.read_manifest(p)
                storage.journal("dry_run", system="lists", file=p.name, count=m["count"], campaign=f"{m['promo']}_{m['title']}{cfg.lists_postfix}",
                                steps=STEPS["lists"])
            except Exception as e:  # noqa: BLE001
                storage.journal("list_invalid", file=p.name, error=f"{type(e).__name__}: {e}"[:300])
        return 0
    ledger = cl.Ledger(cfg.state / "imports.sqlite3")
    cfg.workdir.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Остатки сборок прошлых прогонов (CSV с GUID): прогон один (flock), чужих папок здесь нет.
    for stale in cfg.workdir.glob("import-*"):
        shutil.rmtree(stale, ignore_errors=True)
    failures = 0
    for p in files:
        try:
            r = cl.import_list(p, api=api, ledger=ledger, receipts=receipts, workdir=cfg.workdir, postfix=cfg.lists_postfix,
                               send_to_mo=cfg.lists_send_to_mo, s3_host=cfg.s3_upload_host, uploader=cl.upload_to_storage)
            storage.journal("list_loaded", file=r["file"], campaign=r["name"], loaded=r.get("loaded"), source_rows=r.get("source_rows"),
                            not_uploaded=r.get("not_uploaded"), import_id=r.get("import_id"), receipt_written=r.get("receipt_written"),
                            receipt_error=str(r.get("receipt_error") or "")[:200], resumed=bool(r.get("resumed") or r.get("already_confirmed")))
        except Exception as e:  # noqa: BLE001 — один файл не останавливает остальные
            failures += 1
            storage.journal("list_failed", file=p.name, code=getattr(e, "code", type(e).__name__), error=f"{type(e).__name__}: {e}"[:300])
    return failures


# ---------- Manzana Online: инфокупон для тестового показа ----------

def online_clients(cfg: Config, browser: "Browser") -> tuple[Any, Any]:
    """Клиенты раздела Loyalty (MLP API с groupingid, тот же вход робота) и админки купонов ДИКСИ (своя сессия Devise
    в том же контексте браузера). Создаются только когда в очереди есть задача online."""
    from coupon_setup import Mlp
    from loyalty_admin import LoyaltyAdmin
    from manzana_session import CampaignSession
    session = CampaignSession(browser.ctx, login=cfg.mc_login, password=cfg.mc_password, totp_secret=cfg.mc_totp,
                              app_origin=cfg.mc_url, api_prefix=cfg.mc_api_url, selectors=cfg.mc_selectors,
                              timeout_ms=cfg.timeout_sec * 1000)
    mlp = Mlp(session.api("manzana_loyalty"), identity_api=session.api("manzana_campaign"))
    admin = LoyaltyAdmin(browser.ctx.request, base_url=cfg.loyalty_admin_url, login=cfg.loyalty_admin_login,
                         password=cfg.loyalty_admin_password, timeout_ms=cfg.timeout_sec * 1000)
    admin.ensure()
    return mlp, admin


def setup_online(cfg: Config, online: tuple[Any, Any], promo: dict[str, Any], today: dt.date) -> dict[str, Any]:
    """Инфокупон акции для тестового показа (coupon_setup.setup_coupon_test) из колонок «CVM offline»: «Название
    информационного купона для МП» → название, «Текст на информационном купоне / слип-чеке» → описание, «Кнопка» →
    кнопка в админке ДИКСИ, даты акции → период. Механику и акцию (кампанию лояльности) MCI заводит сама; купон
    остаётся в кампании черновика («Информационные купоны»). Один ключ — одна строка реестра; повтор безопасен."""
    import coupon_setup as cs
    mlp, admin = online
    spec = cs.spec_from_promo(promo)
    return cs.setup_coupon_test(mlp, admin, spec, key=base_of(promo), promo=int(promo["number"]),
                                drafts=cs.DraftRegistry(cfg.coupon_drafts), ledger=cs.CouponLedger(cfg.state / "coupons.sqlite3"),
                                today=today)


def publish_online(cfg: Config, online: tuple[Any, Any], storage: Storage, key: str, audience: str, start_date: dt.date) -> dict[str, Any]:
    """Перевод инфокупона в бой по слову человека (команда --publish-coupon): правило из реестра по ключу
    «<номер>_<старт>», DateFrom = старт, аудитория — МС по точному имени или «все»."""
    import coupon_setup as cs
    mlp, _ = online
    row = cs.CouponLedger(cfg.state / "coupons.sqlite3").get(key)
    if not row or row["status"] != "test_active" or not row["rule_id"]:
        raise RuntimeError(f"Инфокупон {key} не прошёл тестовый показ (реестр coupons.sqlite3); в бой не переводится.")
    result = cs.publish_coupon(mlp, rule_id=int(row["rule_id"]), start_date=start_date, list_name=None if audience == "все" else audience)
    storage.journal("coupon_published", key=key, **result)
    return result


def setup_campaign(cfg: Config, api: Any, promo: dict[str, Any], pushes: list[dict[str, Any]], list_name: str,
                   expected_count: int, ledger: Any, today: dt.date) -> dict[str, Any]:
    """Каждое согласованное сообщение вкладки PUSH акции → свой шаблон и своя неактивная кампания в Manzana Campaign
    (campaign_push.py, протокол ДИКСИ): аудитория — наш список «<номер>_<title>» в Campaign, КГ в Manzana не задаётся.
    Возвращает сводку; сообщение, у которого дата уже прошла, пропускается."""
    import campaign_push as cp
    done, failed, skipped = [], [], []
    for push in pushes:
        send_date = push.get("send_date")
        if not isinstance(send_date, dt.date):
            send_date = dt.date.fromisoformat(str(send_date)) if send_date else None
        if send_date is None or send_date < today:
            skipped.append(push.get("id"))
            continue
        msg_no = push.get("msg_no")
        key = f"{int(promo['number'])}:{msg_no if msg_no is not None else 'id' + str(push.get('id'))}:{send_date.isoformat()}"
        name = str(promo.get("title") or "").strip()
        if msg_no is not None:
            name = f"{name} ({msg_no})" if len(pushes) > 1 else name
        try:
            r = cp.setup_push(api, key=key, promo=int(promo["number"]), msg_no=msg_no, name=name,
                              title=str(push.get("title") or ""), text=str(push.get("body") or ""),
                              url=str(push.get("screen_link") or push.get("button") or ""), send_date=send_date,
                              send_time=push.get("send_time"), include_lists=[list_name], exclude_lists=list(cfg.push_exclude_lists),
                              expected_count=int(expected_count or 0), test_recipients=cfg.push_test_recipients, ledger=ledger,
                              blueprints=cfg.push_blueprints, activate=cfg.push_activate)
            done.append({"key": key, "campaign_sid": r.get("campaign_sid"), "template_sid": r.get("template_sid"),
                         "audience": r.get("audience"), "status": r.get("status"), "already": bool(r.get("already_done"))})
        except Exception as exc:  # noqa: BLE001 — одно сообщение не останавливает остальные
            failed.append({"key": key, "code": getattr(exc, "code", type(exc).__name__), "error": f"{type(exc).__name__}: {exc}"[:300]})
    return {"done": done, "failed": failed, "skipped": skipped}


# ---------- прогон ----------

def wants_online(promo: dict[str, Any]) -> bool:
    return str(promo.get("setup") or "").strip().lower() in YES


def pending(cfg: Config, storage: Storage, promos: list[dict[str, Any]]) -> list[tuple[dict[str, Any], str]]:
    """Очередь задач прогона: по каждой акции — не сделанные системы (отметка по номеру и дате старта).
    online — только если «Настройка» = да."""
    out = []
    for p in promos:
        base = base_of(p)
        for system in cfg.systems:
            if system == "lists":
                continue  # списки идут отдельным циклом по файлам inbox/, а не по акциям
            if system == "online" and not wants_online(p):
                continue
            if not storage.is_done(base, system):
                out.append((p, system))
    return out


def run(cfg: Config, portal: Any, storage: Storage, now: dt.datetime | None = None, dry_run: bool = False,
        limit: int = 0, browser_factory: Callable[[Config], Any] | None = None,
        api_factory: Callable[[Config, Any], Any] | None = None,
        online_factory: Callable[[Config, Any], Any] | None = None) -> int:
    now = now or dt.datetime.now(MSK)
    today = now.astimezone(MSK).date()
    promos = portal.promos_to_setup(today)
    queue = pending(cfg, storage, promos)
    if limit:
        queue = queue[:limit]
    storage.journal("run", promos=len(promos), tasks=len(queue), enabled=cfg.enabled, dry_run=dry_run,
                    systems=list(cfg.systems), marks_removed=cleanup(storage, cfg.retention_days, today))
    failures = 0
    browser = None
    api = None
    online = None
    try:
        if cfg.enabled and not dry_run:
            browser = (browser_factory or Browser)(cfg)
            browser.__enter__()
        if cfg.enabled and not dry_run and ({"lists", "campaign"} & set(cfg.systems)):
            api = (api_factory or campaign_api)(cfg, browser)
        if "lists" in cfg.systems:
            failures += lists_cycle(cfg, storage, api, dry_run)
        for promo, system in queue:
            number, base = int(promo["number"]), base_of(promo)
            list_path = None
            try:
                if system == "online":
                    if dry_run or not cfg.enabled:
                        storage.journal("dry_run", promo=number, system=system, steps=STEPS[system])
                        continue
                    if online is None:
                        online = (online_factory or online_clients)(cfg, browser)
                    result = setup_online(cfg, online, promo, today)
                    storage.mark_done(base, system, **result)
                    storage.journal("task_done", promo=number, system=system, **result)
                    continue
                # campaign и lists: имя кампании — только из паспорта списка («<номер>_<title>», ТЗ раздел 6):
                # title в паспорте считает задание списков по правилу Елены 19.10.2026 (≤ 50, не равно имени в Online).
                if system == "campaign":
                    pushes = portal.pushes_of(number)
                    if not pushes:
                        storage.journal("waiting_pushes", promo=number, system=system)
                        continue
                list_path = list_file_for(storage, base)
                if list_path is None:
                    others = other_list_files(storage, base)
                    storage.journal("list_date_mismatch" if others else "waiting_list", promo=number, system=system,
                                    expected=f"{base}.csv", found=others)
                    continue
                manifest = check_list(list_path)
                name = campaign_name(manifest)
                if dry_run or not cfg.enabled:
                    storage.journal("dry_run", promo=number, system=system, list=name, messages=len(pushes),
                                    steps=STEPS[system])
                    continue
                import campaign_push as cp
                summary = setup_campaign(cfg, api, promo, pushes, name, int(manifest["count"]),
                                         cp.PushLedger(cfg.state / "pushes.sqlite3"), today)
                storage.journal("campaign_setup", promo=number, system=system, list=name, **summary)
                if summary["failed"]:
                    failures += 1
                elif summary["done"] and not summary["skipped"]:
                    storage.mark_done(base, system, list=name, campaigns=[d["campaign_sid"] for d in summary["done"]])
            except HandoverNeeded as e:
                failures += 1
                storage.journal("handover_needed", promo=number, system=system, what=str(e))
            except Exception as e:  # noqa: BLE001 — одна акция не должна останавливать остальные; GUID в тексте маскирует журнал
                failures += 1
                storage.journal("task_failed", promo=number, system=system, error=f"{type(e).__name__}: {e}"[:500])
    finally:
        if browser is not None:
            browser.__exit__(None, None, None)
    return 1 if failures else 0


def check(cfg: Config, storage: Storage, portal_factory: Callable[[dict[str, str]], Any] | None = None) -> int:
    """Проверка без действий: настройки, каталоги, база портала. Код 1 — чего-то не хватает."""
    problems = cfg.problems()
    for name in ("inbox", "receipts"):
        if not storage.path(name).is_dir():
            problems.append(f"нет каталога {storage.path(name)}")
    if "online" in cfg.systems and cfg.coupon_drafts.is_file():
        try:
            import coupon_setup as cs
            free = cs.DraftRegistry(cfg.coupon_drafts).free_count()
            print(f"черновики инфокупонов: свободных {free}")
            if not free:
                problems.append("online: нет свободных черновиков инфокупонов с GUID в реестре")
        except Exception as e:  # noqa: BLE001
            problems.append(f"online: реестр черновиков: {type(e).__name__}: {str(e)[:200]}")
    try:
        portal = (portal_factory or PortalDb)(cfg.pg)
        n = len(portal.promos_to_setup(dt.datetime.now(MSK).date()))
        print(f"база портала: ок, согласованных не начавшихся акций {n}")
    except Exception as e:  # noqa: BLE001
        problems.append(f"база портала: {type(e).__name__}: {str(e)[:200]}")
    for p in problems:
        print(f"нет: {p}")
    print("готово к работе" if not problems else f"не готово: {len(problems)}")
    return 1 if problems else 0


def publish_main(cfg: Config, portal: Any, storage: Storage, key: str, audience: str,
                 browser_factory: Callable[[Config], Any] | None = None,
                 online_factory: Callable[[Config, Any], Any] | None = None) -> int:
    """--publish-coupon: старт акции — из базы портала по номеру ключа; выполняется только при ENABLED=1."""
    if not cfg.enabled:
        print("MANZANA_SETUP_ENABLED=0 — перевод в бой не выполняется", flush=True)
        return 1
    m = re.fullmatch(r"(\d{6})_(\d{8})", key)
    if not m:
        print("ключ — «<номер>_<ГГГГММДД старта>»", flush=True)
        return 1
    number = int(m.group(1))
    promo = next((p for p in portal.promos_to_setup(dt.date(1900, 1, 1)) if int(p["number"]) == number and base_of(p) == key), None)
    if promo is None:
        print(f"акция {key} не найдена среди согласованных с этой датой старта", flush=True)
        return 1
    browser = (browser_factory or Browser)(cfg)
    browser.__enter__()
    try:
        online = (online_factory or online_clients)(cfg, browser)
        result = publish_online(cfg, online, storage, key, audience.strip(), promo["start_date"])
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    except Exception as e:  # noqa: BLE001
        storage.journal("coupon_publish_failed", key=key, error=f"{type(e).__name__}: {e}"[:500])
        print(f"не выполнено: {type(e).__name__}", flush=True)
        return 1
    finally:
        browser.__exit__(None, None, None)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Заведение в Manzana обходным путём (условия, кампания, список)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--check", action="store_true", help="проверить настройки, каталоги и базу портала; ничего не делать")
    g.add_argument("--dry-run", action="store_true", help="показать очередь и шаги; в Manzana ничего не делать")
    g.add_argument("--once", action="store_true", help="выполнить очередь (нужен MANZANA_SETUP_ENABLED=1)")
    g.add_argument("--publish-coupon", metavar="НОМЕР_ГГГГММДД",
                   help="перевести инфокупон акции из тестового показа в бой (нужны --audience и MANZANA_SETUP_ENABLED=1)")
    ap.add_argument("--audience", metavar="МС|все", help="аудитория боевого купона: точное имя МС в Manzana или «все»")
    ap.add_argument("--limit", type=int, default=0, help="не больше N задач за прогон")
    args = ap.parse_args(argv)
    if args.publish_coupon and not args.audience:
        ap.error("--publish-coupon требует --audience")

    cfg = Config.from_env()
    storage = Storage(cfg.storage, cfg.state)
    if args.check:
        return check(cfg, storage)
    # docker stop по таймауту службы шлёт SIGTERM: выходим через SystemExit, чтобы finally закрыл браузер и профиль.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    if not cfg.headless and not os.environ.get("DISPLAY"):
        # Расширение без headless-режима: виртуальный экран. Повторный вход сюда уже с DISPLAY от xvfb-run.
        os.execvp("xvfb-run", ["xvfb-run", "-a", sys.executable, *sys.argv])
    lock_path = cfg.state / ".lock"
    with open(lock_path, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("прогон уже идёт", flush=True)
            return 0
        try:
            portal = PortalDb(cfg.pg)
            if args.publish_coupon:
                return publish_main(cfg, portal, storage, args.publish_coupon, args.audience)
            return run(cfg, portal, storage, dry_run=args.dry_run, limit=args.limit)
        except Exception as e:  # noqa: BLE001 — текст исключения может цитировать страницу Manzana: в вывод только тип
            storage.journal("run_failed", error=type(e).__name__)
            return 1


if __name__ == "__main__":
    sys.exit(main())
