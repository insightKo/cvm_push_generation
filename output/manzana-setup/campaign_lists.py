#!/usr/bin/env python3
"""Загрузка списка CRM_GUID акции из нашей базы списков (cvm_lists, Postgres) в Manzana Campaign — тот же протокол, что
у программы ДИКСИ (Dixy CRM Tools 0.74.21, lib/sftp_exchange/campaign_import.py), написанный заново под сервер ru и учётку
робота. Чужой код сюда не копируется — только порядок вызовов и защиты, снятые с него при разборе 20.10.2026.

Решение Елены 20.10.2026: робот берёт списки прямо из нашего Postgres, а не со своего же SFTP-обмена; файлов на диске,
паспортов и квитанций у робота нет. Порядок одного списка (ключ «<номер>_<ГГГГММДД старта>»):
  1. название МС = имя кампании акции из базы (+ постфикс), не длиннее 120 знаков / 240 байт, без символов имён Windows;
     GUID из базы (различные, без пустых) одним проходом проверяются и пишутся в архив для Campaign — в памяти: CSV
     с заголовком contactid (CRLF) в ZIP «<название>.zip»; число совпадает с guids_send акции (проверяет robot.py);
  2. POST /mlist/exist {Name, LoadType: 4, mListMode: 3} → exist должно быть false (существующий МС не перезаписывается);
  3. (архив уже собран на шаге 1);
  4. GET /import/s3url?key=<название>.zip → временная ссылка хранилища (только https, хост S3_UPLOAD_HOST,
     путь /mlists/<key>, подпись X-Amz-Signature); PUT тела напрямую, без редиректов;
  5. write-ahead «import_requested» в реестр, затем ОДИН PUT /import/s3import {key, importType: contact_id, loadType: 4,
     destination: camp|campml, nameMList, mListMode: 3, sendToMO}; ответ без ID (24 hex) — «uncertain», повтор запрещён;
  6. опрос POST /import/lists/status/<ID> {} каждые 8 с до 20 мин: Error/ErrorML → ошибка; Processing=false →
     нужны LoadedMembersCamp (целое) и SavedToClickhouse=true (при sendToMO ещё UploadedToML=true) → «confirmed».
Итог (done / error) robot.py пишет в lists.manzana. Повторный прогон: «confirmed» — только итог; «import_requested/
importing/uncertain» с ID — только опрос статуса по ID; без ID — остановка, нужен человек. Состав списка в базе
изменился после запуска импорта — остановка (SOURCE_CHANGED). Реестр — SQLite в каталоге состояния, без GUID.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import io
import re
import sqlite3
import ssl
import time
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.error import HTTPError
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
from zipfile import ZIP_DEFLATED, ZipFile
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
S3_UPLOAD_HOST_DEFAULT = "dixy-mlistimport.hb.bizmrg.com"
GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
IMPORT_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")
MAX_GUIDS = 15_000_000
RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
STATUSES = ("preparing", "uploaded", "import_requested", "importing", "awaiting_confirmation", "uncertain",
            "confirmed", "failed_preflight")


class ListImportError(RuntimeError):
    def __init__(self, message: str, code: str = "LIST_IMPORT_FAILED") -> None:
        super().__init__(message)
        self.code = code


# ---------- состав и архив ----------

def validate_list_name(value: str) -> str:
    name = str(value or "").strip()
    if not name or len(name) > 120 or len(name.encode("utf-8")) > 240:
        raise ListImportError("Название МС должно содержать от 1 до 120 символов (до 240 байт UTF-8).", code="NAME_INVALID")
    if re.search(r'[<>:"/\\|?*\x00-\x1f]', name) or name.endswith((".", " ")):
        raise ListImportError("В названии МС недопустимы символы < > : \" / \\ | ? * и точка или пробел в конце.", code="NAME_INVALID")
    if name.split(".")[0].upper() in RESERVED_NAMES:
        raise ListImportError("Название МС зарезервировано Windows.", code="NAME_INVALID")
    return name


def build_list_archive(values: Iterable[Any], list_name: str) -> tuple[bytes, int, str]:
    """Один проход по GUID из базы (уже различные, в порядке базы): каждый — строка GUID (пустой или не-GUID — ошибка:
    такой файл не принимает валидатор ДИКСИ), сразу в CSV с заголовком contactid (CRLF) внутри ZIP «<название>.csv».
    В памяти — только сжатый архив (2 млн GUID ≈ 43 МБ), GUID не ложатся на диск. Возвращает архив, число контактов и
    SHA-256 состава — по нему повтор узнаёт, что список в базе не пересобрали."""
    digest, count = hashlib.sha256(), 0
    buf = io.BytesIO()
    with ZipFile(buf, "w", compression=ZIP_DEFLATED, compresslevel=6) as z:
        with z.open(f"{list_name}.csv", "w", force_zip64=True) as out:
            out.write(b"contactid\r\n")
            batch: list[bytes] = []
            for value in values:
                g = str(value or "").strip().upper()
                count += 1
                if not GUID_RE.fullmatch(g):
                    raise ListImportError(f"Участник {count} списка — не GUID (или пустой).", code="SOURCE_INVALID")
                if count > MAX_GUIDS:
                    raise ListImportError(f"В списке больше {MAX_GUIDS} контактов.", code="SOURCE_INVALID")
                line = g.encode("ascii") + b"\r\n"
                digest.update(line)
                batch.append(line)
                if len(batch) >= 50_000:
                    out.write(b"".join(batch))
                    batch.clear()
            out.write(b"".join(batch))
    if not count:
        raise ListImportError("В списке нет контактов на отправку.", code="SOURCE_INVALID")
    return buf.getvalue(), count, digest.hexdigest()


# ---------- хранилище S3 ----------

class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102
        raise ListImportError("Хранилище перенаправило загрузку на другой адрес; отправка прекращена.", code="UPLOAD_REDIRECT")


def validate_upload_url(url: str, key: str, host: str) -> str:
    try:
        p = urlsplit(str(url))
        if (p.scheme != "https" or p.hostname != host or p.port not in (None, 443) or p.username or p.password
                or unquote(p.path) != "/mlists/" + key or p.fragment or not parse_qs(p.query).get("X-Amz-Signature")):
            raise ValueError
    except (TypeError, ValueError):
        raise ListImportError("Campaign вернул неподходящий временный адрес загрузки; файл не отправлен.", code="UPLOAD_URL") from None
    return str(url)


def upload_to_storage(api: Any, key: str, body: bytes, *, host: str) -> None:
    """GET /import/s3url → PUT архива по временной ссылке (прямым HTTPS, без редиректов)."""
    value = api.get_json_or_text("/import/s3url", params={"key": key})
    url = validate_upload_url(value if isinstance(value, str) else str(value), key, host)
    opener = build_opener(HTTPSHandler(context=ssl.create_default_context()), _NoRedirect())
    try:
        request = Request(url, data=body, method="PUT",
                          headers={"Content-Type": "application/zip", "Content-Length": str(len(body))})
        with opener.open(request, timeout=300) as response:
            if not 200 <= response.status < 300:
                raise ListImportError(f"Хранилище отклонило файл: HTTP {response.status}.", code="UPLOAD_FAILED")
            response.read(4096)
    except ListImportError:
        raise
    except Exception as exc:  # noqa: BLE001
        code = f"HTTP {exc.code}" if isinstance(exc, HTTPError) else type(exc).__name__
        raise ListImportError(f"Загрузка файла в хранилище не удалась ({code}); импорт не запускался.", code="UPLOAD_FAILED") from None


# ---------- реестр (SQLite, без GUID) ----------

class Ledger:
    """Один список (ключ «<номер>_<старт>») — одна запись: что уже сделано с ним в Campaign. Пишется ДО необратимого
    вызова. Технический реестр робота; итог для людей и портала — lists.manzana в базе списков."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("""create table if not exists list_imports (
                key text primary key, list_name text not null, source_sha256 text not null, row_count integer not null,
                send_to_mo integer not null, status text not null, import_id text not null default '',
                list_id text not null default '', loaded integer, error text not null default '',
                created_at text not null, updated_at text not null)""")

    def _db(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _now() -> str:
        return dt.datetime.now(MSK).isoformat(timespec="seconds")

    def get(self, key: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute("select * from list_imports where key = ?", (key,)).fetchone()
            return dict(row) if row else None

    def start(self, key: str, list_name: str, source_sha256: str, row_count: int, send_to_mo: bool) -> dict[str, Any]:
        now = self._now()
        with self._db() as db:
            db.execute("""insert into list_imports (key, list_name, source_sha256, row_count, send_to_mo, status, created_at, updated_at)
                          values (?, ?, ?, ?, ?, 'preparing', ?, ?)
                          on conflict(key) do update set list_name = excluded.list_name, source_sha256 = excluded.source_sha256,
                          row_count = excluded.row_count, send_to_mo = excluded.send_to_mo,
                          status = 'preparing', error = '', updated_at = excluded.updated_at
                          where list_imports.status in ('preparing', 'uploaded', 'failed_preflight')""",
                       (key, list_name, source_sha256, row_count, int(send_to_mo), now, now))
        return self.get(key) or {}

    def transition(self, key: str, status: str, **fields: Any) -> None:
        if status not in STATUSES:
            raise ValueError(f"недопустимое состояние {status}")
        sets, values = ["status = ?", "updated_at = ?"], [status, self._now()]
        for name in ("import_id", "list_id", "loaded", "error"):
            if name in fields and fields[name] is not None:
                sets.append(f"{name} = ?")
                values.append(fields[name] if name == "loaded" else str(fields[name])[:300])
        values.append(key)
        with self._db() as db:
            db.execute(f"update list_imports set {', '.join(sets)} where key = ?", values)


# ---------- импорт одного списка ----------

def _poll_status(api: Any, import_id: str, *, send_to_mo: bool, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic, timeout: int = 1200) -> dict[str, Any]:
    deadline = clock() + timeout
    while True:
        status = api.post_json(f"/import/lists/status/{import_id}", {})
        if not isinstance(status, dict):
            raise ListImportError("Campaign не вернул состояние импорта.", code="STATUS_INVALID")
        if str(status.get("Error") or "").strip() or str(status.get("ErrorML") or "").strip():
            raise ListImportError("Campaign сообщил ошибку выполнения импорта (см. страницу импорта).", code="IMPORT_ERROR")
        if status.get("Processing") is False:
            loaded = status.get("LoadedMembersCamp")
            if type(loaded) is not int or loaded < 0 or status.get("SavedToClickhouse") is not True:
                raise ListImportError("Campaign завершил обработку без подтверждённого числа загруженных контактов.", code="STATUS_INVALID")
            if not send_to_mo or status.get("UploadedToML") is True:
                return {"loaded": loaded, "total": status.get("TotalMembers"), "not_uploaded": status.get("NotUploaded"),
                        "uploaded_to_mo": bool(status.get("UploadedToML")) if send_to_mo else False}
        if clock() >= deadline:
            raise TimeoutError("время ожидания результата импорта истекло; повторный запуск запрещён")
        sleep(8)


def import_guids(key: str, list_name: str, archive: bytes, count: int, source_sha256: str, *, api: Any, ledger: Ledger,
                 send_to_mo: bool = False, s3_host: str = S3_UPLOAD_HOST_DEFAULT, uploader: Callable[..., None] = upload_to_storage,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    """Весь путь одного списка (архив и SHA-256 — build_list_archive с тем же list_name). Возвращает словарь результата (без GUID).
    Повторный вызов безопасен: подтверждённый не грузится второй раз, запущенный — только опрашивается."""
    saved = ledger.get(key)
    if saved and saved["status"] in ("confirmed", "import_requested", "importing", "awaiting_confirmation", "uncertain"):
        if saved["source_sha256"] != source_sha256:
            ledger.transition(key, saved["status"], error="состав списка в базе изменился после запуска импорта")
            raise ListImportError(f"{key}: состав списка в базе изменился после запуска импорта «{saved['list_name']}»; "
                                  "второй раз не грузится, нужен человек.", code="SOURCE_CHANGED")
    # Уже подтверждён — только итог.
    if saved and saved["status"] == "confirmed" and type(saved["loaded"]) is int:
        return {"key": key, "name": saved["list_name"], "loaded": saved["loaded"], "import_id": saved["import_id"],
                "already_confirmed": True}
    # Импорт был запущен — только проверка по ID, без второго POST.
    if saved and saved["status"] in ("import_requested", "importing", "awaiting_confirmation", "uncertain"):
        if not IMPORT_ID_RE.fullmatch(saved["import_id"] or ""):
            raise ListImportError(f"Импорт «{saved['list_name']}» мог быть запущен, но ID не сохранён: повтор запрещён, "
                                  "проверьте список импортов Campaign вручную.", code="UNKNOWN_OUTCOME")
        try:
            result = _poll_status(api, saved["import_id"], send_to_mo=bool(saved["send_to_mo"]), sleep=sleep, clock=clock)
        except TimeoutError:
            ledger.transition(key, "awaiting_confirmation", error="результат ещё не подтверждён")
            raise ListImportError("Импорт запущен, результат ещё не подтверждён; повтор запрещён.", code="AWAITING") from None
        ledger.transition(key, "confirmed", loaded=result["loaded"])
        return {"key": key, "name": saved["list_name"], "loaded": result["loaded"], "not_uploaded": result.get("not_uploaded"),
                "import_id": saved["import_id"], "resumed": True}
    # Новый список.
    list_name = validate_list_name(list_name)
    if send_to_mo and len(list_name) > 50:
        raise ListImportError(f"Для ManzanaOnline название «{list_name}» длиннее 50 знаков; обрезать нельзя.", code="NAME_INVALID")
    ledger.start(key, list_name, source_sha256, count, send_to_mo)
    try:
        body = api.post_json("/mlist/exist", {"Name": list_name, "LoadType": 4, "mListMode": 3})
        if not isinstance(body, dict) or type(body.get("exist")) is not bool:
            raise ListImportError("Campaign не подтвердил проверку существования МС.", code="STATUS_INVALID")
        if body["exist"]:
            raise ListImportError(f"МС «{list_name}» уже существует в Campaign; автоматический переимпорт запрещён.", code="LIST_EXISTS")
        archive_key = f"{list_name}.zip"
        uploader(api, archive_key, archive, host=s3_host)
        ledger.transition(key, "uploaded")
        # Write-ahead: отметка ДО необратимого вызова.
        ledger.transition(key, "import_requested")
        try:
            reply = api.put_json("/import/s3import", {"key": archive_key, "importType": "contact_id", "loadType": 4,
                                                      "destination": "campml" if send_to_mo else "camp", "nameMList": list_name,
                                                      "mListMode": 3, "sendToMO": bool(send_to_mo)})
        except Exception as exc:  # noqa: BLE001 — ответа нет: импорт мог начаться
            ledger.transition(key, "uncertain", error=f"ответ запуска импорта отсутствует: {type(exc).__name__}")
            raise ListImportError("Нет ответа на запуск импорта; повтор запрещён — нужна проверка в Campaign.", code="UNKNOWN_OUTCOME") from exc
        if not isinstance(reply, dict) or not IMPORT_ID_RE.fullmatch(str(reply.get("ID") or "")):
            ledger.transition(key, "uncertain", error="Campaign не вернул ID импорта")
            raise ListImportError("Campaign не подтвердил ID импорта; повторный запуск запрещён.", code="UNKNOWN_OUTCOME")
        if reply.get("Name") not in (None, list_name):
            ledger.transition(key, "uncertain", error="Campaign вернул другое название МС")
            raise ListImportError("Campaign вернул другое название МС; повторный запуск запрещён.", code="UNKNOWN_OUTCOME")
        import_id = str(reply["ID"])
        ledger.transition(key, "importing", import_id=import_id, list_id=str(reply.get("ListID") or reply.get("ExtID") or ""))
        try:
            result = _poll_status(api, import_id, send_to_mo=send_to_mo, sleep=sleep, clock=clock)
        except TimeoutError:
            ledger.transition(key, "awaiting_confirmation", error="импорт начат, результат ещё не подтверждён")
            raise ListImportError("Время ожидания истекло; импорт запущен, повторять нельзя.", code="AWAITING") from None
        except Exception as exc:
            ledger.transition(key, "awaiting_confirmation", error=f"состояние не подтверждено: {type(exc).__name__}")
            raise
        ledger.transition(key, "confirmed", loaded=result["loaded"])
    except ListImportError as exc:
        current = ledger.get(key) or {}
        if current.get("status") in ("preparing", "uploaded"):
            ledger.transition(key, "failed_preflight", error=str(exc))
        raise
    return {"key": key, "name": list_name, "source_rows": count, "loaded": result["loaded"],
            "not_uploaded": result.get("not_uploaded"), "import_id": import_id,
            "import_url": f"https://campaign.manzanagroup.ru/imports/view/{import_id}"}
