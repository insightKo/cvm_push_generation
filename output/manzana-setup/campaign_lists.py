#!/usr/bin/env python3
"""Загрузка списка CRM_GUID из нашего обмена в Manzana Campaign — тот же протокол, что у программы ДИКСИ
(Dixy CRM Tools 0.74.21, lib/sftp_exchange/campaign_import.py, receipts.py, scanner.py), написанный заново под сервер ru
и учётку робота. Чужой код сюда не копируется — только порядок вызовов и защиты, снятые с него при разборе 20.10.2026.

Порядок одного файла (<номер>_<ГГГГММДД>.csv + .manifest.json в inbox/ нашего хранилища):
  1. паспорт: file/promo/start_date совпадают с именем, count и sha256 совпадают с файлом, каждая строка — GUID;
     название МС = «<promo>_<title>» (+ постфикс), не длиннее 120 знаков / 240 байт, без символов имён файлов Windows;
  2. POST /mlist/exist {Name, LoadType: 4, mListMode: 3} → exist должно быть false (существующий МС не перезаписывается);
  3. файл для Campaign: CSV с заголовком contactid (CRLF) в ZIP «<название>.zip», во временной папке 700;
  4. GET /import/s3url?key=<название>.zip → временная ссылка хранилища (только https, хост S3_UPLOAD_HOST,
     путь /mlists/<key>, подпись X-Amz-Signature); PUT тела напрямую, без редиректов;
  5. write-ahead «import_requested» в реестр, затем ОДИН PUT /import/s3import {key, importType: contact_id, loadType: 4,
     destination: camp|campml, nameMList, mListMode: 3, sendToMO}; ответ без ID (24 hex) — «uncertain», повтор запрещён;
  6. опрос POST /import/lists/status/<ID> {} каждые 8 с до 20 мин: Error/ErrorML → ошибка; Processing=false →
     нужны LoadedMembersCamp (целое) и SavedToClickhouse=true (при sendToMO ещё UploadedToML=true) → «confirmed»;
  7. квитанция receipts/<файл>.ok.json {file, campaign_name = название МС, loaded, loaded_at} (.part → rename);
     .error.json — только при расхождении файла с паспортом (sha256/count), не при сбоях сети;
     при существующей чужой OK-квитанции с другими данными ничего не перезаписывается.
Повторный прогон: «confirmed» без квитанции — только квитанция; «import_requested/importing/uncertain» с ID — только опрос
статуса по ID; без ID — остановка, нужен человек. Реестр — SQLite в каталоге состояния, без GUID.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sqlite3
import ssl
import tempfile
import time
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
from zipfile import ZIP_DEFLATED, ZipFile
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
S3_UPLOAD_HOST_DEFAULT = "dixy-mlistimport.hb.bizmrg.com"
MCI_FILENAME = re.compile(r"^(\d{1,20})_(\d{8})\.csv$", re.I)
GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
IMPORT_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")
MAX_CSV_BYTES = 300 * 1024 * 1024
RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
STATUSES = ("queued", "preparing", "uploaded", "import_requested", "importing", "awaiting_confirmation", "uncertain",
            "confirmed", "failed_preflight", "failed_remote")


class ListImportError(RuntimeError):
    def __init__(self, message: str, code: str = "LIST_IMPORT_FAILED") -> None:
        super().__init__(message)
        self.code = code


class SourceIntegrityError(ListImportError):
    """Файл не совпадает с паспортом — единственный случай, когда пишется .error.json."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="SOURCE_INTEGRITY")


# ---------- паспорт и файл ----------

def validate_list_name(value: str) -> str:
    name = str(value or "").strip()
    if not name or len(name) > 120 or len(name.encode("utf-8")) > 240:
        raise ListImportError("Название МС должно содержать от 1 до 120 символов (до 240 байт UTF-8).", code="NAME_INVALID")
    if re.search(r'[<>:"/\\|?*\x00-\x1f]', name) or name.endswith((".", " ")):
        raise ListImportError("В названии МС недопустимы символы < > : \" / \\ | ? * и точка или пробел в конце.", code="NAME_INVALID")
    if name.split(".")[0].upper() in RESERVED_NAMES:
        raise ListImportError("Название МС зарезервировано Windows.", code="NAME_INVALID")
    return name


def read_manifest(csv_path: Path) -> dict[str, Any]:
    """Паспорт рядом с файлом: те же проверки, что у ДИКСИ (scanner._manifest)."""
    match = MCI_FILENAME.fullmatch(csv_path.name)
    if not match:
        raise ListImportError("Имя CSV должно иметь вид НОМЕР_ГГГГММДД.csv.", code="NAME_INVALID")
    promo, yyyymmdd = match.groups()
    manifest_path = csv_path.with_suffix("").with_suffix(".manifest.json")
    raw = manifest_path.read_bytes()
    if len(raw) > 64 * 1024:
        raise ListImportError("Паспорт списка больше 64 КБ.", code="MANIFEST_INVALID")
    data = json.loads(raw.decode("utf-8-sig"))
    if not isinstance(data, dict):
        raise ListImportError("Паспорт списка — не объект JSON.", code="MANIFEST_INVALID")
    if str(data.get("file") or "") != csv_path.name or str(data.get("promo") or "") != promo:
        raise ListImportError("Паспорт содержит другой файл или номер акции.", code="MANIFEST_INVALID")
    try:
        if dt.date.fromisoformat(str(data["start_date"])).strftime("%Y%m%d") != yyyymmdd:
            raise ListImportError("Дата старта в паспорте не совпадает с именем файла.", code="MANIFEST_INVALID")
    except (KeyError, TypeError, ValueError):
        raise ListImportError("В паспорте нет корректной даты старта.", code="MANIFEST_INVALID") from None
    title = str(data.get("title") or "").strip()
    if not title or len(title) > 300 or any(ord(c) < 32 for c in title):
        raise ListImportError("В паспорте нет корректного названия акции.", code="MANIFEST_INVALID")
    count = data.get("count")
    if type(count) is not int or count < 0 or count > 15_000_000:
        raise ListImportError("В паспорте некорректное число контактов.", code="MANIFEST_INVALID")
    sha = str(data.get("sha256") or "").lower()
    if not SHA256_RE.fullmatch(sha):
        raise ListImportError("В паспорте нет корректной SHA-256.", code="MANIFEST_INVALID")
    if not str(data.get("exported_at") or "").strip() or not str(data.get("approved_by") or "").strip():
        raise ListImportError("В паспорте нет времени формирования или согласовавшего.", code="MANIFEST_INVALID")
    return {"promo": promo, "title": title, "count": count, "sha256": sha, "manifest_sha256": hashlib.sha256(raw).hexdigest()}


def verify_source(csv_path: Path, manifest: dict[str, Any]) -> tuple[str, int]:
    """SHA-256 исходных байтов и число строк-GUID; расхождение с паспортом — SourceIntegrityError."""
    size = csv_path.stat().st_size
    if size < 1 or size > MAX_CSV_BYTES:
        raise SourceIntegrityError("Размер CSV вне допустимого диапазона 1 байт – 300 МиБ.")
    digest = hashlib.sha256()
    with open(csv_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 18), b""):
            digest.update(chunk)
    count = 0
    try:
        with open(csv_path, "r", encoding="utf-8-sig", errors="strict", newline="") as f:
            for index, fields in enumerate(csv.reader(f, strict=True), 1):
                if len(fields) != 1 or not GUID_RE.fullmatch(fields[0].strip()):
                    raise SourceIntegrityError(f"Строка {index} CSV — не GUID (или пустая).")
                count += 1
    except (UnicodeError, csv.Error) as exc:
        raise SourceIntegrityError(f"Кодировка или формат CSV: {type(exc).__name__}.") from exc
    if count <= 0:
        raise SourceIntegrityError("CSV не содержит контактов.")
    if digest.hexdigest() != manifest["sha256"]:
        raise SourceIntegrityError("SHA-256 CSV не совпадает с паспортом.")
    if count != manifest["count"]:
        raise SourceIntegrityError(f"Строк в CSV {count}, в паспорте {manifest['count']}.")
    return digest.hexdigest(), count


def build_campaign_zip(csv_path: Path, list_name: str, workdir: Path) -> Path:
    """CSV с заголовком contactid (CRLF) в ZIP — формат, который принимает импорт Campaign."""
    tmp = Path(tempfile.mkdtemp(prefix="import-", dir=str(workdir)))
    try:
        tmp.chmod(0o700)
        prepared = tmp / f"{list_name}.csv"
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as src, open(prepared, "w", encoding="utf-8", newline="") as out:
            writer = csv.writer(out, lineterminator="\r\n")
            writer.writerow(["contactid"])
            for row in csv.reader(src, strict=True):
                writer.writerow([row[0].strip()])
        archive = tmp / f"{list_name}.zip"
        with ZipFile(archive, "w", compression=ZIP_DEFLATED, compresslevel=6) as z:
            z.write(prepared, arcname=prepared.name)
        prepared.unlink(missing_ok=True)
        return archive
    except BaseException:
        # Папка с GUID не должна пережить сбой сборки (место на диске, ошибка формата, остановка).
        shutil.rmtree(tmp, ignore_errors=True)
        raise


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


def upload_to_storage(api: Any, archive: Path, *, host: str) -> None:
    """GET /import/s3url → PUT архива по временной ссылке (прямым HTTPS, без редиректов)."""
    key = archive.name
    value = api.get_json_or_text("/import/s3url", params={"key": key})
    url = validate_upload_url(value if isinstance(value, str) else str(value), key, host)
    opener = build_opener(HTTPSHandler(context=ssl.create_default_context()), _NoRedirect())
    size = archive.stat().st_size
    try:
        with open(archive, "rb") as stream:
            request = Request(url, data=stream, method="PUT",
                              headers={"Content-Type": "application/zip", "Content-Length": str(size)})
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
    """Один файл — одна запись: что уже сделано с ним в Campaign. Пишется ДО необратимого вызова."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("""create table if not exists imports (
                file text primary key, list_name text not null, manifest_sha256 text not null, source_sha256 text not null,
                row_count integer not null, send_to_mo integer not null, status text not null, import_id text not null default '',
                list_id text not null default '', loaded integer, error text not null default '',
                receipt_state text not null default '', created_at text not null, updated_at text not null)""")

    def _db(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _now() -> str:
        return dt.datetime.now(MSK).isoformat(timespec="seconds")

    def get(self, file: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute("select * from imports where file = ?", (file,)).fetchone()
            return dict(row) if row else None

    def start(self, file: str, list_name: str, manifest_sha256: str, source_sha256: str, row_count: int, send_to_mo: bool) -> dict[str, Any]:
        now = self._now()
        with self._db() as db:
            db.execute("""insert into imports (file, list_name, manifest_sha256, source_sha256, row_count, send_to_mo, status, created_at, updated_at)
                          values (?, ?, ?, ?, ?, ?, 'preparing', ?, ?)
                          on conflict(file) do update set list_name = excluded.list_name, manifest_sha256 = excluded.manifest_sha256,
                          source_sha256 = excluded.source_sha256, row_count = excluded.row_count, send_to_mo = excluded.send_to_mo,
                          status = 'preparing', error = '', updated_at = excluded.updated_at
                          where imports.status in ('queued', 'preparing', 'uploaded', 'failed_preflight')""",
                       (file, list_name, manifest_sha256, source_sha256, row_count, int(send_to_mo), now, now))
        return self.get(file) or {}

    def transition(self, file: str, status: str, **fields: Any) -> None:
        if status not in STATUSES:
            raise ValueError(f"недопустимое состояние {status}")
        sets, values = ["status = ?", "updated_at = ?"], [status, self._now()]
        for key in ("import_id", "list_id", "loaded", "error", "receipt_state"):
            if key in fields and fields[key] is not None:
                sets.append(f"{key} = ?")
                values.append(fields[key] if key == "loaded" else str(fields[key])[:300])
        values.append(file)
        with self._db() as db:
            db.execute(f"update imports set {', '.join(sets)} where file = ?", values)

    def receipt(self, file: str, state: str) -> None:
        with self._db() as db:
            db.execute("update imports set receipt_state = ?, updated_at = ? where file = ?", (state, self._now(), file))


# ---------- квитанции (ТЗ обмена, раздел 7) ----------

def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    part = path.with_name(path.name + ".part")
    part.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    os.chmod(part, 0o600)
    part.replace(path)


def acknowledge_success(receipts: Path, file: str, list_name: str, loaded: int) -> bool:
    """OK-квитанция; совпадающая существующая — принимается, другая — не трогается (ошибка)."""
    ok_path, err_path = receipts / f"{file[:-4]}.ok.json", receipts / f"{file[:-4]}.error.json"
    if ok_path.is_file():
        prior = json.loads(ok_path.read_text(encoding="utf-8"))
        if (prior.get("file"), prior.get("campaign_name"), prior.get("loaded")) != (file, list_name, loaded):
            raise ListImportError("В receipts уже есть другая OK-квитанция для этого файла; перезапись запрещена.", code="RECEIPT_CONFLICT")
        written = False
    else:
        receipts.mkdir(mode=0o700, parents=True, exist_ok=True)
        _write_atomic(ok_path, {"file": file, "campaign_name": list_name, "loaded": int(loaded),
                                "loaded_at": dt.datetime.now(MSK).isoformat(timespec="seconds")})
        written = True
    if err_path.is_file():
        err_path.unlink()
    return written


def acknowledge_error(receipts: Path, file: str, message: str) -> bool:
    """ERROR-квитанция только при расхождении файла с паспортом; при существующей OK — запрещена."""
    ok_path, err_path = receipts / f"{file[:-4]}.ok.json", receipts / f"{file[:-4]}.error.json"
    if ok_path.is_file():
        raise ListImportError("Есть OK-квитанция; ERROR записывать запрещено.", code="RECEIPT_CONFLICT")
    if err_path.is_file():
        return False
    receipts.mkdir(mode=0o700, parents=True, exist_ok=True)
    _write_atomic(err_path, {"file": file, "error": str(message)[:250], "at": dt.datetime.now(MSK).isoformat(timespec="seconds")})
    return True


# ---------- импорт одного файла ----------

def _verify_unchanged(csv_path: Path, saved: dict[str, Any], ledger: Ledger) -> None:
    """Возобновление и квитанция — только для того же файла, что импортировался: задание списков могло пересобрать
    <номер>_<старт>.csv с новым составом и паспортом, пока шёл импорт (у ДИКСИ — сверка revision и SHA-256)."""
    manifest = read_manifest(csv_path)
    changed = []
    if saved.get("manifest_sha256") and manifest["manifest_sha256"] != saved["manifest_sha256"]:
        changed.append("паспорт")
    if saved.get("source_sha256"):
        digest = hashlib.sha256()
        with open(csv_path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 18), b""):
                digest.update(chunk)
        if digest.hexdigest() != saved["source_sha256"]:
            changed.append("файл")
    if changed:
        ledger.transition(saved["file"], saved["status"], error="файл в inbox изменился после запуска импорта: " + ", ".join(changed))
        raise ListImportError(f"{saved['file']}: {', '.join(changed)} в inbox изменился после запуска импорта «{saved['list_name']}» "
                              "(пересборка списка?); квитанция не выдана, нужен человек.", code="SOURCE_CHANGED")


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


def import_list(csv_path: Path, *, api: Any, ledger: Ledger, receipts: Path, workdir: Path, postfix: str = "",
                send_to_mo: bool = False, s3_host: str = S3_UPLOAD_HOST_DEFAULT, uploader: Callable[..., None] = upload_to_storage,
                sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    """Весь путь одного файла. Возвращает словарь результата (без GUID). Повторный вызов безопасен."""
    file = csv_path.name
    saved = ledger.get(file)
    if saved and saved["status"] in ("confirmed", "import_requested", "importing", "awaiting_confirmation", "uncertain"):
        _verify_unchanged(csv_path, saved, ledger)
    # Уже подтверждён — только квитанция.
    if saved and saved["status"] == "confirmed" and type(saved["loaded"]) is int:
        written = acknowledge_success(receipts, file, saved["list_name"], saved["loaded"])
        ledger.receipt(file, "sent")
        return {"file": file, "name": saved["list_name"], "loaded": saved["loaded"], "import_id": saved["import_id"],
                "receipt_written": written, "already_confirmed": True}
    # Импорт был запущен — только проверка по ID, без второго POST.
    if saved and saved["status"] in ("import_requested", "importing", "awaiting_confirmation", "uncertain"):
        if not IMPORT_ID_RE.fullmatch(saved["import_id"] or ""):
            raise ListImportError(f"Импорт «{saved['list_name']}» мог быть запущен, но ID не сохранён: повтор запрещён, "
                                  "проверьте список импортов Campaign вручную.", code="UNKNOWN_OUTCOME")
        try:
            result = _poll_status(api, saved["import_id"], send_to_mo=bool(saved["send_to_mo"]), sleep=sleep, clock=clock)
        except TimeoutError:
            ledger.transition(file, "awaiting_confirmation", error="результат ещё не подтверждён")
            raise ListImportError("Импорт запущен, результат ещё не подтверждён; повтор запрещён.", code="AWAITING") from None
        ledger.transition(file, "confirmed", loaded=result["loaded"])
        written = acknowledge_success(receipts, file, saved["list_name"], result["loaded"])
        ledger.receipt(file, "sent")
        return {"file": file, "name": saved["list_name"], "loaded": result["loaded"], "import_id": saved["import_id"],
                "receipt_written": written, "resumed": True}
    # Новый файл: паспорт, сверка, имя.
    manifest = read_manifest(csv_path)
    try:
        sha, count = verify_source(csv_path, manifest)
    except SourceIntegrityError as exc:
        ledger.start(file, f"{manifest['promo']}_{manifest['title']}", manifest["manifest_sha256"], "", 0, send_to_mo)
        ledger.transition(file, "failed_preflight", error=str(exc))
        try:
            acknowledge_error(receipts, file, str(exc))
        except ListImportError:
            pass
        raise
    list_name = validate_list_name(f"{manifest['promo']}_{manifest['title']}{postfix}")
    if send_to_mo and len(list_name) > 50:
        raise ListImportError(f"Для ManzanaOnline название «{list_name}» длиннее 50 знаков; обрезать нельзя.", code="NAME_INVALID")
    ledger.start(file, list_name, manifest["manifest_sha256"], sha, count, send_to_mo)
    try:
        body = api.post_json("/mlist/exist", {"Name": list_name, "LoadType": 4, "mListMode": 3})
        if not isinstance(body, dict) or type(body.get("exist")) is not bool:
            raise ListImportError("Campaign не подтвердил проверку существования МС.", code="STATUS_INVALID")
        if body["exist"]:
            raise ListImportError(f"МС «{list_name}» уже существует в Campaign; автоматический переимпорт запрещён.", code="LIST_EXISTS")
        archive = build_campaign_zip(csv_path, list_name, workdir)
        try:
            uploader(api, archive, host=s3_host)
            ledger.transition(file, "uploaded")
            # Write-ahead: отметка ДО необратимого вызова.
            ledger.transition(file, "import_requested")
            try:
                reply = api.put_json("/import/s3import", {"key": archive.name, "importType": "contact_id", "loadType": 4,
                                                          "destination": "campml" if send_to_mo else "camp", "nameMList": list_name,
                                                          "mListMode": 3, "sendToMO": bool(send_to_mo)})
            except Exception as exc:  # noqa: BLE001 — ответа нет: импорт мог начаться
                ledger.transition(file, "uncertain", error=f"ответ запуска импорта отсутствует: {type(exc).__name__}")
                raise ListImportError("Нет ответа на запуск импорта; повтор запрещён — нужна проверка в Campaign.", code="UNKNOWN_OUTCOME") from exc
            if not isinstance(reply, dict) or not IMPORT_ID_RE.fullmatch(str(reply.get("ID") or "")):
                ledger.transition(file, "uncertain", error="Campaign не вернул ID импорта")
                raise ListImportError("Campaign не подтвердил ID импорта; повторный запуск запрещён.", code="UNKNOWN_OUTCOME")
            if reply.get("Name") not in (None, list_name):
                ledger.transition(file, "uncertain", error="Campaign вернул другое название МС")
                raise ListImportError("Campaign вернул другое название МС; повторный запуск запрещён.", code="UNKNOWN_OUTCOME")
            import_id = str(reply["ID"])
            ledger.transition(file, "importing", import_id=import_id, list_id=str(reply.get("ListID") or reply.get("ExtID") or ""))
        finally:
            shutil.rmtree(archive.parent, ignore_errors=True)
        try:
            result = _poll_status(api, import_id, send_to_mo=send_to_mo, sleep=sleep, clock=clock)
        except TimeoutError:
            ledger.transition(file, "awaiting_confirmation", error="импорт начат, результат ещё не подтверждён")
            raise ListImportError("Время ожидания истекло; импорт запущен, повторять нельзя.", code="AWAITING") from None
        except Exception as exc:
            ledger.transition(file, "awaiting_confirmation", error=f"состояние не подтверждено: {type(exc).__name__}")
            raise
        ledger.transition(file, "confirmed", loaded=result["loaded"])
    except ListImportError as exc:
        current = ledger.get(file) or {}
        if current.get("status") in ("preparing", "uploaded"):
            ledger.transition(file, "failed_preflight", error=str(exc))
        raise
    try:
        written = acknowledge_success(receipts, file, list_name, result["loaded"])
        ledger.receipt(file, "sent")
    except ListImportError as exc:
        ledger.receipt(file, "failed")
        return {"file": file, "name": list_name, "loaded": result["loaded"], "not_uploaded": result.get("not_uploaded"),
                "import_id": import_id, "receipt_written": False, "receipt_error": str(exc)}
    return {"file": file, "name": list_name, "source_rows": count, "loaded": result["loaded"], "not_uploaded": result.get("not_uploaded"),
            "import_id": import_id, "import_url": f"https://campaign.manzanagroup.ru/imports/view/{import_id}",
            "receipt_written": written}


def pending_files(inbox: Path, receipts: Path) -> list[Path]:
    """Файлы с паспортом, по которым ещё нет квитанции."""
    out = []
    for p in sorted(inbox.glob("*.csv")):
        if not MCI_FILENAME.fullmatch(p.name) or not p.with_suffix("").with_suffix(".manifest.json").is_file():
            continue
        if (receipts / f"{p.name[:-4]}.ok.json").is_file() or (receipts / f"{p.name[:-4]}.error.json").is_file():
            continue
        out.append(p)
    return out
