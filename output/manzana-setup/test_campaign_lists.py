"""Тесты campaign_lists.py без сети: подставной API Campaign, подставная загрузка в хранилище, файлы во временном каталоге."""
import hashlib
import json
import sys
import uuid
from pathlib import Path
from zipfile import ZipFile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import campaign_lists as cl  # noqa: E402


def put_list(inbox: Path, number=101430, start="20261023", rows=3, title="Кешбэк на кофе", bad_sha=False, bad_count=False, empty_row=False):
    lines = [str(uuid.uuid4()).upper() + "\r\n" for _ in range(rows)]
    if empty_row:
        lines.insert(1, "\r\n")
    body = "".join(lines).encode("utf-8")
    name = f"{number}_{start}.csv"
    (inbox / name).write_bytes(body)
    manifest = {"promo": number, "title": title, "start_date": "2026-10-23", "approved_by": "elena", "count": rows + (1 if bad_count else 0),
                "sha256": ("0" * 64) if bad_sha else hashlib.sha256(body).hexdigest(), "file": name, "exported_at": "2026-10-20T10:00:00+03:00"}
    (inbox / f"{number}_{start}.manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return inbox / name


class Api:
    def __init__(self, *, exists=False, lose_import=False, missing_id=False, loaded=3, send_mo=False, error=""):
        self.exists, self.lose_import, self.missing_id, self.loaded, self.send_mo, self.error = exists, lose_import, missing_id, loaded, send_mo, error
        self.calls = []

    def get_json_or_text(self, path, params=None):
        self.calls.append(("GET", path, params))
        assert path == "/import/s3url"
        return f"https://{cl.S3_UPLOAD_HOST_DEFAULT}/mlists/{params['key']}?X-Amz-Signature=abc"

    def post_json(self, path, body, params=None):
        self.calls.append(("POST", path, body))
        if path == "/mlist/exist":
            return {"exist": self.exists}
        if path.startswith("/import/lists/status/"):
            return {"Processing": False, "SavedToClickhouse": True, "LoadedMembersCamp": self.loaded, "TotalMembers": 3,
                    "NotUploaded": 3 - self.loaded, "Error": self.error, "ErrorML": "", "UploadedToML": self.send_mo}
        raise AssertionError(path)

    def put_json(self, path, body, params=None):
        self.calls.append(("PUT", path, body))
        assert path == "/import/s3import"
        if self.lose_import:
            raise TimeoutError("no response")
        if self.missing_id:
            return {"Name": body["nameMList"]}
        return {"ID": "0123456789abcdef01234567", "Name": body["nameMList"], "ListID": "list-guid"}


def fake_uploader(api, archive, *, host):
    api.calls.append(("UPLOAD", archive.name, host))
    with ZipFile(archive) as z:
        assert z.namelist() == [archive.stem + ".csv"]
        content = z.read(archive.stem + ".csv")
        assert content.startswith(b"contactid\r\n") and content.count(b"\r\n") == 4


@pytest.fixture
def env(tmp_path):
    inbox, receipts, work = tmp_path / "inbox", tmp_path / "receipts", tmp_path / "work"
    for d in (inbox, receipts, work):
        d.mkdir()
    return inbox, receipts, work, cl.Ledger(tmp_path / "state" / "imports.sqlite3")


def run(path, api, ledger, receipts, work, **kw):
    return cl.import_list(path, api=api, ledger=ledger, receipts=receipts, workdir=work, uploader=fake_uploader, sleep=lambda s: None, **kw)


def test_full_import_writes_ok_receipt_and_single_post(env):
    inbox, receipts, work, ledger = env
    path = put_list(inbox)
    api = Api(loaded=2)
    r = run(path, api, ledger, receipts, work)
    assert r["name"] == "101430_Кешбэк на кофе" and r["loaded"] == 2 and r["source_rows"] == 3 and r["receipt_written"]
    put = [c for c in api.calls if c[0] == "PUT"]
    assert len(put) == 1 and put[0][2] == {"key": "101430_Кешбэк на кофе.zip", "importType": "contact_id", "loadType": 4, "destination": "camp",
                                           "nameMList": "101430_Кешбэк на кофе", "mListMode": 3, "sendToMO": False}
    ok = json.loads((receipts / "101430_20261023.ok.json").read_text(encoding="utf-8"))
    assert ok["campaign_name"] == "101430_Кешбэк на кофе" and ok["loaded"] == 2 and set(ok) == {"file", "campaign_name", "loaded", "loaded_at"}
    assert ledger.get(path.name)["status"] == "confirmed" and not list(work.iterdir())
    # повтор — только квитанция, без запросов к Campaign
    before = len(api.calls)
    r2 = run(path, api, ledger, receipts, work)
    assert r2["already_confirmed"] and len(api.calls) == before
    assert cl.pending_files(inbox, receipts) == []


def test_sha_mismatch_writes_error_receipt_without_campaign_calls(env):
    inbox, receipts, work, ledger = env
    path = put_list(inbox, bad_sha=True)
    api = Api()
    with pytest.raises(cl.SourceIntegrityError):
        run(path, api, ledger, receipts, work)
    assert api.calls == [] and (receipts / "101430_20261023.error.json").is_file()
    assert ledger.get(path.name)["status"] == "failed_preflight"


def test_empty_guid_row_is_source_error(env):
    inbox, receipts, work, ledger = env
    path = put_list(inbox, empty_row=True)
    with pytest.raises(cl.SourceIntegrityError, match="не GUID"):
        run(path, Api(), ledger, receipts, work)


def test_existing_list_is_not_reimported(env):
    inbox, receipts, work, ledger = env
    path = put_list(inbox)
    api = Api(exists=True)
    with pytest.raises(cl.ListImportError, match="уже существует"):
        run(path, api, ledger, receipts, work)
    assert not [c for c in api.calls if c[0] in ("PUT", "UPLOAD")] and not list(receipts.iterdir())
    assert ledger.get(path.name)["status"] == "failed_preflight"


def test_lost_import_response_never_repeats_post(env):
    inbox, receipts, work, ledger = env
    path = put_list(inbox)
    api = Api(lose_import=True)
    with pytest.raises(cl.ListImportError) as e:
        run(path, api, ledger, receipts, work)
    assert e.value.code == "UNKNOWN_OUTCOME" and ledger.get(path.name)["status"] == "uncertain"
    api2 = Api()
    with pytest.raises(cl.ListImportError) as e2:
        run(path, api2, ledger, receipts, work)
    assert e2.value.code == "UNKNOWN_OUTCOME" and api2.calls == []


def test_resume_by_import_id_only_polls(env):
    inbox, receipts, work, ledger = env
    path = put_list(inbox)
    m = cl.read_manifest(path)
    sha, count = cl.verify_source(path, m)
    ledger.start(path.name, "101430_Кешбэк на кофе", m["manifest_sha256"], sha, count, False)
    ledger.transition(path.name, "importing", import_id="0123456789abcdef01234567")
    api = Api(loaded=3)
    r = run(path, api, ledger, receipts, work)
    assert r["resumed"] and r["loaded"] == 3 and [c[0] for c in api.calls] == ["POST"]
    assert (receipts / "101430_20261023.ok.json").is_file()


def test_resume_refuses_changed_file(env):
    inbox, receipts, work, ledger = env
    path = put_list(inbox)
    m = cl.read_manifest(path)
    sha, count = cl.verify_source(path, m)
    ledger.start(path.name, "101430_Кешбэк на кофе", m["manifest_sha256"], sha, count, False)
    ledger.transition(path.name, "importing", import_id="0123456789abcdef01234567")
    put_list(inbox, rows=4)  # задание списков пересобрало файл и паспорт
    api = Api(loaded=4)
    with pytest.raises(cl.ListImportError) as e:
        run(path, api, ledger, receipts, work)
    assert e.value.code == "SOURCE_CHANGED" and api.calls == [] and not list(receipts.iterdir())
    assert ledger.get(path.name)["status"] == "importing"


def test_foreign_ok_receipt_is_not_overwritten(env):
    inbox, receipts, work, ledger = env
    path = put_list(inbox)
    (receipts / "101430_20261023.ok.json").write_text(json.dumps({"file": path.name, "campaign_name": "other", "loaded": 1, "loaded_at": "x"}))
    assert cl.pending_files(inbox, receipts) == []
    r = run(path, Api(), ledger, receipts, work)
    assert r["receipt_written"] is False and "перезапись" in r["receipt_error"]
    assert json.loads((receipts / "101430_20261023.ok.json").read_text())["campaign_name"] == "other"


def test_upload_url_validation():
    key = "101430_x.zip"
    good = f"https://{cl.S3_UPLOAD_HOST_DEFAULT}/mlists/{key}?X-Amz-Signature=s"
    assert cl.validate_upload_url(good, key, cl.S3_UPLOAD_HOST_DEFAULT) == good
    for bad in (good.replace("https", "http"), f"https://evil.example/mlists/{key}?X-Amz-Signature=s",
                f"https://{cl.S3_UPLOAD_HOST_DEFAULT}/other/{key}?X-Amz-Signature=s", f"https://{cl.S3_UPLOAD_HOST_DEFAULT}/mlists/{key}"):
        with pytest.raises(cl.ListImportError):
            cl.validate_upload_url(bad, key, cl.S3_UPLOAD_HOST_DEFAULT)


def test_list_name_rules():
    assert cl.validate_list_name("101430_Кешбэк на кофе") == "101430_Кешбэк на кофе"
    for bad in ("", "a/b", "name.", "CON", "x" * 121):
        with pytest.raises(cl.ListImportError):
            cl.validate_list_name(bad)
