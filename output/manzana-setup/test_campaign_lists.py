"""Тесты campaign_lists.py без сети: подставной API Campaign, подставная загрузка в хранилище, реестр во временном каталоге.
Список — GUID из базы списков (как их отдаёт robot.ListsDb.guids), без файлов на диске."""
import io
import sys
import uuid
from pathlib import Path
from zipfile import ZipFile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import campaign_lists as cl  # noqa: E402

KEY, NAME = "500001_20261023", "500001_Кешбэк на кофе"


def guids(n=3):
    return [str(uuid.uuid4()) for _ in range(n)]


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


def fake_uploader(api, key, body, *, host):
    api.calls.append(("UPLOAD", key, host))
    with ZipFile(io.BytesIO(body)) as z:
        assert z.namelist() == [key[:-4] + ".csv"]
        content = z.read(key[:-4] + ".csv")
    assert content.startswith(b"contactid\r\n") and content.endswith(b"\r\n")
    api.uploaded = content.decode("ascii").split("\r\n")[1:-1]


@pytest.fixture
def ledger(tmp_path):
    return cl.Ledger(tmp_path / "state" / "imports.sqlite3")


def run(values, api, ledger, **kw):
    name = kw.pop("name", NAME)
    archive, count, sha = cl.build_list_archive(values, name)
    return cl.import_guids(KEY, name, archive, count, sha, api=api, ledger=ledger, uploader=fake_uploader,
                           sleep=lambda s: None, **kw)


def test_build_list_archive_one_pass_uppercase_and_composition_hash():
    a, b = guids(2)
    archive, count, sha = cl.build_list_archive(iter([a, b.upper()]), NAME)
    with ZipFile(io.BytesIO(archive)) as z:
        assert z.read(f"{NAME}.csv") == f"contactid\r\n{a.upper()}\r\n{b.upper()}\r\n".encode()
    assert count == 2 and sha == cl.build_list_archive([a.upper(), b], NAME)[2] and sha != cl.build_list_archive([b, a], NAME)[2]
    for bad in ([], [a, ""], [a, None], ["not-a-guid"]):
        with pytest.raises(cl.ListImportError) as e:
            cl.build_list_archive(bad, NAME)
        assert e.value.code == "SOURCE_INVALID"


def test_full_import_single_post_and_repeat_does_nothing(ledger):
    values = guids(3)
    api = Api(loaded=2)
    r = run(values, api, ledger)
    assert r["name"] == NAME and r["loaded"] == 2 and r["source_rows"] == 3 and r["not_uploaded"] == 1
    assert api.uploaded == [g.upper() for g in values]
    put = [c for c in api.calls if c[0] == "PUT"]
    assert len(put) == 1 and put[0][2] == {"key": f"{NAME}.zip", "importType": "contact_id", "loadType": 4, "destination": "camp",
                                           "nameMList": NAME, "mListMode": 3, "sendToMO": False}
    assert ledger.get(KEY)["status"] == "confirmed" and ledger.get(KEY)["loaded"] == 2
    before = len(api.calls)
    r2 = run(values, api, ledger)
    assert r2["already_confirmed"] and r2["loaded"] == 2 and len(api.calls) == before


def test_existing_list_is_not_reimported(ledger):
    api = Api(exists=True)
    with pytest.raises(cl.ListImportError, match="уже существует"):
        run(guids(), api, ledger)
    assert not [c for c in api.calls if c[0] in ("PUT", "UPLOAD")]
    assert ledger.get(KEY)["status"] == "failed_preflight"


def test_lost_import_response_never_repeats_post(ledger):
    values = guids()
    with pytest.raises(cl.ListImportError) as e:
        run(values, Api(lose_import=True), ledger)
    assert e.value.code == "UNKNOWN_OUTCOME" and ledger.get(KEY)["status"] == "uncertain"
    api2 = Api()
    with pytest.raises(cl.ListImportError) as e2:
        run(values, api2, ledger)
    assert e2.value.code == "UNKNOWN_OUTCOME" and api2.calls == []


def test_missing_import_id_is_uncertain(ledger):
    with pytest.raises(cl.ListImportError) as e:
        run(guids(), Api(missing_id=True), ledger)
    assert e.value.code == "UNKNOWN_OUTCOME" and ledger.get(KEY)["status"] == "uncertain"


def test_resume_by_import_id_only_polls(ledger):
    values = guids()
    _, count, sha = cl.build_list_archive(values, NAME)
    ledger.start(KEY, NAME, sha, count, False)
    ledger.transition(KEY, "importing", import_id="0123456789abcdef01234567")
    api = Api(loaded=3)
    r = run(values, api, ledger)
    assert r["resumed"] and r["loaded"] == 3 and [c[0] for c in api.calls] == ["POST"]


def test_resume_refuses_changed_composition(ledger):
    values = guids()
    _, count, sha = cl.build_list_archive(values, NAME)
    ledger.start(KEY, NAME, sha, count, False)
    ledger.transition(KEY, "importing", import_id="0123456789abcdef01234567")
    api = Api(loaded=4)
    with pytest.raises(cl.ListImportError) as e:
        run(values + guids(1), api, ledger)  # список в базе пересобрали после запуска импорта
    assert e.value.code == "SOURCE_CHANGED" and api.calls == [] and ledger.get(KEY)["status"] == "importing"


def test_import_error_in_status_keeps_import_unrepeatable(ledger):
    values = guids()
    with pytest.raises(cl.ListImportError) as e:
        run(values, Api(error="bad file"), ledger)
    assert e.value.code == "IMPORT_ERROR" and ledger.get(KEY)["status"] == "awaiting_confirmation"
    api2 = Api()
    assert run(values, api2, ledger)["resumed"]  # повтор только опрашивает статус по ID
    assert [c[0] for c in api2.calls] == ["POST"]


def test_send_to_mo_name_limit(ledger):
    with pytest.raises(cl.ListImportError) as e:
        run(guids(), Api(), ledger, name="500001_" + "x" * 50, send_to_mo=True)
    assert e.value.code == "NAME_INVALID"
    api = Api(send_mo=True)
    assert run(guids(), api, ledger, send_to_mo=True)["loaded"] == 3
    assert [c for c in api.calls if c[0] == "PUT"][0][2]["destination"] == "campml"


def test_upload_url_validation():
    key = "500001_x.zip"
    good = f"https://{cl.S3_UPLOAD_HOST_DEFAULT}/mlists/{key}?X-Amz-Signature=s"
    assert cl.validate_upload_url(good, key, cl.S3_UPLOAD_HOST_DEFAULT) == good
    for bad in (good.replace("https", "http"), f"https://evil.example/mlists/{key}?X-Amz-Signature=s",
                f"https://{cl.S3_UPLOAD_HOST_DEFAULT}/other/{key}?X-Amz-Signature=s", f"https://{cl.S3_UPLOAD_HOST_DEFAULT}/mlists/{key}"):
        with pytest.raises(cl.ListImportError):
            cl.validate_upload_url(bad, key, cl.S3_UPLOAD_HOST_DEFAULT)


def test_list_name_rules():
    assert cl.validate_list_name(NAME) == NAME
    for bad in ("", "a/b", "name.", "CON", "x" * 121):
        with pytest.raises(cl.ListImportError):
            cl.validate_list_name(bad)
