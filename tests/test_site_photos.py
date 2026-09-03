"""Unit tests for member Site Photos Drive helpers (no live Drive)."""

from unittest.mock import MagicMock

import pytest

from tools.site_photos import (
    SitePhotosError,
    is_allowed_image,
    is_site_photos_folder_name,
    list_site_photos,
    prefixed_filename,
    upload_site_photos,
)


def test_is_site_photos_folder_name():
    assert is_site_photos_folder_name("Site Photos") is True
    assert is_site_photos_folder_name("site photos") is True
    assert is_site_photos_folder_name("Site Photo") is True
    assert is_site_photos_folder_name("site_photos") is True
    assert is_site_photos_folder_name("Additional Documents") is False


def test_is_allowed_image():
    assert is_allowed_image("yard.jpg", "image/jpeg") is True
    assert is_allowed_image("yard.PNG", "") is True
    assert is_allowed_image("scan.heic", "image/heic") is True
    assert is_allowed_image("notes.pdf", "application/pdf") is False
    assert is_allowed_image("notes.docx", "") is False


def test_prefixed_filename():
    assert prefixed_filename("Acme Pty Ltd", "IMG_1.jpg") == "Acme Pty Ltd - IMG_1.jpg"
    assert prefixed_filename("Acme Pty Ltd", "Acme Pty Ltd - IMG_1.jpg") == "Acme Pty Ltd - IMG_1.jpg"
    assert prefixed_filename("", "IMG_1.jpg") == "IMG_1.jpg"


def _drive_mock(
    exact_folders: list[dict] | None = None,
    child_folders: list[dict] | None = None,
    children: list[dict] | None = None,
    created_folder_id: str = "sitePhotosFolder1",
    uploaded: list[dict] | None = None,
):
    drive = MagicMock()
    files = MagicMock()
    drive.files.return_value = files
    uploaded_iter = iter(uploaded or [])

    def files_list(**kwargs):
        q = kwargs.get("q") or ""
        mock = MagicMock()
        if "name=" in q and "mimeType='application/vnd.google-apps.folder'" in q:
            mock.execute.return_value = {"files": exact_folders or []}
        elif "mimeType='application/vnd.google-apps.folder'" in q:
            mock.execute.return_value = {"files": child_folders or []}
        else:
            mock.execute.return_value = {"files": children or []}
        return mock

    def files_create(**kwargs):
        body = kwargs.get("body") or {}
        mock = MagicMock()
        if body.get("mimeType") == "application/vnd.google-apps.folder":
            mock.execute.return_value = {"id": created_folder_id}
            return mock
        try:
            payload = next(uploaded_iter)
        except StopIteration:
            payload = {
                "id": "photo1",
                "name": body.get("name") or "photo.jpg",
                "mimeType": "image/jpeg",
                "webViewLink": "https://drive.google.com/file/d/photo1/view",
                "thumbnailLink": "https://example.com/thumb.jpg",
                "createdTime": "2026-09-03T00:00:00.000Z",
            }
        mock.execute.return_value = payload
        return mock

    files.list.side_effect = files_list
    files.create.side_effect = files_create
    files.get.return_value.execute.return_value = {"id": created_folder_id, "driveId": "shared1"}
    return drive


def test_list_when_folder_missing():
    drive = _drive_mock(exact_folders=[], child_folders=[])
    result = list_site_photos(
        "https://drive.google.com/drive/folders/parentFolderId123",
        drive=drive,
    )
    assert result["ok"] is True
    assert result["exists"] is False
    assert result["folder_id"] is None
    assert result["files"] == []


def test_list_reuses_alias_folder_name():
    drive = _drive_mock(
        exact_folders=[],
        child_folders=[{"id": "aliasFolder1", "name": "site photos"}],
        children=[
            {
                "id": "p1",
                "name": "yard.jpg",
                "mimeType": "image/jpeg",
                "webViewLink": "https://drive/p1",
                "createdTime": "2026-09-03T00:00:00.000Z",
            },
            {
                "id": "skip1",
                "name": "notes.pdf",
                "mimeType": "application/pdf",
            },
        ],
    )
    result = list_site_photos(
        "https://drive.google.com/drive/folders/parentFolderId123",
        drive=drive,
    )
    assert result["exists"] is True
    assert result["folder_id"] == "aliasFolder1"
    assert [row["id"] for row in result["files"]] == ["p1"]


def test_list_requires_drive_url():
    with pytest.raises(SitePhotosError) as exc:
        list_site_photos("", drive=MagicMock())
    assert exc.value.status_code == 400


def test_upload_creates_folder_and_keeps_original_image():
    drive = _drive_mock(exact_folders=[], child_folders=[])
    result = upload_site_photos(
        "https://drive.google.com/drive/folders/parentFolderId123",
        [("IMG_99.jpg", "image/jpeg", b"fakepngbytes")],
        business_name="Acme Pty Ltd",
        drive=drive,
    )
    assert result["created"] is True
    assert result["folder_id"] == "sitePhotosFolder1"
    assert result["files"][0]["name"] == "Acme Pty Ltd - IMG_99.jpg"
    assert result["errors"] == []


def test_upload_rejects_non_image():
    drive = _drive_mock(exact_folders=[{"id": "sitePhotosFolder1", "name": "Site Photos"}])
    with pytest.raises(SitePhotosError) as exc:
        upload_site_photos(
            "https://drive.google.com/drive/folders/parentFolderId123",
            [("contract.pdf", "application/pdf", b"%PDF")],
            drive=drive,
        )
    assert exc.value.status_code == 400
