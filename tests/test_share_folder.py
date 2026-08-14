"""Unit tests for member Shared Folder copy/share helpers (no live Drive)."""

from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError

from tools.share_folder import (
    ShareFolderError,
    get_share_folder_status,
    is_direct_grant,
    is_direct_user_sharee,
    is_sa_quota_error,
    is_valid_email,
    parse_share_emails,
    share_member_folder,
)


def test_is_valid_email():
    assert is_valid_email("jane@client.com")
    assert not is_valid_email("")
    assert not is_valid_email("not-an-email")
    assert not is_valid_email("missing-domain@")


def test_parse_share_emails():
    assert parse_share_emails("jane@client.com") == ["jane@client.com"]
    assert parse_share_emails(
        "max.h@acesolutions.com.au, morgan.h@acesolutions.com.au"
    ) == ["max.h@acesolutions.com.au", "morgan.h@acesolutions.com.au"]
    assert parse_share_emails("a@x.com; b@x.com a@x.com") == ["a@x.com", "b@x.com"]
    assert parse_share_emails("  ") == []


def test_is_direct_user_sharee_hides_service_account_only():
    sa = "robot@project.iam.gserviceaccount.com"
    assert is_direct_user_sharee("jane@client.com", "user", sa) is True
    assert is_direct_user_sharee("max.h@acesolutions.com.au", "user", sa) is True
    assert is_direct_user_sharee(sa, "user", sa) is False
    assert is_direct_user_sharee("other@x.iam.gserviceaccount.com", "user", sa) is False
    assert is_direct_user_sharee("jane@client.com", "group", sa) is False
    assert is_direct_user_sharee("", "user", sa) is False


def test_is_direct_grant_uses_inherited_flag():
    assert is_direct_grant({}) is True
    assert is_direct_grant({"permissionDetails": [{"inherited": False}]}) is True
    assert is_direct_grant({"permissionDetails": [{"inherited": True}]}) is False
    assert is_direct_grant({"permissionDetails": [{"inherited": True}, {"inherited": False}]}) is True


def test_is_sa_quota_error():
    err = HttpError(MagicMock(status=403, reason="Service Accounts do not have storage quota."), b"{}")
    assert is_sa_quota_error(err) is True


def _http_error(status: int, reason: str = "error") -> HttpError:
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    return HttpError(resp, b"{}")


def _drive_mock(
    *,
    folders: list[dict] | None = None,
    children: list[dict] | None = None,
    permissions: list[dict] | None = None,
    source_files: dict[str, dict] | None = None,
    copy_id: str = "copied1",
    dest_drive_id: str | None = "sharedDrive1",
    create_permission_error: HttpError | None = None,
    copy_error: HttpError | None = None,
):
    drive = MagicMock()
    files = MagicMock()
    perms = MagicMock()
    drive.files.return_value = files
    drive.permissions.return_value = perms

    files.list.return_value.execute.side_effect = [
        {"files": folders or []},
        {"files": children or []},
        {"files": children or []},
    ]

    def files_get(**kwargs):
        fid = kwargs.get("fileId")
        mock = MagicMock()
        if source_files and fid in source_files:
            mock.execute.return_value = source_files[fid]
            return mock
        fields = kwargs.get("fields") or ""
        if "driveId" in fields:
            payload = {"id": fid}
            if dest_drive_id:
                payload["driveId"] = dest_drive_id
            mock.execute.return_value = payload
            return mock
        mock.execute.return_value = {"id": fid, "name": "Doc.pdf", "mimeType": "application/pdf"}
        return mock

    def files_create(**kwargs):
        body = kwargs.get("body") or {}
        mock = MagicMock()
        if body.get("mimeType") == "application/vnd.google-apps.folder":
            mock.execute.return_value = {"id": "sharedFolder1"}
        else:
            mock.execute.return_value = {
                "id": "shortcut1",
                "name": body.get("name") or "Doc.pdf",
                "webViewLink": "https://drive.google.com/file/d/shortcut1/view",
            }
        return mock

    files.get.side_effect = files_get
    files.create.side_effect = files_create
    if copy_error:
        files.copy.return_value.execute.side_effect = copy_error
    else:
        files.copy.return_value.execute.return_value = {
            "id": copy_id,
            "name": "Doc.pdf",
            "webViewLink": "https://drive.google.com/file/d/copied1/view",
        }

    perms.list.return_value.execute.return_value = {"permissions": permissions or []}
    if create_permission_error:
        perms.create.return_value.execute.side_effect = create_permission_error
    else:
        perms.create.return_value.execute.return_value = {
            "id": "perm1",
            "emailAddress": "jane@client.com",
            "role": "reader",
            "type": "user",
        }
    return drive


def test_status_when_folder_missing():
    drive = _drive_mock(folders=[])
    result = get_share_folder_status(
        "https://drive.google.com/drive/folders/parentFolderId123",
        drive=drive,
    )
    assert result["ok"] is True
    assert result["exists"] is False
    assert result["files"] == []
    assert result["shared_with"] == []


def test_status_lists_direct_people_including_aces():
    drive = _drive_mock(
        folders=[{"id": "sharedFolder1", "name": "Shared Folder"}],
        children=[{"id": "f1", "name": "Quote.pdf", "webViewLink": "https://drive/f1", "mimeType": "application/pdf"}],
        permissions=[
            {
                "emailAddress": "jane@client.com",
                "role": "reader",
                "type": "user",
                "permissionDetails": [{"inherited": False}],
            },
            {
                "emailAddress": "ops@acesolutions.com.au",
                "role": "writer",
                "type": "user",
                "permissionDetails": [{"inherited": True}],
            },
            {
                "emailAddress": "max.h@acesolutions.com.au",
                "role": "reader",
                "type": "user",
                "permissionDetails": [{"inherited": False}],
            },
            {"emailAddress": "robot@x.iam.gserviceaccount.com", "role": "writer", "type": "user"},
        ],
    )
    with patch("tools.share_folder.get_configured_service_account_email", return_value="robot@x.iam.gserviceaccount.com"):
        result = get_share_folder_status(
            "https://drive.google.com/drive/folders/parentFolderId123",
            drive=drive,
        )
    assert result["exists"] is True
    assert result["folder_id"] == "sharedFolder1"
    assert len(result["files"]) == 1
    assert [p["email"] for p in result["shared_with"]] == [
        "jane@client.com",
        "max.h@acesolutions.com.au",
    ]


def test_share_rejects_invalid_email():
    with pytest.raises(ShareFolderError) as exc:
        share_member_folder(
            gdrive_url="https://drive.google.com/drive/folders/parentFolderId123",
            file_ids=["abc1234567890"],
            email="nope",
            drive=MagicMock(),
        )
    assert exc.value.status_code == 400


def test_share_creates_folder_copies_and_shares():
    drive = _drive_mock(
        folders=[],
        children=[],
        source_files={"abc1234567890": {"id": "abc1234567890", "name": "Quote.pdf", "mimeType": "application/pdf"}},
    )
    drive.files.return_value.list.return_value.execute.side_effect = [
        {"files": []},
        {"files": []},
        {"files": [{"id": "copied1", "name": "Quote.pdf", "webViewLink": "https://drive/copied1"}]},
    ]
    with patch("tools.share_folder.get_configured_service_account_email", return_value=None):
        result = share_member_folder(
            gdrive_url="https://drive.google.com/drive/folders/parentFolderId123",
            file_ids=["abc1234567890"],
            email="jane@client.com",
            send_notification=False,
            drive=drive,
        )
    assert result["ok"] is True
    assert result["folder_created"] is True
    assert result["copy_results"][0]["action"] == "copied"
    assert result["permission"]["action"] == "added"
    drive.files.return_value.copy.assert_called_once()
    drive.permissions.return_value.create.assert_called_once()
    perm_kwargs = drive.permissions.return_value.create.call_args.kwargs
    assert perm_kwargs["sendNotificationEmail"] is False


def test_share_sends_branded_email_when_notification_enabled():
    drive = _drive_mock(
        folders=[{"id": "sharedFolder1", "name": "Shared Folder"}],
        children=[],
        source_files={"abc1234567890": {"id": "abc1234567890", "name": "Quote.pdf", "mimeType": "application/pdf"}},
    )
    drive.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "sharedFolder1", "name": "Shared Folder"}]},
        {"files": []},
        {"files": [{"id": "copied1", "name": "Quote.pdf"}]},
    ]
    with (
        patch("tools.share_folder.get_configured_service_account_email", return_value=None),
        patch(
            "tools.share_folder.send_share_folder_emails",
            return_value=[{"email": "jane@client.com", "action": "sent"}],
        ) as send_email,
    ):
        result = share_member_folder(
            gdrive_url="https://drive.google.com/drive/folders/parentFolderId123",
            file_ids=["abc1234567890"],
            email="jane@client.com",
            send_notification=True,
            business_name="Frankston RSL",
            sender_name="Max H",
            sender_email="max.h@acesolutions.com.au",
            drive=drive,
        )
    send_email.assert_called_once()
    kwargs = send_email.call_args.kwargs
    assert kwargs["recipients"] == ["jane@client.com"]
    assert kwargs["business_name"] == "Frankston RSL"
    assert kwargs["sender_email"] == "max.h@acesolutions.com.au"
    assert result["email_results"][0]["action"] == "sent"
    assert "Carbon Zero Australasia" in result["email_preview"]["subject"]
    perm_kwargs = drive.permissions.return_value.create.call_args.kwargs
    assert perm_kwargs["sendNotificationEmail"] is False


def test_share_accepts_multiple_emails():
    drive = _drive_mock(
        folders=[{"id": "sharedFolder1", "name": "Shared Folder"}],
        children=[],
        source_files={"abc1234567890": {"id": "abc1234567890", "name": "Quote.pdf", "mimeType": "application/pdf"}},
    )
    drive.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "sharedFolder1", "name": "Shared Folder"}]},
        {"files": []},
        {"files": [{"id": "copied1", "name": "Quote.pdf"}]},
    ]
    with patch("tools.share_folder.get_configured_service_account_email", return_value=None):
        result = share_member_folder(
            gdrive_url="https://drive.google.com/drive/folders/parentFolderId123",
            file_ids=["abc1234567890"],
            email="max.h@acesolutions.com.au, morgan.h@acesolutions.com.au",
            send_notification=False,
            drive=drive,
        )
    assert len(result["permissions"]) == 2
    assert [item["email"] for item in result["permissions"]] == [
        "max.h@acesolutions.com.au",
        "morgan.h@acesolutions.com.au",
    ]
    assert drive.permissions.return_value.create.call_count == 2


def test_share_rejects_invalid_email_in_list():
    with pytest.raises(ShareFolderError) as exc:
        share_member_folder(
            gdrive_url="https://drive.google.com/drive/folders/parentFolderId123",
            file_ids=["abc1234567890"],
            email="max.h@acesolutions.com.au, not-an-email",
            drive=MagicMock(),
        )
    assert "Not a valid email" in str(exc.value)


def test_share_uses_shortcut_when_not_on_shared_drive():
    drive = _drive_mock(
        folders=[],
        children=[],
        dest_drive_id=None,
        source_files={"abc1234567890": {"id": "abc1234567890", "name": "Quote.pdf", "mimeType": "application/pdf"}},
    )
    drive.files.return_value.list.return_value.execute.side_effect = [
        {"files": []},
        {"files": []},
        {"files": [{"id": "shortcut1", "name": "Quote.pdf"}]},
    ]
    with patch("tools.share_folder.get_configured_service_account_email", return_value=None):
        result = share_member_folder(
            gdrive_url="https://drive.google.com/drive/folders/parentFolderId123",
            file_ids=["abc1234567890"],
            email="max.h@acesolutions.com.au",
            send_notification=False,
            drive=drive,
        )
    assert result["copy_results"][0]["action"] == "shortcut"
    drive.files.return_value.copy.assert_not_called()
    assert drive.permissions.return_value.create.call_count == 2


def test_share_falls_back_to_shortcut_on_quota_error():
    drive = _drive_mock(
        folders=[{"id": "sharedFolder1", "name": "Shared Folder"}],
        children=[],
        dest_drive_id="sharedDrive1",
        copy_error=_http_error(403, "Service Accounts do not have storage quota."),
        source_files={"abc1234567890": {"id": "abc1234567890", "name": "Quote.pdf", "mimeType": "application/pdf"}},
    )
    drive.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "sharedFolder1", "name": "Shared Folder"}]},
        {"files": []},
        {"files": [{"id": "shortcut1", "name": "Quote.pdf"}]},
    ]
    with patch("tools.share_folder.get_configured_service_account_email", return_value=None):
        result = share_member_folder(
            gdrive_url="https://drive.google.com/drive/folders/parentFolderId123",
            file_ids=["abc1234567890"],
            email="jane@client.com",
            send_notification=False,
            drive=drive,
        )
    assert result["copy_results"][0]["action"] == "shortcut"


def test_share_skips_duplicate_filename():
    drive = _drive_mock(
        folders=[{"id": "sharedFolder1", "name": "Shared Folder"}],
        children=[{"id": "already1", "name": "Quote.pdf"}],
        source_files={"abc1234567890": {"id": "abc1234567890", "name": "Quote.pdf", "mimeType": "application/pdf"}},
    )
    drive.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "sharedFolder1", "name": "Shared Folder"}]},
        {"files": [{"id": "already1", "name": "Quote.pdf"}]},
        {"files": [{"id": "already1", "name": "Quote.pdf"}]},
    ]
    with patch("tools.share_folder.get_configured_service_account_email", return_value=None):
        result = share_member_folder(
            gdrive_url="https://drive.google.com/drive/folders/parentFolderId123",
            file_ids=["abc1234567890"],
            email="jane@client.com",
            send_notification=False,
            drive=drive,
        )
    assert result["copy_results"][0]["action"] == "already_present"
    drive.files.return_value.copy.assert_not_called()
