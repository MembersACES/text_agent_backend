"""Create a member 'Shared Folder', copy Additional Documents into it, and share it.

Drive helpers for this feature live here — do not modify one_month_savings.py.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

from googleapiclient.errors import HttpError

from tools.one_month_savings import (
    extract_folder_id_from_url,
    get_configured_service_account_email,
    get_drive_service,
)
from tools.share_folder_email import (
    build_share_folder_email,
    send_share_folder_emails,
)

logger = logging.getLogger(__name__)

SHARED_FOLDER_NAME = "Shared Folder"
SERVICE_ACCOUNT_SUFFIX = ".iam.gserviceaccount.com"
_SHORTCUT_MIME = "application/vnd.google-apps.shortcut"
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DRIVE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{10,}$")
_FOLDER_MIME = "application/vnd.google-apps.folder"


class ShareFolderError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def drive_folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def drive_file_url(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/view?usp=drivesdk"


def normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def parse_share_emails(value: str) -> list[str]:
    """Split a share-with field into unique emails (comma, semicolon, or whitespace)."""
    tokens = [part.strip() for part in re.split(r"[,;\s]+", value or "") if part.strip()]
    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(token)
    return unique


def is_valid_email(value: str) -> bool:
    return bool(_EMAIL_RE.match((value or "").strip()))


def is_direct_user_sharee(
    email: Optional[str],
    perm_type: Optional[str],
    sa_email: Optional[str] = None,
) -> bool:
    """True for a user permission that is not the backend service account."""
    kind = (perm_type or "").strip().lower()
    if kind != "user":
        return False
    em = normalize_email(email or "")
    if not em or "@" not in em:
        return False
    if sa_email and em == normalize_email(sa_email):
        return False
    if em.endswith(SERVICE_ACCOUNT_SUFFIX):
        return False
    return True


def is_direct_grant(perm: dict[str, Any]) -> bool:
    """Shared Drive inherited members are hidden; people added on this folder are shown."""
    details = perm.get("permissionDetails") or []
    if not details:
        return True
    return any(not bool(item.get("inherited")) for item in details if isinstance(item, dict))


def is_sa_quota_error(exc: HttpError) -> bool:
    reason = (getattr(exc, "reason", None) or str(exc)).lower()
    return "storage quota" in reason or "storagequotaexceeded" in reason


def _escape_drive_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _drive_error_message(exc: HttpError) -> str:
    reason = getattr(exc, "reason", None) or str(exc)
    code = getattr(exc, "status_code", None)
    if code:
        return f"Drive API {code}: {reason}"
    return f"Drive API error: {reason}"


def _require_drive(drive: Any | None) -> Any:
    service = drive or get_drive_service()
    if not service:
        raise ShareFolderError(
            "Google Drive is not configured. Check SERVICE_ACCOUNT_JSON.",
            status_code=503,
        )
    return service


def _require_parent_id(gdrive_url: str) -> str:
    parent_id = extract_folder_id_from_url((gdrive_url or "").strip())
    if not parent_id:
        raise ShareFolderError(
            "No member Drive folder URL was provided, or it could not be parsed.",
            status_code=400,
        )
    return parent_id


def _find_subfolder(drive: Any, parent_id: str, name: str) -> Optional[str]:
    query = (
        f"name='{_escape_drive_query(name)}' and '{parent_id}' in parents "
        f"and mimeType='{_FOLDER_MIME}' and trashed=false"
    )
    result = (
        drive.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id,name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        .execute()
    )
    folders = result.get("files") or []
    if not folders:
        return None
    folder_id = folders[0].get("id")
    return folder_id if isinstance(folder_id, str) and folder_id.strip() else None


def _get_or_create_shared_folder(drive: Any, parent_id: str) -> tuple[str, bool]:
    existing = _find_subfolder(drive, parent_id, SHARED_FOLDER_NAME)
    if existing:
        return existing, False
    created = (
        drive.files()
        .create(
            body={
                "name": SHARED_FOLDER_NAME,
                "mimeType": _FOLDER_MIME,
                "parents": [parent_id],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    folder_id = created.get("id")
    if not isinstance(folder_id, str) or not folder_id.strip():
        raise ShareFolderError("Drive created a folder but returned no id.", status_code=502)
    logger.info("[share_folder] created %s id=%s parent=%s", SHARED_FOLDER_NAME, folder_id, parent_id)
    return folder_id, True


def _list_children(drive: Any, folder_id: str) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    page_token: Optional[str] = None
    while True:
        result = (
            drive.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces="drive",
                fields="nextPageToken, files(id,name,mimeType,webViewLink,modifiedTime,shortcutDetails(targetId))",
                pageSize=100,
                pageToken=page_token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        files.extend(result.get("files") or [])
        page_token = result.get("nextPageToken")
        if not page_token:
            break
    return files


def _serialize_files(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in files:
        fid = item.get("id")
        if not isinstance(fid, str) or not fid:
            continue
        name = str(item.get("name") or "Untitled")
        link = item.get("webViewLink") or drive_file_url(fid)
        out.append(
            {
                "id": fid,
                "name": name,
                "mime_type": item.get("mimeType") or "",
                "web_view_link": link,
                "modified_time": item.get("modifiedTime"),
            }
        )
    return out


def _list_external_permissions(drive: Any, folder_id: str) -> list[dict[str, Any]]:
    sa_email = get_configured_service_account_email()
    result = (
        drive.permissions()
        .list(
            fileId=folder_id,
            fields="permissions(id,emailAddress,role,type,displayName,domain,deleted,permissionDetails(inherited))",
            supportsAllDrives=True,
        )
        .execute()
    )
    sharees: list[dict[str, Any]] = []
    for perm in result.get("permissions") or []:
        if perm.get("deleted"):
            continue
        if not is_direct_grant(perm):
            continue
        kind = (perm.get("type") or "").strip().lower()
        role = (perm.get("role") or "reader").strip().lower()
        if kind == "anyone":
            sharees.append(
                {
                    "email": "Anyone with the link",
                    "role": role,
                    "kind": "anyone",
                    "display_name": perm.get("displayName") or "",
                }
            )
            continue
        email = perm.get("emailAddress") or ""
        if not is_direct_user_sharee(email, kind, sa_email):
            continue
        sharees.append(
            {
                "email": email,
                "role": role,
                "kind": "user",
                "display_name": perm.get("displayName") or "",
            }
        )
    return sharees


def _status_payload(
    *,
    exists: bool,
    folder_id: Optional[str] = None,
    files: Optional[list[dict[str, Any]]] = None,
    shared_with: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "exists": exists,
        "folder_name": SHARED_FOLDER_NAME,
        "folder_id": folder_id,
        "folder_url": drive_folder_url(folder_id) if folder_id else None,
        "files": files or [],
        "shared_with": shared_with or [],
    }


def get_share_folder_status(gdrive_url: str, drive: Any | None = None) -> dict[str, Any]:
    """Return Shared Folder contents and client-facing sharees. Does not create the folder."""
    parent_id = _require_parent_id(gdrive_url)
    service = _require_drive(drive)
    try:
        folder_id = _find_subfolder(service, parent_id, SHARED_FOLDER_NAME)
    except HttpError as e:
        raise ShareFolderError(_drive_error_message(e), status_code=502) from e
    if not folder_id:
        return _status_payload(exists=False)
    try:
        children = _list_children(service, folder_id)
        sharees = _list_external_permissions(service, folder_id)
    except HttpError as e:
        raise ShareFolderError(_drive_error_message(e), status_code=502) from e
    return _status_payload(
        exists=True,
        folder_id=folder_id,
        files=_serialize_files(children),
        shared_with=sharees,
    )


def _folder_drive_id(drive: Any, folder_id: str) -> Optional[str]:
    info = (
        drive.files()
        .get(fileId=folder_id, fields="id,driveId", supportsAllDrives=True)
        .execute()
    )
    drive_id = info.get("driveId")
    return drive_id if isinstance(drive_id, str) and drive_id.strip() else None


def _create_shortcut(
    drive: Any,
    *,
    file_id: str,
    name: str,
    dest_folder_id: str,
) -> dict[str, Any]:
    created = (
        drive.files()
        .create(
            body={
                "name": name,
                "mimeType": _SHORTCUT_MIME,
                "parents": [dest_folder_id],
                "shortcutDetails": {"targetId": file_id},
            },
            fields="id,name,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    shortcut_id = created.get("id")
    return {
        "file_id": file_id,
        "name": name,
        "action": "shortcut",
        "copied_file_id": shortcut_id,
        "web_view_link": created.get("webViewLink")
        or (drive_file_url(file_id) if file_id else None),
    }


def _add_file_to_shared_folder(
    drive: Any,
    *,
    file_id: str,
    dest_folder_id: str,
    dest_drive_id: Optional[str],
    existing_names: dict[str, str],
    existing_targets: dict[str, str],
) -> dict[str, Any]:
    if not _DRIVE_ID_RE.match(file_id):
        return {
            "file_id": file_id,
            "name": "",
            "action": "failed",
            "error": "Invalid Drive file id.",
        }
    existing_id = existing_targets.get(file_id)
    if existing_id:
        return {
            "file_id": file_id,
            "name": "",
            "action": "already_present",
            "copied_file_id": existing_id,
        }
    try:
        src = (
            drive.files()
            .get(
                fileId=file_id,
                fields="id,name,mimeType,trashed",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as e:
        return {
            "file_id": file_id,
            "name": "",
            "action": "failed",
            "error": _drive_error_message(e),
        }
    name = str(src.get("name") or "Untitled")
    if src.get("trashed"):
        return {"file_id": file_id, "name": name, "action": "failed", "error": "File is in trash."}
    if src.get("mimeType") == _FOLDER_MIME:
        return {
            "file_id": file_id,
            "name": name,
            "action": "failed",
            "error": "Folders cannot be added into Shared Folder.",
        }
    existing_id = existing_names.get(name.lower())
    if existing_id:
        return {
            "file_id": file_id,
            "name": name,
            "action": "already_present",
            "copied_file_id": existing_id,
        }

    if dest_drive_id:
        try:
            copied = (
                drive.files()
                .copy(
                    fileId=file_id,
                    body={"name": name, "parents": [dest_folder_id]},
                    fields="id,name,webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
            )
            copied_id = copied.get("id")
            if isinstance(copied_id, str) and copied_id:
                existing_names[name.lower()] = copied_id
                existing_targets[file_id] = copied_id
            return {
                "file_id": file_id,
                "name": name,
                "action": "copied",
                "copied_file_id": copied_id,
                "web_view_link": copied.get("webViewLink")
                or (drive_file_url(copied_id) if copied_id else None),
            }
        except HttpError as e:
            if not is_sa_quota_error(e):
                return {
                    "file_id": file_id,
                    "name": name,
                    "action": "failed",
                    "error": _drive_error_message(e),
                }
            logger.info("[share_folder] copy hit SA quota; falling back to shortcut for %s", file_id)

    try:
        result = _create_shortcut(
            drive,
            file_id=file_id,
            name=name,
            dest_folder_id=dest_folder_id,
        )
    except HttpError as e:
        return {"file_id": file_id, "name": name, "action": "failed", "error": _drive_error_message(e)}
    shortcut_id = result.get("copied_file_id")
    if isinstance(shortcut_id, str) and shortcut_id:
        existing_names[name.lower()] = shortcut_id
        existing_targets[file_id] = shortcut_id
    return result


def _ensure_user_permission(
    drive: Any,
    *,
    folder_id: str,
    email: str,
    send_notification: bool,
) -> dict[str, Any]:
    try:
        drive.permissions().create(
            fileId=folder_id,
            body={
                "type": "user",
                "role": "reader",
                "emailAddress": email,
            },
            sendNotificationEmail=bool(send_notification),
            fields="id,emailAddress,role,type",
            supportsAllDrives=True,
        ).execute()
        return {"email": email, "role": "reader", "action": "added"}
    except HttpError as e:
        code = getattr(e, "status_code", None)
        reason = (getattr(e, "reason", None) or str(e)).lower()
        if code == 409 or "already" in reason:
            return {"email": email, "role": "reader", "action": "already_shared"}
        return {
            "email": email,
            "role": "reader",
            "action": "failed",
            "error": _drive_error_message(e),
        }


def share_member_folder(
    *,
    gdrive_url: str,
    file_ids: list[str],
    email: str,
    send_notification: bool = True,
    business_name: str = "",
    sender_name: str = "",
    sender_email: str = "",
    drive: Any | None = None,
) -> dict[str, Any]:
    """Create Shared Folder if needed, add selected files, share with one or more emails as Viewer."""
    share_emails = parse_share_emails(email)
    if not share_emails:
        raise ShareFolderError("Enter a valid email address to share with.", status_code=400)
    invalid = [item for item in share_emails if not is_valid_email(item)]
    if invalid:
        raise ShareFolderError(
            f"Not a valid email: {', '.join(invalid)}",
            status_code=400,
        )
    unique_ids: list[str] = []
    seen: set[str] = set()
    for raw in file_ids or []:
        fid = str(raw or "").strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        unique_ids.append(fid)

    parent_id = _require_parent_id(gdrive_url)
    service = _require_drive(drive)
    try:
        folder_id, created = _get_or_create_shared_folder(service, parent_id)
        children = _list_children(service, folder_id)
        dest_drive_id = _folder_drive_id(service, folder_id)
    except HttpError as e:
        raise ShareFolderError(_drive_error_message(e), status_code=502) from e

    existing_names = {
        str(item.get("name") or "").lower(): str(item.get("id"))
        for item in children
        if item.get("id") and item.get("name")
    }
    existing_targets: dict[str, str] = {}
    for item in children:
        details = item.get("shortcutDetails") or {}
        target = details.get("targetId") if isinstance(details, dict) else None
        if isinstance(target, str) and target and item.get("id"):
            existing_targets[target] = str(item.get("id"))
    copy_results = [
        _add_file_to_shared_folder(
            service,
            file_id=fid,
            dest_folder_id=folder_id,
            dest_drive_id=dest_drive_id,
            existing_names=existing_names,
            existing_targets=existing_targets,
        )
        for fid in unique_ids
    ]
    for row in copy_results:
        if row.get("action") != "shortcut":
            continue
        for share_email in share_emails:
            file_perm = _ensure_user_permission(
                service,
                folder_id=str(row.get("file_id") or ""),
                email=share_email,
                send_notification=False,
            )
            if file_perm.get("action") == "failed":
                row["action"] = "failed"
                row["error"] = (
                    file_perm.get("error")
                    or "Added to Shared Folder but could not share the original file with that email."
                )
                break
    permissions = [
        _ensure_user_permission(
            service,
            folder_id=folder_id,
            email=share_email,
            send_notification=False,
        )
        for share_email in share_emails
    ]
    failed_permissions = [item for item in permissions if item.get("action") == "failed"]
    if failed_permissions and len(failed_permissions) == len(permissions):
        raise ShareFolderError(
            failed_permissions[0].get("error")
            or "Could not share the folder. The service account may lack sharing permission.",
            status_code=502,
        )
    permission = permissions[0]

    try:
        refreshed_files = _serialize_files(_list_children(service, folder_id))
        sharees = _list_external_permissions(service, folder_id)
    except HttpError as e:
        raise ShareFolderError(_drive_error_message(e), status_code=502) from e

    failed_copies = [row for row in copy_results if row.get("action") == "failed"]
    added_names = [
        str(row.get("name") or "")
        for row in copy_results
        if row.get("action") in {"copied", "shortcut", "already_present"} and row.get("name")
    ]
    if not added_names:
        added_names = [str(item.get("name") or "") for item in refreshed_files if item.get("name")]
    email_preview = build_share_folder_email(
        business_name=business_name,
        folder_url=drive_folder_url(folder_id),
        file_names=added_names,
        sender_name=sender_name,
        sender_email=sender_email,
    )
    email_results: list[dict[str, Any]] = []
    if send_notification:
        email_results = send_share_folder_emails(
            recipients=share_emails,
            business_name=business_name,
            folder_url=drive_folder_url(folder_id),
            file_names=added_names,
            sender_name=sender_name,
            sender_email=sender_email,
        )
    return {
        "ok": True,
        "folder_created": created,
        "folder_id": folder_id,
        "folder_url": drive_folder_url(folder_id),
        "folder_name": SHARED_FOLDER_NAME,
        "copy_results": copy_results,
        "copy_failures": failed_copies,
        "permission": permission,
        "permissions": permissions,
        "files": refreshed_files,
        "shared_with": sharees,
        "exists": True,
        "email_preview": email_preview,
        "email_results": email_results,
    }
