"""Association Drive folders under the shared Associations parent."""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError

from tools.member_folder_drive import (
    MemberFolderDriveError,
    find_or_create_folder,
    upload_bytes_to_folder,
    _user_drive_service,
)
from tools.one_month_savings import get_drive_service
from tools.share_folder import drive_file_url, drive_folder_url, is_sa_quota_error
from tools.supplier_folders import (
    FOLDER_MIME,
    MAX_UPLOAD_BYTES,
    _get_drive_meta,
    _list_children,
    _normalize_file,
    _resolve_under_supplier,
    mimetype_for_filename,
    resolve_upload_filename,
)

logger = logging.getLogger(__name__)

# Shared with the dashboard service account. Override with ASSOCIATIONS_FOLDER_ID.
DEFAULT_ASSOCIATIONS_FOLDER_ID = "1TZ9eyCMDfMPmcsh55pe-flmyXnq-Vnb7"
TESTIMONIALS_FOLDER_NAME = "Testimonials"
ASSOCIATION_SOLUTION_TYPE_ID = "association_endorsement"
ASSOCIATION_SOLUTION_TYPE_LABEL = "Association Endorsement"
NO_INVOICE_RECORDED = "No invoice recorded"

_INVALID_FILENAME = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')
_MAX_FOLDER_NAME_LEN = 120

_STATUS_ALIASES = {
    "targeting": "targeting",
    "target": "targeting",
    "working_with": "working_with",
    "working": "working_with",
    "workingwith": "working_with",
}


def get_associations_parent_id() -> str:
    return (os.getenv("ASSOCIATIONS_FOLDER_ID") or "").strip() or DEFAULT_ASSOCIATIONS_FOLDER_ID


def clean_association_name(name: str) -> str:
    cleaned = _INVALID_FILENAME.sub("-", (name or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-")
    return cleaned[:_MAX_FOLDER_NAME_LEN]


def normalize_association_status(value: Optional[str]) -> Optional[str]:
    raw = (value or "").strip().lower().replace("-", " ").replace("_", " ")
    raw = "_".join(part for part in raw.split() if part)
    if not raw:
        return None
    return _STATUS_ALIASES.get(raw)


def _drive_or_error() -> Tuple[Any, Optional[str]]:
    drive = get_drive_service()
    if not drive:
        return None, (
            "Google Drive is not configured. Set SERVICE_ACCOUNT_FILE or "
            "SERVICE_ACCOUNT_JSON, and share the Associations folder with the service account."
        )
    return drive, None


def _candidate_drives(user_access_token: Optional[str]) -> Tuple[List[Any], Optional[str]]:
    drives: List[Any] = []
    drive, err = _drive_or_error()
    if drive:
        drives.append(drive)
    token = (user_access_token or "").strip()
    if token:
        drives.append(_user_drive_service(token))
    if not drives:
        return [], err or "Google Drive is not configured."
    return drives, None


def _parent_or_error() -> Tuple[str, Optional[str]]:
    parent_id = get_associations_parent_id()
    if not parent_id:
        return "", (
            "Associations parent folder is not configured. Set ASSOCIATIONS_FOLDER_ID "
            "and share that folder with the service account."
        )
    return parent_id, None


def create_named_folder(
    parent_id: str,
    name: str,
    user_access_token: Optional[str] = None,
) -> Tuple[str, bool]:
    """Find or create a child folder. Falls back to the signed-in user when the service account cannot write."""
    drives, err = _candidate_drives(user_access_token)
    if not drives:
        raise MemberFolderDriveError(err or "Google Drive is not configured.", status_code=503)
    last: Optional[MemberFolderDriveError] = None
    for drive in drives:
        try:
            return find_or_create_folder(parent_id, name, drive=drive)
        except MemberFolderDriveError as exc:
            last = exc
            logger.info("Association folder create retry parent=%s name=%r: %s", parent_id, name, exc.message)
    raise last or MemberFolderDriveError("Could not create the Drive folder.", status_code=502)


def ensure_testimonials_folder(
    association_folder_id: str,
    user_access_token: Optional[str] = None,
) -> str:
    folder_id, _created = create_named_folder(
        association_folder_id,
        TESTIMONIALS_FOLDER_NAME,
        user_access_token,
    )
    return folder_id


def rename_drive_folder(
    folder_id: str,
    name: str,
    user_access_token: Optional[str] = None,
) -> None:
    drives, err = _candidate_drives(user_access_token)
    if not drives:
        raise MemberFolderDriveError(err or "Google Drive is not configured.", status_code=503)
    last: Optional[Exception] = None
    for drive in drives:
        try:
            drive.files().update(
                fileId=folder_id,
                body={"name": name},
                supportsAllDrives=True,
            ).execute()
            return
        except HttpError as exc:
            last = exc
            if not is_sa_quota_error(exc):
                logger.info("Association rename retry folder=%s: %s", folder_id, exc)
    if isinstance(last, HttpError):
        raise MemberFolderDriveError(f"Google Drive error: {getattr(last, 'reason', last)}", status_code=502) from last
    raise MemberFolderDriveError("Could not rename the Drive folder.", status_code=502)


def list_association_drive_folders(
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    drives, err = _candidate_drives(user_access_token)
    if not drives:
        return None, err, 503
    parent_id, parent_err = _parent_or_error()
    if parent_err:
        return None, parent_err, 503

    last_err = "folder_not_found"
    folders: List[Dict[str, Any]] = []
    for drive in drives:
        found, list_err = _list_children(drive, parent_id, folders_only=True)
        if not list_err:
            folders = found
            last_err = ""
            break
        last_err = list_err

    if last_err:
        if last_err == "folder_not_found":
            return None, (
                "The Associations folder was not found, or the service account cannot access it. "
                "Share it with the service account as Editor."
            ), 502
        return None, last_err.replace("drive_error:", "Google Drive error: "), 502

    rows = []
    for item in folders:
        if not item.get("id"):
            continue
        fid = str(item["id"])
        rows.append(
            {
                "id": fid,
                "name": item.get("name") or "Untitled",
                "folder_id": fid,
                "folder_url": item.get("webViewLink") or drive_folder_url(fid),
                "modified_time": item.get("modifiedTime"),
            }
        )
    rows.sort(key=lambda row: (row.get("name") or "").lower())
    return {
        "parent_folder_id": parent_id,
        "parent_folder_url": drive_folder_url(parent_id),
        "folders": rows,
    }, None, 200


def _confirm_under_association(
    drive: Any,
    folder_id: str,
    association_folder_id: str,
    parent_id: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    scope, confirm_err = _resolve_under_supplier(drive, folder_id, parent_id)
    if confirm_err or not scope:
        return None, "folder_not_under_association"
    top = scope["supplier"]
    if str(top.get("id") or "") != association_folder_id:
        return None, "folder_not_under_association"
    return scope, None


def file_is_under_association(
    association_folder_id: str,
    file_id: str,
    user_access_token: Optional[str] = None,
) -> Tuple[bool, Optional[str], int]:
    """True when file_id is the association folder or sits inside it."""
    root_id = (association_folder_id or "").strip()
    fid = (file_id or "").strip()
    if not root_id or not fid:
        return False, "missing_folder_id", 400
    drives, err = _candidate_drives(user_access_token)
    if not drives:
        return False, err, 503
    parent_id, parent_err = _parent_or_error()
    if parent_err:
        return False, parent_err, 503

    for drive in drives:
        meta = _get_drive_meta(drive, fid)
        if not meta or meta.get("trashed"):
            continue
        check_id = fid
        if meta.get("mimeType") != FOLDER_MIME:
            parents = [str(parent) for parent in (meta.get("parents") or []) if parent]
            check_id = parents[0] if parents else ""
            if not check_id:
                continue
        scope, confirm_err = _confirm_under_association(drive, check_id, root_id, parent_id)
        if scope and not confirm_err:
            return True, None, 200
    return False, "folder_not_under_association", 404


def list_association_documents(
    association_folder_id: str,
    folder_id: str,
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    fid = (folder_id or association_folder_id or "").strip()
    root_id = (association_folder_id or "").strip()
    if not fid or not root_id:
        return None, "missing_folder_id", 400

    drives, err = _candidate_drives(user_access_token)
    if not drives:
        return None, err, 503
    parent_id, parent_err = _parent_or_error()
    if parent_err:
        return None, parent_err, 503

    scope = None
    drive_used = None
    last_err = "folder_not_under_association"
    for drive in drives:
        found, confirm_err = _confirm_under_association(drive, fid, root_id, parent_id)
        if found:
            scope = found
            drive_used = drive
            last_err = ""
            break
        last_err = confirm_err or last_err
    if not scope or drive_used is None:
        return None, last_err, 404

    current = scope["current"]
    current_id = str(current.get("id") or fid)
    children, list_err = _list_children(drive_used, current_id)
    if list_err:
        return None, "Google Drive error while listing this folder.", 502

    files: List[Dict[str, Any]] = []
    subfolders: List[Dict[str, Any]] = []
    for item in children:
        if not item.get("id"):
            continue
        normalized = _normalize_file(item)
        if normalized["file_type"] == "folder":
            subfolders.append(normalized)
        else:
            files.append(normalized)
    files.sort(key=lambda row: (row.get("name") or "").lower())
    subfolders.sort(key=lambda row: (row.get("name") or "").lower())
    current_name = str(current.get("name") or "Folder")
    return {
        "current_folder": {
            "id": current_id,
            "name": current_name,
            "folder_url": current.get("webViewLink") or drive_folder_url(current_id),
        },
        "path": scope["path"],
        "folders": subfolders,
        "files": files,
    }, None, 200


def upload_association_document(
    association_folder_id: str,
    folder_id: str,
    file_bytes: bytes,
    filename: str,
    *,
    content_type: Optional[str] = None,
    display_name: Optional[str] = None,
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    root_id = (association_folder_id or "").strip()
    fid = (folder_id or root_id).strip()
    if not fid or not root_id:
        return None, "missing_folder_id", 400
    name = resolve_upload_filename(filename, display_name)
    if not file_bytes:
        return None, "empty_file", 400
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        return None, "file_too_large", 400

    drives, err = _candidate_drives(user_access_token)
    if not drives:
        return None, err, 503
    parent_id, parent_err = _parent_or_error()
    if parent_err:
        return None, parent_err, 503

    confirmed = False
    for drive in drives:
        scope, confirm_err = _confirm_under_association(drive, fid, root_id, parent_id)
        if scope and not confirm_err:
            confirmed = True
            break
    if not confirmed:
        return None, "folder_not_under_association", 404

    mime = mimetype_for_filename(name, content_type)
    token = (user_access_token or "").strip() or None
    last_message = "Could not upload the file."
    last_status = 502
    for drive in drives:
        try:
            created = upload_bytes_to_folder(
                file_bytes,
                name,
                fid,
                mimetype=mime,
                drive=drive,
                user_access_token=token,
            )
            file_id = created.get("id") or ""
            return {
                "id": file_id,
                "name": name,
                "web_view_link": created.get("url") or drive_file_url(file_id),
                "folder_id": fid,
                "folder_url": drive_folder_url(fid),
            }, None, 200
        except MemberFolderDriveError as exc:
            last_message = exc.message
            last_status = exc.status_code
            logger.info("Association upload retry folder=%s: %s", fid, exc.message)
        except HttpError as exc:
            last_message = f"Google Drive error: {getattr(exc, 'reason', exc)}"
            if is_sa_quota_error(exc):
                last_message = (
                    "Google blocked the upload: the service account has no My Drive storage. "
                    "Sign in again so the file can be uploaded as you, "
                    "or move the Associations folder into a Shared Drive."
                )
            last_status = 502
    return None, last_message, last_status
