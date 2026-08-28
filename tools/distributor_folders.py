"""Distributor Drive folders — list and upload under 003-Distributors."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError

from tools.member_folder_drive import (
    DISTRIBUTORS_FOLDER_ID,
    MemberFolderDriveError,
    upload_bytes_to_folder,
    _user_drive_service,
)
from tools.one_month_savings import get_drive_service
from tools.share_folder import drive_file_url, drive_folder_url, is_sa_quota_error
from tools.supplier_folders import (
    MAX_UPLOAD_BYTES,
    _list_children,
    _normalize_file,
    _resolve_under_supplier,
    mimetype_for_filename,
    resolve_upload_filename,
)

logger = logging.getLogger(__name__)

DOCUMENTS_FOLDER_NAME = "Distributor Documents"
_PREFIX = "A - "


def display_distributor_name(name: str) -> str:
    n = (name or "").strip()
    if n.lower().startswith(_PREFIX.lower()):
        return n[len(_PREFIX) :].strip() or n
    return n


def get_distributors_parent_id() -> str:
    return (DISTRIBUTORS_FOLDER_ID or "").strip()


def _drive_or_error() -> Tuple[Any, Optional[str]]:
    drive = get_drive_service()
    if not drive:
        return None, (
            "Google Drive is not configured. Set SERVICE_ACCOUNT_FILE or "
            "SERVICE_ACCOUNT_JSON, and share 003-Distributors with the service account."
        )
    return drive, None


def _not_configured_message() -> str:
    return (
        "003-Distributors parent is not configured. Set DISTRIBUTORS_FOLDER_ID "
        "to the Drive folder ID (share that folder with the service account)."
    )


def _folder_payload(item: Dict[str, Any]) -> Dict[str, Any]:
    fid = item.get("id") or ""
    name = item.get("name") or "Untitled"
    return {
        "id": fid,
        "name": name,
        "display_name": display_distributor_name(name),
        "folder_id": fid,
        "folder_url": item.get("webViewLink") or drive_folder_url(fid),
        "modified_time": item.get("modifiedTime"),
    }


def list_distributor_folders() -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    drive, err = _drive_or_error()
    if err:
        return None, err, 503

    parent_id = get_distributors_parent_id()
    if not parent_id:
        return None, _not_configured_message(), 503

    folders, list_err = _list_children(drive, parent_id, folders_only=True)
    if list_err:
        if list_err == "folder_not_found":
            return None, (
                "003-Distributors was not found, or the service account cannot access it. "
                "Share that folder with the service account."
            ), 502
        return None, list_err.replace("drive_error:", "Google Drive error: "), 502

    distributors = []
    for item in folders:
        if not item.get("id"):
            continue
        distributors.append(_folder_payload(item))
    distributors.sort(key=lambda row: (row.get("display_name") or row.get("name") or "").lower())
    return {
        "parent_folder_id": parent_id,
        "parent_folder_url": drive_folder_url(parent_id),
        "distributors": distributors,
    }, None, 200


def list_distributor_documents(
    folder_id: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    fid = (folder_id or "").strip()
    if not fid:
        return None, "missing_folder_id", 400

    drive, err = _drive_or_error()
    if err:
        return None, err, 503

    parent_id = get_distributors_parent_id()
    if not parent_id:
        return None, _not_configured_message(), 503

    scope, confirm_err = _resolve_under_supplier(drive, fid, parent_id)
    if confirm_err or not scope:
        return None, "distributor_not_found", 404

    current = scope["current"]
    distributor = scope["supplier"]
    current_id = str(current.get("id") or fid)
    distributor_id = str(distributor.get("id") or "")
    distributor_name = str(distributor.get("name") or "Distributor")

    children, list_err = _list_children(drive, current_id)
    if list_err:
        return None, "drive_error", 502

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

    return {
        "distributor": {
            "id": distributor_id,
            "name": distributor_name,
            "display_name": display_distributor_name(distributor_name),
            "folder_id": distributor_id,
            "folder_url": distributor.get("webViewLink") or drive_folder_url(distributor_id),
        },
        "current_folder": {
            "id": current_id,
            "name": str(current.get("name") or distributor_name),
            "folder_url": current.get("webViewLink") or drive_folder_url(current_id),
        },
        "path": scope["path"],
        "folders": subfolders,
        "files": files,
    }, None, 200


def upload_distributor_document(
    folder_id: str,
    file_bytes: bytes,
    filename: str,
    *,
    content_type: Optional[str] = None,
    display_name: Optional[str] = None,
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    fid = (folder_id or "").strip()
    if not fid:
        return None, "missing_folder_id", 400
    name = resolve_upload_filename(filename, display_name)
    if not file_bytes:
        return None, "empty_file", 400
    if len(file_bytes) > MAX_UPLOAD_BYTES:
        return None, "file_too_large", 400

    drive, err = _drive_or_error()
    if err:
        return None, err, 503

    parent_id = get_distributors_parent_id()
    if not parent_id:
        return None, _not_configured_message(), 503

    scope, confirm_err = _resolve_under_supplier(drive, fid, parent_id)
    if confirm_err or not scope:
        return None, "distributor_not_found", 404
    meta = scope["current"]
    distributor = scope["supplier"]

    mime = mimetype_for_filename(name, content_type)
    token = (user_access_token or "").strip() or None
    try:
        created = upload_bytes_to_folder(
            file_bytes,
            name,
            fid,
            mimetype=mime,
            drive=drive,
            user_access_token=token,
        )
    except MemberFolderDriveError as e:
        if token:
            logger.info("Distributor upload retrying as signed-in user folder=%s", fid)
            try:
                created = upload_bytes_to_folder(
                    file_bytes,
                    name,
                    fid,
                    mimetype=mime,
                    drive=_user_drive_service(token),
                )
            except MemberFolderDriveError as user_err:
                logger.warning(
                    "Distributor upload failed folder=%s: %s", fid, user_err.message
                )
                return None, user_err.message, user_err.status_code
        else:
            logger.warning("Distributor upload failed folder=%s: %s", fid, e.message)
            return None, e.message, e.status_code
    except HttpError as e:
        if is_sa_quota_error(e):
            return None, (
                "Google blocked the upload: the service account has no My Drive storage. "
                "Re-auth Google in the dashboard so the file can be uploaded as you, "
                "or move 003-Distributors into a Shared Drive."
            ), 502
        return None, f"Google Drive error: {getattr(e, 'reason', e)}", 502

    file_id = created.get("id") or ""
    return {
        "id": file_id,
        "name": name,
        "web_view_link": created.get("url") or drive_file_url(file_id),
        "folder_id": fid,
        "folder_url": drive_folder_url(fid),
        "distributor_name": distributor.get("name") or meta.get("name") or "",
    }, None, 200
