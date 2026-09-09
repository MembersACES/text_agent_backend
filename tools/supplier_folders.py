"""Supplier Drive folders — list and upload under 005-Suppliers / Supplier Folders."""

from __future__ import annotations

import logging
import os
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError

from tools.member_folder_drive import (
    MemberFolderDriveError,
    find_or_create_folder,
    upload_bytes_to_folder,
    _user_drive_service,
)
from tools.one_month_savings import extract_folder_id_from_url, get_drive_service
from tools.share_folder import drive_file_url, drive_folder_url, is_sa_quota_error

logger = logging.getLogger(__name__)

FOLDER_MIME = "application/vnd.google-apps.folder"
SUPPLIER_FOLDERS_NAME = "Supplier Folders"
PREFERRED_PARENT_NAME = "005-Suppliers"
MAX_UPLOAD_BYTES = 50 * 1024 * 1024
DEFAULT_SUPPLIER_FOLDERS_PARENT_ID = "1mUQrw6CiTOiOpCLrguEPsypTl9FU3XUk"

_DRIVE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{10,}$")


class SupplierCategory(str, Enum):
    ENERGY = "energy"
    WASTE = "waste"
    OTHER = "other"


# Matched against alphanumeric-only lowercase folder names (substring).
_WASTE_KEYS = (
    "veolia",
    "visy",
    "vizy",
    "cleanaway",
)

_ENERGY_KEYS = (
    "alinta",
    "origin",
    "momentum",
    "shell",
    "covau",
    "pluses",
    "obee",
    "energyaustralia",
    "agl",
    "bluenrg",
    "nextbusinessenergy",
    "1stenergy",
    "redenergy",
    "globird",
    "powerdirect",
    "sumo",
    "tango",
    "sunretail",
    "ergon",
    "powermetric",
    "simplyenergy",
    "lumo",
    "goodwe",
    "solarppa",
    "enelx",
    "enel",
    "nbe",
)


def _norm_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower())


def classify_supplier(name: str) -> SupplierCategory:
    key = _norm_name(name)
    if not key:
        return SupplierCategory.OTHER
    if any(token in key for token in _WASTE_KEYS):
        return SupplierCategory.WASTE
    if any(token in key for token in _ENERGY_KEYS):
        return SupplierCategory.ENERGY
    return SupplierCategory.OTHER


def _configured_parent_id() -> str:
    raw = (
        os.getenv("SUPPLIER_FOLDERS_PARENT_ID")
        or os.getenv("SUPPLIER_FOLDERS_PARENT_URL")
        or DEFAULT_SUPPLIER_FOLDERS_PARENT_ID
        or ""
    ).strip()
    if not raw:
        return ""
    if _DRIVE_ID_RE.match(raw) and "http" not in raw.lower() and "/" not in raw:
        return raw
    extracted = extract_folder_id_from_url(raw)
    return (extracted or "").strip()


def get_supplier_folders_parent_id() -> str:
    return _configured_parent_id()


def _escape_drive_query(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def _drive_or_error() -> Tuple[Any, Optional[str]]:
    drive = get_drive_service()
    if not drive:
        return None, (
            "Google Drive is not configured. Set SERVICE_ACCOUNT_FILE or "
            "SERVICE_ACCOUNT_JSON, and share Supplier Folders with the service account."
        )
    return drive, None


def _not_configured_message() -> str:
    return (
        "Supplier Folders parent is not configured. Set SUPPLIER_FOLDERS_PARENT_ID "
        "to the Drive folder ID of 005-Suppliers → Supplier Folders (share that folder "
        "with the service account)."
    )


def _discover_parent_id(drive: Any) -> Tuple[str, Optional[str]]:
    configured = _configured_parent_id()
    if configured:
        return configured, None

    query = (
        f"name = '{_escape_drive_query(SUPPLIER_FOLDERS_NAME)}' "
        f"and mimeType = '{FOLDER_MIME}' and trashed = false"
    )
    try:
        result = (
            drive.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id, name, parents)",
                pageSize=20,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
    except HttpError as e:
        logger.exception("Drive search for Supplier Folders failed")
        status = getattr(e.resp, "status", None)
        if status == 404:
            return "", _not_configured_message()
        return "", f"Google Drive error: {getattr(e, 'reason', e)}"

    files = [f for f in (result.get("files") or []) if f.get("id")]
    if not files:
        return "", _not_configured_message()
    if len(files) == 1:
        return str(files[0]["id"]), None

    for item in files:
        for parent_id in item.get("parents") or []:
            try:
                parent = (
                    drive.files()
                    .get(
                        fileId=parent_id,
                        fields="id, name",
                        supportsAllDrives=True,
                    )
                    .execute()
                )
            except HttpError:
                continue
            if (parent.get("name") or "").strip() == PREFERRED_PARENT_NAME:
                return str(item["id"]), None
    return str(files[0]["id"]), None


def _list_children(
    drive: Any,
    parent_id: str,
    *,
    folders_only: bool = False,
    files_only: bool = False,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if folders_only:
        q = (
            f"'{parent_id}' in parents and trashed = false "
            f"and mimeType = '{FOLDER_MIME}'"
        )
        fields = (
            "nextPageToken, files(id, name, mimeType, webViewLink, "
            "createdTime, modifiedTime)"
        )
    elif files_only:
        q = (
            f"'{parent_id}' in parents and trashed = false "
            f"and mimeType != '{FOLDER_MIME}'"
        )
        fields = (
            "nextPageToken, files(id, name, mimeType, webViewLink, "
            "createdTime, modifiedTime, size)"
        )
    else:
        q = f"'{parent_id}' in parents and trashed = false"
        fields = (
            "nextPageToken, files(id, name, mimeType, webViewLink, "
            "createdTime, modifiedTime, size)"
        )

    items: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    try:
        while True:
            kwargs: Dict[str, Any] = {
                "q": q,
                "spaces": "drive",
                "fields": fields,
                "pageSize": 100,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
            }
            if page_token:
                kwargs["pageToken"] = page_token
            result = drive.files().list(**kwargs).execute()
            items.extend(result.get("files") or [])
            page_token = result.get("nextPageToken")
            if not page_token:
                break
    except HttpError as e:
        logger.exception("Drive list supplier children failed parent=%s", parent_id)
        status = getattr(e.resp, "status", None)
        if status == 404:
            return [], "folder_not_found"
        return [], f"drive_error:{getattr(e, 'reason', e)}"
    return items, None


_MAX_FOLDER_DEPTH = 24


def _get_drive_meta(drive: Any, file_id: str) -> Optional[Dict[str, Any]]:
    try:
        return (
            drive.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType, parents, trashed, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as e:
        status = getattr(e.resp, "status", None)
        logger.warning("Drive get folder failed id=%s status=%s", file_id, status)
        return None
    except Exception:
        logger.exception("Drive get folder unexpected id=%s", file_id)
        return None


def _folder_crumb(meta: Dict[str, Any]) -> Dict[str, str]:
    fid = str(meta.get("id") or "")
    return {
        "id": fid,
        "name": str(meta.get("name") or "Folder"),
        "folder_url": meta.get("webViewLink") or drive_folder_url(fid),
    }


def _resolve_under_supplier(
    drive: Any, folder_id: str, suppliers_parent_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Confirm folder_id is a supplier folder or nested under one.

    Returns ({"current", "supplier", "path"}, error_code).
    path is supplier → … → current folder.
    """
    current = _get_drive_meta(drive, folder_id)
    if not current or current.get("trashed"):
        return None, "not_found"
    if current.get("mimeType") != FOLDER_MIME:
        return None, "not_found"

    chain = [current]
    seen = {str(current.get("id") or folder_id)}
    cursor = current
    for _ in range(_MAX_FOLDER_DEPTH):
        parents = cursor.get("parents") or []
        if suppliers_parent_id in parents:
            path = [_folder_crumb(item) for item in reversed(chain)]
            return {
                "current": current,
                "supplier": cursor,
                "path": path,
            }, None
        next_id = next((str(p) for p in parents if p and str(p) not in seen), "")
        if not next_id:
            return None, "not_found"
        seen.add(next_id)
        parent_meta = _get_drive_meta(drive, next_id)
        if not parent_meta or parent_meta.get("trashed"):
            return None, "not_found"
        if parent_meta.get("mimeType") != FOLDER_MIME:
            return None, "not_found"
        chain.append(parent_meta)
        cursor = parent_meta
    return None, "not_found"


def file_type_from_mime(mime: str, name: str) -> str:
    m = (mime or "").lower()
    n = (name or "").lower()
    if m == FOLDER_MIME:
        return "folder"
    if m == "application/pdf" or n.endswith(".pdf"):
        return "pdf"
    if (
        "spreadsheet" in m
        or m == "application/vnd.google-apps.spreadsheet"
        or n.endswith((".xlsx", ".xls", ".csv"))
    ):
        return "sheet"
    if m == "application/vnd.google-apps.presentation" or n.endswith((".pptx", ".ppt")):
        return "slides"
    if (
        "document" in m
        or m == "application/vnd.google-apps.document"
        or n.endswith((".docx", ".doc"))
    ):
        return "doc"
    if m.startswith("image/") or n.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
        return "image"
    return "other"


def mimetype_for_filename(filename: str, content_type: Optional[str] = None) -> str:
    hinted = (content_type or "").strip()
    if hinted and hinted != "application/octet-stream":
        return hinted
    n = (filename or "").lower()
    mapping = {
        ".pdf": "application/pdf",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".xls": "application/vnd.ms-excel",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".csv": "text/csv",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".txt": "text/plain",
        ".zip": "application/zip",
    }
    for ext, mime in mapping.items():
        if n.endswith(ext):
            return mime
    return hinted or "application/octet-stream"


_INVALID_FILENAME = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')
_MAX_FILENAME_LEN = 180
_MAX_FOLDER_NAME_LEN = 120


def resolve_upload_filename(original: str, override: Optional[str] = None) -> str:
    """Use override when set; keep the original extension if the override has none."""
    original_base = os.path.basename((original or "").strip()) or "upload.bin"
    _orig_stem, orig_ext = os.path.splitext(original_base)
    raw = os.path.basename((override or "").strip())
    if not raw:
        raw = original_base
    cleaned = _INVALID_FILENAME.sub("-", raw).strip(" .")
    if not cleaned:
        cleaned = _orig_stem or "upload"
    _stem, new_ext = os.path.splitext(cleaned)
    if orig_ext and not new_ext:
        cleaned = f"{cleaned}{orig_ext}"
    return cleaned[:_MAX_FILENAME_LEN]


def resolve_supplier_folder_name(name: str) -> str:
    cleaned = _INVALID_FILENAME.sub("-", (name or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-")
    return cleaned[:_MAX_FOLDER_NAME_LEN]


def _normalize_file(item: Dict[str, Any]) -> Dict[str, Any]:
    fid = item.get("id") or ""
    name = item.get("name") or "Document"
    mime = item.get("mimeType") or ""
    is_folder = mime == FOLDER_MIME
    return {
        "id": fid,
        "name": name,
        "mime_type": mime,
        "file_type": file_type_from_mime(mime, name),
        "web_view_link": item.get("webViewLink")
        or (drive_folder_url(fid) if is_folder else drive_file_url(fid)),
        "preview_url": None if is_folder else f"https://drive.google.com/file/d/{fid}/preview",
        "created_time": item.get("createdTime"),
        "modified_time": item.get("modifiedTime"),
        "size": item.get("size"),
    }


def list_supplier_folders() -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    drive, err = _drive_or_error()
    if err:
        return None, err, 503

    parent_id, parent_err = _discover_parent_id(drive)
    if parent_err or not parent_id:
        return None, parent_err or _not_configured_message(), 503

    folders, list_err = _list_children(drive, parent_id, folders_only=True)
    if list_err:
        if list_err == "folder_not_found":
            return None, (
                "Supplier Folders was not found, or the service account cannot access it. "
                "Share 005-Suppliers → Supplier Folders with the service account."
            ), 502
        return None, list_err.replace("drive_error:", "Google Drive error: "), 502

    suppliers = []
    for item in folders:
        fid = item.get("id")
        if not fid:
            continue
        name = item.get("name") or "Untitled"
        category = classify_supplier(name)
        suppliers.append(
            {
                "id": fid,
                "name": name,
                "folder_id": fid,
                "folder_url": item.get("webViewLink") or drive_folder_url(fid),
                "category": category.value,
                "modified_time": item.get("modifiedTime"),
            }
        )
    suppliers.sort(key=lambda row: (row["name"] or "").lower())
    return {
        "parent_folder_id": parent_id,
        "parent_folder_url": drive_folder_url(parent_id),
        "suppliers": suppliers,
    }, None, 200


def _supplier_row(fid: str, name: str, folder_url: str, modified_time: Optional[str] = None) -> Dict[str, Any]:
    return {
        "id": fid,
        "name": name,
        "folder_id": fid,
        "folder_url": folder_url,
        "category": classify_supplier(name).value,
        "modified_time": modified_time,
    }


def create_supplier_folder(
    name: str,
    *,
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    folder_name = resolve_supplier_folder_name(name)
    if not folder_name:
        return None, "missing_name", 400

    drive, err = _drive_or_error()
    if err:
        return None, err, 503

    parent_id, parent_err = _discover_parent_id(drive)
    if parent_err or not parent_id:
        return None, parent_err or _not_configured_message(), 503

    folders, list_err = _list_children(drive, parent_id, folders_only=True)
    if list_err:
        if list_err == "folder_not_found":
            return None, (
                "Supplier Folders was not found, or the service account cannot access it. "
                "Share 005-Suppliers → Supplier Folders with the service account."
            ), 502
        return None, list_err.replace("drive_error:", "Google Drive error: "), 502

    wanted = folder_name.lower()
    for item in folders:
        existing_name = (item.get("name") or "").strip()
        fid = item.get("id")
        if fid and existing_name.lower() == wanted:
            return {
                **_supplier_row(
                    str(fid),
                    existing_name,
                    item.get("webViewLink") or drive_folder_url(str(fid)),
                    item.get("modifiedTime"),
                ),
                "created": False,
            }, None, 200

    token = (user_access_token or "").strip() or None

    def _create(service: Any) -> Tuple[str, bool]:
        return find_or_create_folder(parent_id, folder_name, drive=service)

    try:
        folder_id, created = _create(drive)
    except MemberFolderDriveError as e:
        if token:
            logger.info("Supplier folder create retrying as signed-in user name=%r", folder_name)
            try:
                folder_id, created = _create(_user_drive_service(token))
            except MemberFolderDriveError as user_err:
                logger.warning("Supplier folder create failed: %s", user_err.message)
                return None, user_err.message, user_err.status_code
        else:
            logger.warning("Supplier folder create failed: %s", e.message)
            return None, e.message, e.status_code
    except HttpError as e:
        if is_sa_quota_error(e):
            if token:
                try:
                    folder_id, created = _create(_user_drive_service(token))
                except MemberFolderDriveError as user_err:
                    return None, user_err.message, user_err.status_code
            else:
                return None, (
                    "Google blocked the folder create: the service account has no My Drive storage. "
                    "Re-auth Google in the dashboard so the folder can be created as you, "
                    "or move Supplier Folders into a Shared Drive."
                ), 502
        else:
            return None, f"Google Drive error: {getattr(e, 'reason', e)}", 502

    return {
        **_supplier_row(folder_id, folder_name, drive_folder_url(folder_id)),
        "created": created,
    }, None, 200


def list_supplier_documents(
    folder_id: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    fid = (folder_id or "").strip()
    if not fid:
        return None, "missing_folder_id", 400

    drive, err = _drive_or_error()
    if err:
        return None, err, 503

    parent_id, parent_err = _discover_parent_id(drive)
    if parent_err or not parent_id:
        return None, parent_err or _not_configured_message(), 503

    scope, confirm_err = _resolve_under_supplier(drive, fid, parent_id)
    if confirm_err or not scope:
        return None, "supplier_not_found", 404

    current = scope["current"]
    supplier = scope["supplier"]
    current_id = str(current.get("id") or fid)
    supplier_id = str(supplier.get("id") or "")
    supplier_name = str(supplier.get("name") or "Supplier")

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
        "supplier": {
            "id": supplier_id,
            "name": supplier_name,
            "folder_id": supplier_id,
            "folder_url": supplier.get("webViewLink") or drive_folder_url(supplier_id),
            "category": classify_supplier(supplier_name).value,
        },
        "current_folder": {
            "id": current_id,
            "name": str(current.get("name") or supplier_name),
            "folder_url": current.get("webViewLink") or drive_folder_url(current_id),
        },
        "path": scope["path"],
        "folders": subfolders,
        "files": files,
    }, None, 200


def upload_supplier_document(
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

    parent_id, parent_err = _discover_parent_id(drive)
    if parent_err or not parent_id:
        return None, parent_err or _not_configured_message(), 503

    scope, confirm_err = _resolve_under_supplier(drive, fid, parent_id)
    if confirm_err or not scope:
        return None, "supplier_not_found", 404
    meta = scope["current"]
    supplier = scope["supplier"]

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
            logger.info("Supplier upload retrying as signed-in user folder=%s", fid)
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
                    "Supplier upload failed folder=%s: %s", fid, user_err.message
                )
                return None, user_err.message, user_err.status_code
        else:
            logger.warning("Supplier upload failed folder=%s: %s", fid, e.message)
            return None, e.message, e.status_code
    except HttpError as e:
        if is_sa_quota_error(e):
            return None, (
                "Google blocked the upload: the service account has no My Drive storage. "
                "Re-auth Google in the dashboard so the file can be uploaded as you, "
                "or move Supplier Folders into a Shared Drive."
            ), 502
        return None, f"Google Drive error: {getattr(e, 'reason', e)}", 502

    file_id = created.get("id") or ""
    return {
        "id": file_id,
        "name": name,
        "web_view_link": created.get("url") or drive_file_url(file_id),
        "folder_id": fid,
        "folder_url": drive_folder_url(fid),
        "supplier_name": supplier.get("name") or meta.get("name") or "",
    }, None, 200
