"""
Invoicing Drive browser — list businesses and invoice documents via service account.

Folder-based categories discover business folders under a configured parent.
OMS uses flat-file grouping by filename convention: "{business} - {invoice}.pdf".
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple

from googleapiclient.errors import HttpError

from tools.one_month_savings import INVOICE_STORAGE_FOLDER_ID, get_drive_service

logger = logging.getLogger(__name__)

FOLDER_MIME = "application/vnd.google-apps.folder"

DiscoveryMode = Literal["child_folders", "flat_files_group_by_filename"]

# Best-effort invoice number from filenames like "Acme - INV-123.pdf"
_INVOICE_AFTER_DASH = re.compile(
    r"^.+\s+-\s+(.+?)\.(?:pdf|docx?|xlsx?|png|jpe?g)$",
    re.IGNORECASE,
)
_CLEAR_INV = re.compile(r"\b(INV[- ]?\d[\w-]*)\b", re.IGNORECASE)


@dataclass(frozen=True)
class CategoryConfig:
    key: str
    business_parent_folder_id: str
    discovery: DiscoveryMode


def _automation_parent() -> str:
    return (
        os.getenv("INVOICING_DRIVE_AUTOMATION_PARENT_FOLDER_ID")
        or "1Ndt41WEPS1jI6kSHyQ_0aY_6pqAldeQg"  # Automation Services / Invoices Generated
    ).strip()


def _equipment_parent() -> str:
    return (
        os.getenv("INVOICING_DRIVE_EQUIPMENT_PARENT_FOLDER_ID")
        or "1helMnTD4-Iq2r1t6fBQilXGFWH81P8Ca"  # Equipment / Client Invoice Folders
    ).strip()


def _solar_parent() -> str:
    return (
        os.getenv("INVOICING_DRIVE_SOLAR_PARENT_FOLDER_ID")
        or "1AZlUPjUlMZjhMlhnihl9KPQ_M7zl_dE7"  # Solar Cleaning Invoice / Invoices
    ).strip()


def _scrubber_parent() -> str:
    return (
        os.getenv("INVOICING_DRIVE_SCRUBBER_PARENT_FOLDER_ID")
        or "1gusmTJo8olUh4GpbTllutMWVtpmyXWHl"  # Cleaning Scrubber Invoicing
    ).strip()


def _oms_parent() -> str:
    return (
        os.getenv("ONE_MONTH_SAVINGS_DRIVE_FOLDER_ID") or INVOICE_STORAGE_FOLDER_ID or ""
    ).strip()


def get_category_config(category_key: str) -> Optional[CategoryConfig]:
    key = (category_key or "").strip().lower().replace(" ", "_").replace("-", "_")
    configs: Dict[str, CategoryConfig] = {
        "automation_services": CategoryConfig(
            key="automation_services",
            business_parent_folder_id=_automation_parent(),
            discovery="child_folders",
        ),
        "equipment_rental": CategoryConfig(
            key="equipment_rental",
            business_parent_folder_id=_equipment_parent(),
            discovery="child_folders",
        ),
        "solar_cleaning": CategoryConfig(
            key="solar_cleaning",
            business_parent_folder_id=_solar_parent(),
            discovery="child_folders",
        ),
        "cleaning_scrubber": CategoryConfig(
            key="cleaning_scrubber",
            business_parent_folder_id=_scrubber_parent(),
            discovery="child_folders",
        ),
        "one_month_savings": CategoryConfig(
            key="one_month_savings",
            business_parent_folder_id=_oms_parent(),
            discovery="flat_files_group_by_filename",
        ),
    }
    return configs.get(key)


def list_category_keys() -> list[str]:
    return [
        "automation_services",
        "one_month_savings",
        "equipment_rental",
        "solar_cleaning",
        "cleaning_scrubber",
    ]


def folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def file_view_url(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/view"


def file_preview_url(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/preview"


def file_type_from_mime(mime: str, name: str) -> str:
    m = (mime or "").lower()
    n = (name or "").lower()
    if m == "application/pdf" or n.endswith(".pdf"):
        return "pdf"
    if "spreadsheet" in m or n.endswith(".xlsx") or n.endswith(".xls") or n.endswith(".csv"):
        return "sheet"
    if "document" in m or n.endswith(".docx") or n.endswith(".doc"):
        return "doc"
    if m.startswith("image/") or n.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
        return "image"
    return "other"


def infer_invoice_number(filename: str) -> Optional[str]:
    """Best-effort only. Return a clear token or None — no heavy parsing."""
    if not filename:
        return None
    m = _CLEAR_INV.search(filename)
    if m:
        return m.group(1).strip()
    m2 = _INVOICE_AFTER_DASH.match(filename.strip())
    if m2:
        token = m2.group(1).strip()
        # Only treat as invoice # if it looks like a short code, not a long description
        if token and len(token) <= 40 and " " not in token:
            return token
        if token and len(token) <= 24 and " " not in token:
            return token
    return None


def oms_business_key(business_name: str) -> str:
    """Stable opaque id for OMS flat grouping (not a Drive folder id)."""
    normalized = " ".join((business_name or "").strip().lower().split())
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
    return f"oms_{digest}"


def parse_oms_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse '{business} - {invoiceNumber}.ext' → (business_name, invoice_token).
    Returns (None, None) if pattern does not match.
    """
    stem = filename.rsplit(".", 1)[0] if "." in filename else filename
    if " - " not in stem:
        return None, None
    business, rest = stem.split(" - ", 1)
    business = business.strip()
    rest = rest.strip()
    if not business:
        return None, None
    return business, rest or None


def _list_children(
    drive,
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
        fields = "nextPageToken, files(id, name)"
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
        logger.exception("Drive list children failed parent=%s", parent_id)
        status = getattr(e.resp, "status", None)
        if status == 404:
            return [], "folder_not_found"
        return [], f"drive_error:{e.reason}"
    except Exception as e:
        logger.exception("Drive list children unexpected parent=%s", parent_id)
        return [], str(e)
    return items, None


def _normalize_document(f: Dict[str, Any], *, inferred: Optional[str] = None) -> Dict[str, Any]:
    fid = f.get("id") or ""
    name = f.get("name") or "Document"
    mime = f.get("mimeType") or ""
    invoice_no = inferred if inferred is not None else infer_invoice_number(name)
    return {
        "id": fid,
        "name": name,
        "mime_type": mime,
        "file_type": file_type_from_mime(mime, name),
        "web_view_link": f.get("webViewLink") or file_view_url(fid),
        "preview_url": file_preview_url(fid),
        "created_time": f.get("createdTime"),
        "modified_time": f.get("modifiedTime"),
        "inferred_invoice_number": invoice_no,
    }


def _drive_or_error() -> Tuple[Any, Optional[str]]:
    drive = get_drive_service()
    if not drive:
        return None, (
            "Google Drive is not configured. Set SERVICE_ACCOUNT_FILE or "
            "SERVICE_ACCOUNT_JSON and share folders with the service account."
        )
    return drive, None


def list_businesses(category_key: str) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    """
    Returns (payload, error_code, http_hint).
    http_hint: 400 unknown category, 502 drive errors.
    document_count is always null (avoid N+1 Drive calls).
    """
    cfg = get_category_config(category_key)
    if not cfg:
        return None, "unknown_category", 400
    if not cfg.business_parent_folder_id:
        return None, "folder_not_configured", 502

    drive, err = _drive_or_error()
    if err:
        return None, err, 502

    if cfg.discovery == "child_folders":
        folders, list_err = _list_children(
            drive, cfg.business_parent_folder_id, folders_only=True
        )
        if list_err:
            code = "folder_not_found" if list_err == "folder_not_found" else "drive_error"
            return None, code, 502
        businesses = [
            {
                "id": f["id"],
                "name": f.get("name") or "Untitled",
                "folder_id": f["id"],
                "folder_url": folder_url(f["id"]),
                "document_count": None,
            }
            for f in folders
            if f.get("id")
        ]
        businesses.sort(key=lambda b: (b["name"] or "").lower())
        return {
            "category": cfg.key,
            "business_parent_folder_id": cfg.business_parent_folder_id,
            "business_parent_folder_url": folder_url(cfg.business_parent_folder_id),
            "discovery": cfg.discovery,
            "businesses": businesses,
        }, None, 200

    # OMS flat files
    files, list_err = _list_children(
        drive, cfg.business_parent_folder_id, files_only=True
    )
    if list_err:
        code = "folder_not_found" if list_err == "folder_not_found" else "drive_error"
        return None, code, 502

    by_business: Dict[str, str] = {}  # key -> display name
    for f in files:
        name = f.get("name") or ""
        business, _inv = parse_oms_filename(name)
        if not business:
            continue
        key = oms_business_key(business)
        if key not in by_business:
            by_business[key] = business

    businesses = [
        {
            "id": key,
            "name": display,
            "folder_id": cfg.business_parent_folder_id,
            "folder_url": folder_url(cfg.business_parent_folder_id),
            "document_count": None,
        }
        for key, display in by_business.items()
    ]
    businesses.sort(key=lambda b: (b["name"] or "").lower())
    return {
        "category": cfg.key,
        "business_parent_folder_id": cfg.business_parent_folder_id,
        "business_parent_folder_url": folder_url(cfg.business_parent_folder_id),
        "discovery": cfg.discovery,
        "businesses": businesses,
    }, None, 200


def _confirm_direct_child_folder(
    drive, parent_id: str, child_folder_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Confirm child_folder_id is a folder whose parents include parent_id.
    On failure returns (None, error) without leaking unrelated metadata.
    """
    try:
        meta = (
            drive.files()
            .get(
                fileId=child_folder_id,
                fields="id, name, mimeType, parents, trashed",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as e:
        status = getattr(e.resp, "status", None)
        logger.warning(
            "Drive get business folder failed id=%s status=%s",
            child_folder_id,
            status,
        )
        return None, "not_found"
    except Exception:
        logger.exception("Drive get business folder unexpected id=%s", child_folder_id)
        return None, "not_found"

    if meta.get("trashed"):
        return None, "not_found"
    if meta.get("mimeType") != FOLDER_MIME:
        return None, "not_found"
    parents = meta.get("parents") or []
    if parent_id not in parents:
        return None, "not_found"
    return meta, None


def list_documents(
    category_key: str, business_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    cfg = get_category_config(category_key)
    if not cfg:
        return None, "unknown_category", 400
    bid = (business_id or "").strip()
    if not bid:
        return None, "missing_business_id", 400
    if not cfg.business_parent_folder_id:
        return None, "folder_not_configured", 502

    drive, err = _drive_or_error()
    if err:
        return None, err, 502

    if cfg.discovery == "child_folders":
        meta, not_ok = _confirm_direct_child_folder(
            drive, cfg.business_parent_folder_id, bid
        )
        if not_ok or not meta:
            return None, "business_not_found", 404

        files, list_err = _list_children(drive, bid, files_only=True)
        if list_err:
            return None, "drive_error", 502

        docs = [_normalize_document(f) for f in files if f.get("id")]
        docs.sort(key=lambda d: (d.get("name") or "").lower())
        return {
            "category": cfg.key,
            "business": {
                "id": meta["id"],
                "name": meta.get("name") or "Untitled",
                "folder_id": meta["id"],
                "folder_url": folder_url(meta["id"]),
            },
            "documents": docs,
        }, None, 200

    # OMS: business_id is oms_* key; filter files in shared parent folder
    if not bid.startswith("oms_"):
        return None, "business_not_found", 404

    files, list_err = _list_children(
        drive, cfg.business_parent_folder_id, files_only=True
    )
    if list_err:
        return None, "drive_error", 502

    matched: List[Dict[str, Any]] = []
    display_name: Optional[str] = None
    for f in files:
        name = f.get("name") or ""
        business, inv_token = parse_oms_filename(name)
        if not business:
            continue
        if oms_business_key(business) != bid:
            continue
        if display_name is None:
            display_name = business
        inferred = inv_token if inv_token and len(inv_token) <= 40 else infer_invoice_number(name)
        matched.append(_normalize_document(f, inferred=inferred))

    if display_name is None:
        return None, "business_not_found", 404

    matched.sort(key=lambda d: (d.get("name") or "").lower())
    return {
        "category": cfg.key,
        "business": {
            "id": bid,
            "name": display_name,
            "folder_id": cfg.business_parent_folder_id,
            "folder_url": folder_url(cfg.business_parent_folder_id),
        },
        "documents": matched,
    }, None, 200
