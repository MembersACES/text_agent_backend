"""
Solar panel cleaning: signed offer file → n8n Drive upload, CRM activity, optional Sheets log.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import HTTPException
from sqlalchemy.orm import Session

from crm_enums import OfferActivityType
from models import Client, Offer, OfferActivity
from tools.business_info import get_business_information
from tools.one_month_savings import (
    extract_folder_id_from_url,
    get_drive_service,
    get_sheets_service,
)

logger = logging.getLogger(__name__)

N8N_ADDITIONAL_DOC_WEBHOOK = (
    "https://membersaces.app.n8n.cloud/webhook/additional_document_upload"
)

SOLAR_PANEL_CLEANING_UTILITY = "Solar panel cleaning"

DEFAULT_SIGNED_QUOTES_SHEET_ID = "1WiLksDOwrQkEwVhF25F_RQ1G0zxF5VHiu9lAxQhQox4"
DEFAULT_SIGNED_QUOTES_TAB = "Dashboard Quotes Signed"
SIGNED_OFFER_SUBFOLDER_NAME = "Signed Agreements"


def validate_signed_offer_filename(filename: str) -> str:
    """Same allowed types as member Additional Documents (DocumentsTab)."""
    name = (filename or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="filename is required")
    lower = name.lower()
    ok = (
        lower.endswith(".pdf")
        or lower.endswith(".png")
        or lower.endswith(".jpg")
        or lower.endswith(".jpeg")
        or lower.endswith(".xlsx")
        or lower.endswith(".xls")
        or lower.endswith(".docx")
        or lower.endswith(".doc")
    )
    if not ok:
        raise HTTPException(
            status_code=400,
            detail=(
                "File must be PDF, image (PNG/JPG), Excel (.xlsx, .xls), "
                "or Word (.docx, .doc)"
            ),
        )
    return name


def pick_document_link_from_upload_response(d: Any) -> Optional[str]:
    """Match DocumentsTab.pickDocumentLinkFromUploadResponse."""
    if isinstance(d, list) and d:
        d = d[0]
    if not d or not isinstance(d, dict):
        return None
    o: Dict[str, Any] = d
    keys = [
        "webViewLink",
        "web_view_link",
        "file_url",
        "fileUrl",
        "url",
        "link",
        "document_url",
        "view_link",
    ]
    for k in keys:
        v = o.get(k)
        if isinstance(v, str) and (
            v.startswith("http://") or v.startswith("https://")
        ):
            return v
    id_val = (
        o.get("file_id")
        or o.get("fileId")
        or o.get("File_ID")
        or o.get("File ID")
        or o.get("id")
    )
    if isinstance(id_val, str) and re.match(r"^[\w-]{10,}$", id_val):
        return f"https://drive.google.com/file/d/{id_val}/view?usp=drivesdk"
    return None


def _parse_activity_metadata_row(meta_raw: Any) -> Optional[dict]:
    if meta_raw is None:
        return None
    if isinstance(meta_raw, dict):
        return meta_raw
    if isinstance(meta_raw, str):
        try:
            return json.loads(meta_raw)
        except (TypeError, json.JSONDecodeError):
            return None
    return None


def _resolve_client_root_gdrive_folder_url(client: Client) -> str:
    direct = (client.gdrive_folder_url or "").strip()
    if direct:
        return direct
    bn = (client.business_name or "").strip()
    if not bn:
        return ""
    try:
        info = get_business_information(bn)
    except Exception:
        logger.exception("get_business_information failed for %s", bn)
        return ""
    if not isinstance(info, dict):
        return ""
    g = info.get("gdrive") or {}
    return (g.get("folder_url") or "").strip()


def _ensure_signed_agreements_subfolder_url(parent_folder_url: str) -> str:
    """
    Resolve/create the 'Signed Agreements' subfolder and return its folder URL.
    Falls back to parent_folder_url if Drive API lookup/create fails.
    """
    parent_url = (parent_folder_url or "").strip()
    if not parent_url:
        return ""
    parent_id = extract_folder_id_from_url(parent_url)
    if not parent_id:
        return parent_url
    drive_service = get_drive_service()
    if not drive_service:
        logger.warning(
            "Drive service unavailable; using parent folder for signed offer upload"
        )
        return parent_url
    try:
        query = (
            f"name='{SIGNED_OFFER_SUBFOLDER_NAME}' "
            f"and '{parent_id}' in parents "
            "and mimeType='application/vnd.google-apps.folder' "
            "and trashed=false"
        )
        result = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id,name)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        folders = result.get("files", [])
        if folders:
            subfolder_id = folders[0].get("id")
        else:
            created = (
                drive_service.files()
                .create(
                    body={
                        "name": SIGNED_OFFER_SUBFOLDER_NAME,
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": [parent_id],
                    },
                    fields="id",
                    supportsAllDrives=True,
                )
                .execute()
            )
            subfolder_id = created.get("id")
        if isinstance(subfolder_id, str) and subfolder_id.strip():
            return f"https://drive.google.com/drive/folders/{subfolder_id}"
    except Exception:
        logger.exception(
            "Failed to resolve/create Signed Agreements subfolder for parent %s",
            parent_id,
        )
    return parent_url


def resolve_client_gdrive_folder_url(client: Client) -> str:
    """
    Return the Drive folder URL used for signed solar offer uploads.
    We target the member's 'Signed Agreements' subfolder when possible.
    """
    root = _resolve_client_root_gdrive_folder_url(client)
    if not root:
        return ""
    return _ensure_signed_agreements_subfolder_url(root)


def resolve_contact_email(client: Client) -> str:
    em = (client.primary_contact_email or "").strip()
    if em:
        return em
    bn = (client.business_name or "").strip()
    if not bn:
        return ""
    try:
        info = get_business_information(bn)
    except Exception:
        logger.exception("get_business_information failed for contact email %s", bn)
        return ""
    if not isinstance(info, dict):
        return ""
    ci = info.get("contact_information") or {}
    return (ci.get("email") or "").strip()


def resolve_contact_name(client: Client) -> str:
    """
    Best-effort contact name for sheet logging.
    Uses business info representative_details.contact_name when available.
    """
    bn = (client.business_name or "").strip()
    if not bn:
        return ""
    try:
        info = get_business_information(bn)
    except Exception:
        logger.exception("get_business_information failed for contact name %s", bn)
        return ""
    if not isinstance(info, dict):
        return ""
    rep = info.get("representative_details") or {}
    contact = (rep.get("contact_name") or "").strip()
    if contact:
        return contact
    # Fallback: sometimes n8n payloads expose contact details in other sections.
    ci = info.get("contact_information") or {}
    return (ci.get("contact_name") or "").strip()


def latest_solar_quote_fields(
    db: Session, offer_id: int
) -> Tuple[Optional[str], Optional[float], Optional[str]]:
    """
    From newest solar quote activities, first non-empty quote_number,
    amount_total_inc_gst, site_name.
    """
    types = (
        OfferActivityType.SOLAR_CLEANING_QUOTE_GENERATED.value,
        OfferActivityType.SOLAR_CLEANING_QUOTE_SENT.value,
    )
    rows = (
        db.query(OfferActivity)
        .filter(
            OfferActivity.offer_id == offer_id,
            OfferActivity.activity_type.in_(types),
        )
        .order_by(OfferActivity.created_at.desc())
        .all()
    )
    quote_number: Optional[str] = None
    amount_total: Optional[float] = None
    site_name: Optional[str] = None
    for a in rows:
        meta = _parse_activity_metadata_row(getattr(a, "metadata_", None))
        if not meta:
            continue
        if quote_number is None and meta.get("quote_number"):
            qn = str(meta.get("quote_number")).strip()
            if qn:
                quote_number = qn
        if amount_total is None and meta.get("amount_total_inc_gst") is not None:
            try:
                amount_total = float(meta["amount_total_inc_gst"])
            except (TypeError, ValueError):
                pass
        if site_name is None and meta.get("site_name"):
            sn = str(meta.get("site_name")).strip()
            if sn:
                site_name = sn
        if quote_number and amount_total is not None and site_name:
            break
    return quote_number, amount_total, site_name


def utility_offer_title(offer: Offer) -> str:
    parts = [
        (offer.utility_type_identifier or "").strip(),
        (offer.utility_type or "").strip(),
    ]
    return " / ".join(p for p in parts if p) or SOLAR_PANEL_CLEANING_UTILITY


def append_dashboard_quotes_signed_row(row: List[Any]) -> Tuple[bool, Optional[str]]:
    sheet_id = (
        os.getenv("SOLAR_SIGNED_QUOTES_SHEET_ID") or DEFAULT_SIGNED_QUOTES_SHEET_ID
    ).strip()
    tab = (
        os.getenv("SOLAR_SIGNED_QUOTES_TAB") or DEFAULT_SIGNED_QUOTES_TAB
    ).strip()
    service = get_sheets_service()
    if not service:
        return False, "Google Sheets service unavailable (check service account config)"
    range_name = f"'{tab}'!A:L"
    try:
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
        return True, None
    except Exception as e:
        logger.exception("append_dashboard_quotes_signed_row failed")
        return False, str(e)


def upload_signed_offer_to_n8n(
    *,
    file_bytes: bytes,
    filename: str,
    content_type: Optional[str],
    business_name: str,
    gdrive_url: str,
    new_filename: str,
) -> Tuple[dict, bool]:
    """POST to n8n; returns (parsed_json_or_dict, http_ok)."""
    ct = content_type or "application/octet-stream"
    files = {"file": (filename, file_bytes, ct)}
    data = {
        "business_name": business_name,
        "gdrive_url": gdrive_url,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "new_filename": new_filename,
    }
    resp = requests.post(
        N8N_ADDITIONAL_DOC_WEBHOOK,
        data=data,
        files=files,
        timeout=120,
    )
    text = resp.text or ""
    parsed: Any = {}
    try:
        if text.strip():
            parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = {"message": text}
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        parsed = parsed[0]
    if not isinstance(parsed, dict):
        parsed = {"message": text}
    return parsed, resp.ok


def assert_solar_cleaning_offer_with_client(offer: Offer) -> None:
    if (offer.utility_type or "").strip() != SOLAR_PANEL_CLEANING_UTILITY:
        raise HTTPException(
            status_code=400,
            detail="Signed offer upload is only available for Solar panel cleaning offers",
        )
    if not offer.client_id:
        raise HTTPException(
            status_code=400,
            detail="Offer must be linked to a member (client_id) to upload a signed offer",
        )
