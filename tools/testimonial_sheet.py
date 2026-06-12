"""
Read member testimonials from Google Sheets (legacy register + new rows).

Columns (Sheet1):
  A Business Name
  B Testimonial Type
  C Savings (if applicable)
  D File Name
  E File Link
  F Status (Draft | Sent for approval | Approved; empty = legacy row, counts as approved for invoice guard)
  G Linked Invoice (optional invoice number e.g. RA5711)
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from googleapiclient.errors import HttpError

from tools.one_month_savings import get_sheets_service

logger = logging.getLogger(__name__)

TESTIMONIAL_SHEET_ID = os.getenv(
    "TESTIMONIAL_SHEET_ID",
    "19xZzUo6ei1tX6RPu6J1BeRPURHgnr8iaGwLkXsMKApA",
)
TESTIMONIAL_SHEET_NAME = os.getenv("TESTIMONIAL_SHEET_NAME", "Sheet1")

VALID_STATUSES = frozenset({"Draft", "Sent for approval", "Approved"})

# Case-insensitive aliases → canonical CRM status
_STATUS_ALIASES = {
    "draft": "Draft",
    "sent for approval": "Sent for approval",
    "sent": "Sent for approval",
    "pending": "Sent for approval",
    "approved": "Approved",
    "approve": "Approved",
}


def extract_file_id_from_link(url: str) -> Optional[str]:
    """Extract Google Drive file or Google Doc ID from a sharing URL."""
    if not url or not str(url).strip():
        return None
    text = str(url).strip()
    for pattern in (
        r"/file/d/([a-zA-Z0-9_-]+)",
        r"/document/d/([a-zA-Z0-9_-]+)",
        r"[?&]id=([a-zA-Z0-9_-]+)",
        r"/d/([a-zA-Z0-9_-]+)",
    ):
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def _canonical_status(raw: Optional[str]) -> Optional[str]:
    """Map sheet column F to CRM status, or None if blank/unknown."""
    value = (raw or "").strip().rstrip(":")
    if not value:
        return None
    if value in VALID_STATUSES:
        return value
    lowered = value.lower()
    if lowered in _STATUS_ALIASES:
        return _STATUS_ALIASES[lowered]
    return None


def _parse_sheet_status(raw: Optional[str]) -> tuple[str, bool]:
    """
    Returns (display_status, counts_for_invoice_guard).
    Empty column F → display 'Not set', still counts for guard (legacy rows).
    """
    canonical = _canonical_status(raw)
    if canonical:
        return canonical, canonical == "Approved"
    raw_stripped = (raw or "").strip()
    if not raw_stripped:
        return "Not set", True
    return raw_stripped, False


def _sheet_row_to_testimonial(row: List[Any], sheet_row_number: int) -> Optional[Dict[str, Any]]:
    """Map sheet row to TestimonialResponse-shaped dict. id is negative (read-only in CRM UI)."""
    while len(row) < 7:
        row.append("")

    business_name = str(row[0]).strip() if row[0] is not None else ""
    if not business_name:
        return None

    testimonial_type = str(row[1]).strip() if row[1] is not None else ""
    testimonial_savings = str(row[2]).strip() if row[2] is not None else ""
    file_name = str(row[3]).strip() if row[3] is not None else ""
    file_link = str(row[4]).strip() if row[4] is not None else ""
    status_raw = str(row[5]).strip() if row[5] is not None else ""
    status, _counts_for_guard = _parse_sheet_status(status_raw)
    linked_invoice = str(row[6]).strip() if row[6] is not None else ""

    file_id = extract_file_id_from_link(file_link) if file_link else ""
    if not file_id:
        logger.warning(
            "[TESTIMONIAL_SHEET] Skipping row %s for %r — no file id in link %r",
            sheet_row_number,
            business_name,
            file_link[:80] if file_link else "",
        )
        return None

    if not file_name:
        file_name = f"Testimonial — {business_name}"

    placeholder_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)

    return {
        "id": -sheet_row_number,
        "business_name": business_name,
        "file_name": file_name,
        "file_id": file_id,
        "file_link": file_link or None,
        "invoice_number": linked_invoice or None,
        "status": status,
        "sheet_status_raw": status_raw or None,
        "testimonial_type": testimonial_type or None,
        "testimonial_solution_type_id": None,
        "testimonial_savings": testimonial_savings or None,
        "created_at": placeholder_ts,
        "updated_at": placeholder_ts,
        "source": "sheet",
    }


def get_all_testimonials_from_sheet() -> List[Dict[str, Any]]:
    """Read all testimonial rows from the configured sheet."""
    if not TESTIMONIAL_SHEET_ID:
        logger.warning("[TESTIMONIAL_SHEET] TESTIMONIAL_SHEET_ID not configured")
        return []

    service = get_sheets_service()
    if not service:
        logger.error("[TESTIMONIAL_SHEET] Could not create Google Sheets service")
        return []

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=TESTIMONIAL_SHEET_ID,
            range=f"{TESTIMONIAL_SHEET_NAME}!A2:G",
            valueRenderOption="FORMATTED_VALUE",
        ).execute()
    except HttpError as e:
        logger.error("[TESTIMONIAL_SHEET] Sheets API error: %s", e)
        return []

    values = result.get("values") or []
    items: List[Dict[str, Any]] = []
    for idx, row in enumerate(values):
        sheet_row_number = idx + 2
        mapped = _sheet_row_to_testimonial(list(row), sheet_row_number)
        if mapped:
            items.append(mapped)

    logger.info("[TESTIMONIAL_SHEET] Loaded %s rows from sheet", len(items))
    return items


def get_testimonials_from_sheet_for_business(business_name: str) -> List[Dict[str, Any]]:
    if not business_name or not business_name.strip():
        return []
    target = business_name.strip().lower()
    return [
        item
        for item in get_all_testimonials_from_sheet()
        if item.get("business_name", "").strip().lower() == target
    ]


def count_sheet_testimonials_for_business(business_name: str) -> int:
    """Sheet rows that count toward the approved testimonial guard (empty F or Approved)."""
    count = 0
    for item in get_testimonials_from_sheet_for_business(business_name):
        _, counts = _parse_sheet_status(item.get("sheet_status_raw"))
        if counts:
            count += 1
    return count


def merge_db_and_sheet_testimonials(
    db_items: List[Any],
    sheet_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    DB testimonials first; append sheet rows whose file_id is not already in DB.
    DB dicts get source=crm.
    """
    merged: List[Dict[str, Any]] = []
    seen_file_ids: Set[str] = set()

    for row in db_items:
        data = {
            "id": row.id,
            "business_name": row.business_name,
            "file_name": row.file_name,
            "file_id": row.file_id,
            "file_link": None,
            "invoice_number": row.invoice_number,
            "status": row.status,
            "testimonial_type": row.testimonial_type,
            "testimonial_solution_type_id": row.testimonial_solution_type_id,
            "testimonial_savings": row.testimonial_savings,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
            "source": "crm",
        }
        merged.append(data)
        if row.file_id:
            seen_file_ids.add(str(row.file_id).strip())

    for sheet_row in sheet_items:
        fid = str(sheet_row.get("file_id") or "").strip()
        if fid and fid in seen_file_ids:
            continue
        merged.append(sheet_row)
        if fid:
            seen_file_ids.add(fid)

    return merged
