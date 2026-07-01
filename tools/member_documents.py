"""
Member profile document lookups: EOI IDs and WIP (additional docs + engagement forms).
Direct Google Sheets reads with n8n fallback (replaces return_EOIIDs and pull_wip_both).
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any, Optional

import requests
from googleapiclient.errors import HttpError

from tools.business_info import (
    FILE_IDS_SHEET_ID,
    USE_N8N_BUSINESS_INFO_FALLBACK,
    _normalize_business_name_for_match,
    get_file_ids_from_sheets,
    get_sheets_service,
)

logger = logging.getLogger(__name__)

EOI_SHEET_TAB = os.getenv("EOI_SHEET_TAB", "Signed EOIs")
SIGNED_EF_SHEET_TAB = os.getenv("SIGNED_EF_SHEET_TAB", "Signed EFs")
WIP_ADDITIONAL_DOCS_TAB = os.getenv("WIP_ADDITIONAL_DOCS_TAB", "Additional Documents")
WIP_MEMBER_SIGNED_EF_TAB = os.getenv("WIP_MEMBER_SIGNED_EF_TAB", "Signed EFs")

N8N_RETURN_EOI_IDS_URL = os.getenv(
    "N8N_RETURN_EOI_IDS_URL",
    "https://membersaces.app.n8n.cloud/webhook/return_EOIIDs",
)
N8N_PULL_WIP_BOTH_URL = os.getenv(
    "N8N_PULL_WIP_BOTH_URL",
    "https://membersaces.app.n8n.cloud/webhook/pull_wip_both",
)


def _extract_google_id(value: object) -> str:
    """Extract a Drive/Sheets resource id from a URL or raw cell value."""
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    for pattern in (
        r"/spreadsheets/d/([a-zA-Z0-9_-]+)",
        r"/file/d/([a-zA-Z0-9_-]+)",
        r"/folders/([a-zA-Z0-9_-]+)",
        r"^([a-zA-Z0-9_-]{20,})$",
    ):
        m = re.search(pattern, s)
        if m:
            return m.group(1)
    return s.split("/", 1)[0].strip()


def _read_sheet_table(
    spreadsheet_id: str,
    tab_name: str,
    *,
    max_rows: int = 5000,
) -> tuple[list[str], list[list[Any]]]:
    service = get_sheets_service()
    if not service or not spreadsheet_id:
        return [], []
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab_name}'!A1:Z{max_rows}",
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
        rows = result.get("values", [])
        if not rows:
            return [], []
        headers = [str(h).strip() for h in rows[0]]
        return headers, rows[1:]
    except HttpError as e:
        logger.warning(
            "Sheets read failed spreadsheet=%s tab=%r: %s",
            spreadsheet_id[:12],
            tab_name,
            e,
        )
        return [], []
    except Exception as e:
        logger.warning("Sheets read failed tab=%r: %s", tab_name, e)
        return [], []


def _row_to_dict(headers: list[str], row: list[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for i, header in enumerate(headers):
        if not header:
            continue
        out[header] = row[i] if i < len(row) else ""
    return out


def _rows_for_business(
    headers: list[str],
    data_rows: list[list[Any]],
    business_name: str,
    business_column: str = "Business",
) -> list[dict[str, Any]]:
    target = _normalize_business_name_for_match(business_name)
    matches: list[dict[str, Any]] = []
    for row_idx, row in enumerate(data_rows, start=2):
        row_dict = _row_to_dict(headers, row)
        bn = row_dict.get(business_column) or row_dict.get("Business Name") or ""
        if _normalize_business_name_for_match(str(bn)) != target:
            continue
        row_dict["row_number"] = row_idx
        matches.append(row_dict)
    return matches


def get_eoi_ids_from_sheets(business_name: str) -> list[dict[str, Any]]:
    """Read Signed EOIs tab; return n8n-compatible array of rows."""
    if not business_name or not FILE_IDS_SHEET_ID:
        return []
    headers, data_rows = _read_sheet_table(FILE_IDS_SHEET_ID, EOI_SHEET_TAB)
    if not headers:
        return []
    rows = _rows_for_business(headers, data_rows, business_name)
    if rows:
        logger.info(
            "EOI IDs loaded from Google Sheets for business_name=%r (%s rows)",
            business_name,
            len(rows),
        )
    return rows


def get_eoi_ids_from_n8n(business_name: str) -> list[dict[str, Any]]:
    if not business_name:
        return []
    try:
        response = requests.post(
            N8N_RETURN_EOI_IDS_URL,
            json={"business_name": business_name.strip()},
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        return []
    except Exception as e:
        logger.warning("n8n return_EOIIDs failed: %s", e)
        return []


def get_eoi_ids(business_name: str) -> list[dict[str, Any]]:
    rows = get_eoi_ids_from_sheets(business_name)
    if rows:
        return rows
    if USE_N8N_BUSINESS_INFO_FALLBACK:
        logger.info("Falling back to n8n return_EOIIDs for business_name=%r", business_name)
        return get_eoi_ids_from_n8n(business_name)
    return []


def _read_additional_documents(wip_spreadsheet_id: str) -> list[dict[str, Any]]:
    headers, data_rows = _read_sheet_table(wip_spreadsheet_id, WIP_ADDITIONAL_DOCS_TAB)
    if not headers:
        return []
    docs: list[dict[str, Any]] = []
    for row_idx, row in enumerate(data_rows, start=2):
        row_dict = _row_to_dict(headers, row)
        file_name = (
            row_dict.get("File Name")
            or row_dict.get("file_name")
            or row_dict.get("fileName")
            or ""
        )
        file_id = (
            row_dict.get("File ID")
            or row_dict.get("file_id")
            or row_dict.get("fileId")
            or row_dict.get("id")
            or ""
        )
        if not str(file_id).strip():
            continue
        docs.append(
            {
                "row_number": row_idx,
                "File Name": str(file_name).strip() or "Unknown",
                "File ID": str(file_id).strip(),
            }
        )
    return docs


def _signed_ef_row_from_member_wip(
    wip_spreadsheet_id: str,
    business_name: str,
) -> Optional[dict[str, Any]]:
    headers, data_rows = _read_sheet_table(wip_spreadsheet_id, WIP_MEMBER_SIGNED_EF_TAB)
    if not headers:
        return None
    rows = _rows_for_business(headers, data_rows, business_name)
    if not rows:
        return None
    row = rows[0]
    return {
        "row_number": row.get("row_number"),
        "EF Type": row.get("EF Type") or row.get("ef_type") or "",
        "Business": row.get("Business") or business_name,
        "Signed Date": row.get("Signed Date") or row.get("Sign Date") or "",
        "File Name": (row.get("File Name") or row.get("File Name ") or "").strip(),
    }


def _engagement_forms_from_central_sheet(business_name: str) -> list[dict[str, Any]]:
    headers, data_rows = _read_sheet_table(FILE_IDS_SHEET_ID, SIGNED_EF_SHEET_TAB)
    if not headers:
        return []
    forms: list[dict[str, Any]] = []
    for row in _rows_for_business(headers, data_rows, business_name):
        file_id = (
            row.get("EF File ID")
            or row.get("File ID")
            or row.get("file_id")
            or ""
        )
        file_id = str(file_id).strip()
        if not file_id:
            continue
        name = (row.get("File Name") or row.get("File Name ") or row.get("EF Type") or "Unknown")
        forms.append(
            {
                "fileId": file_id,
                "name": str(name).strip(),
                "webViewLink": None,
                "mimeType": None,
                "modifiedTime": None,
            }
        )
    return forms


def get_member_wip_from_sheets(business_name: str) -> Optional[dict[str, Any]]:
    if not business_name or not FILE_IDS_SHEET_ID:
        return None

    file_ids_row = get_file_ids_from_sheets(business_name)
    wip_spreadsheet_id = _extract_google_id(file_ids_row.get("WIP"))
    additional_documents: list[dict[str, Any]] = []
    signed_ef_row: Optional[dict[str, Any]] = None

    if wip_spreadsheet_id:
        additional_documents = _read_additional_documents(wip_spreadsheet_id)
        signed_ef_row = _signed_ef_row_from_member_wip(wip_spreadsheet_id, business_name)

    engagement_forms = _engagement_forms_from_central_sheet(business_name)
    if not signed_ef_row and engagement_forms:
        first = engagement_forms[0]
        signed_ef_row = {
            "row_number": 2,
            "EF Type": "",
            "Business": business_name,
            "Signed Date": "",
            "File Name": first.get("name") or "",
        }

    has_files = bool(additional_documents or engagement_forms)
    if not has_files and not signed_ef_row:
        return None

    logger.info(
        "WIP data loaded from Google Sheets for business_name=%r (docs=%s, forms=%s)",
        business_name,
        len(additional_documents),
        len(engagement_forms),
    )
    return {
        "ok": True,
        "business_name": business_name,
        "additional_documents": additional_documents,
        "signedEF_row": signed_ef_row or {},
        "engagement_forms": engagement_forms,
        "file_count": len(engagement_forms),
        "has_files": has_files,
    }


def get_member_wip_from_n8n(business_name: str) -> Optional[dict[str, Any]]:
    if not business_name:
        return None
    try:
        response = requests.post(
            N8N_PULL_WIP_BOTH_URL,
            json={"business_name": business_name.strip()},
            timeout=90,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        if not (response.text or "").strip():
            return None
        data = response.json()
        if isinstance(data, dict) and isinstance(data.get("body"), dict):
            data = data["body"]
        if isinstance(data, list) and data:
            data = data[0]
        if isinstance(data, dict):
            return data
        return None
    except Exception as e:
        logger.warning("n8n pull_wip_both failed: %s", e)
        return None


def get_member_wip(business_name: str) -> dict[str, Any]:
    """Return unified WIP payload (n8n pull_wip_both shape)."""
    data = get_member_wip_from_sheets(business_name)
    if data is not None:
        return data
    if USE_N8N_BUSINESS_INFO_FALLBACK:
        logger.info("Falling back to n8n pull_wip_both for business_name=%r", business_name)
        n8n_data = get_member_wip_from_n8n(business_name)
        if n8n_data is not None:
            return n8n_data
    return {
        "ok": True,
        "business_name": business_name,
        "additional_documents": [],
        "signedEF_row": {},
        "engagement_forms": [],
        "file_count": 0,
        "has_files": False,
    }
