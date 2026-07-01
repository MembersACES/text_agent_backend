"""
Latest LOA business details from Member ACES Data sheet (replaces return_business_details n8n webhook).

After LOA document processing, aces-invoice-api inserts the extracted row at index 2 on
``1st Sheet - LOA Business Details``. This module reads that latest row directly.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

import requests
from googleapiclient.errors import HttpError

try:
    from langchain_core.tools import tool
except ImportError:  # pragma: no cover - agent runtime only
    def tool(func):  # type: ignore[misc]
        return func

from tools.business_info import USE_N8N_BUSINESS_INFO_FALLBACK, get_sheets_service

logger = logging.getLogger(__name__)

MEMBER_ACES_DATA_SHEET_ID = os.getenv(
    "MEMBER_ACES_DATA_SHEET_ID",
    "1ozwxJjqBQE3fJeMHmsXzPFCsekupwXZ3A7F0jstOfVw",
)
LOA_BUSINESS_DETAILS_TAB = os.getenv(
    "LOA_BUSINESS_DETAILS_TAB",
    "1st Sheet - LOA Business Details",
)
N8N_RETURN_BUSINESS_DETAILS_URL = os.getenv(
    "N8N_RETURN_BUSINESS_DETAILS_URL",
    "https://membersaces.app.n8n.cloud/webhook/return_business_details",
)

# Row 2 is always the most recently lodged LOA (insert_row index=2 in aces-invoice-api).
LATEST_LOA_SHEET_ROW_NUMBER = 2


def _row_dict_from_sheet(headers: list[str], row: list[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for i, header in enumerate(headers):
        if not header:
            continue
        out[header] = row[i] if i < len(row) else ""
    return out


def get_latest_loa_business_details_from_sheets() -> Optional[dict[str, Any]]:
    """Read the latest LOA row (sheet row 2) from Member ACES Data."""
    if not MEMBER_ACES_DATA_SHEET_ID:
        return None
    service = get_sheets_service()
    if not service:
        return None
    tab = LOA_BUSINESS_DETAILS_TAB
    try:
        header_resp = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
                range=f"'{tab}'!A1:AZ1",
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
        headers = [str(h).strip() for h in (header_resp.get("values") or [[]])[0]]
        if not headers:
            return None

        row_resp = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
                range=f"'{tab}'!A{LATEST_LOA_SHEET_ROW_NUMBER}:AZ{LATEST_LOA_SHEET_ROW_NUMBER}",
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
        rows = row_resp.get("values", [])
        if not rows or not rows[0]:
            return None
        row_dict = _row_dict_from_sheet(headers, rows[0])
        if not str(row_dict.get("Business Name", "")).strip():
            return None
        row_dict["row_number"] = LATEST_LOA_SHEET_ROW_NUMBER
        logger.info(
            "LOA business details loaded from Google Sheets: business_name=%r",
            row_dict.get("Business Name"),
        )
        return row_dict
    except HttpError as e:
        logger.warning("Google Sheets error reading LOA business details: %s", e)
        return None
    except Exception as e:
        logger.warning("Failed to read LOA business details from Sheets: %s", e)
        return None


def get_latest_loa_business_details_from_n8n() -> list[dict[str, Any]]:
    try:
        response = requests.post(N8N_RETURN_BUSINESS_DETAILS_URL, timeout=60)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [data]
        return []
    except Exception as e:
        logger.warning("n8n return_business_details failed: %s", e)
        return []


def get_return_business_details() -> list[dict[str, Any]]:
    """
    Return latest LOA business details in n8n-compatible shape (array of one row dict).
    """
    row = get_latest_loa_business_details_from_sheets()
    if row:
        return [row]
    if USE_N8N_BUSINESS_INFO_FALLBACK:
        logger.info("Falling back to n8n return_business_details")
        return get_latest_loa_business_details_from_n8n()
    return []


@tool
def get_loa_business_details():
    """Get business details from the latest LOA document processing row."""
    try:
        rows = get_return_business_details()
        if not rows:
            return (
                "No business details found in the response. "
                "Please check if the LOA document was processed correctly."
            )

        details = rows[0]
        formatted_response = "**Business Details Retrieved:**\n\n"
        field_labels = [
            ("Business Name", "Business Name"),
            ("Trading As", "Trading As"),
            ("Business ABN", "ABN"),
            ("Postal Address", "Postal Address"),
            ("Site Address", "Site Address"),
            ("Contact Name", "Contact Name"),
            ("Contact Position", "Position"),
            ("Contact  Email  :", "Email"),
            ("Contact Number:", "Phone"),
            ("Date", "Date"),
        ]
        for key, label in field_labels:
            val = details.get(key)
            if val and str(val).strip():
                formatted_response += f"**{label}:** {val}\n"

        formatted_response += (
            "\nPlease confirm if these details are correct for the member folder creation."
        )
        return formatted_response
    except Exception as e:
        logger.error("Error in get_loa_business_details: %s", e)
        return f"Error retrieving business details: {str(e)}"
