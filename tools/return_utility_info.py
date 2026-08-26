"""
Latest utility invoice row from Member ACES Data sheet (replaces return_utility_info n8n webhook).

After utility invoice processing, aces-invoice-api inserts the extracted row at index 2 on the
utility-type tab (same pattern as LOA business details on ``1st Sheet - LOA Business Details``).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

import requests
from googleapiclient.errors import HttpError

from tools.business_info import USE_N8N_BUSINESS_INFO_FALLBACK, get_sheets_service
from tools.loa_business_details import (
    LOA_BUSINESS_DETAILS_TAB,
    MEMBER_ACES_DATA_SHEET_ID,
    _row_dict_from_sheet,
    get_return_business_details,
)

logger = logging.getLogger(__name__)

N8N_RETURN_UTILITY_INFO_URL = os.getenv(
    "N8N_RETURN_UTILITY_INFO_URL",
    "https://membersaces.app.n8n.cloud/webhook/return_utility_info",
)

# Frontend utility_type keys -> Member ACES Data tab name.
UTILITY_TYPE_TO_TAB: dict[str, str] = {
    "ELECTRICITY_CI": "2nd Sheet - Electricity details from the invoice",
    "ELECTRICITY_SME": "3rd Sheet - SME Electricity",
    "GAS_CI": "4th Sheet - Large Gas",
    "GAS_SME": "5th Sheet - Small Gas",
    "WATER": "6th Sheet - Water",
    "WASTE": "7th Sheet - Waste",
    "COOKING_OIL": "8th Sheet - Oil",
    "GREASE_TRAP": "9th Sheet - Grease Trap",
    "CLEANING": "14th Sheet - Cleaning Invoices",
    "LOA": LOA_BUSINESS_DETAILS_TAB,
}

# Row 2 is always the most recently processed utility invoice (insert_row index=2 in aces-invoice-api).
LATEST_UTILITY_SHEET_ROW_NUMBER = 2


def _row_has_data(row_dict: dict[str, Any]) -> bool:
    """True if the row has at least one non-empty value besides row_number."""
    for key, val in row_dict.items():
        if key == "row_number":
            continue
        if val is not None and str(val).strip():
            return True
    return False


def get_latest_utility_info_from_sheets(utility_type: str) -> Optional[dict[str, Any]]:
    """Read the latest utility row (sheet row 2) for the given utility type."""
    utility_type = (utility_type or "").strip().upper()
    tab = UTILITY_TYPE_TO_TAB.get(utility_type)
    if not tab or not MEMBER_ACES_DATA_SHEET_ID:
        return None

    service = get_sheets_service()
    if not service:
        return None

    try:
        header_resp = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
                range=f"'{tab}'!A1:CZ1",
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
                range=f"'{tab}'!A{LATEST_UTILITY_SHEET_ROW_NUMBER}:CZ{LATEST_UTILITY_SHEET_ROW_NUMBER}",
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
        rows = row_resp.get("values", [])
        if not rows or not rows[0]:
            return None

        row_dict = _row_dict_from_sheet(headers, rows[0])
        if not _row_has_data(row_dict):
            return None

        row_dict["row_number"] = LATEST_UTILITY_SHEET_ROW_NUMBER
        logger.info(
            "Utility info loaded from Google Sheets: utility_type=%s tab=%r",
            utility_type,
            tab,
        )
        return row_dict
    except HttpError as e:
        logger.warning("Google Sheets error reading utility info (%s): %s", utility_type, e)
        return None
    except Exception as e:
        logger.warning("Failed to read utility info from Sheets (%s): %s", utility_type, e)
        return None


def get_return_utility_info_from_n8n(
    utility_type: str,
    business_name: str = "",
) -> list[dict[str, Any]]:
    try:
        response = requests.post(
            N8N_RETURN_UTILITY_INFO_URL,
            json={
                "utility_type": utility_type,
                "business_name": business_name or "",
            },
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [data]
        return []
    except Exception as e:
        logger.warning("n8n return_utility_info failed: %s", e)
        return []


def get_return_utility_info(
    utility_type: str,
    business_name: str = "",
) -> list[dict[str, Any]]:
    """
    Return latest utility invoice row in n8n-compatible shape (array of one row dict).

    ``business_name`` is accepted for API compatibility; n8n does not filter by it and neither
    does the direct Sheets path (always row 2 on the utility tab).
    """
    utility_type = (utility_type or "").strip().upper()
    if not utility_type:
        return []

    if utility_type == "LOA":
        return get_return_business_details()

    row = get_latest_utility_info_from_sheets(utility_type)
    if row:
        return [row]

    if USE_N8N_BUSINESS_INFO_FALLBACK:
        logger.info(
            "Falling back to n8n return_utility_info utility_type=%s business_name=%r",
            utility_type,
            business_name,
        )
        return get_return_utility_info_from_n8n(utility_type, business_name)
    return []
