"""
C&I Gas Discrepancy Check – reads from Google Sheet (FILE_IDS spreadsheet).
Tab: "C&I Gas Descrepancy Check" (note spelling in tab name).
Uses the same Sheets service and sheet ID as business_info (FILE_IDS_SHEET_ID).
"""

import logging
from typing import Any

from tools.business_info import FILE_IDS_SHEET_ID, get_sheets_service

logger = logging.getLogger(__name__)

DISCREPANCY_TAB_NAME = "C&I Gas Descrepancy Check"

# Sheet column header (normalized lower) -> our key
HEADER_TO_KEY = {
    "descrpancy type": "discrepancy_type",
    "discrepancy type": "discrepancy_type",
    "utility identifier": "utility_identifier",
    "linked business name": "linked_business_name",
    "invoice period": "invoice_period",
    "invoice rate": "invoice_rate",
    "contract period": "contract_period",
    "contract rate": "contract_rate",
    "rate difference": "rate_difference",
    "% difference": "pct_difference",
    "pct difference": "pct_difference",
    "annual quantity gj": "annual_quantity_gj",
    "annual potential overcharge": "annual_potential_overcharge",
    "take or pay invoice": "take_or_pay_invoice",
}

NORMALIZED_KEYS = [
    "discrepancy_type",
    "utility_identifier",
    "linked_business_name",
    "invoice_period",
    "invoice_rate",
    "contract_period",
    "contract_rate",
    "rate_difference",
    "pct_difference",
    "annual_quantity_gj",
    "annual_potential_overcharge",
    "take_or_pay_invoice",
]


def _normalize_header(h: Any) -> str:
    if h is None:
        return ""
    return str(h).strip().lower()


def get_discrepancy_rows(business_name: str | None = None) -> list[dict[str, str]]:
    """
    Read the C&I Gas Discrepancy Check tab and return a list of row objects
    with normalized keys. If business_name is provided, filter to rows where
    Linked Business Name matches (trim/case-normalized).
    """
    if not FILE_IDS_SHEET_ID:
        logger.warning("[discrepancy_check_sheet] FILE_IDS_SHEET_ID not set")
        return []

    service = get_sheets_service()
    if not service:
        logger.warning("[discrepancy_check_sheet] could not get Sheets service")
        return []

    range_str = f"'{DISCREPANCY_TAB_NAME}'!A1:Z1000"
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=FILE_IDS_SHEET_ID,
            range=range_str,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except Exception as e:
        logger.warning("[discrepancy_check_sheet] read sheet %r failed: %s", DISCREPANCY_TAB_NAME, e)
        return []

    values = resp.get("values", [])
    if not values:
        return []

    raw_headers = values[0]
    rows: list[dict[str, str]] = []
    for row in values[1:]:
        obj: dict[str, str] = {k: "" for k in NORMALIZED_KEYS}
        for i, raw in enumerate(row):
            if i >= len(raw_headers):
                break
            h = _normalize_header(raw_headers[i])
            key = HEADER_TO_KEY.get(h)
            if key:
                obj[key] = "" if raw is None else str(raw).strip()

        if business_name:
            linked = (obj.get("linked_business_name") or "").strip()
            if linked.lower() != business_name.strip().lower():
                continue
        rows.append(obj)

    return rows
