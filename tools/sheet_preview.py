"""
Top-of-sheet preview rows from Member ACES Data (live UI feedback after document processing).

Newly processed LOA / utility rows are always inserted at sheet row 2 (aces-invoice-api).
This module returns the header row plus the first N data rows for in-app preview tables.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from googleapiclient.errors import HttpError

from tools.business_info import get_sheets_service
from tools.loa_business_details import (
    MEMBER_ACES_DATA_SHEET_ID,
    _row_dict_from_sheet,
)
from tools.return_utility_info import LATEST_UTILITY_SHEET_ROW_NUMBER, UTILITY_TYPE_TO_TAB

logger = logging.getLogger(__name__)

LATEST_ROW_NUMBER = LATEST_UTILITY_SHEET_ROW_NUMBER  # row 2

# Columns shown in the LOA preview table (order preserved).
LOA_PREVIEW_COLUMNS: list[str] = [
    "Business Name",
    "Business ABN",
    "Trading As",
    "Postal Address",
    "Site Address",
    "Contact Name",
    "Contact Position",
    "Contact  Email  :",
    "Contact Number:",
    "Date",
]

PREVIEW_COLUMNS_BY_UTILITY: dict[str, list[str]] = {
    "LOA": LOA_PREVIEW_COLUMNS,
    "ELECTRICITY_CI": [
        "NMI",
        "Client Name",
        "Retailer",
        "Site Address",
        "Supply Address",
    ],
    "ELECTRICITY_SME": [
        "NMI",
        "Client Name",
        "Retailer",
        "Site Address",
        "Supply Address",
    ],
    "GAS_CI": [
        "MRIN",
        "Client Name",
        "Retailer",
        "Site Address:",
        "Site Address",
    ],
    "GAS_SME": [
        "MRIN",
        "Client Name",
        "Retailer",
        "Site Address:",
        "Site Address",
    ],
    "WASTE": [
        "Account Number or Customer Number",
        "Client Name",
        "Provider",
        "Supply Address",
    ],
    "COOKING_OIL": [
        "Account Number / Customer Code",
        "Client Name",
        "Retailer",
        "Site Address",
    ],
    "GREASE_TRAP": [
        "Account Number / Customer Code",
        "Client Name",
        "Retailer",
        "Site Address",
    ],
    "WATER": [
        "Account Number",
        "Account Name",
        "Client Name",
        "Provider",
        "Supply Address",
    ],
    "CLEANING": [
        "invoice_number",
        "Invoice Number",
        "client_name",
        "Client Name",
        "supplier_name",
        "Supplier Name",
        "client_address",
        "Client Address",
    ],
}


def _normalize_utility_type(utility_type: str) -> str:
    return (utility_type or "").strip().upper()


def _pick_display_columns(headers: list[str], utility_type: str) -> list[str]:
    preferred = PREVIEW_COLUMNS_BY_UTILITY.get(utility_type, [])
    cols = [c for c in preferred if c in headers]
    if cols:
        return cols
    return [h for h in headers if h][:6]


def _row_has_values(row: dict[str, Any], columns: list[str]) -> bool:
    for col in columns:
        val = row.get(col)
        if val is not None and str(val).strip():
            return True
    return False


def get_sheet_preview(utility_type: str, row_count: int = 5) -> dict[str, Any]:
    """
    Return the latest ``row_count`` data rows from the utility tab (starting at row 2).

    Response shape is stable for the frontend preview component.
    """
    utility_type = _normalize_utility_type(utility_type)
    row_count = max(1, min(int(row_count or 5), 10))
    tab = UTILITY_TYPE_TO_TAB.get(utility_type)
    fetched_at = datetime.now(timezone.utc).isoformat()

    base: dict[str, Any] = {
        "utility_type": utility_type,
        "tab": tab,
        "spreadsheet_id": MEMBER_ACES_DATA_SHEET_ID,
        "spreadsheet_url": (
            f"https://docs.google.com/spreadsheets/d/{MEMBER_ACES_DATA_SHEET_ID}/edit"
            if MEMBER_ACES_DATA_SHEET_ID
            else None
        ),
        "latest_row_number": LATEST_ROW_NUMBER,
        "columns": [],
        "rows": [],
        "fetched_at": fetched_at,
    }

    if not utility_type or not tab or not MEMBER_ACES_DATA_SHEET_ID:
        return base

    service = get_sheets_service()
    if not service:
        return base

    end_row = LATEST_ROW_NUMBER + row_count - 1
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
            return base

        columns = _pick_display_columns(headers, utility_type)
        base["columns"] = columns

        row_resp = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
                range=f"'{tab}'!A{LATEST_ROW_NUMBER}:AZ{end_row}",
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
        raw_rows = row_resp.get("values") or []
        preview_rows: list[dict[str, Any]] = []
        for offset, raw in enumerate(raw_rows):
            if not raw:
                continue
            row_number = LATEST_ROW_NUMBER + offset
            full = _row_dict_from_sheet(headers, raw)
            cells = {col: full.get(col, "") for col in columns}
            if not _row_has_values(cells, columns):
                continue
            preview_rows.append(
                {
                    "row_number": row_number,
                    "cells": cells,
                    "is_latest": row_number == LATEST_ROW_NUMBER,
                }
            )

        base["rows"] = preview_rows
        logger.info(
            "Sheet preview loaded utility_type=%s tab=%r rows=%s",
            utility_type,
            tab,
            len(preview_rows),
        )
        return base
    except HttpError as e:
        logger.warning("Google Sheets error reading sheet preview (%s): %s", utility_type, e)
        return base
    except Exception as e:
        logger.warning("Failed to read sheet preview (%s): %s", utility_type, e)
        return base


def row_fingerprint(row: Optional[dict[str, Any]], columns: list[str]) -> str:
    """Stable string for comparing whether the latest row changed."""
    if not row:
        return ""
    cells = row.get("cells") if isinstance(row.get("cells"), dict) else row
    parts = [str(cells.get(col, "")).strip() for col in columns]
    return "|".join(parts)
