"""
Shared EGB invoice numbering (RA####).

1st Month Savings and Discrepancy / New Revenue invoices both bill from the
Environmental Global Benefits account, so they share one sequence.

Next number is max(RA in Bank Rec EGB tab, OMS tracking sheet, new-revenue
tracking sheet) + 1.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, List

from tools.one_month_savings import get_sheets_service

logger = logging.getLogger(__name__)

RA_RE = re.compile(r"RA(\d+)", re.IGNORECASE)
INVOICE_PREFIX = "RA"

BANK_REC_SHEET_ID = os.getenv(
    "BANK_REC_SHEET_ID",
    "1ONg6g9kn-TmNaJ50yoX-oXu-2pJdW_qYibblv2pbQuU",
)
BANK_REC_EGB_TAB = os.getenv(
    "BANK_REC_EGB_TAB",
    "Environmental Global Benefits - 1057 8739",
)

OMS_SHEET_ID = os.getenv("ONE_MONTH_SAVINGS_SHEET_ID", "")
OMS_SHEET_NAME = os.getenv("ONE_MONTH_SAVINGS_SHEET_NAME", "Sheet1").strip().strip('"')

NEW_REVENUE_SHEET_ID = os.getenv("NEW_REVENUE_SHEET_ID") or OMS_SHEET_ID
NEW_REVENUE_SHEET_NAME = os.getenv(
    "NEW_REVENUE_SHEET_NAME", "New Revenue Invoices"
).strip().strip('"')


def _a1(tab: str, cells: str) -> str:
    escaped = (tab or "").replace("'", "''")
    return f"'{escaped}'!{cells}"


def _max_ra_in_values(values: List[List[Any]]) -> int:
    max_n = 0
    for row in values:
        for cell in row:
            if cell is None:
                continue
            for match in RA_RE.finditer(str(cell)):
                max_n = max(max_n, int(match.group(1)))
    return max_n


def _max_ra_in_range(service: Any, sheet_id: str, a1_range: str, label: str) -> int:
    if not service or not sheet_id:
        return 0
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=a1_range)
            .execute()
        )
        max_n = _max_ra_in_values(result.get("values", []))
        logger.info("EGB invoice scan %s range=%s max_ra=%s", label, a1_range, max_n)
        return max_n
    except Exception:
        logger.exception("EGB invoice scan failed for %s (%s)", label, a1_range)
        return 0


def get_next_ra_invoice_number() -> str:
    """Return the next shared EGB invoice number, e.g. RA5802."""
    service = get_sheets_service()
    max_n = 0
    if service:
        max_n = max(
            max_n,
            _max_ra_in_range(
                service,
                BANK_REC_SHEET_ID,
                _a1(BANK_REC_EGB_TAB, "A:Z"),
                "bank_rec_egb",
            ),
            _max_ra_in_range(
                service,
                OMS_SHEET_ID,
                _a1(OMS_SHEET_NAME, "F2:F"),
                "one_month_savings",
            ),
            _max_ra_in_range(
                service,
                NEW_REVENUE_SHEET_ID,
                _a1(NEW_REVENUE_SHEET_NAME, "H2:H"),
                "new_revenue",
            ),
        )
    next_n = max_n + 1
    if next_n < 1000:
        next_n = 1000
    invoice_number = f"{INVOICE_PREFIX}{next_n:04d}"
    logger.info("Next EGB invoice number: %s (previous max RA: %s)", invoice_number, max_n)
    return invoice_number
