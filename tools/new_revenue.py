"""
Discrepancy / New Revenue invoice logging.

Fee model (Service Fee Agreement): 20% of the gross outcome (rebate / recovered
discrepancy / new revenue), plus 10% GST on the fee.

Uses a dedicated sheet tab (default: "New Revenue Invoices") on NEW_REVENUE_SHEET_ID,
falling back to the existing 1st Month Savings spreadsheet so local testing works
without a new Sheet ID.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional

from googleapiclient.errors import HttpError

from tools.one_month_savings import (
    INVOICE_STORAGE_FOLDER_ID,
    get_drive_service,
    get_sheets_service,
    upload_pdf_to_drive,
)

logger = logging.getLogger(__name__)

SHEET_ID = os.getenv("NEW_REVENUE_SHEET_ID") or os.getenv("ONE_MONTH_SAVINGS_SHEET_ID", "")
SHEET_NAME = os.getenv("NEW_REVENUE_SHEET_NAME", "New Revenue Invoices").strip().strip('"')
INVOICE_PREFIX = "RA"

HEADERS = [
    "Member",
    "Solution",
    "Gross Outcome",
    "Fee %",
    "Fee Amount",
    "GST",
    "Total Invoice",
    "Invoice Number",
    "Due Date",
    "Invoice ID",
    "Status",
]


def _parse_money(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).replace("$", "").replace(",", "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _money(value: float) -> str:
    return f"${value:.2f}"


def _invoice_number_int(invoice_number: str) -> Optional[int]:
    match = re.match(rf"^{INVOICE_PREFIX}(\d+)$", (invoice_number or "").strip(), re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def _ensure_sheet() -> Optional[Any]:
    if not SHEET_ID:
        logger.warning("NEW_REVENUE_SHEET_ID / ONE_MONTH_SAVINGS_SHEET_ID not configured")
        return None

    service = get_sheets_service()
    if not service:
        return None

    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        titles = [sheet["properties"]["title"] for sheet in spreadsheet.get("sheets", [])]
        if SHEET_NAME not in titles:
            logger.info("Creating sheet tab %r on spreadsheet %s", SHEET_NAME, SHEET_ID)
            service.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]},
            ).execute()

        header_range = f"{SHEET_NAME}!A1:K1"
        existing = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SHEET_ID, range=header_range)
            .execute()
            .get("values", [])
        )
        if not existing or not existing[0] or not str(existing[0][0]).strip():
            service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=header_range,
                valueInputOption="RAW",
                body={"values": [HEADERS]},
            ).execute()
        return service
    except HttpError as exc:
        logger.error("Failed to ensure new-revenue sheet tab: %s", exc)
        return None
    except Exception:
        logger.exception("Failed to ensure new-revenue sheet tab")
        return None


def log_invoice_to_sheets(invoice_data: Dict[str, Any]) -> Dict[str, Any]:
    if not invoice_data.get("invoice_number") or not invoice_data.get("business_name"):
        return {"success": False, "error": "Missing required fields: invoice_number and business_name"}

    service = _ensure_sheet()
    if not service:
        return {"success": False, "error": "Google Sheet is not configured for new-revenue invoices"}

    line_items = invoice_data.get("line_items") or []
    if not line_items:
        return {"success": False, "error": "No line items provided"}

    invoice_file_id = invoice_data.get("invoice_file_id", "") or invoice_data.get("file_id", "")
    status = invoice_data.get("status", "Generated")
    rows: List[List[Any]] = []
    for item in line_items:
        gross = _parse_money(item.get("gross_amount"))
        fee_percent = _parse_money(item.get("fee_percent", 20))
        fee_amount = _parse_money(item.get("fee_amount", item.get("savings_amount")))
        gst = _parse_money(item.get("gst", fee_amount * 0.1))
        total = _parse_money(item.get("total", fee_amount + gst))
        rows.append(
            [
                invoice_data.get("business_name", ""),
                (item.get("solution_label") or "").strip(),
                _money(gross),
                f"{fee_percent:g}%",
                _money(fee_amount),
                _money(gst),
                _money(total),
                invoice_data.get("invoice_number", ""),
                invoice_data.get("due_date", ""),
                invoice_file_id,
                status,
            ]
        )

    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A:K",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

    return {
        "success": True,
        "invoice_number": invoice_data.get("invoice_number"),
        "rows_added": len(rows),
    }


def get_invoice_history(business_name: str) -> Dict[str, Any]:
    if not business_name:
        return {"invoices": [], "count": 0, "error": "business_name is required"}

    service = _ensure_sheet()
    if not service:
        return {"invoices": [], "count": 0, "message": "Sheet not configured"}

    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A2:K")
        .execute()
    )
    values = result.get("values", [])
    grouped: Dict[str, Dict[str, Any]] = {}
    target = business_name.strip().lower()

    for row in values:
        padded = list(row) + [""] * (11 - len(row))
        member, solution, gross, fee_pct, fee_amount, gst, total, invoice_number, due_date, file_id, status = padded[:11]
        if (member or "").strip().lower() != target:
            continue
        invoice_number = str(invoice_number).strip()
        if not invoice_number:
            continue

        line_item = {
            "solution_label": str(solution).strip(),
            "gross_amount": _parse_money(gross),
            "fee_percent": _parse_money(str(fee_pct).replace("%", "")),
            "fee_amount": _parse_money(fee_amount),
            "savings_amount": _parse_money(fee_amount),
            "gst": _parse_money(gst),
            "total": _parse_money(total),
        }
        if invoice_number not in grouped:
            grouped[invoice_number] = {
                "invoice_number": invoice_number,
                "business_name": member,
                "due_date": due_date,
                "status": status or "Generated",
                "invoice_file_id": str(file_id).strip(),
                "line_items": [],
                "subtotal": 0.0,
                "total_gst": 0.0,
                "total_amount": 0.0,
            }
        grouped[invoice_number]["line_items"].append(line_item)
        grouped[invoice_number]["subtotal"] += line_item["fee_amount"]
        grouped[invoice_number]["total_gst"] += line_item["gst"]
        grouped[invoice_number]["total_amount"] += line_item["total"]
        if file_id and not grouped[invoice_number].get("invoice_file_id"):
            grouped[invoice_number]["invoice_file_id"] = str(file_id).strip()

    invoices = list(grouped.values())
    return {"invoices": invoices, "count": len(invoices)}


def _matching_rows(service: Any, business_name: str, invoice_number: str) -> List[int]:
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A2:H")
        .execute()
    )
    values = result.get("values", [])
    business_clean = business_name.strip().lower()
    invoice_clean = invoice_number.strip()
    rows: List[int] = []
    for idx, row in enumerate(values, start=2):
        member = str(row[0]).strip().lower() if row else ""
        number = str(row[7]).strip() if len(row) > 7 else ""
        if member == business_clean and number == invoice_clean:
            rows.append(idx)
    return rows


def update_invoice_status(business_name: str, invoice_number: str, new_status: str) -> Dict[str, Any]:
    service = _ensure_sheet()
    if not service:
        return {"success": False, "error": "Sheet not configured"}
    rows = _matching_rows(service, business_name, invoice_number)
    if not rows:
        return {"success": False, "error": "Invoice not found"}
    data = [{"range": f"{SHEET_NAME}!K{row}", "values": [[new_status]]} for row in rows]
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()
    return {"success": True, "updated_rows": len(rows)}


def update_invoice_file_id(business_name: str, invoice_number: str, file_id: str) -> Dict[str, Any]:
    service = _ensure_sheet()
    if not service:
        return {"success": False, "error": "Sheet not configured"}
    rows = _matching_rows(service, business_name, invoice_number)
    if not rows:
        return {"success": False, "error": "Invoice not found"}
    data = [{"range": f"{SHEET_NAME}!J{row}", "values": [[file_id]]} for row in rows]
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()
    return {"success": True, "updated_rows": len(rows)}


def get_next_sequential_invoice_number() -> str:
    from tools.egb_invoice_number import get_next_ra_invoice_number

    return get_next_ra_invoice_number()


def resolve_upload_folder_id() -> str:
    return (os.getenv("NEW_REVENUE_DRIVE_FOLDER_ID") or INVOICE_STORAGE_FOLDER_ID or "").strip()


def upload_invoice_pdf(pdf_bytes: bytes, filename: str) -> Optional[str]:
    folder_id = resolve_upload_folder_id()
    if not folder_id:
        return None
    return upload_pdf_to_drive(pdf_bytes, filename, folder_id, credential_label="new_revenue")
