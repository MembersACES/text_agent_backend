"""Append an open Alinta agreement request to the tracker sheet.

The active tabs stay a queue. days_waiting is a plain integer written here as 0.
A later job rewrites it. This module never writes a spreadsheet formula.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

SHEET_ID = "1tWlXBSsmYH1528FxYivRn5j86NThXjHlOn1YCwb66AM"
GAS_TAB = "Gas"
ELECTRICITY_TAB = "Electricity"
_SYDNEY = ZoneInfo("Australia/Sydney")

GAS_COLUMNS: tuple[str, ...] = (
    "status",
    "sent_at",
    "sent_by",
    "client_id",
    "company_name",
    "mirn",
    "request_kind",
    "email_subject",
    "gmail_thread_id",
    "gmail_message_id",
    "drive_folder_url",
    "days_waiting",
    "follow_up_sent_at",
    "acn_abn",
    "address",
    "tel",
    "contact_name",
    "email",
    "start_date",
    "end_date",
    "price_per_gj",
    "commission_per_gj",
    "cpq_gj",
    "min_cpq_gj",
    "min_cpq_pct",
    "mdq_gj",
    "received_at",
    "contract_pdf_link",
    "returned_company_name",
    "returned_acn_abn",
    "returned_mirn",
    "returned_start_date",
    "returned_end_date",
    "returned_price_per_gj",
    "returned_commission_per_gj",
    "returned_cpq_gj",
    "returned_min_cpq_gj",
    "returned_min_cpq_pct",
    "returned_mdq_gj",
    "check_result",
    "issues",
    "ready_at",
)

ELECTRICITY_COLUMNS: tuple[str, ...] = (
    "status",
    "sent_at",
    "sent_by",
    "client_id",
    "company_name",
    "nmis",
    "request_kind",
    "email_subject",
    "gmail_thread_id",
    "gmail_message_id",
    "drive_folder_url",
    "days_waiting",
    "follow_up_sent_at",
    "acn_abn",
    "address",
    "tel",
    "contact_name",
    "email",
    "start_date",
    "end_date",
    "peak_rate_c_kwh",
    "off_peak_rate_c_kwh",
    "shoulder_rate_c_kwh",
    "commission_c_kwh",
    "annual_kwh",
    "take_or_pay_pct",
    "received_at",
    "contract_pdf_link",
    "returned_company_name",
    "returned_acn_abn",
    "returned_nmis",
    "returned_start_date",
    "returned_end_date",
    "returned_peak_rate_c_kwh",
    "returned_off_peak_rate_c_kwh",
    "returned_shoulder_rate_c_kwh",
    "returned_commission_c_kwh",
    "returned_annual_kwh",
    "returned_take_or_pay_pct",
    "check_result",
    "issues",
    "ready_at",
)


def sydney_sent_at() -> str:
    return datetime.now(_SYDNEY).strftime("%Y-%m-%d %H:%M")


def gmail_ids_from_webhook(body: str) -> tuple[str, str]:
    """Read thread and message ids from an n8n Respond-to-Webhook body."""
    payload = _json_object(body)
    if payload is None:
        return "", ""
    thread = _first(payload, ("threadId", "thread_id"))
    message = _first(payload, ("messageId", "message_id"))
    if not message and thread:
        message = _first(payload, ("id",))
    return thread, message


def tracker_row(columns: tuple[str, ...], values: dict[str, Any]) -> list[Any]:
    row: list[Any] = []
    for column in columns:
        if column == "days_waiting":
            row.append(0)
            continue
        if column == "status":
            row.append(_text(values.get("status")) or "Waiting")
            continue
        row.append(_text(values.get(column)))
    return row


def append_tracker_row(tab: str, columns: tuple[str, ...], values: dict[str, Any]) -> Optional[str]:
    """Append one queue row. Returns an error string, or None when the row is written."""
    row = tracker_row(columns, values)
    try:
        from tools.business_info import get_sheets_service

        service = get_sheets_service()
        if not service:
            return "Google Sheets is not configured"
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{_quote_tab(tab)}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except Exception as e:
        logger.warning("alinta agreement tracker append failed tab=%s: %s", tab, e)
        return str(e)
    logger.info(
        "alinta agreement tracker row appended tab=%s subject=%s",
        tab,
        _text(values.get("email_subject")),
    )
    return None


def _json_object(body: str) -> Optional[dict[str, Any]]:
    text = (body or "").strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(data, list):
        data = data[0] if data else None
    if not isinstance(data, dict):
        return None
    nested = data.get("body")
    if isinstance(nested, dict) and not _first(data, ("threadId", "thread_id")):
        return nested
    return data


def _first(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        text = _text(payload.get(key))
        if text:
            return text
    return ""


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _quote_tab(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"
