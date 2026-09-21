"""
Direct-client invoice ledgers for the invoicing dashboard.

1 Month Savings keeps its existing tracking sheet. Automation / Equipment /
Solar use each workbook's "Invoices Sent" tab. Cleaning Scrubber has no
ledger sheet, so invoices are Drive PDFs with status in appProperties.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError

from tools.invoicing_drive import (
    _is_pdf,
    _list_children,
    get_category_config,
    infer_invoice_number,
)
from tools.one_month_savings import (
    get_drive_service,
    get_invoice_history,
    get_sheets_service,
    update_invoice_status as update_oms_invoice_status,
)

logger = logging.getLogger(__name__)

ALLOWED_STATUSES = ("Generated", "Sent", "Paid")
DRIVE_STATUS_KEY = "invoice_status"
_DRIVE_FILE_ID_RE = re.compile(r"/d/([a-zA-Z0-9_-]+)")
_MONEY_NOISE_RE = re.compile(r"[$,]|aud", re.IGNORECASE)

DIRECT_CLIENT_STREAMS: dict[str, dict[str, str]] = {
    "one-month-savings": {"kind": "oms"},
    "automation-services": {
        "kind": "sheet",
        "spreadsheet_id": "1qycTrM4TnJRhaVTXc-cowQCCAujiGMlRknuWT1qgb-Y",
        "tab_name": "Invoices Sent",
    },
    "equipment-rental": {
        "kind": "sheet",
        "spreadsheet_id": "13g2tQQ1f65K3icPR1JNk5X1sfxSP5kbsJwSn-fGHtDo",
        "tab_name": "Invoices Sent",
    },
    "solar-cleaning": {
        "kind": "sheet",
        "spreadsheet_id": "1WiLksDOwrQkEwVhF25F_RQ1G0zxF5VHiu9lAxQhQox4",
        "tab_name": "Invoices Sent",
    },
    "cleaning-scrubber": {
        "kind": "drive",
        "category": "cleaning_scrubber",
    },
}

_CLIENT_HEADERS = ("client", "client name", "business name", "member", "customer")
_INVOICE_HEADERS = ("invoice number",)
_AMOUNT_HEADERS = (
    "invoice total",
    "total invoice",
    "total inc gst ($)",
    "total amount",
    "invoice cost",
)
_DUE_HEADERS = ("due date",)
_DESC_HEADERS = (
    "invoice period",
    "one off type",
    "description",
    "utility / offer title",
)
_STATUS_HEADERS = ("status",)
_FILE_HEADERS = ("pdf url", "invoice id", "signed document url")


def list_direct_client_stream_ids() -> list[str]:
    return list(DIRECT_CLIENT_STREAMS.keys())


def normalize_direct_status(value: object, *, default: str = "Generated") -> str:
    raw = str(value or "").strip()
    if not raw:
        return default if default in ALLOWED_STATUSES else "Generated"
    key = raw.casefold()
    if key in {"paid"}:
        return "Paid"
    if key in {"sent", "unpaid", "outstanding"}:
        return "Sent"
    if key in {"generated", "issued"}:
        return "Generated"
    if raw in ALLOWED_STATUSES:
        return raw
    return default if default in ALLOWED_STATUSES else "Generated"


def extract_drive_file_id(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if re.fullmatch(r"[a-zA-Z0-9_-]{10,}", text):
        return text
    match = _DRIVE_FILE_ID_RE.search(text)
    return match.group(1) if match else ""


def a1_column(index: int) -> str:
    if index < 0:
        raise ValueError("column index must be >= 0")
    n = index + 1
    letters: list[str] = []
    while n:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))


def _normalize_header(value: object) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _header_index(header: list, names: tuple[str, ...]) -> Optional[int]:
    normalized = [_normalize_header(cell) for cell in header]
    by_name = {cell: idx for idx, cell in enumerate(normalized) if cell}
    for name in names:
        idx = by_name.get(_normalize_header(name))
        if idx is not None:
            return idx
    return None


def _cell(row: list, index: Optional[int]) -> object:
    if index is None or index < 0 or index >= len(row):
        return ""
    return row[index]


def _parse_money(value: object) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = _MONEY_NOISE_RE.sub("", str(value)).strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def parse_ledger_rows(
    header: list,
    rows: list,
    *,
    default_status: str = "Sent",
) -> list[dict[str, Any]]:
    invoice_idx = _header_index(header, _INVOICE_HEADERS)
    client_idx = _header_index(header, _CLIENT_HEADERS)
    amount_idx = _header_index(header, _AMOUNT_HEADERS)
    due_idx = _header_index(header, _DUE_HEADERS)
    desc_idx = _header_index(header, _DESC_HEADERS)
    status_idx = _header_index(header, _STATUS_HEADERS)
    file_idx = _header_index(header, _FILE_HEADERS)
    if invoice_idx is None or client_idx is None:
        return []

    invoices: list[dict[str, Any]] = []
    for row in rows:
        if not row:
            continue
        invoice_number = _text(_cell(row, invoice_idx))
        business_name = _text(_cell(row, client_idx))
        if not invoice_number or not business_name:
            continue
        description = _text(_cell(row, desc_idx))
        invoices.append(
            {
                "invoice_number": invoice_number,
                "business_name": business_name,
                "due_date": _text(_cell(row, due_idx)),
                "total_amount": _parse_money(_cell(row, amount_idx)),
                "status": normalize_direct_status(
                    _cell(row, status_idx), default=default_status
                ),
                "invoice_file_id": extract_drive_file_id(_cell(row, file_idx)),
                "line_items": [{"solution_label": description}] if description else [],
            }
        )
    return invoices


def _escape_sheet_title(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def _shape_oms_invoice(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "invoice_number": _text(raw.get("invoice_number")),
        "business_name": _text(raw.get("business_name")),
        "due_date": _text(raw.get("due_date")),
        "total_amount": _parse_money(raw.get("total_amount")),
        "status": normalize_direct_status(raw.get("status"), default="Generated"),
        "invoice_file_id": _text(raw.get("invoice_file_id")),
        "line_items": raw.get("line_items") if isinstance(raw.get("line_items"), list) else [],
    }


def _read_sheet_values(
    service, spreadsheet_id: str, tab_name: str
) -> Tuple[list, list]:
    rng = f"{_escape_sheet_title(tab_name)}!A1:AZ"
    resp = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueRenderOption="FORMATTED_VALUE",
        )
        .execute()
    )
    values = resp.get("values") or []
    header = values[0] if values else []
    rows = values[1:] if len(values) > 1 else []
    return header, rows


def _list_sheet_invoices(cfg: dict[str, str]) -> Dict[str, Any]:
    service = get_sheets_service()
    if not service:
        return {"invoices": [], "error": "Could not connect to Google Sheets"}
    try:
        header, rows = _read_sheet_values(
            service, cfg["spreadsheet_id"], cfg["tab_name"]
        )
    except HttpError as e:
        logger.warning("direct invoice sheet read failed: %s", e)
        return {"invoices": [], "error": "Could not read invoice sheet"}
    except Exception:
        logger.exception("direct invoice sheet read failed")
        return {"invoices": [], "error": "Could not read invoice sheet"}

    invoices = parse_ledger_rows(header, rows, default_status="Sent")
    invoices.sort(
        key=lambda row: str(row.get("invoice_number") or ""),
        reverse=True,
    )
    return {"invoices": invoices, "count": len(invoices)}


def _created_date(iso: object) -> str:
    text = _text(iso)
    if not text:
        return ""
    return text[:10]


def _list_drive_invoices(category_key: str) -> Dict[str, Any]:
    cfg = get_category_config(category_key)
    if not cfg or not cfg.business_parent_folder_id:
        return {"invoices": [], "error": "Drive folder not configured"}
    drive = get_drive_service()
    if not drive:
        return {"invoices": [], "error": "Could not connect to Google Drive"}

    folders, folder_err = _list_children(
        drive, cfg.business_parent_folder_id, folders_only=True
    )
    if folder_err:
        return {"invoices": [], "error": "Could not list invoice folders"}

    invoices: list[dict[str, Any]] = []
    for folder in folders:
        folder_id = folder.get("id") or ""
        business_name = _text(folder.get("name")) or "Untitled"
        if not folder_id:
            continue
        files, file_err = _list_drive_files(drive, folder_id)
        if file_err:
            logger.warning(
                "direct invoice drive list failed folder=%s err=%s",
                folder_id,
                file_err,
            )
            continue
        for f in files:
            if not _is_pdf(f):
                continue
            file_id = _text(f.get("id"))
            if not file_id:
                continue
            name = _text(f.get("name")) or "Invoice"
            invoice_number = infer_invoice_number(name) or name.rsplit(".", 1)[0]
            props = f.get("appProperties") or {}
            status_raw = props.get(DRIVE_STATUS_KEY) if isinstance(props, dict) else ""
            invoices.append(
                {
                    "invoice_number": invoice_number,
                    "business_name": business_name,
                    "due_date": _created_date(f.get("createdTime")),
                    "total_amount": 0.0,
                    "status": normalize_direct_status(status_raw, default="Generated"),
                    "invoice_file_id": file_id,
                    "line_items": [],
                }
            )

    invoices.sort(
        key=lambda row: (
            str(row.get("due_date") or ""),
            str(row.get("invoice_number") or ""),
        ),
        reverse=True,
    )
    return {"invoices": invoices, "count": len(invoices)}


def _list_drive_files(drive, parent_id: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    q = (
        f"'{parent_id}' in parents and trashed = false "
        "and mimeType != 'application/vnd.google-apps.folder'"
    )
    fields = (
        "nextPageToken, files(id, name, mimeType, webViewLink, "
        "createdTime, modifiedTime, appProperties)"
    )
    items: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    try:
        while True:
            kwargs: Dict[str, Any] = {
                "q": q,
                "spaces": "drive",
                "fields": fields,
                "pageSize": 100,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
            }
            if page_token:
                kwargs["pageToken"] = page_token
            result = drive.files().list(**kwargs).execute()
            items.extend(result.get("files") or [])
            page_token = result.get("nextPageToken")
            if not page_token:
                break
    except HttpError as e:
        logger.exception("direct invoice drive list failed parent=%s", parent_id)
        return [], str(e)
    except Exception as e:
        logger.exception("direct invoice drive list unexpected parent=%s", parent_id)
        return [], str(e)
    return items, None


def list_direct_invoices(stream: str) -> Dict[str, Any]:
    cfg = DIRECT_CLIENT_STREAMS.get(stream)
    if not cfg:
        return {"invoices": [], "error": "unknown_stream"}
    kind = cfg["kind"]
    if kind == "oms":
        result = get_invoice_history("")
        invoices = [
            _shape_oms_invoice(row)
            for row in (result.get("invoices") or [])
            if isinstance(row, dict) and _text(row.get("invoice_number"))
        ]
        err = result.get("error")
        payload: Dict[str, Any] = {
            "invoices": invoices,
            "count": len(invoices),
        }
        if err and not invoices:
            payload["error"] = str(err)
        return payload
    if kind == "sheet":
        return _list_sheet_invoices(cfg)
    if kind == "drive":
        return _list_drive_invoices(cfg["category"])
    return {"invoices": [], "error": "unknown_stream"}


def _ensure_status_column(
    service, spreadsheet_id: str, tab_name: str, header: list
) -> int:
    existing = _header_index(header, _STATUS_HEADERS)
    if existing is not None:
        return existing
    col = len(header)
    rng = f"{_escape_sheet_title(tab_name)}!{a1_column(col)}1"
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="USER_ENTERED",
        body={"values": [["Status"]]},
    ).execute()
    header.append("Status")
    return col


def _update_sheet_status(
    cfg: dict[str, str],
    business_name: str,
    invoice_number: str,
    status: str,
) -> Dict[str, Any]:
    service = get_sheets_service()
    if not service:
        return {"success": False, "error": "Could not connect to Google Sheets"}
    try:
        header, rows = _read_sheet_values(
            service, cfg["spreadsheet_id"], cfg["tab_name"]
        )
        invoice_idx = _header_index(header, _INVOICE_HEADERS)
        client_idx = _header_index(header, _CLIENT_HEADERS)
        if invoice_idx is None or client_idx is None:
            return {"success": False, "error": "Invoice Number / Client columns not found"}
        status_idx = _ensure_status_column(
            service, cfg["spreadsheet_id"], cfg["tab_name"], header
        )
        want_business = business_name.strip().casefold()
        want_invoice = invoice_number.strip()
        sheet_rows: list[int] = []
        for idx, row in enumerate(rows):
            if _text(_cell(row, invoice_idx)) != want_invoice:
                continue
            if _text(_cell(row, client_idx)).casefold() != want_business:
                continue
            sheet_rows.append(idx + 2)
        if not sheet_rows:
            return {"success": False, "error": "No matching rows found for this invoice"}
        col_letter = a1_column(status_idx)
        tab = cfg["tab_name"]
        data = [
            {
                "range": f"{_escape_sheet_title(tab)}!{col_letter}{r}:{col_letter}{r}",
                "values": [[status]],
            }
            for r in sheet_rows
        ]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=cfg["spreadsheet_id"],
            body={"valueInputOption": "USER_ENTERED", "data": data},
        ).execute()
        return {"success": True, "updated_rows": len(sheet_rows)}
    except HttpError as e:
        logger.warning("direct invoice status update failed: %s", e)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.exception("direct invoice status update failed")
        return {"success": False, "error": str(e)}


def _update_drive_status(
    category_key: str,
    business_name: str,
    invoice_number: str,
    status: str,
    invoice_file_id: str,
) -> Dict[str, Any]:
    drive = get_drive_service()
    if not drive:
        return {"success": False, "error": "Could not connect to Google Drive"}

    file_id = extract_drive_file_id(invoice_file_id)
    if not file_id:
        listed = _list_drive_invoices(category_key)
        matches = [
            row
            for row in listed.get("invoices") or []
            if _text(row.get("business_name")).casefold() == business_name.strip().casefold()
            and _text(row.get("invoice_number")) == invoice_number.strip()
        ]
        if len(matches) != 1:
            return {"success": False, "error": "No matching invoice PDF found"}
        file_id = _text(matches[0].get("invoice_file_id"))
    if not file_id:
        return {"success": False, "error": "No matching invoice PDF found"}

    try:
        drive.files().update(
            fileId=file_id,
            body={"appProperties": {DRIVE_STATUS_KEY: status}},
            fields="id, appProperties",
            supportsAllDrives=True,
        ).execute()
        return {"success": True, "updated_rows": 1}
    except HttpError as e:
        logger.warning("direct invoice drive status update failed: %s", e)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.exception("direct invoice drive status update failed")
        return {"success": False, "error": str(e)}


def update_direct_invoice_status(
    stream: str,
    business_name: str,
    invoice_number: str,
    status: str,
    invoice_file_id: str = "",
) -> Dict[str, Any]:
    if status not in ALLOWED_STATUSES:
        return {
            "success": False,
            "error": f"Status must be one of: {', '.join(ALLOWED_STATUSES)}",
        }
    if not business_name or not invoice_number:
        return {
            "success": False,
            "error": "business_name and invoice_number are required",
        }
    cfg = DIRECT_CLIENT_STREAMS.get(stream)
    if not cfg:
        return {"success": False, "error": "unknown_stream"}
    kind = cfg["kind"]
    if kind == "oms":
        return update_oms_invoice_status(business_name, invoice_number, status)
    if kind == "sheet":
        return _update_sheet_status(cfg, business_name, invoice_number, status)
    if kind == "drive":
        return _update_drive_status(
            cfg["category"],
            business_name,
            invoice_number,
            status,
            invoice_file_id,
        )
    return {"success": False, "error": "unknown_stream"}
