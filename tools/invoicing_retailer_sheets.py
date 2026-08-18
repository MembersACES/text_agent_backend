"""
Row counts for retailer "Commission Figures" tabs (invoicing UI).
Uses the same service account as other Sheets integrations.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

from googleapiclient.errors import HttpError

from tools.one_month_savings import get_sheets_service

logger = logging.getLogger(__name__)

# Commission Figures tab gid is shared across these retailer workbooks (see invoicing page).
COMMISSION_FIGURES_GID = 1703322444
# First sheet in Origin / Alinta retailer workbooks is Commission Up to Date.
COMMISSION_UP_TO_DATE_GID = 0

_RETAILER_SHEETS: dict[str, str] = {
    "origin-gas": "13KUaL34dV8TCUtcExCZI9tC8yAb2XiYK3-MyVLglphE",
    "origin-elec": "1cqi0rFfcD8fLFehPIg6IDHJqwRL1AHR3b-_t2Gsyz7k",
    "alinta-gas": "16t1eFN8gIXr-EmcI08POzEMfCNwO3LazHYB2RSKDmk0",
    "alinta-ci-elec": "1t_Eta4M8bgWVuj9UPgKLWz7A7WBw8U8GCSgrKrzgYzY",
}

ORIGIN_COMMISSION_READY_KEYS = frozenset({"origin-gas", "origin-elec"})

_TOTAL_COMMISSION_HEADER = "total commission"
_IDENTIFIER_HEADERS = ("mrin", "mirn", "nmi")
_TOTALS_ROW_LABELS = frozenset({"total", "totals", "sum", "grand total"})
_MONEY_NOISE_RE = re.compile(r"[$,]|aud", re.IGNORECASE)

# Trojan Oil mass invoice database — tab "All Data" (invoicing page gid).
TROJAN_OIL_SPREADSHEET_ID = "1lFAUB1nl7yh2JkwgEI7Zd_lSDGusDtVnDIuZFPVmhdU"
TROJAN_OIL_ALL_DATA_GID = 2013429471


def list_retailer_keys() -> list[str]:
    return sorted(_RETAILER_SHEETS.keys())


def _escape_sheet_title(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def _sheet_title_for_gid(service, spreadsheet_id: str, sheet_gid: int) -> Optional[str]:
    meta = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )
    for s in meta.get("sheets", []):
        props = s.get("properties") or {}
        if props.get("sheetId") == sheet_gid:
            return props.get("title")
    return None


def _list_sheet_properties(service, spreadsheet_id: str) -> list[dict[str, Any]]:
    meta = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )
    out: list[dict[str, Any]] = []
    for s in meta.get("sheets", []):
        props = s.get("properties") or {}
        title = props.get("title")
        gid = props.get("sheetId")
        if title is None or gid is None:
            continue
        out.append({"name": str(title), "gid": str(gid)})
    return out


def _normalize_header(value: object) -> str:
    return " ".join(str(value).strip().casefold().split())


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


def _is_data_identifier(value: object) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    return text.casefold() not in _TOTALS_ROW_LABELS


def summarise_commission_up_to_date(
    header: list, rows: list
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """
    Sum the Total Commission column and count identifier rows (MRIN / MIRN / NMI).
    Header is row 1; rows are data from row 2 onward.
    """
    if not header:
        return None, "missing_header"

    normalized = [_normalize_header(cell) for cell in header]
    try:
        commission_idx = normalized.index(_TOTAL_COMMISSION_HEADER)
    except ValueError:
        return None, "total_commission_column_not_found"

    identifier_idx = 0
    row_label = "row"
    for name in _IDENTIFIER_HEADERS:
        if name in normalized:
            identifier_idx = normalized.index(name)
            row_label = header[identifier_idx]
            if isinstance(row_label, str):
                row_label = row_label.strip() or name.upper()
            else:
                row_label = name.upper()
            break
    else:
        if header and str(header[0]).strip():
            row_label = str(header[0]).strip()

    count = 0
    total = 0.0
    for row in rows:
        ident = row[identifier_idx] if identifier_idx < len(row) else None
        if not _is_data_identifier(ident):
            continue
        count += 1
        commission = row[commission_idx] if commission_idx < len(row) else None
        total += _parse_money(commission)

    return {
        "row_count": count,
        "total_commission": round(total, 2),
        "row_label": row_label,
    }, None


def _commission_up_to_date_title(tabs: list[dict[str, Any]]) -> Optional[str]:
    exact = [
        t["name"]
        for t in tabs
        if _normalize_header(t.get("name")) == "commission up to date"
    ]
    if exact:
        return exact[0]
    partial = [
        t["name"]
        for t in tabs
        if "commission up to date" in _normalize_header(t.get("name"))
    ]
    if partial:
        return partial[0]
    gid_match = [t["name"] for t in tabs if str(t.get("gid")) == str(COMMISSION_UP_TO_DATE_GID)]
    return gid_match[0] if gid_match else None


def list_retailer_sheet_tabs(retailer_key: str) -> tuple[Optional[list[dict[str, str]]], Optional[str]]:
    sid = _RETAILER_SHEETS.get(retailer_key)
    if not sid:
        return None, "unknown_retailer"

    service = get_sheets_service()
    if not service:
        return None, "sheets_unavailable"

    try:
        tabs = _list_sheet_properties(service, sid)
    except HttpError as e:
        logger.warning(
            "retailer sheet tabs HttpError retailer=%s status=%s",
            retailer_key,
            getattr(e.resp, "status", e),
        )
        return None, "http_error"
    except Exception:
        logger.exception("retailer sheet tabs failed retailer=%s", retailer_key)
        return None, "http_error"

    return [{"name": t["name"], "gid": t["gid"]} for t in tabs], None


def get_commission_up_to_date_summary(
    retailer_key: str,
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """
    Row count + Total Commission sum on the Commission Up to Date tab.
    Restricted to Origin Gas / Origin Elec workbooks.
    """
    if retailer_key not in ORIGIN_COMMISSION_READY_KEYS:
        return None, "unknown_retailer"

    sid = _RETAILER_SHEETS.get(retailer_key)
    if not sid:
        return None, "unknown_retailer"

    service = get_sheets_service()
    if not service:
        return None, "sheets_unavailable"

    try:
        tabs = _list_sheet_properties(service, sid)
        title = _commission_up_to_date_title(tabs)
        if not title:
            return None, "tab_not_found"

        rng = f"{_escape_sheet_title(title)}!A1:ZZ"
        resp = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=sid,
                range=rng,
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
        )
    except HttpError as e:
        logger.warning(
            "commission_up_to_date summary HttpError retailer=%s status=%s",
            retailer_key,
            getattr(e.resp, "status", e),
        )
        return None, "http_error"
    except Exception:
        logger.exception("commission_up_to_date summary failed retailer=%s", retailer_key)
        return None, "http_error"

    values = resp.get("values") or []
    header = values[0] if values else []
    rows = values[1:] if len(values) > 1 else []
    return summarise_commission_up_to_date(header, rows)


def _row_looks_like_client(row: list) -> bool:
    """True if MIRN (col A) or Customer name (col B) has content — matches sheet layout."""
    if not row:
        return False
    a = row[0] if len(row) > 0 else None
    b = row[1] if len(row) > 1 else None
    sa = str(a).strip() if a is not None else ""
    sb = str(b).strip() if b is not None else ""
    return bool(sa or sb)


def get_commission_figures_client_count(retailer_key: str) -> tuple[Optional[int], Optional[str]]:
    """
    Returns (client_count, error_message). Count excludes the header row.
    """
    sid = _RETAILER_SHEETS.get(retailer_key)
    if not sid:
        return None, "unknown_retailer"

    service = get_sheets_service()
    if not service:
        return None, "sheets_unavailable"

    try:
        title = _sheet_title_for_gid(service, sid, COMMISSION_FIGURES_GID)
        if not title:
            return None, "tab_not_found"

        rng = f"{_escape_sheet_title(title)}!A2:Z"
        resp = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=sid,
                range=rng,
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
        )
    except HttpError as e:
        logger.warning(
            "commission_figures count HttpError retailer=%s status=%s",
            retailer_key,
            getattr(e.resp, "status", e),
        )
        return None, "http_error"
    except Exception as e:
        logger.exception("commission_figures count failed retailer=%s", retailer_key)
        return None, str(e)

    values = resp.get("values") or []
    count = sum(1 for row in values if _row_looks_like_client(row))
    return count, None


def get_trojan_oil_unique_client_count() -> tuple[Optional[int], Optional[str]]:
    """
    Count distinct client names in column A on the 'All Data' tab (header excluded).
    Comparison is case-insensitive after strip so duplicate rows for the same client roll up.
    """
    service = get_sheets_service()
    if not service:
        return None, "sheets_unavailable"

    try:
        title = _sheet_title_for_gid(service, TROJAN_OIL_SPREADSHEET_ID, TROJAN_OIL_ALL_DATA_GID)
        if not title:
            return None, "tab_not_found"

        rng = f"{_escape_sheet_title(title)}!A2:A"
        resp = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=TROJAN_OIL_SPREADSHEET_ID,
                range=rng,
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
        )
    except HttpError as e:
        logger.warning(
            "trojan_oil unique clients HttpError status=%s",
            getattr(e.resp, "status", e),
        )
        return None, "http_error"
    except Exception as e:
        logger.exception("trojan_oil unique clients failed")
        return None, str(e)

    values = resp.get("values") or []
    seen: set[str] = set()
    for row in values:
        if not row:
            continue
        raw = row[0]
        if raw is None:
            continue
        name = str(raw).strip()
        if not name:
            continue
        seen.add(name.casefold())
    return len(seen), None
