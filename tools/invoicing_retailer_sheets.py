"""
Row counts for retailer "Commission Figures" tabs (invoicing UI).
Uses the same service account as other Sheets integrations.
"""

from __future__ import annotations

import logging
from typing import Optional

from googleapiclient.errors import HttpError

from tools.one_month_savings import get_sheets_service

logger = logging.getLogger(__name__)

# Commission Figures tab gid is shared across these retailer workbooks (see invoicing page).
COMMISSION_FIGURES_GID = 1703322444

_RETAILER_SHEETS: dict[str, str] = {
    "origin-gas": "13KUaL34dV8TCUtcExCZI9tC8yAb2XiYK3-MyVLglphE",
    "origin-elec": "1cqi0rFfcD8fLFehPIg6IDHJqwRL1AHR3b-_t2Gsyz7k",
    "alinta-gas": "16t1eFN8gIXr-EmcI08POzEMfCNwO3LazHYB2RSKDmk0",
}

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
