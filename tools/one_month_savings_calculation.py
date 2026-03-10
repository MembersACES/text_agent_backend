"""
One Month Savings Calculation from Member ACES Data sheet.
Reads invoice data by identifier and utility type, finds pre- and post-agreement
month rows, and returns the calculated savings (no n8n).
Uses the same Google Sheets service account as one_month_savings.
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from dotenv import load_dotenv

backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(dotenv_path=os.path.join(backend_root, ".env"))

logger = logging.getLogger(__name__)

# Member ACES Data spreadsheet (shared with service account)
MEMBER_ACES_DATA_SHEET_ID = os.getenv(
    "MEMBER_ACES_DATA_SHEET_ID",
    "1ozwxJjqBQE3fJeMHmsXzPFCsekupwXZ3A7F0jstOfVw",
)

# Utility type -> sheet name and column config (0-based indices: id_col, period_col, total_col)
# Period column: range "dd/mm/yyyy-dd/mm/yyyy" (use end date) or single date for Oil
SHEET_CONFIG: Dict[str, Dict[str, Any]] = {
    "C&I Electricity": {
        "sheet": "2nd Sheet - Electricity details from the invoice",
        "id_col": 0,
        "period_col": 3,
        "total_col": 89,
        "period_is_range": True,
    },
    "SME Electricity": {
        "sheet": "3rd Sheet - SME Electricity",
        "id_col": 0,
        "period_col": 3,
        "total_col": 35,
        "period_is_range": True,
    },
    "C&I Gas": {
        "sheet": "4th Sheet - Large Gas",
        "id_col": 0,
        "period_col": 3,
        "total_col": 35,
        "period_is_range": True,
    },
    "SME Gas": {
        "sheet": "5th Sheet - Small Gas",
        "id_col": 0,
        "period_col": 3,
        "total_col": 28,
        "period_is_range": True,
    },
    "Waste": {
        "sheet": "7th Sheet - Waste",
        "id_col": 0,
        "period_col": 5,
        "total_col": 47,
        "period_is_range": True,
    },
    "Oil": {
        "sheet": "8th Sheet - Oil",
        "id_col": 0,
        "period_col": 5,
        "total_col": 16,
        "period_is_range": False,
    },
}


def _col_letter(col_index_0based: int) -> str:
    """Convert 0-based column index to Sheets column letter(s), e.g. 0->A, 26->AA, 89->CL."""
    n = col_index_0based + 1
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s or "A"


def _get_sheets_service():
    """Use the same Sheets service as one_month_savings (same service account)."""
    from tools.one_month_savings import get_sheets_service
    return get_sheets_service()


def _parse_period_end(period_str: str, period_is_range: bool) -> Optional[Tuple[int, int]]:
    """
    Return (year, month) for the period end date.
    period_str: "01/12/2025-31/12/2025" or "4 Mar 2026" (Oil)
    """
    if not period_str or not str(period_str).strip():
        return None
    period_str = str(period_str).strip()
    try:
        if period_is_range and "-" in period_str:
            parts = period_str.split("-", 1)
            end_part = parts[1].strip()
            # dd/mm/yyyy
            d = datetime.strptime(end_part, "%d/%m/%Y")
            return (d.year, d.month)
        else:
            # Single date: try dd/mm/yyyy then "4 Mar 2026"
            for fmt in ("%d/%m/%Y", "%d %b %Y", "%d %B %Y"):
                try:
                    d = datetime.strptime(period_str.strip(), fmt)
                    return (d.year, d.month)
                except ValueError:
                    continue
            return None
    except Exception as e:
        logger.debug("Parse period %r: %s", period_str, e)
        return None


def _parse_total(value: Any) -> float:
    """Parse total cost from cell (may have commas)."""
    if value is None or value == "":
        return 0.0
    s = str(value).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _normalize_identifier(identifier: str) -> str:
    """Strip and normalize for comparison (e.g. NMI/MRIN may be stored with spaces)."""
    if not identifier:
        return ""
    return str(identifier).strip()


def calculate_one_month_savings(
    identifier: str,
    utility_type: str,
    agreement_start_month: str,
    business_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Calculate 1-month savings by reading Member ACES Data sheet for the given
    identifier and utility type, selecting rows for the month before and the
    first month of the agreement, and returning pre_total - post_total.

    Args:
        identifier: NMI, MRIN, or account number (must match sheet).
        utility_type: One of "C&I Electricity", "SME Electricity", "C&I Gas",
                      "SME Gas", "Waste", "Oil".
        agreement_start_month: "YYYY-MM" or "YYYY-MM-DD" (first month of agreement).
        business_name: Optional; not used for sheet lookup (identifier is used).

    Returns:
        {
            "success": bool,
            "savings_amount": float,
            "pre_period_total": float,
            "post_period_total": float,
            "pre_period_label": str,
            "post_period_label": str,
            "pre_rows": list,
            "post_rows": list,
            "error": str (if success is False),
        }
    """
    result = {
        "success": False,
        "savings_amount": 0.0,
        "pre_period_total": 0.0,
        "post_period_total": 0.0,
        "pre_period_label": "",
        "post_period_label": "",
        "pre_rows": [],
        "post_rows": [],
        "error": "",
    }
    if not MEMBER_ACES_DATA_SHEET_ID:
        result["error"] = "MEMBER_ACES_DATA_SHEET_ID is not configured"
        return result
    config = SHEET_CONFIG.get(utility_type)
    if not config:
        result["error"] = f"Unsupported utility_type: {utility_type}"
        return result
    identifier = _normalize_identifier(identifier)
    if not identifier:
        result["error"] = "identifier is required"
        return result
    try:
        # Parse agreement start -> (year, month); pre = previous month
        if "-" in agreement_start_month:
            parts = agreement_start_month.strip().split("-")
            year = int(parts[0])
            month = int(parts[1].strip()[:2])
        else:
            result["error"] = "agreement_start_month must be YYYY-MM or YYYY-MM-DD"
            return result
        post_ym = (year, month)
        if month == 1:
            pre_ym = (year - 1, 12)
        else:
            pre_ym = (year, month - 1)
    except (ValueError, IndexError):
        result["error"] = "Invalid agreement_start_month; use YYYY-MM or YYYY-MM-DD"
        return result

    result["pre_period_label"] = f"{pre_ym[0]}-{pre_ym[1]:02d}"
    result["post_period_label"] = f"{post_ym[0]}-{post_ym[1]:02d}"

    service = _get_sheets_service()
    if not service:
        result["error"] = "Could not create Google Sheets service (check service account)"
        return result

    logger.info(
        "One-month-savings calculation: using same Sheets service account as invoice history "
        "(see 'Sheets API will use service account:' in logs above). "
        "Ensure Member ACES Data spreadsheet is shared with that email."
    )

    sheet_name = config["sheet"]
    id_col = config["id_col"]
    period_col = config["period_col"]
    total_col = config["total_col"]
    period_is_range = config["period_is_range"]

    # Request header row + data (row 1 = headers, row 2+ = data)
    max_col_index = max(id_col, period_col, total_col)
    end_letter = _col_letter(max_col_index + 5)
    range_str = f"'{sheet_name}'!A1:{end_letter}"

    logger.info(
        f"Member ACES Data request: spreadsheet_id={MEMBER_ACES_DATA_SHEET_ID}, "
        f"sheet={sheet_name!r}, range={range_str}, identifier={identifier!r}"
    )

    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
            range=range_str,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except Exception as e:
        logger.exception("Sheets API get failed")
        result["error"] = f"Failed to read sheet: {str(e)}"
        return result

    all_rows = resp.get("values", [])
    if not all_rows:
        result["error"] = f"No data in sheet '{sheet_name}'"
        return result

    headers = all_rows[0] if all_rows else []
    rows = all_rows[1:]

    def make_row_detail(row: list) -> List[Dict[str, Any]]:
        """Build list of { header, value } for each column (align by index)."""
        detail = []
        for i in range(max(len(headers), len(row))):
            h = (headers[i] if i < len(headers) else "").strip() or f"Col {i + 1}"
            v = row[i] if i < len(row) else ""
            detail.append({"header": h, "value": v})
        return detail

    pre_total = 0.0
    post_total = 0.0
    pre_rows: List[Dict[str, Any]] = []
    post_rows: List[Dict[str, Any]] = []
    pre_row_detail: List[Dict[str, Any]] = []
    post_row_detail: List[Dict[str, Any]] = []
    # Use only the first row per period to avoid double-counting duplicate sheet rows
    pre_done = False
    post_done = False

    for row in rows:
        if not isinstance(row, list) or len(row) <= max(id_col, period_col, total_col):
            continue
        row_id = _normalize_identifier(row[id_col]) if id_col < len(row) else ""
        if row_id != identifier:
            continue
        period_val = row[period_col] if period_col < len(row) else ""
        ym = _parse_period_end(period_val, period_is_range)
        if not ym:
            continue
        total_val = row[total_col] if total_col < len(row) else None
        total = _parse_total(total_val)
        if ym == pre_ym and not pre_done:
            pre_total = total
            pre_rows = [{"period": period_val, "total": total}]
            pre_row_detail = make_row_detail(row)
            pre_done = True
        elif ym == post_ym and not post_done:
            post_total = total
            post_rows = [{"period": period_val, "total": total}]
            post_row_detail = make_row_detail(row)
            post_done = True
        if pre_done and post_done:
            break

    result["pre_period_total"] = round(pre_total, 2)
    result["post_period_total"] = round(post_total, 2)
    result["savings_amount"] = round(pre_total - post_total, 2)
    result["pre_rows"] = pre_rows
    result["post_rows"] = post_rows
    result["pre_row_detail"] = pre_row_detail
    result["post_row_detail"] = post_row_detail
    result["success"] = True

    if not pre_rows:
        result["error"] = f"No invoice found for identifier {identifier} in period {result['pre_period_label']}"
        result["success"] = False
    elif not post_rows:
        result["error"] = f"No invoice found for identifier {identifier} in period {result['post_period_label']}"
        result["success"] = False

    return result
