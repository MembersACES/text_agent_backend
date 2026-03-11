"""
Contract end dates from Member ACES Data Google Sheet.
Reads C&I Electricity (17th sheet) and C&I Gas (13th sheet) contract end dates,
and can sync missing end dates into Airtable.
Uses the same spreadsheet and Sheets service as one_month_savings_calculation.
"""

import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional

from dotenv import load_dotenv

backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(dotenv_path=os.path.join(backend_root, ".env"))

logger = logging.getLogger(__name__)

try:
    from tools.one_month_savings_calculation import MEMBER_ACES_DATA_SHEET_ID
except Exception:
    MEMBER_ACES_DATA_SHEET_ID = os.getenv(
        "MEMBER_ACES_DATA_SHEET_ID",
        "1ozwxJjqBQE3fJeMHmsXzPFCsekupwXZ3A7F0jstOfVw",
    )

# Sheet names in Member ACES Data spreadsheet (by name, not index)
CONTRACT_SHEET_NAME_CI_ELECTRICITY = "17th Sheet - C&I E contracts"
CONTRACT_SHEET_NAME_CI_GAS = "13th Sheet - Signed C&I Gas"


def _get_sheets_service():
    """Use the same Sheets service as one_month_savings."""
    from tools.one_month_savings import get_sheets_service
    return get_sheets_service()


def _get_sheet_title_by_index(service, spreadsheet_id: str, index: int) -> Optional[str]:
    """Return the title of the sheet at 0-based index, or None."""
    try:
        meta = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(title))",
        ).execute()
        sheets = meta.get("sheets", [])
        if index < len(sheets):
            return (sheets[index].get("properties") or {}).get("title")
    except Exception as e:
        logger.warning("[contract_ending_sheet] get_sheet_title_by_index failed: %s", e)
    return None


def _parse_date_cell(value) -> Optional[str]:
    """
    Parse date from cell value: dd/mm/yyyy, d/m/yy, Excel serial, or YYYY-MM-DD.
    Returns YYYY-MM-DD or None.
    """
    if value is None or value == "":
        return None
    # Google Sheets UNFORMATTED_VALUE often returns dates as Excel serial number
    if isinstance(value, (int, float)):
        try:
            # Excel serial: 1 = 1900-01-01. Use 25569 for 1970-01-01 offset.
            serial = int(value) if value == int(value) else float(value)
            if serial < 1 or serial > 100000:
                return None
            d = datetime(1899, 12, 30) + timedelta(days=serial)
            return d.strftime("%Y-%m-%d")
        except (ValueError, OverflowError, OSError):
            return None
    s = str(value).strip()
    if not s:
        return None
    # Already YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # dd/mm/yyyy or d/m/yyyy
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if m:
        d, mon, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        try:
            dt = datetime(y, mon, d)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None
    # Try datetime.strptime for other formats
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _find_column_index(headers: list, names: list) -> int:
    """Return 0-based column index of first header that matches any of names (case-insensitive)."""
    for i, h in enumerate(headers):
        if h is None:
            continue
        h_str = str(h).strip().lower()
        for n in names:
            if n.lower() in h_str or h_str in n.lower():
                return i
    return -1


def _read_sheet_contract_end_dates(
    service,
    spreadsheet_id: str,
    sheet_title: str,
    identifier_names: list,
    date_column_names: list,
) -> dict[str, str]:
    """
    Read sheet and return a map identifier -> YYYY-MM-DD (max date per identifier).
    identifier_names: e.g. ["NMI"] or ["MRIN"]
    date_column_names: e.g. ["Contract End Date"]
    """
    out: dict[str, str] = {}
    if not sheet_title or not spreadsheet_id:
        return out
    try:
        range_str = f"'{sheet_title}'!A1:ZZ1000"
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_str,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except Exception as e:
        logger.warning("[contract_ending_sheet] read sheet %r failed: %s", sheet_title, e)
        return out
    rows = resp.get("values", [])
    if not rows:
        return out
    headers = [str(h).strip() if h is not None else "" for h in rows[0]]
    id_col = _find_column_index(headers, identifier_names)
    date_col = _find_column_index(headers, date_column_names)
    if id_col < 0 or date_col < 0:
        logger.info(
            "[contract_ending_sheet] sheet %r: id_col=%s (names=%s), date_col=%s (names=%s). Headers (first 15)=%s",
            sheet_title, id_col, identifier_names, date_col, date_column_names,
            headers[:15] if len(headers) > 15 else headers,
        )
        print(f"[contract-ending-sheet] Sheet {sheet_title!r}: id_col={id_col}, date_col={date_col}. Header names (first 15): {headers[:15]}", flush=True)
        return out
    rows_with_date = 0
    for row in rows[1:]:
        if id_col >= len(row) or date_col >= len(row):
            continue
        ident = str(row[id_col]).strip() if row[id_col] is not None else ""
        if not ident:
            continue
        date_str = _parse_date_cell(row[date_col])
        if not date_str:
            continue
        rows_with_date += 1
        existing = out.get(ident)
        if existing is None or date_str > existing:
            out[ident] = date_str
    print(f"[contract-ending-sheet] Sheet {sheet_title!r}: {len(rows)-1} data rows, {rows_with_date} with parseable date, {len(out)} unique IDs in map", flush=True)
    return out


def get_contract_end_dates_from_sheet() -> tuple[dict[str, str], dict[str, str]]:
    """
    Read Member ACES Data spreadsheet by sheet name:
    - "17th Sheet - C&I E contracts": NMI -> max Contract End Date / End Date
    - "13th Sheet - Signed C&I Gas": MRIN -> max Contract End Date / End Date
    Returns (nmi_to_date, mrin_to_date).
    """
    nmi_to_date: dict[str, str] = {}
    mrin_to_date: dict[str, str] = {}
    if not MEMBER_ACES_DATA_SHEET_ID:
        logger.warning("[contract_ending_sheet] MEMBER_ACES_DATA_SHEET_ID not set")
        print("[contract-ending-sync] MEMBER_ACES_DATA_SHEET_ID not set", flush=True)
        return nmi_to_date, mrin_to_date
    service = _get_sheets_service()
    if not service:
        logger.warning("[contract_ending_sheet] could not get Sheets service")
        print("[contract-ending-sync] Could not get Google Sheets service (check service account)", flush=True)
        return nmi_to_date, mrin_to_date
    # Use sheet names directly (not index)
    title_ci_e = CONTRACT_SHEET_NAME_CI_ELECTRICITY
    title_ci_gas = CONTRACT_SHEET_NAME_CI_GAS
    print(f"[contract-ending-sheet] Using sheets by name: {title_ci_e!r}, {title_ci_gas!r}", flush=True)
    if title_ci_e:
        nmi_to_date = _read_sheet_contract_end_dates(
            service,
            MEMBER_ACES_DATA_SHEET_ID,
            title_ci_e,
            identifier_names=["NMI"],
            date_column_names=["Contract End Date", "Contract end date", "End Date"],
        )
        logger.info("[contract_ending_sheet] C&I E sheet %r: %s NMIs with end date", title_ci_e, len(nmi_to_date))
    else:
        logger.warning("[contract_ending_sheet] C&I E sheet name not configured")
    if title_ci_gas:
        mrin_to_date = _read_sheet_contract_end_dates(
            service,
            MEMBER_ACES_DATA_SHEET_ID,
            title_ci_gas,
            identifier_names=["MRIN"],
            date_column_names=["Contract End Date", "Contract end date", "End Date"],
        )
        logger.info("[contract_ending_sheet] Signed C&I Gas sheet %r: %s MRINs with end date", title_ci_gas, len(mrin_to_date))
    else:
        logger.warning("[contract_ending_sheet] Signed C&I Gas sheet name not configured")
    return nmi_to_date, mrin_to_date


def sync_contract_end_dates_to_airtable() -> dict:
    """
    For each C&I Electricity and C&I Gas record in Airtable that has no contract end date,
    if the sheet has an end date for that NMI/MRIN, update Airtable.
    Returns {"updated_electricity": int, "updated_gas": int, "errors": list, "updates": list}.
    Each item in updates: {"utility_type", "identifier", "contract_end_date", "source_sheet"}.
    """
    from services import airtable_client
    result = {
        "updated_electricity": 0,
        "updated_gas": 0,
        "errors": [],
        "updates": [],
    }
    if not getattr(airtable_client, "AIRTABLE_API_KEY", None):
        result["errors"].append("Airtable not configured")
        logger.warning("[contract_ending_sheet] sync skipped: Airtable not configured")
        print("[contract-ending-sync] SKIP: Airtable not configured", flush=True)
        return result

    logger.info("[contract_ending_sheet] ========== CONTRACT END DATE SYNC START ==========")
    print("[contract-ending-sync] START: reading sheet then updating Airtable (missing end dates only)", flush=True)
    nmi_to_date, mrin_to_date = get_contract_end_dates_from_sheet()
    sheet_sources = {
        "C&I Electricity": "Member ACES Data (17th sheet - C&I E contracts)",
        "C&I Gas": "Member ACES Data (13th sheet - Signed C&I Gas)",
    }

    for utility_type, id_to_date in [("C&I Electricity", nmi_to_date), ("C&I Gas", mrin_to_date)]:
        id_label = "NMI" if utility_type == "C&I Electricity" else "MRIN"
        source_sheet = sheet_sources.get(utility_type, "Google Sheet")
        try:
            records = airtable_client.list_all_utility_records(utility_type)
            logger.info("[contract_ending_sheet] %s: %s Airtable records, %s identifiers with end date in sheet %r",
                        utility_type, len(records), len(id_to_date), source_sheet)
            print(f"[contract-ending-sync] {utility_type}: {len(records)} Airtable records, {len(id_to_date)} IDs with end date in sheet", flush=True)
            for rec in records:
                if rec.get("contract_end_date"):
                    continue
                ident = (rec.get("identifier") or "").strip()
                if not ident:
                    continue
                date_str = id_to_date.get(ident)
                if not date_str:
                    continue
                ok = airtable_client.update_utility_record(
                    utility_type,
                    ident,
                    contract_end_date=date_str,
                )
                if ok:
                    if utility_type == "C&I Electricity":
                        result["updated_electricity"] += 1
                    else:
                        result["updated_gas"] += 1
                    result["updates"].append({
                        "utility_type": utility_type,
                        "identifier": ident,
                        "identifier_label": id_label,
                        "contract_end_date": date_str,
                        "source_sheet": source_sheet,
                    })
                    logger.info(
                        "[contract_ending_sheet] UPDATED %s %s=%s -> Contract End Date=%s (from %s)",
                        utility_type, id_label, ident, date_str, source_sheet,
                    )
                    print(f"[contract-ending-sync] UPDATED {utility_type} {id_label}={ident} -> {date_str} (from {source_sheet})", flush=True)
                else:
                    err_msg = f"Failed to update {utility_type} {id_label} {ident}"
                    result["errors"].append(err_msg)
                    logger.warning("[contract_ending_sheet] %s", err_msg)
        except Exception as e:
            logger.exception("[contract_ending_sheet] sync failed for %s: %s", utility_type, e)
            result["errors"].append(str(e))

    logger.info(
        "[contract_ending_sheet] ========== SYNC DONE: C&I E=%s updated, C&I G=%s updated, errors=%s ==========",
        result["updated_electricity"], result["updated_gas"], len(result["errors"]),
    )
    print(f"[contract-ending-sync] DONE: C&I E={result['updated_electricity']} updated, C&I G={result['updated_gas']} updated, errors={len(result['errors'])}", flush=True)
    return result
