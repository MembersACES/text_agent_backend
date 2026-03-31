"""
Direct Airtable API client for Member ACES Data base.
Used for get-business-info (LOA + linked utilities) and updating Data Requested / Contract End Date.
"""
import logging
import os
import re
import statistics
from datetime import date, datetime
from typing import Any, Optional
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

# Load env from backend root
_backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_env_path = os.path.join(_backend_root, ".env")
if os.path.exists(_env_path):
    from dotenv import load_dotenv

    # override=True: same as main.py — beat empty placeholders from shell/OS or earlier load_dotenv(cwd).
    load_dotenv(dotenv_path=_env_path, override=True)

AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appG1WoHcJt10iO5K")
USE_AIRTABLE_DIRECT = os.environ.get("USE_AIRTABLE_DIRECT", "").lower() in ("1", "true", "yes")

# LOA Business Details table (main record per member)
LOA_TABLE_ID = "tblSZOvZ3AqIvnbEv"

# Utility type -> (link field on LOA, table name for records, identifier field, retailer field)
# Table names are URL-encoded when used in API path.
UTILITY_CONFIG = [
    {
        "app_key": "C&I Electricity",
        "loa_link_field": "Link to C&I Electricity Records",
        "table_name": "C&I Electricity Records",
        "identifier_field": "NMI",
        "retailer_field": "Retailer (from Link to C&I Electricity Records)",  # or primary field name if lookup
    },
    {
        "app_key": "SME Electricity",
        "loa_link_field": "3rd Sheet - SME Electricity",
        "table_name": "SME Electricity Records",
        "identifier_field": "NMI",
        "retailer_field": "Retailer SME Electricity",
    },
    {
        "app_key": "C&I Gas",
        "loa_link_field": "4th Sheet - Large Gas",
        "table_name": "C&I Gas Clients",
        "identifier_field": "MRIN",
        "retailer_field": "Retailer C&I Gas",
    },
    {
        "app_key": "SME Gas",
        "loa_link_field": "5th Sheet - Small Gas",
        "table_name": "SME Gas Accounts",
        "identifier_field": "MRIN",
        "retailer_field": "Retailer SME Gas",
    },
    {
        "app_key": "Waste",
        "loa_link_field": "7th Sheet - Waste",  # may be singleLineText; handle both
        "table_name": "Waste",  # adjust if different table name
        "identifier_field": "Account Number",
        "retailer_field": "Retailer",
    },
    {
        "app_key": "Oil",
        "loa_link_field": "8th Sheet - Oil",
        "table_name": "Oil",  # adjust if different
        "identifier_field": "Account Name",
        "retailer_field": "Retailer",
    },
]

# Fields we read from utility tables (identifier, retailer, and the new tracking fields)
UTILITY_EXTRA_FIELDS = ["Data Requested", "Data Recieved", "Contract End Date"]


def _normalize_identifier_raw(ident: Any) -> str:
    """
    Normalize NMI/MRIN from Airtable (number, string, or list) to a canonical string
    so contract-ending matching works. Handles: 41036565463, 41036565463.0, "41036565463.0", list.
    """
    if ident is None:
        return ""
    if isinstance(ident, list):
        ident = ident[0] if ident else None
        if ident is None:
            return ""
    s = str(ident).strip()
    if not s:
        return ""
    s = s.replace(",", "")
    if not s:
        return ""
    if s.endswith(".0") and len(s) >= 3 and s[:-2].replace("-", "").replace("+", "").isdigit():
        s = s[:-2]
    try:
        n = float(s)
        if n == int(n):
            return str(int(n))
    except (ValueError, OverflowError):
        pass
    return s


def _headers() -> dict:
    if not AIRTABLE_API_KEY:
        return {}
    return {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }


def _url(table_id_or_name: str, record_id: Optional[str] = None) -> str:
    base = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"
    # Table ID (tbl...) or URL-encoded table name
    if table_id_or_name.startswith("tbl"):
        table_part = table_id_or_name
    else:
        table_part = quote(table_id_or_name, safe="")
    if record_id:
        return f"{base}/{table_part}/{record_id}"
    return f"{base}/{table_part}"


def _escape_formula_value(s: str) -> str:
    """Escape single quotes for Airtable formula."""
    return s.replace("\\", "\\\\").replace("'", "''")


def get_loa_record_by_business_name(business_name: str) -> Optional[dict]:
    """
    Find LOA Business Details record: first by Business Name, then by Trading As.
    Returns the first matching record or None.
    """
    if not business_name or not AIRTABLE_API_KEY:
        return None
    search = (business_name or "").strip()
    if not search:
        return None
    escaped = _escape_formula_value(search)
    # OR({Business Name}='X',{Trading As}='X') - try both so we match either reference
    formula = f"OR({{Business Name}}='{escaped}',{{Trading As}}='{escaped}')"
    try:
        logger.info("[utility-extra] Airtable LOA lookup: business_name=%r", search)
        r = requests.get(
            _url(LOA_TABLE_ID),
            headers=_headers(),
            params={"filterByFormula": formula, "maxRecords": 1},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        records = data.get("records", [])
        if records:
            logger.info("[utility-extra] Airtable LOA found: record id=%s", records[0].get("id", "")[:12])
            return records[0]
        logger.info("[utility-extra] Airtable LOA: no record matched business_name=%r", search)
        return None
    except requests.RequestException as e:
        logger.warning("Airtable get_loa_record request failed: %s", e)
        return None


def _fetch_record(table_name: str, record_id: str) -> Optional[dict]:
    """Fetch a single record by ID from a table."""
    if not AIRTABLE_API_KEY:
        return None
    try:
        r = requests.get(_url(table_name, record_id), headers=_headers(), timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.debug("Airtable get record %s failed: %s", record_id, e)
        return None


def _fetch_record_detailed(
    table_name: str, record_id: str
) -> tuple[Optional[dict], Optional[int], str]:
    """
    GET one record; return (body_dict, None, "") on success.
    On failure: (None, status_code_or_None, message_snippet).
    """
    if not AIRTABLE_API_KEY or not record_id:
        return None, None, "missing_api_key_or_record_id"
    url = _url(table_name, record_id)
    try:
        r = requests.get(url, headers=_headers(), timeout=15)
        if r.status_code == 200:
            try:
                return r.json(), None, ""
            except ValueError:
                return None, r.status_code, "invalid_json_response"
        snippet = (r.text or "").replace("\n", " ")[:500]
        return None, r.status_code, snippet
    except requests.RequestException as e:
        return None, None, str(e)[:500]


def _loa_field_to_record_ids(loa_record: dict, link_field: str) -> list:
    """Get list of linked record IDs from LOA record field. Handles both link (array) and text."""
    fields = loa_record.get("fields") or {}
    val = fields.get(link_field)
    if val is None:
        return []
    if isinstance(val, list):
        return [x for x in val if isinstance(x, str) and x.startswith("rec")]
    if isinstance(val, str) and val.startswith("rec"):
        return [val]
    return []


def get_linked_utility_records(loa_record: dict) -> tuple[dict, dict, dict]:
    """
    From an LOA record, resolve linked utility records and build:
    - linked_utilities: { "C&I Electricity": [id1, id2], ... }
    - utility_retailers: { "C&I Electricity": [retailer1, retailer2], ... }
    - linked_utility_extra: { "C&I Electricity": [ { identifier, retailer, contract_end_date, data_requested, data_recieved }, ... ], ... }
    """
    linked_utilities = {}
    utility_retailers = {}
    linked_utility_extra = {}
    for cfg in UTILITY_CONFIG:
        app_key = cfg["app_key"]
        link_field = cfg["loa_link_field"]
        table_name = cfg["table_name"]
        id_field = cfg["identifier_field"]
        retailer_field = cfg.get("retailer_field") or "Retailer"
        record_ids = _loa_field_to_record_ids(loa_record, link_field)
        if not record_ids:
            continue
        identifiers = []
        retailers = []
        extras = []
        for rec_id in record_ids:
            rec = _fetch_record(table_name, rec_id)
            if not rec:
                continue
            f = rec.get("fields") or {}
            # Log raw field keys once per table to debug Airtable column names (e.g. "Data Received" vs "Data Recieved")
            if not identifiers and f:
                logger.info(
                    "[utility-extra] Airtable record fields keys for %s (record %s): %s",
                    app_key, rec_id[:12],
                    list(f.keys()),
                )
            ident = f.get(id_field)
            if ident is not None:
                identifiers.append(str(ident).strip())
            else:
                identifiers.append("")
            ret = f.get(retailer_field)
            if isinstance(ret, list):
                ret = ret[0] if ret else ""
            retailers.append(str(ret).strip() if ret else "")
            # New tracking fields (exact names as in Airtable - note "Data Recieved" is typo in some bases)
            data_req = f.get("Data Requested")
            data_rec = f.get("Data Recieved") or f.get("Data Received")  # support both spellings
            contract_end = f.get("Contract End Date")
            if isinstance(data_req, str) and len(data_req) >= 10:
                data_req = data_req[:10]
            if isinstance(data_rec, str) and len(data_rec) >= 10:
                data_rec = data_rec[:10]
            elif data_rec is True:
                data_rec = "Yes"
            elif data_rec is False:
                data_rec = ""
            if isinstance(contract_end, str) and len(contract_end) >= 10:
                contract_end = contract_end[:10]
            # Log what we read for date fields (first record per utility type) to debug missing end dates
            if len(extras) == 0 and (data_req or data_rec or contract_end):
                logger.info(
                    "[utility-extra] First record for %s: identifier=%r, contract_end_date=%r, data_requested=%r, data_recieved=%r",
                    app_key, str(ident).strip() if ident is not None else "", contract_end, data_req, data_rec,
                )
            extras.append({
                "identifier": str(ident).strip() if ident is not None else "",
                "retailer": retailers[-1],
                "contract_end_date": contract_end,
                "data_requested": data_req,
                "data_recieved": data_rec,
            })
        # Deduplicate by identifier (same record linked multiple times in LOA => one row per identifier)
        if identifiers:
            seen = set()
            new_identifiers = []
            new_retailers = []
            new_extras = []
            for i, ident in enumerate(identifiers):
                if ident in seen:
                    continue
                seen.add(ident)
                new_identifiers.append(ident)
                new_retailers.append(retailers[i])
                new_extras.append(extras[i])
            linked_utilities[app_key] = new_identifiers
            utility_retailers[app_key] = new_retailers
            linked_utility_extra[app_key] = new_extras
    logger.info(
        "[utility-extra] get_linked_utility_records result: linked_utilities keys=%s, linked_utility_extra keys=%s, extra sample=%s",
        list(linked_utilities.keys()),
        list(linked_utility_extra.keys()),
        {k: (v[:1] if v else []) for k, v in linked_utility_extra.items()} if linked_utility_extra else {},
    )
    return linked_utilities, utility_retailers, linked_utility_extra


def build_business_info_from_loa(loa_record: dict) -> dict:
    """
    Map LOA Business Details record fields to the shape expected by the app:
    business_details, contact_information, representative_details, gdrive, record_ID.
    """
    fields = loa_record.get("fields") or {}
    record_id = loa_record.get("id", "")

    # LOA field names from inspect (multilineText / singleLineText)
    business_details = {
        "name": (fields.get("Business Name") or "").strip() or None,
        "trading_name": (fields.get("Trading As") or "").strip() or None,
        "abn": (fields.get("Business ABN") or "").strip() or None,
    }
    contact_information = {
        "postal_address": (fields.get("Postal Address") or "").strip() or None,
        "site_address": (fields.get("Site Address") or "").strip() or None,
        "telephone": (fields.get("Contact Number") or "").strip() or None,
        "email": (fields.get("Contact Email") or "").strip() or None,
    }
    representative_details = {
        "contact_name": (fields.get("Contact Name") or "").strip() or None,
        "position": (fields.get("Contact Position") or "").strip() or None,
        "signed_date": (fields.get("Date") or fields.get("Postal Date") or "").strip() or None,
    }
    # Google Drive folder URL - may be in a lookup or direct field
    folder_url = (fields.get("File ID Google Drive Client Folder") or "").strip()
    if folder_url and not folder_url.startswith("http"):
        folder_url = f"https://drive.google.com/drive/folders/{folder_url}"
    gdrive = {"folder_url": folder_url or None}

    return {
        "record_ID": record_id,
        "business_details": business_details,
        "contact_information": contact_information,
        "representative_details": representative_details,
        "gdrive": gdrive,
    }


# Possible Airtable field names for contract end date (C&I Electricity may differ from C&I Gas)
CONTRACT_END_DATE_KEYS = ("Contract End Date", "Contract end date", "ContractEndDate", "Contract End date")


def _get_contract_end_date_from_fields(fields: dict) -> Any:
    """Get contract end date from record fields, trying multiple possible field names."""
    for key in CONTRACT_END_DATE_KEYS:
        if key in fields and fields[key] is not None:
            return fields[key]
    # Fallback: find any field whose name looks like "contract end date"
    for k, v in fields.items():
        if v is None:
            continue
        normalized = (k or "").strip().lower().replace(" ", "").replace("_", "")
        if "contract" in normalized and "end" in normalized and "date" in normalized:
            return v
    return None


def _normalize_contract_end_date(value: Any) -> Optional[str]:
    """Normalize contract end date to YYYY-MM-DD or None if missing/invalid."""
    if value is None or value == "":
        return None
    s = str(value).strip()
    if not s:
        return None
    # Already YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # US format MM/DD/YYYY or M/D/YYYY
    if "/" in s and len(s) >= 8:
        parts = s.split("/")
        if len(parts) == 3:
            try:
                m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
                if y < 100:
                    y += 2000
                dt = datetime(y, m, d)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass
    if len(s) >= 10:
        return s[:10]
    return None


def list_all_utility_records(utility_type: str) -> list[dict]:
    """
    List all records from the Airtable table for the given utility type.
    utility_type must be "C&I Electricity" or "C&I Gas".
    Returns a list of dicts: { "identifier", "contract_end_date", "retailer", "record_id" }.
    Uses pagination (offset) to fetch all records. contract_end_date is YYYY-MM-DD or None.
    """
    if utility_type not in ("C&I Electricity", "C&I Gas"):
        logger.warning("[list_all_utility_records] unsupported utility_type=%r", utility_type)
        return []
    if not AIRTABLE_API_KEY:
        return []
    cfg = None
    for c in UTILITY_CONFIG:
        if c["app_key"] == utility_type:
            cfg = c
            break
    if not cfg:
        return []
    table_name = cfg["table_name"]
    id_field = cfg["identifier_field"]
    retailer_field = cfg.get("retailer_field") or "Retailer"
    # Don't pass fields[] - Airtable can return 422 for field names (e.g. lookup names). Fetch all and use what we need.
    out: list[dict] = []
    try:
        print(f"[airtable] list_all_utility_records: {utility_type} (fetching...)", flush=True)
        offset: Optional[str] = None
        page = 0
        max_pages = 50
        while page < max_pages:
            page += 1
            # Use pageSize (per-page), not maxRecords (total cap). maxRecords=100 can prevent offset.
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset
            r = requests.get(
                _url(table_name),
                headers=_headers(),
                params=params,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            records = data.get("records", [])
            for rec in records:
                rid = rec.get("id", "")
                f = rec.get("fields") or {}
                ident = f.get(id_field)
                identifier = _normalize_identifier_raw(ident)
                contract_end_raw = _get_contract_end_date_from_fields(f)
                contract_end = _normalize_contract_end_date(contract_end_raw)
                ret = f.get(retailer_field)
                if isinstance(ret, list):
                    ret = ret[0] if ret else ""
                retailer = str(ret).strip() if ret else ""
                out.append({
                    "identifier": identifier,
                    "contract_end_date": contract_end,
                    "retailer": retailer,
                    "record_id": rid,
                })
            next_offset = data.get("offset")
            print(f"[airtable] list_all_utility_records: {utility_type} page {page} -> {len(records)} records (total so far: {len(out)}), next_offset={bool(next_offset)}", flush=True)
            if not next_offset:
                if len(records) == 100:
                    logger.warning(
                        "[list_all_utility_records] %s: got exactly 100 records with no offset; table may have more records (Airtable caps at 100/page). Check if offset is in response: %s",
                        utility_type, list(data.keys()),
                    )
                    print(f"[airtable] list_all_utility_records: WARNING got 100 records but no offset - response keys: {list(data.keys())}", flush=True)
                break
            offset = next_offset
        logger.info("[list_all_utility_records] %s: fetched %s records in %s page(s)", utility_type, len(out), page)
    except requests.RequestException as e:
        logger.warning("[list_all_utility_records] Airtable request failed: %s", e)
    print(f"[airtable] list_all_utility_records: {utility_type} -> {len(out)} records", flush=True)
    return out


def find_utility_record_by_identifier(
    utility_type_identifier: str,
    identifier: str,
) -> Optional[tuple[str, str]]:
    """
    Find an Airtable utility record by utility type (e.g. 'C&I Electricity') and identifier (NMI/MRIN/account).
    Returns (table_name, record_id) or None.
    """
    if not identifier or not AIRTABLE_API_KEY:
        return None
    identifier = str(identifier).strip()
    for cfg in UTILITY_CONFIG:
        if cfg["app_key"] != utility_type_identifier:
            continue
        table_name = cfg["table_name"]
        id_field = cfg["identifier_field"]
        # filterByFormula: {NMI}='value' (escape value)
        escaped = _escape_formula_value(identifier)
        formula = f"{{{id_field}}}='{escaped}'"
        try:
            r = requests.get(
                _url(table_name),
                headers=_headers(),
                params={"filterByFormula": formula, "maxRecords": 1},
                timeout=15,
            )
            r.raise_for_status()
            records = r.json().get("records", [])
            if records:
                return (table_name, records[0]["id"])
        except requests.RequestException as e:
            logger.debug("Airtable find_utility_record failed: %s", e)
            return None
    return None


def update_utility_record_data_requested(
    utility_type_identifier: str,
    identifier: str,
    date_str: str,
    data_recieved: Optional[Any] = None,
) -> bool:
    """
    Set "Data Requested" on the first matching utility record to date_str (YYYY-MM-DD).
    Optionally set Data Received checkbox (e.g. False when sending a new request).
    Returns True if update succeeded.
    """
    return update_utility_record(
        utility_type_identifier,
        identifier,
        data_requested=date_str,
        data_recieved=data_recieved,
    )


def update_utility_record(
    utility_type_identifier: str,
    identifier: str,
    *,
    data_requested: Optional[str] = None,
    data_recieved: Optional[Any] = None,  # Checkbox: pass True/False (or "Yes"/"No" string, we convert to bool)
    contract_end_date: Optional[str] = None,
) -> bool:
    """
    Update one or more of Data Requested, Data Received (checkbox), Contract End Date on the
    first matching utility record. data_recieved is sent as boolean to Airtable (checkbox field).
    Returns True if update succeeded.
    """
    found = find_utility_record_by_identifier(utility_type_identifier, identifier)
    if not found:
        logger.warning("[utility-record] find_utility_record_by_identifier returned None for type=%r identifier=%r", utility_type_identifier, identifier)
        return False
    table_name, record_id = found
    fields: dict = {}
    if data_requested is not None and data_requested != "":
        fields["Data Requested"] = data_requested
    if data_recieved is not None:
        # Airtable checkbox expects True/False. Field name may be "Data Received" or "Data Recieved".
        if isinstance(data_recieved, bool):
            bool_val = data_recieved
        elif isinstance(data_recieved, str):
            bool_val = data_recieved.strip().lower() in ("yes", "true", "1", "y")
        else:
            bool_val = bool(data_recieved)
        fields["Data Received"] = bool_val
    if contract_end_date is not None and contract_end_date != "":
        fields["Contract End Date"] = contract_end_date
    if not fields:
        return True
    url = _url(table_name, record_id)
    logger.info("[utility-record] PATCH %s fields=%s", url, fields)
    try:
        r = requests.patch(
            url,
            headers=_headers(),
            json={"fields": fields},
            timeout=15,
        )
        if not r.ok:
            logger.warning(
                "[utility-record] Airtable PATCH failed: status=%s url=%r response_body=%s",
                r.status_code, url, r.text[:500] if r.text else "",
            )
            # If 422 and we sent "Data Received", retry with "Data Recieved" (typo used in some bases)
            if r.status_code == 422 and "Data Received" in fields:
                alt_fields = {k: v for k, v in fields.items() if k != "Data Received"}
                alt_fields["Data Recieved"] = fields["Data Received"]
                logger.info("[utility-record] Retrying PATCH with Data Recieved (typo) fields=%s", alt_fields)
                r2 = requests.patch(url, headers=_headers(), json={"fields": alt_fields}, timeout=15)
                if r2.ok:
                    return True
                logger.warning("[utility-record] Retry PATCH failed: status=%s response=%s", r2.status_code, r2.text[:300] if r2.text else "")
            return False
        return True
    except requests.RequestException as e:
        logger.warning("Airtable update utility record failed: %s", e)
        return False


# --- Base 2 SME Gas → C&I reference: median energy $ / invoice total $ from C&I Gas Clients by postcode ---
# Comma-separated field names; first present on a record wins.
_CI_GAS_REF_ADDRESS_FIELDS = [
    s.strip()
    for s in os.environ.get(
        "AIRTABLE_CI_GAS_REF_ADDRESS_FIELDS",
        "Site Address:,Site Address,Address,Site address",
    ).split(",")
    if s.strip()
]
_CI_GAS_REF_ENERGY_FIELDS = [
    s.strip()
    for s in os.environ.get(
        "AIRTABLE_CI_GAS_REF_ENERGY_FIELDS",
        "Energy Charges in $,Energy Charge Cost,Energy charges",
    ).split(",")
    if s.strip()
]
_CI_GAS_REF_TOTAL_FIELDS = [
    s.strip()
    for s in os.environ.get(
        "AIRTABLE_CI_GAS_REF_TOTAL_FIELDS",
        "Total Invoice Cost:,Total ex GST,Subtotal,Invoice Total,Total (ex GST)",
    ).split(",")
    if s.strip()
]
_CI_GAS_REF_BUSINESS_NAME_FIELDS = [
    s.strip()
    for s in os.environ.get(
        "AIRTABLE_CI_GAS_REF_BUSINESS_NAME_FIELDS",
        "Bus Name Copy (from Link to LOA),Trading As,Client Name",
    ).split(",")
    if s.strip()
]
_CI_GAS_REF_MIN_EXACT = int(os.environ.get("AIRTABLE_CI_GAS_REF_MIN_EXACT_SAMPLES", "1"))
_CI_GAS_REF_DEFAULT_SHARE = float(os.environ.get("AIRTABLE_CI_GAS_REF_DEFAULT_ENERGY_SHARE", "0.72"))

# C&I Gas *Clients* often have no $ fields; charges live on linked *Invoices* rows.
# Comma-separated table names or tbl... IDs (try in order until a linked recId returns 200).
_CI_GAS_REF_CLIENT_INVOICE_LINK_FIELDS = [
    s.strip()
    for s in os.environ.get(
        "AIRTABLE_CI_GAS_REF_CLIENT_INVOICE_LINK_FIELDS",
        "Link to C&I Gas Invoices,Invoices for this account",
    ).split(",")
    if s.strip()
]
_CI_GAS_REF_MAX_INVOICE_FETCHES = int(os.environ.get("AIRTABLE_CI_GAS_REF_MAX_INVOICE_FETCHES", "120"))
# Tier 3: nearest by numeric postcode difference (not geographic proximity — API labels this clearly).
_CI_GAS_REF_NEAREST_MAX_POSTCODES = int(os.environ.get("AIRTABLE_CI_GAS_REF_NEAREST_MAX_POSTCODES", "3"))
_CI_GAS_REF_NEAREST_MAX_NUMERIC_GAP = int(os.environ.get("AIRTABLE_CI_GAS_REF_NEAREST_MAX_NUMERIC_GAP", "150"))
# Tier 5: max client rows to scan for global median (linked-invoice fetches still capped by budget).
_CI_GAS_REF_GLOBAL_MEDIAN_MAX_ROWS = int(os.environ.get("AIRTABLE_CI_GAS_REF_GLOBAL_MEDIAN_MAX_ROWS", "200"))

_CI_GAS_REF_CONFIDENCE_BY_STRATEGY = {
    "exact_postcode": "high",
    "prefix_3digit": "medium",
    "nearest_numeric_postcode": "medium",
    "global_dataset_median": "low",
    "default_share": "low",
}


def get_ci_gas_invoices_table_candidates() -> list[str]:
    """
    Read AIRTABLE_CI_GAS_INVOICES_TABLE at call time and reload backend .env.

    Important: os.environ.get("X", "default") returns "" if X is set but empty, which used to
    collapse to the fallback list ['C&I Gas Invoices'] only — ignoring .env after a blank export.
    """
    if os.path.exists(_env_path):
        try:
            from dotenv import load_dotenv

            load_dotenv(dotenv_path=_env_path, override=False)
        except Exception:
            pass
    raw = os.environ.get("AIRTABLE_CI_GAS_INVOICES_TABLE")
    if raw is None:
        raw = ""
    raw = str(raw).strip()
    if not raw:
        raw = "C&I Gas Invoices"
    parts = [s.strip() for s in raw.split(",") if s.strip()]
    return parts if parts else ["C&I Gas Invoices"]


def _normalize_au_postcode(raw: str) -> Optional[str]:
    """Extract a 4-digit Australian postcode string from user input or address."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw).strip())
    if len(digits) >= 4:
        return digits[-4:]
    return None


def _extract_postcode_from_address_text(text: str) -> Optional[str]:
    if not text or not isinstance(text, str):
        return None
    found = re.findall(r"\b\d{4}\b", text)
    if not found:
        return None
    return found[-1]


def _coerce_airtable_number(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        if isinstance(val, float) and (val != val):  # NaN
            return None
        return float(val)
    if isinstance(val, str):
        s = val.replace("$", "").replace(",", "").strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _first_numeric_from_fields(fields: dict, candidates: list[str]) -> Optional[float]:
    for name in candidates:
        v = fields.get(name)
        n = _coerce_airtable_number(v)
        if n is not None:
            return n
    return None


def _address_text_from_fields(fields: dict) -> str:
    for name in _CI_GAS_REF_ADDRESS_FIELDS:
        v = fields.get(name)
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)):
            return str(v)
    return ""


def _ci_gas_ref_business_display_name(fields: dict) -> Optional[str]:
    """First non-empty business label from C&I Gas client row (configurable field list)."""
    for name in _CI_GAS_REF_BUSINESS_NAME_FIELDS:
        v = fields.get(name)
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            s = str(v).strip()
            if s:
                return s
        if isinstance(v, list):
            for item in v:
                if isinstance(item, str) and item.strip():
                    return item.strip()
    return None


def _record_energy_share_ratio(fields: dict) -> Optional[float]:
    energy = _first_numeric_from_fields(fields, _CI_GAS_REF_ENERGY_FIELDS)
    total = _first_numeric_from_fields(fields, _CI_GAS_REF_TOTAL_FIELDS)
    if energy is None or total is None or total <= 0:
        return None
    if energy < 0 or energy > total * 1.15:
        return None
    return energy / total


def _linked_invoice_record_ids(client_fields: dict) -> list[str]:
    for lf in _CI_GAS_REF_CLIENT_INVOICE_LINK_FIELDS:
        v = client_fields.get(lf)
        if v is None:
            continue
        if isinstance(v, list):
            return [x for x in v if isinstance(x, str) and x.startswith("rec")]
        if isinstance(v, str) and v.startswith("rec"):
            return [v]
    return []


def _append_invoice_probe_failure(
    bucket: Optional[list[dict[str, Any]]],
    payload: dict[str, Any],
    *,
    max_items: int = 16,
) -> None:
    if bucket is None or len(bucket) >= max_items:
        return
    bucket.append(payload)


def _energy_share_ratio_from_client_or_linked_invoices(
    client_fields: dict,
    *,
    invoice_fetches_remaining: list[int],
    invoice_table_candidates: list[str],
    invoice_probe_failures: Optional[list[dict[str, Any]]] = None,
    max_probe_logged: int = 16,
    invoice_table_cache: Optional[list[str]] = None,
) -> Optional[float]:
    """Use $ fields on the client row if present; otherwise follow link(s) to C&I Gas Invoices."""
    direct = _record_energy_share_ratio(client_fields)
    if direct is not None:
        return direct
    if not invoice_table_candidates:
        return None
    link_ids = _linked_invoice_record_ids(client_fields)
    if not link_ids:
        return None
    for inv_id in reversed(link_ids):
        if invoice_fetches_remaining[0] <= 0:
            break
        invoice_fetches_remaining[0] -= 1
        inv_doc: Optional[dict] = None
        last_status: Optional[int] = None
        last_detail = ""
        last_table = ""
        if invoice_table_cache is not None and len(invoice_table_cache) >= 1:
            tbl = invoice_table_cache[0]
            inv_doc, last_status, last_detail = _fetch_record_detailed(tbl, inv_id)
            last_table = tbl
        else:
            for tbl in invoice_table_candidates:
                inv_doc, st, det = _fetch_record_detailed(tbl, inv_id)
                last_status, last_detail, last_table = st, det, tbl
                if inv_doc is not None:
                    if invoice_table_cache is not None:
                        invoice_table_cache.clear()
                        invoice_table_cache.append(tbl)
                        _ci_gas_ref_log(
                            "resolved linked-invoice Airtable table to %r (GET ok for record %s…)",
                            tbl,
                            inv_id[:14],
                        )
                    break
                # Try every candidate (403/404 can mean wrong table id vs name for this base).
        if not inv_doc:
            _append_invoice_probe_failure(
                invoice_probe_failures,
                {
                    "invoice_record_id": (inv_id or "")[:17],
                    "error": "fetch_failed",
                    "http_status": last_status,
                    "detail_preview": (last_detail[:350] + "…")
                    if len(last_detail) > 350
                    else last_detail or None,
                    "table_attempted": last_table or None,
                    "table_candidates": list(invoice_table_candidates),
                    "hint": "403/404: wrong table or PAT lacks data.records:read for this base — use tbl... from Airtable API docs; regenerate token with schema read if needed",
                },
                max_items=max_probe_logged,
            )
            continue
        inv_fields = inv_doc.get("fields") or {}
        r = _record_energy_share_ratio(inv_fields)
        if r is not None:
            return r
        eng = _first_numeric_from_fields(inv_fields, _CI_GAS_REF_ENERGY_FIELDS)
        tot = _first_numeric_from_fields(inv_fields, _CI_GAS_REF_TOTAL_FIELDS)
        _append_invoice_probe_failure(
            invoice_probe_failures,
            {
                "invoice_record_id": (inv_id or "")[:17],
                "field_key_count": len(inv_fields),
                "field_keys_sample": sorted(inv_fields.keys())[:50],
                "energy_numeric": eng,
                "total_numeric": tot,
                "energy_field_candidates": _CI_GAS_REF_ENERGY_FIELDS,
                "total_field_candidates": _CI_GAS_REF_TOTAL_FIELDS,
            },
            max_items=max_probe_logged,
        )
    return None


def _paginate_full_table(table_name: str) -> list[dict]:
    """All records from a table with id + fields (paginated)."""
    if not AIRTABLE_API_KEY:
        return []
    out: list[dict] = []
    offset: Optional[str] = None
    page = 0
    max_pages = 80
    while page < max_pages:
        page += 1
        params: dict[str, Any] = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        try:
            r = requests.get(_url(table_name), headers=_headers(), params=params, timeout=45)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logger.warning("[ci-gas-ref] paginate %s failed: %s", table_name, e)
            break
        for rec in data.get("records", []):
            out.append({"id": rec.get("id", ""), "fields": rec.get("fields") or {}})
        offset = data.get("offset")
        if not offset:
            break
    return out


def _ci_gas_ref_log(msg: str, *args: Any) -> None:
    """Terminal + logger so uvicorn stdout shows Base 2 Airtable diagnostics."""
    line = msg % args if args else msg
    logger.info("[ci-gas-ref] %s", line)
    print(f"[ci-gas-ref] {line}", flush=True)


def fetch_ci_gas_energy_share_reference(
    postcode: str,
    *,
    relax_postcode: bool = False,
    debug: bool = False,
) -> dict[str, Any]:
    """
    Scan C&I Gas Clients for rows whose address contains a postcode matching `postcode`.
    For each matching row, compute energy_charges / total_ex_gst using env-configurable field names.
    Returns median share in (0,1], or a default with used_fallback=True when Airtable is empty or misconfigured.
    """
    diagnostics: dict[str, Any] = {}

    norm = _normalize_au_postcode(postcode)
    if not norm:
        return {
            "postcode_normalized": None,
            "median_energy_share": None,
            "sample_count": 0,
            "used_fallback": True,
            "relax_used": False,
            "message": "Invalid postcode",
            "match_strategy": "invalid_postcode",
            "matched_postcodes": [],
            "matched_postcode_reference": [],
            "confidence": "low",
            "fallback_reason": "invalid_postcode",
        }

    if not AIRTABLE_API_KEY:
        _ci_gas_ref_log("skip: AIRTABLE_API_KEY not set")
        return {
            "postcode_normalized": norm,
            "median_energy_share": _CI_GAS_REF_DEFAULT_SHARE,
            "sample_count": 0,
            "used_fallback": True,
            "relax_used": False,
            "message": "Airtable not configured",
            "match_strategy": "default_share",
            "matched_postcodes": [],
            "matched_postcode_reference": [],
            "confidence": "low",
            "fallback_reason": "airtable_not_configured",
        }

    cfg = next((c for c in UTILITY_CONFIG if c["app_key"] == "C&I Gas"), None)
    if not cfg:
        _ci_gas_ref_log("skip: C&I Gas not in UTILITY_CONFIG")
        return {
            "postcode_normalized": norm,
            "median_energy_share": _CI_GAS_REF_DEFAULT_SHARE,
            "sample_count": 0,
            "used_fallback": True,
            "relax_used": False,
            "message": "C&I Gas table not configured",
            "match_strategy": "default_share",
            "matched_postcodes": [],
            "matched_postcode_reference": [],
            "confidence": "low",
            "fallback_reason": "ci_gas_table_not_configured",
        }

    table_name = cfg["table_name"]
    invoice_table_candidates = get_ci_gas_invoices_table_candidates()
    _ci_gas_ref_log(
        "start postcode_raw=%r postcode_norm=%r relax=%s table=%r invoice_table_candidates=%s address_fields=%s energy_fields=%s total_fields=%s business_name_fields=%s min_exact=%s default_share=%s",
        postcode,
        norm,
        relax_postcode,
        table_name,
        invoice_table_candidates,
        _CI_GAS_REF_ADDRESS_FIELDS,
        _CI_GAS_REF_ENERGY_FIELDS,
        _CI_GAS_REF_TOTAL_FIELDS,
        _CI_GAS_REF_BUSINESS_NAME_FIELDS,
        _CI_GAS_REF_MIN_EXACT,
        _CI_GAS_REF_DEFAULT_SHARE,
    )

    rows = _paginate_full_table(table_name)
    total_rows = len(rows)
    if not rows:
        _ci_gas_ref_log("no rows returned from Airtable table=%r (check base id / table name / API key)", table_name)

    # Field names present in Airtable (union of first 20 records) — align env vars to these.
    key_union: set[str] = set()
    for rec in rows[:20]:
        key_union.update((rec.get("fields") or {}).keys())
    keys_sorted = sorted(key_union)
    _ci_gas_ref_log(
        "fetched row_count=%s distinct_field_keys_from_first_20_rows=%s keys_sample=%s",
        total_rows,
        len(keys_sorted),
        keys_sorted[:60],
    )
    if len(keys_sorted) > 60:
        _ci_gas_ref_log("... and %s more field keys (truncated in log)", len(keys_sorted) - 60)

    address_hits = [k for k in _CI_GAS_REF_ADDRESS_FIELDS if k in key_union]
    energy_hits = [k for k in _CI_GAS_REF_ENERGY_FIELDS if k in key_union]
    total_hits = [k for k in _CI_GAS_REF_TOTAL_FIELDS if k in key_union]
    business_hits = [k for k in _CI_GAS_REF_BUSINESS_NAME_FIELDS if k in key_union]
    _ci_gas_ref_log(
        "config_vs_airtable: address_candidates_found=%s energy_candidates_found=%s total_candidates_found=%s business_name_candidates_found=%s",
        address_hits or "(none — check AIRTABLE_CI_GAS_REF_ADDRESS_FIELDS)",
        energy_hits or "(none — check AIRTABLE_CI_GAS_REF_ENERGY_FIELDS)",
        total_hits or "(none — check AIRTABLE_CI_GAS_REF_TOTAL_FIELDS)",
        business_hits or "(none — check AIRTABLE_CI_GAS_REF_BUSINESS_NAME_FIELDS)",
    )
    link_hits = [k for k in _CI_GAS_REF_CLIENT_INVOICE_LINK_FIELDS if k in key_union]
    _ci_gas_ref_log(
        "linked_invoices: table_candidates=%s link_field_candidates_found=%s max_record_fetches=%s",
        invoice_table_candidates,
        link_hits or "(none — set AIRTABLE_CI_GAS_REF_CLIENT_INVOICE_LINK_FIELDS / AIRTABLE_CI_GAS_INVOICES_TABLE)",
        _CI_GAS_REF_MAX_INVOICE_FETCHES,
    )

    invoice_budget_start = _CI_GAS_REF_MAX_INVOICE_FETCHES
    invoice_fetches_remaining = [invoice_budget_start]
    invoice_probe_failures: list[dict[str, Any]] = []
    resolved_invoice_table_cache: list[str] = []

    def row_pc(f: dict) -> Optional[str]:
        addr = _address_text_from_fields(f)
        return _extract_postcode_from_address_text(addr)

    entries: list[tuple[float, str, Optional[str]]] = []
    rows_with_address = 0
    rows_with_pc = 0
    exact_pc_rows = 0
    exact_pc_with_ratio = 0
    no_ratio_samples: list[dict[str, Any]] = []
    postcode_histogram: dict[str, int] = {}

    for rec in rows:
        f = rec.get("fields") or {}
        addr = _address_text_from_fields(f)
        if addr:
            rows_with_address += 1
        pc = row_pc(f)
        if pc:
            rows_with_pc += 1
            postcode_histogram[pc] = postcode_histogram.get(pc, 0) + 1
        if pc != norm:
            continue
        exact_pc_rows += 1
        ratio = _energy_share_ratio_from_client_or_linked_invoices(
            f,
            invoice_fetches_remaining=invoice_fetches_remaining,
            invoice_table_candidates=invoice_table_candidates,
            invoice_probe_failures=invoice_probe_failures,
            invoice_table_cache=resolved_invoice_table_cache,
        )
        if ratio is not None:
            entries.append((ratio, norm, _ci_gas_ref_business_display_name(f)))
            exact_pc_with_ratio += 1
        elif len(no_ratio_samples) < 8:
            eng = _first_numeric_from_fields(f, _CI_GAS_REF_ENERGY_FIELDS)
            tot = _first_numeric_from_fields(f, _CI_GAS_REF_TOTAL_FIELDS)
            fk = sorted(f.keys())
            link_n = len(_linked_invoice_record_ids(f))
            no_ratio_samples.append(
                {
                    "record_id": (rec.get("id") or "")[:14],
                    "address_field_used": next(
                        (n for n in _CI_GAS_REF_ADDRESS_FIELDS if f.get(n)), None
                    ),
                    "address_preview": (addr[:70] + "…") if len(addr) > 70 else addr,
                    "extracted_postcode": pc,
                    "energy_numeric": eng,
                    "total_numeric": tot,
                    "linked_invoice_record_count": link_n,
                    "field_key_count": len(fk),
                    "field_keys_sample": fk[:35],
                }
            )

    prefix = norm[:3] if len(norm) >= 3 else ""
    prefix_pc_rows = sum(
        1 for rec in rows if (p := row_pc(rec.get("fields") or {})) and p.startswith(prefix)
    )

    if exact_pc_rows == 0 and rows_with_pc > 0:
        top_pcs = sorted(postcode_histogram.items(), key=lambda x: -x[1])[:12]
        _ci_gas_ref_log("postcode_histogram_top=%s (no row matched target %s)", top_pcs, norm)

    if exact_pc_rows > 0 and exact_pc_with_ratio == 0 and no_ratio_samples:
        _ci_gas_ref_log(
            "exact_postcode_rows_found_but_no_ratio: showing up to %s samples (fix field names or data types)",
            len(no_ratio_samples),
        )
        for i, s in enumerate(no_ratio_samples):
            _ci_gas_ref_log("  sample[%s] %s", i, s)

    relax_used = False
    relax_added = 0
    if (
        relax_postcode
        and len(entries) < _CI_GAS_REF_MIN_EXACT
        and len(norm) >= 3
    ):
        relax_used = True
        seen_ids = {rec["id"] for rec in rows if row_pc(rec.get("fields") or {}) == norm}
        for rec in rows:
            if rec.get("id") in seen_ids:
                continue
            f = rec.get("fields") or {}
            pc = row_pc(f)
            if not pc or not pc.startswith(prefix):
                continue
            ratio = _energy_share_ratio_from_client_or_linked_invoices(
                f,
                invoice_fetches_remaining=invoice_fetches_remaining,
                invoice_table_candidates=invoice_table_candidates,
                invoice_probe_failures=invoice_probe_failures,
                invoice_table_cache=resolved_invoice_table_cache,
            )
            if ratio is not None:
                entries.append((ratio, pc, _ci_gas_ref_business_display_name(f)))
                relax_added += 1
        _ci_gas_ref_log(
            "relax_postcode: prefix=%s ratios_added_beyond_exact=%s total_ratios=%s",
            prefix,
            relax_added,
            len(entries),
        )

    tier3_filled = False
    nearest_numeric_candidates: list[str] = []
    if len(entries) == 0 and len(norm) == 4 and norm.isdigit():
        target_i = int(norm)
        sorted_pcs = sorted(
            (p for p in postcode_histogram if len(p) == 4 and p.isdigit()),
            key=lambda p: abs(int(p) - target_i),
        )
        nearest_numeric_candidates = [
            p
            for p in sorted_pcs
            if abs(int(p) - target_i) <= _CI_GAS_REF_NEAREST_MAX_NUMERIC_GAP
        ][: _CI_GAS_REF_NEAREST_MAX_POSTCODES]
        _ci_gas_ref_log(
            "fallback_nearest_numeric_postcodes: target=%s max_gap=%s max_pc_groups=%s candidates=%s",
            norm,
            _CI_GAS_REF_NEAREST_MAX_NUMERIC_GAP,
            _CI_GAS_REF_NEAREST_MAX_POSTCODES,
            nearest_numeric_candidates,
        )
        for pc_pick in nearest_numeric_candidates:
            for rec in rows:
                if row_pc(rec.get("fields") or {}) != pc_pick:
                    continue
                f = rec.get("fields") or {}
                ratio = _energy_share_ratio_from_client_or_linked_invoices(
                    f,
                    invoice_fetches_remaining=invoice_fetches_remaining,
                    invoice_table_candidates=invoice_table_candidates,
                    invoice_probe_failures=invoice_probe_failures,
                    invoice_table_cache=resolved_invoice_table_cache,
                )
                if ratio is not None:
                    entries.append((ratio, pc_pick, _ci_gas_ref_business_display_name(f)))
        if entries:
            tier3_filled = True
        _ci_gas_ref_log(
            "fallback_nearest_numeric_postcodes: valid_ratio_count=%s matched_postcodes=%s",
            len(entries),
            sorted({e[1] for e in entries}),
        )

    tier5_filled = False
    if len(entries) == 0:
        _ci_gas_ref_log(
            "fallback_global_dataset: scanning up to %s rows (invoice_fetches_remaining=%s)",
            _CI_GAS_REF_GLOBAL_MEDIAN_MAX_ROWS,
            invoice_fetches_remaining[0],
        )
        scanned = 0
        for rec in rows:
            if scanned >= _CI_GAS_REF_GLOBAL_MEDIAN_MAX_ROWS:
                break
            scanned += 1
            f = rec.get("fields") or {}
            pc = row_pc(f) or ""
            ratio = _energy_share_ratio_from_client_or_linked_invoices(
                f,
                invoice_fetches_remaining=invoice_fetches_remaining,
                invoice_table_candidates=invoice_table_candidates,
                invoice_probe_failures=invoice_probe_failures,
                invoice_table_cache=resolved_invoice_table_cache,
            )
            if ratio is not None:
                entries.append((ratio, pc if pc else "unknown", _ci_gas_ref_business_display_name(f)))
        if entries:
            tier5_filled = True
        _ci_gas_ref_log(
            "fallback_global_dataset: valid_ratio_count=%s",
            len(entries),
        )

    invoice_fetches_used = invoice_budget_start - invoice_fetches_remaining[0]
    if invoice_probe_failures:
        _ci_gas_ref_log(
            "invoice_linked_probes_no_ratio: count=%s (energy/total not found on invoice rows — align AIRTABLE_CI_GAS_REF_ENERGY_FIELDS / TOTAL_FIELDS with keys below)",
            len(invoice_probe_failures),
        )
        for i, probe in enumerate(invoice_probe_failures[:10]):
            _ci_gas_ref_log("  probe[%s] %s", i, probe)
        if len(invoice_probe_failures) > 10:
            _ci_gas_ref_log("  ... and %s more probes (truncated)", len(invoice_probe_failures) - 10)
        if not resolved_invoice_table_cache and invoice_probe_failures:
            st0 = invoice_probe_failures[0].get("http_status")
            _ci_gas_ref_log(
                "no_invoice_GET_succeeded: first_probe_http_status=%r — 404=wrong AIRTABLE_CI_GAS_INVOICES_TABLE (use exact name or tbl... from Airtable API docs); 403=pat cannot read table; see detail_preview on probe lines",
                st0,
            )

    _ci_gas_ref_log(
        "match_stats: rows_with_address_text=%s rows_with_any_postcode=%s exact_postcode_%s=%s exact_with_valid_energy_total_ratio=%s rows_same_prefix_%s=%s invoice_record_fetches_used=%s resolved_invoice_table=%s",
        rows_with_address,
        rows_with_pc,
        norm,
        exact_pc_rows,
        exact_pc_with_ratio,
        prefix,
        prefix_pc_rows,
        invoice_fetches_used,
        resolved_invoice_table_cache[0] if resolved_invoice_table_cache else None,
    )

    if entries:
        ratios_only = [e[0] for e in entries]
        med = float(statistics.median(ratios_only))
        med = max(0.01, min(1.0, med))
        matched_postcodes = sorted({pc for _, pc, _ in entries if pc and pc != "unknown"})
        pc_to_names: dict[str, list[str]] = {}
        for _, pc, biz in entries:
            if not pc or pc == "unknown":
                continue
            if not biz:
                continue
            name = str(biz).strip()
            if not name:
                continue
            lst = pc_to_names.setdefault(pc, [])
            if name not in lst:
                lst.append(name)
        matched_postcode_reference = [
            {"postcode": pc, "business_names": pc_to_names.get(pc, [])} for pc in matched_postcodes
        ]
        if tier5_filled:
            match_strategy = "global_dataset_median"
        elif tier3_filled:
            match_strategy = "nearest_numeric_postcode"
        elif relax_used and any(pc != norm for _, pc, _ in entries):
            match_strategy = "prefix_3digit"
        else:
            match_strategy = "exact_postcode"
        used_fb = match_strategy != "exact_postcode"
        confidence = _CI_GAS_REF_CONFIDENCE_BY_STRATEGY.get(match_strategy, "low")
        msg: Optional[str] = None
        if match_strategy == "nearest_numeric_postcode":
            pcs_disp_parts: list[str] = []
            for pc in matched_postcodes:
                names = pc_to_names.get(pc, [])
                if names:
                    pcs_disp_parts.append(f"{pc} ({', '.join(names)})")
                else:
                    pcs_disp_parts.append(pc)
            pcs_disp = ", ".join(pcs_disp_parts) if pcs_disp_parts else "(see data)"
            msg = (
                f"No exact or same-prefix C&I gas match for {norm}; energy share uses "
                f"nearest_numeric_postcode_fallback (numeric distance, not map geography). "
                f"Postcodes used: {pcs_disp}."
            )
        elif match_strategy == "global_dataset_median":
            msg = (
                f"No local postcode match for {norm}; energy share is the median from "
                f"scanned C&I client records (up to {_CI_GAS_REF_GLOBAL_MEDIAN_MAX_ROWS} rows)."
            )
        _ci_gas_ref_log(
            "success match_strategy=%s median_energy_share=%.4f sample_count=%s relax_used=%s invoice_record_fetches_used=%s",
            match_strategy,
            med,
            len(entries),
            relax_used,
            invoice_fetches_used,
        )
        out = {
            "postcode_normalized": norm,
            "median_energy_share": med,
            "sample_count": len(entries),
            "used_fallback": used_fb,
            "relax_used": relax_used,
            "message": msg,
            "match_strategy": match_strategy,
            "matched_postcodes": matched_postcodes,
            "matched_postcode_reference": matched_postcode_reference,
            "confidence": confidence,
            "fallback_reason": None,
        }
        if debug:
            diagnostics = {
                "table_name": table_name,
                "invoices_table_candidates": list(invoice_table_candidates),
                "resolved_invoices_table": resolved_invoice_table_cache[0]
                if resolved_invoice_table_cache
                else None,
                "invoice_link_field_hits": link_hits,
                "invoice_record_fetches_used": invoice_fetches_used,
                "total_rows": total_rows,
                "rows_with_address_text": rows_with_address,
                "rows_with_extracted_postcode": rows_with_pc,
                "exact_postcode_rows": exact_pc_rows,
                "exact_postcode_with_ratio": exact_pc_with_ratio,
                "prefix": prefix,
                "rows_matching_prefix": prefix_pc_rows,
                "relax_added_ratios": relax_added,
                "tier3_nearest_numeric_candidates": nearest_numeric_candidates,
                "tier3_filled": tier3_filled,
                "tier5_filled": tier5_filled,
                "match_strategy": match_strategy,
                "matched_postcodes": matched_postcodes,
                "airtable_field_keys_sample": keys_sorted[:80],
                "address_config_hits": address_hits,
                "energy_config_hits": energy_hits,
                "total_config_hits": total_hits,
                "business_name_config_hits": business_hits,
                "invoice_linked_probe_failures_sample": invoice_probe_failures[:8],
            }
            out["diagnostics"] = diagnostics
        return out

    _ci_gas_ref_log(
        "FALLBACK default_share=%s reason=no_ratios (exact_pc_rows=%s exact_with_ratio=%s relax_added=%s invoice_record_fetches_used=%s)",
        _CI_GAS_REF_DEFAULT_SHARE,
        exact_pc_rows,
        exact_pc_with_ratio,
        relax_added,
        invoice_fetches_used,
    )
    out = {
        "postcode_normalized": norm,
        "median_energy_share": _CI_GAS_REF_DEFAULT_SHARE,
        "sample_count": 0,
        "used_fallback": True,
        "relax_used": relax_used,
        "message": "No matching C&I gas rows with energy and total fields; using default share",
        "match_strategy": "default_share",
        "matched_postcodes": [],
        "matched_postcode_reference": [],
        "confidence": "low",
        "fallback_reason": "no_ratios_after_exact_prefix_nearest_global",
    }
    if debug:
        diagnostics = {
            "table_name": table_name,
            "invoices_table_candidates": list(invoice_table_candidates),
            "resolved_invoices_table": resolved_invoice_table_cache[0]
            if resolved_invoice_table_cache
            else None,
            "invoice_link_field_hits": link_hits,
            "invoice_record_fetches_used": invoice_fetches_used,
            "total_rows": total_rows,
            "rows_with_address_text": rows_with_address,
            "rows_with_extracted_postcode": rows_with_pc,
            "exact_postcode_rows": exact_pc_rows,
            "exact_postcode_with_ratio": exact_pc_with_ratio,
            "prefix": prefix,
            "rows_matching_prefix": prefix_pc_rows,
            "relax_added_ratios": relax_added,
            "tier3_nearest_numeric_candidates": nearest_numeric_candidates,
            "tier3_filled": tier3_filled,
            "tier5_filled": tier5_filled,
            "airtable_field_keys_sample": keys_sorted[:80],
            "address_config_hits": address_hits,
            "energy_config_hits": energy_hits,
            "total_config_hits": total_hits,
            "business_name_config_hits": business_hits,
            "no_ratio_samples": no_ratio_samples,
            "postcode_histogram_top": sorted(postcode_histogram.items(), key=lambda x: -x[1])[:15],
            "invoice_linked_probe_failures": invoice_probe_failures,
        }
        out["diagnostics"] = diagnostics
    return out


# --- Base 2 SME Gas: aggregate invoice history from Airtable by MRIN (annual GJ, 1000 GJ threshold) ---
def _reload_backend_dotenv_for_sme_gas() -> None:
    """Re-read backend/.env from disk so vars are picked up after save (editor buffer != on-disk file)."""
    from dotenv import load_dotenv

    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path, override=True)


def _sme_gas_hist_table_name() -> str:
    raw = os.environ.get("AIRTABLE_SME_GAS_USAGE_TABLE", "").strip()
    if raw:
        return raw
    cfg = next((c for c in UTILITY_CONFIG if c["app_key"] == "SME Gas"), None)
    return cfg["table_name"] if cfg else "SME Gas Accounts"


def _sme_gas_hist_threshold_gj() -> float:
    return float(os.environ.get("AIRTABLE_SME_GAS_CI_THRESHOLD_GJ", "1000"))


def _sme_gas_hist_near_screen_low_gj() -> float:
    """Lower bound of the 'near 1000 GJ' review band on bill-day annual (inclusive)."""
    return float(os.environ.get("AIRTABLE_SME_GAS_NEAR_SCREEN_GJ", "850"))


def _sme_gas_hist_days_fields() -> list[str]:
    default = (
        "Invoice Review Number of Days,Invoice Period Days,Bill Days,Days in Period,Number of Days,Days"
    )
    return [
        s.strip()
        for s in os.environ.get("AIRTABLE_SME_GAS_HIST_DAYS_FIELDS", default).split(",")
        if s.strip()
    ]


def _sme_gas_hist_period_fields() -> list[str]:
    default = "Invoice Review Period,Billing Period,Period,Invoice Period,Bill Period"
    return [
        s.strip()
        for s in os.environ.get("AIRTABLE_SME_GAS_HIST_PERIOD_FIELDS", default).split(",")
        if s.strip()
    ]


def _sme_gas_hist_usage_fields() -> list[str]:
    default = (
        "Total Consumption MJ,Total MJ,Total Usage MJ,Consumption (MJ),Consumption MJ,"
        "General Usage MJ,Energy Quantity (MJ),Invoice Consumption MJ,Total Consumption (MJ),"
        "Total usage (MJ),Annual consumption MJ"
    )
    return [
        s.strip()
        for s in os.environ.get("AIRTABLE_SME_GAS_HIST_USAGE_FIELDS", default).split(",")
        if s.strip()
    ]


def _sme_gas_hist_log(msg: str, *args: Any) -> None:
    """Terminal + logger so uvicorn PowerShell shows SME gas Airtable history diagnostics."""
    line = msg % args if args else msg
    logger.info("[sme-gas-hist] %s", line)
    print(f"[sme-gas-hist] {line}", flush=True)


def _paginate_with_filter_formula(table_name: str, formula: str) -> list[dict[str, Any]]:
    """Return raw Airtable records {id, fields} matching formula (paginated)."""
    out: list[dict[str, Any]] = []
    if not AIRTABLE_API_KEY or not formula:
        return out
    offset: Optional[str] = None
    page = 0
    max_pages = 120
    while page < max_pages:
        page += 1
        params: dict[str, Any] = {"pageSize": 100, "filterByFormula": formula}
        if offset:
            params["offset"] = offset
        try:
            r = requests.get(_url(table_name), headers=_headers(), params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logger.warning("[sme-gas-hist] paginate %s failed: %s", table_name, e)
            break
        for rec in data.get("records", []):
            out.append({"id": rec.get("id", ""), "fields": rec.get("fields") or {}})
        offset = data.get("offset")
        if not offset:
            break
    return out


def _first_non_empty_string_from_fields(fields: dict, candidates: list[str]) -> str:
    for name in candidates:
        v = fields.get(name)
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _parse_sme_gas_period_to_days(period_text: str) -> Optional[int]:
    """
    Parse SME bill period strings into inclusive day count.
    Supports: dd/mm/yyyy-dd/mm/yyyy, '01 Nov 2023 to 03 Jan 2024'.
    """

    def _parse_one_date(chunk: str) -> Optional[datetime]:
        chunk = chunk.strip()
        if not chunk:
            return None
        for fmt in (
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d %b %Y",
            "%d %B %Y",
            "%d %b %y",
            "%d %B %y",
        ):
            try:
                return datetime.strptime(chunk, fmt)
            except ValueError:
                continue
        return None

    if not period_text or not str(period_text).strip():
        return None
    s = str(period_text).strip()
    for sep in ("–", "—"):
        s = s.replace(sep, "-")

    parts: Optional[list[str]] = None
    if re.search(r"\s+to\s+", s, flags=re.I):
        parts = re.split(r"\s+to\s+", s, maxsplit=1, flags=re.I)
    elif "-" in s:
        dash_parts = s.split("-", 1)
        if len(dash_parts) == 2:
            left, right = dash_parts[0].strip(), dash_parts[1].strip()
            if re.search(r"\d", left) and re.search(r"\d", right):
                parts = [left, right]

    if parts and len(parts) == 2:
        d0 = _parse_one_date(parts[0])
        d1 = _parse_one_date(parts[1])
        if d0 and d1:
            return max(1, (d1 - d0).days + 1)
    d = _parse_one_date(s)
    if d:
        return 1
    return None


def _parse_sme_gas_period_date_bounds(period_text: str) -> Optional[tuple[datetime, datetime]]:
    """Return (start_date, end_date) inclusive when period text is a range; else None."""

    def _parse_one_date(chunk: str) -> Optional[datetime]:
        chunk = chunk.strip()
        if not chunk:
            return None
        for fmt in (
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d %b %Y",
            "%d %B %Y",
            "%d %b %y",
            "%d %B %y",
        ):
            try:
                return datetime.strptime(chunk, fmt)
            except ValueError:
                continue
        return None

    if not period_text or not str(period_text).strip():
        return None
    s = str(period_text).strip()
    for sep in ("–", "—"):
        s = s.replace(sep, "-")

    parts: Optional[list[str]] = None
    if re.search(r"\s+to\s+", s, flags=re.I):
        parts = re.split(r"\s+to\s+", s, maxsplit=1, flags=re.I)
    elif "-" in s:
        dash_parts = s.split("-", 1)
        if len(dash_parts) == 2:
            left, right = dash_parts[0].strip(), dash_parts[1].strip()
            if re.search(r"\d", left) and re.search(r"\d", right):
                parts = [left, right]

    if parts and len(parts) == 2:
        d0 = _parse_one_date(parts[0])
        d1 = _parse_one_date(parts[1])
        if d0 and d1:
            if d1 < d0:
                d0, d1 = d1, d0
            return (d0, d1)
    return None


def _sme_gas_hist_preview_field_value(val: Any) -> str:
    """Short, log-safe description of an Airtable field value (type + sample)."""
    if val is None:
        return "None"
    if isinstance(val, bool):
        return f"bool({val})"
    if isinstance(val, (int, float)):
        return f"number({val})"
    if isinstance(val, str):
        s = val.strip()
        if len(s) > 80:
            return f"str(len={len(s)}, head={s[:40]!r}…)"
        return f"str({s!r})"
    if isinstance(val, list):
        if len(val) == 0:
            return "list(empty)"
        first = val[0]
        return f"list(n={len(val)}, first={_sme_gas_hist_preview_field_value(first)})"
    if isinstance(val, dict):
        return f"dict(keys={list(val.keys())[:8]!r}…)" if len(val) > 8 else f"dict(keys={list(val.keys())!r})"
    return f"{type(val).__name__}(…)"


def _row_usage_to_gj(raw: float, days: int, mode: str) -> tuple[float, str]:
    if raw is None or raw <= 0 or raw != raw:
        return 0.0, "none"
    if mode == "gj":
        return float(raw), "gj"
    if mode == "mj":
        return float(raw) / 1000.0, "mj"
    # auto
    if days > 0:
        daily_if_mj = (float(raw) / 1000.0) / days
        daily_if_gj = float(raw) / days
        if daily_if_mj > 120:
            return float(raw), "gj_auto_high_daily_if_mj"
        if daily_if_gj <= 0.05 and raw >= 500:
            return float(raw) / 1000.0, "mj_auto_low_daily_if_gj"
        if raw <= 450 and days >= 18 and daily_if_gj <= 35:
            return float(raw), "gj_auto_small_total"
    return float(raw) / 1000.0, "mj_auto_default"


def _sme_gas_mrin_match_formula(id_field: str, mrin_norm: str) -> str:
    esc = _escape_formula_value(mrin_norm)
    parts = [f"{{{id_field}}}='{esc}'"]
    if mrin_norm.isdigit():
        parts.append(f"{{{id_field}}}={mrin_norm}")
    return "OR(" + ",".join(parts) + ")"


def fetch_sme_gas_airtable_annual_usage(
    mrin: str,
    *,
    usage_unit: str = "auto",
    debug: bool = False,
) -> dict[str, Any]:
    """
    Load all SME Gas invoice rows from Airtable for MRIN; sum bill-period usage, annualise by total days.
    usage_unit: mj | gj | auto (auto applies heuristics; SME data is often MJ).
    """
    mrin_raw = (mrin or "").strip()
    mrin_norm = _normalize_identifier_raw(mrin_raw)
    threshold_gj = _sme_gas_hist_threshold_gj()
    near_low_gj = _sme_gas_hist_near_screen_low_gj()
    if not mrin_norm:
        return {
            "error": "mrin_required",
            "mrin_normalized": None,
            "bill_count": 0,
            "total_days": 0,
            "annual_usage_gj": None,
            "meets_ci_threshold_1000gj": None,
            "meets_1000gj_screen": None,
            "near_1000gj_screen": None,
            "threshold_gj": threshold_gj,
            "confidence": "low",
        }

    mode = (usage_unit or "auto").strip().lower()
    if mode not in ("auto", "mj", "gj"):
        mode = "auto"

    if not AIRTABLE_API_KEY:
        return {
            "error": "airtable_not_configured",
            "mrin_normalized": mrin_norm,
            "bill_count": 0,
            "total_days": 0,
            "annual_usage_gj": None,
            "meets_ci_threshold_1000gj": None,
            "meets_1000gj_screen": None,
            "near_1000gj_screen": None,
            "threshold_gj": threshold_gj,
            "confidence": "low",
        }

    _reload_backend_dotenv_for_sme_gas()

    days_fields = _sme_gas_hist_days_fields()
    usage_fields = _sme_gas_hist_usage_fields()
    period_fields = _sme_gas_hist_period_fields()
    table = _sme_gas_hist_table_name()
    cfg = next((c for c in UTILITY_CONFIG if c["app_key"] == "SME Gas"), None)
    id_field = cfg["identifier_field"] if cfg else "MRIN"
    formula = _sme_gas_mrin_match_formula(id_field, mrin_norm)

    usage_table_env_raw = os.environ.get("AIRTABLE_SME_GAS_USAGE_TABLE", "")
    _sme_gas_hist_log(
        "env AIRTABLE_SME_GAS_USAGE_TABLE=%r (empty uses SME Gas Accounts / UTILITY_CONFIG)",
        usage_table_env_raw,
    )
    _sme_gas_hist_log(
        "start mrin_raw=%r mrin_norm=%r table=%r id_field=%r usage_unit=%r",
        mrin_raw,
        mrin_norm,
        table,
        id_field,
        mode,
    )
    _sme_gas_hist_log(
        "resolved field lists: days_n=%s usage_n=%s period_n=%s | days[0]=%r usage[0]=%r period[0]=%r",
        len(days_fields),
        len(usage_fields),
        len(period_fields),
        days_fields[0] if days_fields else None,
        usage_fields[0] if usage_fields else None,
        period_fields[0] if period_fields else None,
    )

    rows = _paginate_with_filter_formula(table, formula)
    _sme_gas_hist_log("airtable filter returned records=%s formula=%s", len(rows), formula)

    matched = []
    dropped_normalize = 0
    for rec in rows:
        f = rec.get("fields") or {}
        row_id = _normalize_identifier_raw(f.get(id_field))
        if row_id != mrin_norm:
            dropped_normalize += 1
            continue
        matched.append(rec)

    if dropped_normalize:
        _sme_gas_hist_log(
            "after Python MRIN filter: matched=%s dropped_wrong_mrin=%s (id_field=%r)",
            len(matched),
            dropped_normalize,
            id_field,
        )
    else:
        _sme_gas_hist_log("after Python MRIN filter: matched=%s", len(matched))

    if matched:
        sample_fields = matched[0].get("fields") or {}
        keys_sorted = sorted(sample_fields.keys())
        _sme_gas_hist_log(
            "first_matched_record keys_sample (up to 40)=%s",
            keys_sorted[:40],
        )
        overlap_days = [n for n in days_fields if n in sample_fields]
        overlap_usage = [n for n in usage_fields if n in sample_fields]
        overlap_period = [n for n in period_fields if n in sample_fields]
        _sme_gas_hist_log(
            "config_vs_airtable on first row: days_hits=%s usage_hits=%s period_hits=%s",
            overlap_days or "(none — field name mismatch or empty table row)",
            overlap_usage or "(none — field name mismatch or empty table row)",
            overlap_period or "(none)",
        )
        for fn in usage_fields[:5]:
            if fn in sample_fields:
                _sme_gas_hist_log(
                    "sample usage candidate %r -> %s",
                    fn,
                    _sme_gas_hist_preview_field_value(sample_fields.get(fn)),
                )
        for fn in days_fields[:5]:
            if fn in sample_fields:
                _sme_gas_hist_log(
                    "sample days candidate %r -> %s",
                    fn,
                    _sme_gas_hist_preview_field_value(sample_fields.get(fn)),
                )

    bills: list[dict[str, Any]] = []
    total_gj = 0.0
    total_days_acc = 0
    skipped_rows = 0

    for rec in matched:
        f = rec.get("fields") or {}
        rid = (rec.get("id") or "")[:14]
        days_val = _first_numeric_from_fields(f, days_fields)
        days_i = int(days_val) if days_val is not None and days_val > 0 else 0
        period_s = ""
        if days_i <= 0:
            period_s = _first_non_empty_string_from_fields(f, period_fields)
            parsed_days = _parse_sme_gas_period_to_days(period_s)
            if parsed_days:
                days_i = parsed_days
        raw_usage = _first_numeric_from_fields(f, usage_fields)
        gj, unit_note = _row_usage_to_gj(float(raw_usage or 0), days_i, mode)
        if gj <= 0 or days_i <= 0:
            skipped_rows += 1
            # Log first few skips in detail so PowerShell shows the real blocker
            if skipped_rows <= 12:
                usage_raw_by_name = {name: f.get(name) for name in usage_fields if name in f}
                days_raw_by_name = {name: f.get(name) for name in days_fields if name in f}
                _sme_gas_hist_log(
                    "skip record=%s days_i=%s days_val_coerced=%s period=%r raw_usage_coerced=%s -> gj=%.6f note=%s | raw_usage_fields=%s | raw_days_fields=%s",
                    rid,
                    days_i,
                    days_val,
                    (period_s[:60] + "…") if len(period_s) > 60 else period_s,
                    raw_usage,
                    gj,
                    unit_note,
                    {k: _sme_gas_hist_preview_field_value(v) for k, v in usage_raw_by_name.items()},
                    {k: _sme_gas_hist_preview_field_value(v) for k, v in days_raw_by_name.items()},
                )
            continue
        period_lbl = _first_non_empty_string_from_fields(f, period_fields)
        bounds = _parse_sme_gas_period_date_bounds(period_lbl) if period_lbl else None
        bill_row: dict[str, Any] = {
            "record_id": (rec.get("id") or "")[:14],
            "days": days_i,
            "usage_gj": round(gj, 6),
            "usage_unit_note": unit_note,
            "period": period_lbl or None,
        }
        if bounds:
            bill_row["period_start"] = bounds[0].date().isoformat()
            bill_row["period_end"] = bounds[1].date().isoformat()
        bills.append(bill_row)
        total_gj += gj
        total_days_acc += days_i

    annual_bill_days: Optional[float] = None
    if total_days_acc > 0 and total_gj > 0:
        annual_bill_days = (total_gj / total_days_acc) * 365.0

    period_coverage: dict[str, Any] = {}
    bills_with_dates = [b for b in bills if b.get("period_start") and b.get("period_end")]
    n_parsed = len(bills_with_dates)
    bc = len(bills)
    calendar_span: Optional[int] = None
    annual_calendar: Optional[float] = None
    use_calendar_primary = False
    min_calendar_days_for_primary = int(os.environ.get("AIRTABLE_SME_GAS_MIN_CALENDAR_DAYS_PRIMARY", "365"))

    if bills:
        period_coverage["bills_with_parsed_period_dates"] = n_parsed
        period_coverage["bill_count"] = bc
        period_coverage["sum_of_bill_period_days"] = total_days_acc
        if n_parsed > 0:
            starts = [date.fromisoformat(str(b["period_start"])) for b in bills_with_dates]
            ends = [date.fromisoformat(str(b["period_end"])) for b in bills_with_dates]
            c_start = min(starts)
            c_end = max(ends)
            period_coverage["calendar_start"] = c_start.isoformat()
            period_coverage["calendar_end"] = c_end.isoformat()
            calendar_span = (c_end - c_start).days + 1
            period_coverage["calendar_span_inclusive_days"] = calendar_span
            period_coverage["period_range_label_au"] = (
                f"{c_start.strftime('%d/%m/%Y')} – {c_end.strftime('%d/%m/%Y')}"
            )
        cb = n_parsed
        use_calendar_primary = (
            total_gj > 0
            and calendar_span is not None
            and calendar_span >= min_calendar_days_for_primary
            and n_parsed == bc
            and bc > 0
        )
        if total_gj > 0 and calendar_span and calendar_span > 0:
            annual_calendar = (total_gj / float(calendar_span)) * 365.0
            period_coverage["annual_usage_gj_calendar_window"] = round(annual_calendar, 2)

        if annual_bill_days is not None:
            period_coverage["annual_usage_gj_bill_period_days"] = round(annual_bill_days, 2)

        lines: list[str] = []
        if n_parsed > 0 and period_coverage.get("period_range_label_au"):
            cspan = period_coverage.get("calendar_span_inclusive_days")
            lines.append(
                f"Bill dates in Airtable span {period_coverage['period_range_label_au']}"
                + (f" ({cspan} calendar days from earliest start to latest end)." if cspan else ".")
            )

        if use_calendar_primary and annual_calendar is not None:
            period_coverage["annual_usage_gj_primary_method"] = "calendar_window"
            lines.append(
                "Estimated annual usage uses the calendar window between earliest bill start and latest bill end: "
                f"(total {round(total_gj, 2)} GJ) ÷ ({calendar_span} calendar days) × 365, because every bill had parseable "
                f"period dates and the span is at least {min_calendar_days_for_primary} days."
            )
            if annual_bill_days is not None and abs(annual_calendar - annual_bill_days) > 0.05:
                lines.append(
                    f"Bill-period alternative (sum of each bill’s day lengths = {total_days_acc} days): "
                    f"{round(annual_bill_days, 1)} GJ/yr — use when calendar coverage may be incomplete."
                )
        else:
            period_coverage["annual_usage_gj_primary_method"] = "bill_period_days"
            lines.append(
                "Estimated annual usage uses bill-period lengths: (total usage GJ) ÷ (sum of each bill’s day count) × 365."
            )
            if annual_calendar is not None:
                reason = []
                if n_parsed < bc:
                    reason.append(
                        f"only {n_parsed} of {bc} bills had parseable period text for calendar bounds"
                    )
                if calendar_span is not None and calendar_span < min_calendar_days_for_primary:
                    reason.append(
                        f"calendar span ({calendar_span} days) is under {min_calendar_days_for_primary} days"
                    )
                lines.append(
                    f"Calendar-window figure ({round(annual_calendar, 1)} GJ/yr) is shown for reference only"
                    + (f" ({'; '.join(reason)})." if reason else ".")
                )

        if cb < bc:
            lines.append(
                f"Explicit date ranges were parsed from the period field on {cb} of {bc} bills; "
                f"the rest contributed only day counts (no parseable period text)."
            )
        if n_parsed > 0 and total_days_acc > 0 and calendar_span:
            if total_days_acc != calendar_span:
                lines.append(
                    f"Sum of bill lengths is {total_days_acc} days vs {calendar_span} calendar days in that window—"
                    "normal if periods overlap or have small gaps."
                )
        period_coverage["explanation"] = " ".join(lines)

    annual: Optional[float] = None
    if use_calendar_primary and annual_calendar is not None:
        annual = annual_calendar
    elif annual_bill_days is not None:
        annual = annual_bill_days

    confidence = "low"
    if total_days_acc >= 320 and len(bills) >= 4:
        confidence = "high"
    elif total_days_acc >= 180 and len(bills) >= 2:
        confidence = "medium"

    # Eligibility / screening: bill-day annualisation only (calendar is contextual).
    meets_bill_days: Optional[bool]
    near_bill_days: Optional[bool]
    if annual_bill_days is None:
        meets_bill_days = None
        near_bill_days = None
    else:
        meets_bill_days = annual_bill_days >= threshold_gj
        near_bill_days = near_low_gj <= annual_bill_days < threshold_gj

    out: dict[str, Any] = {
        "mrin_normalized": mrin_norm,
        "table_name": table,
        "bill_count": len(bills),
        "total_days": total_days_acc,
        "total_usage_gj_sum": round(total_gj, 4) if total_gj else 0.0,
        "annual_usage_gj": round(annual, 2) if annual is not None else None,
        "annual_usage_gj_bill_period_days": round(annual_bill_days, 2) if annual_bill_days is not None else None,
        "annual_usage_gj_calendar_window": round(annual_calendar, 2) if annual_calendar is not None else None,
        "annual_usage_gj_primary_method": (
            "calendar_window"
            if use_calendar_primary and annual_calendar is not None
            else ("bill_period_days" if annual is not None else None)
        ),
        # Back-compat: same as meets_1000gj_screen (bill-day threshold), not calendar-primary annual.
        "meets_ci_threshold_1000gj": meets_bill_days,
        "meets_1000gj_screen": meets_bill_days,
        "near_1000gj_screen": near_bill_days,
        "threshold_gj": threshold_gj,
        "confidence": confidence,
        "usage_unit_mode": mode,
    }
    if period_coverage:
        out["period_coverage"] = period_coverage
    if annual is None and len(matched) == 0:
        out["error"] = "no_matching_rows"
        out["message"] = f"No SME gas invoice rows in Airtable for MRIN {mrin_norm} (check table {table!r} and field mapping)."
    elif annual is None:
        out["error"] = "no_usable_usage_rows"
        hint = ""
        if matched:
            sf = (matched[0].get("fields") or {})
            sk = set(sf.keys())
            row_has_link_fields = any(
                "invoice" in k.lower() or k.startswith("Link to") for k in sk
            )
            no_usage_cols = not any(n in sk for n in usage_fields) and not any(
                n in sk for n in days_fields
            )
            if row_has_link_fields and no_usage_cols:
                hint = (
                    " This row looks like an account header (links only, no usage columns). "
                    "Set AIRTABLE_SME_GAS_USAGE_TABLE to your SME gas invoice-lines table id (tbl…), "
                    "where each record has MRIN + usage + bill days."
                )
        out["message"] = (
            f"Found {len(matched)} row(s) for MRIN {mrin_norm} but none had positive usage and bill days "
            f"(set AIRTABLE_SME_GAS_HIST_USAGE_FIELDS / _DAYS_FIELDS / _PERIOD_FIELDS to match your base)."
            f"{hint}"
        )

    _sme_gas_hist_log(
        "done mrin_norm=%r bills=%s total_days=%s annual_gj=%s error=%r skipped_unusable_rows=%s",
        mrin_norm,
        len(bills),
        total_days_acc,
        out.get("annual_usage_gj"),
        out.get("error"),
        skipped_rows,
    )

    if debug:
        key_union: set[str] = set()
        for rec in matched[:25]:
            key_union.update((rec.get("fields") or {}).keys())
        out["diagnostics"] = {
            "filter_formula": formula,
            "rows_matched_mrin": len(matched),
            "bills_used": bills[:40],
            "field_keys_sample": sorted(key_union)[:80],
            "airtable_sme_gas_usage_table_env": usage_table_env_raw or None,
            "days_fields_config": days_fields,
            "usage_fields_config": usage_fields,
            "period_fields_config": period_fields,
        }
    return out
