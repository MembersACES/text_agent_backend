"""
Direct Airtable API client for Member ACES Data base.
Used for get-business-info (LOA + linked utilities) and updating Data Requested / Contract End Date.
"""
import logging
import os
from typing import Any, Optional
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

# Load env from backend root
_backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_env_path = os.path.join(_backend_root, ".env")
if os.path.exists(_env_path):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=_env_path)

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
