"""
Discrepancy checks – read from Google Sheet (FILE_IDS spreadsheet).
Tabs: C&I Gas, C&I Electricity (Contract), DMA, Demand Check.
Uses the same Sheets service and sheet ID as business_info (FILE_IDS_SHEET_ID).
"""

import logging
import re
from typing import Any

from tools.business_info import FILE_IDS_SHEET_ID, get_sheets_service

logger = logging.getLogger(__name__)

# --- C&I Gas ---
GAS_TAB_NAME = "C&I Gas Descrepancy Check"

HEADER_TO_KEY_GAS = {
    "descrpancy type": "discrepancy_type",
    "discrepancy type": "discrepancy_type",
    "utility identifier": "utility_identifier",
    "linked business name": "linked_business_name",
    "invoice period": "invoice_period",
    "invoice rate": "invoice_rate",
    "contract period": "contract_period",
    "contract rate": "contract_rate",
    "rate difference": "rate_difference",
    "% difference": "pct_difference",
    "pct difference": "pct_difference",
    "annual quantity gj": "annual_quantity_gj",
    "annual potential overcharge": "annual_potential_overcharge",
    "take or pay invoice": "take_or_pay_invoice",
}

GAS_NORMALIZED_KEYS = [
    "discrepancy_type",
    "utility_identifier",
    "linked_business_name",
    "invoice_period",
    "invoice_rate",
    "contract_period",
    "contract_rate",
    "rate_difference",
    "pct_difference",
    "annual_quantity_gj",
    "annual_potential_overcharge",
    "take_or_pay_invoice",
]

# --- C&I Electricity (Contract) ---
ELECTRICITY_CONTRACT_TAB_NAME = "C&I Electricity Descrepancy Check"

HEADER_TO_KEY_ELECTRICITY_CONTRACT = {
    "descrpancy type": "discrepancy_type",
    "discrepancy type": "discrepancy_type",
    "utility identifier (nmi)": "utility_identifier",
    "utility identifier": "utility_identifier",
    "linked business name": "linked_business_name",
    "retailer": "retailer",
    "site address": "site_address",
    "invoice period": "invoice_period",
    "contract period": "contract_period",
    "peak quantity (kwh)": "peak_quantity_kwh",
    "peak contract rate (c/kwh)": "peak_contract_rate",
    "peak invoice rate (c/kwh)": "peak_invoice_rate",
    "peak rate difference": "peak_rate_difference",
    "peak % difference": "peak_pct_difference",
    "shoulder quantity (kwh)": "shoulder_quantity_kwh",
    "shoulder contract rate (c/kwh)": "shoulder_contract_rate",
    "shoulder invoice rate (c/kwh)": "shoulder_invoice_rate",
    "shoulder rate difference": "shoulder_rate_difference",
    "shoulder % difference": "shoulder_pct_difference",
    "off-peak quantity (kwh)": "off_peak_quantity_kwh",
    "off-peak contract rate (c/kwh)": "off_peak_contract_rate",
    "off-peak invoice rate (c/kwh)": "off_peak_invoice_rate",
    "off-peak rate difference": "off_peak_rate_difference",
    "off-peak % difference": "off_peak_pct_difference",
    "service charge contract ($)": "service_charge_contract",
    "service charge invoice ($)": "service_charge_invoice",
    "service charge difference": "service_charge_difference",
    "service charge % difference": "service_charge_pct_difference",
    "contract target consumption (kwh)": "contract_target_consumption_kwh",
    "discrepancy detected": "discrepancy_detected",
    "notes": "notes",
}

ELECTRICITY_CONTRACT_KEYS = [
    "discrepancy_type",
    "utility_identifier",
    "linked_business_name",
    "retailer",
    "site_address",
    "invoice_period",
    "contract_period",
    "peak_quantity_kwh",
    "peak_contract_rate",
    "peak_invoice_rate",
    "peak_rate_difference",
    "peak_pct_difference",
    "shoulder_quantity_kwh",
    "shoulder_contract_rate",
    "shoulder_invoice_rate",
    "shoulder_rate_difference",
    "shoulder_pct_difference",
    "off_peak_quantity_kwh",
    "off_peak_contract_rate",
    "off_peak_invoice_rate",
    "off_peak_rate_difference",
    "off_peak_pct_difference",
    "service_charge_contract",
    "service_charge_invoice",
    "service_charge_difference",
    "service_charge_pct_difference",
    "contract_target_consumption_kwh",
    "discrepancy_detected",
    "notes",
]

# --- DMA ---
DMA_TAB_NAME = "DMA Descrepancy Check"

HEADER_TO_KEY_DMA = {
    "descrpancy type": "discrepancy_type",
    "discrepancy type": "discrepancy_type",
    "utility identifier": "utility_identifier",
    "linked business name": "linked_business_name",
    "dma annual fee": "dma_annual_fee",
    "dma daily rate": "dma_daily_rate",
    "invoice period": "invoice_period",
    "invoice comparison days": "invoice_comparison_days",
    "expected charge": "expected_charge",
    "actual invoice charge": "actual_invoice_charge",
    "difference": "difference",
    "status": "status",
}

DMA_KEYS = [
    "discrepancy_type",
    "utility_identifier",
    "linked_business_name",
    "dma_annual_fee",
    "dma_daily_rate",
    "invoice_period",
    "invoice_comparison_days",
    "expected_charge",
    "actual_invoice_charge",
    "difference",
    "status",
]

# --- Demand Check (Maximum demand review, interval data vs invoice) ---
# This lives on a separate tab called "Demand Check". It is used to flag sites
# where interval data has been received and a maximum demand review has been run.
DEMAND_TAB_NAME = "Demand Check"

HEADER_TO_KEY_DEMAND = {
    # Core identifying columns
    "review type": "review_type",
    "maximum demand review invoice vs data": "review_type",
    "risk / opportunity": "risk_or_opportunity",
    "risk/opportunity": "risk_or_opportunity",
    "risk / opporunity": "risk_or_opportunity",
    "utility identifier (nmi)": "utility_identifier",
    "utility identifier (nmi / mrin)": "utility_identifier",
    "utility identifier": "utility_identifier",
    "nmi": "utility_identifier",
    "mrin": "utility_identifier",
    "mirn": "utility_identifier",
    "linked business name": "linked_business_name",
    "linked business": "linked_business_name",
    "site address": "site_address",
    "network provider": "network_provider",
    "network": "network_provider",
    "demand type": "demand_type",
    # Demand numbers
    "highest invoice demand": "highest_invoice_demand",
    "highest invoice demand (kw)": "highest_invoice_demand",
    "highest invoice demand (kva)": "highest_invoice_demand",
    "actual interval demand": "actual_interval_demand",
    "actual interval demand (kw)": "actual_interval_demand",
    "actual interval demand (kva)": "actual_interval_demand",
    "demand difference": "demand_difference",
    "demand difference (kw)": "demand_difference",
    "demand difference (kva)": "demand_difference",
    # Charges
    "actual invoice charge": "actual_invoice_charge",
    "actual invoice charge ($)": "actual_invoice_charge",
    "expected charge": "expected_charge",
    "expected charge ($)": "expected_charge",
    "difference": "difference",
    "difference ($)": "difference",
    "charge difference": "difference",
    "status": "status",
    "result": "status",
    "notes": "status",
}

DEMAND_KEYS = [
    "review_type",
    "risk_or_opportunity",
    "utility_identifier",
    "linked_business_name",
    "site_address",
    "network_provider",
    "demand_type",
    "highest_invoice_demand",
    "actual_interval_demand",
    "demand_difference",
    "actual_invoice_charge",
    "expected_charge",
    "difference",
    "status",
]


def _normalize_header(h: Any) -> str:
    if h is None:
        return ""
    s = str(h).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _header_without_parens(h: str) -> str:
    """Strip parenthetical units so 'Highest Invoice Demand (kW)' maps like 'highest invoice demand'."""
    s = re.sub(r"\s*\([^)]*\)", "", h)
    return re.sub(r"\s+", " ", s).strip()


def _build_header_map(base: dict[str, str]) -> dict[str, str]:
    """Expand header map with parenthesis-stripped variants."""
    out = dict(base)
    for header, key in base.items():
        stripped = _header_without_parens(header)
        if stripped and stripped not in out:
            out[stripped] = key
    return out


def _resolve_header_key(header: str, header_map: dict[str, str]) -> str | None:
    if not header:
        return None
    key = header_map.get(header)
    if key:
        return key
    return header_map.get(_header_without_parens(header))


def _find_header_row_index(values: list[list[Any]], header_map: dict[str, str]) -> int:
    """Pick the row that looks most like column headers (within first 5 rows)."""
    best_idx = 0
    best_score = -1
    scan = min(5, len(values))
    for idx in range(scan):
        row = values[idx]
        score = sum(
            1
            for cell in row
            if _resolve_header_key(_normalize_header(cell), header_map)
        )
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx


def _row_has_content(obj: dict[str, str]) -> bool:
    return any((v or "").strip() for v in obj.values())


def _normalize_identifier(raw: Any) -> str:
    """Normalize NMI/MRIN from sheet (may be number or string) to string for matching."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        return s[:-2]
    return s


def _read_tab(
    tab_name: str,
    header_to_key: dict[str, str],
    normalized_keys: list[str],
    business_name: str | None,
) -> list[dict[str, str]]:
    """Read a tab and return rows with normalized keys; optional filter by linked_business_name."""
    if not FILE_IDS_SHEET_ID:
        logger.warning("[discrepancy_check_sheet] FILE_IDS_SHEET_ID not set")
        return []

    service = get_sheets_service()
    if not service:
        logger.warning("[discrepancy_check_sheet] could not get Sheets service")
        return []

    range_str = f"'{tab_name}'!A1:AZ1000"
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=FILE_IDS_SHEET_ID,
            range=range_str,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except Exception as e:
        logger.warning("[discrepancy_check_sheet] read sheet %r failed: %s", tab_name, e)
        return []

    values = resp.get("values", [])
    if not values:
        return []

    header_map = _build_header_map(header_to_key)
    header_idx = _find_header_row_index(values, header_map)
    raw_headers = values[header_idx]
    rows: list[dict[str, str]] = []
    for row in values[header_idx + 1 :]:
        obj: dict[str, str] = {k: "" for k in normalized_keys}
        for i, raw in enumerate(row):
            if i >= len(raw_headers):
                break
            h = _normalize_header(raw_headers[i])
            key = _resolve_header_key(h, header_map)
            if key:
                val = raw if raw is None else str(raw).strip()
                if key == "utility_identifier":
                    val = _normalize_identifier(raw)
                obj[key] = val

        if not _row_has_content(obj):
            continue

        if business_name:
            linked = (obj.get("linked_business_name") or "").strip()
            if linked.lower() != business_name.strip().lower():
                continue
        rows.append(obj)

    return rows


def get_discrepancy_rows(business_name: str | None = None) -> list[dict[str, str]]:
    """
    Read the Discrepancy Check tab and return a list of row objects
    with normalized keys. If business_name is provided, filter to rows where
    Linked Business Name matches (trim/case-normalized).
    """
    return _read_tab(
        GAS_TAB_NAME,
        HEADER_TO_KEY_GAS,
        GAS_NORMALIZED_KEYS,
        business_name,
    )


def get_electricity_contract_discrepancy_rows(
    business_name: str | None = None,
) -> list[dict[str, str]]:
    """
    Read the C&I Electricity Discrepancy Check tab (invoice vs contract).
    Filter by linked_business_name if provided.
    """
    return _read_tab(
        ELECTRICITY_CONTRACT_TAB_NAME,
        HEADER_TO_KEY_ELECTRICITY_CONTRACT,
        ELECTRICITY_CONTRACT_KEYS,
        business_name,
    )


def get_dma_discrepancy_rows(business_name: str | None = None) -> list[dict[str, str]]:
    """
    Read the DMA Discrepancy Check tab. Filter by linked_business_name if provided.
    """
    return _read_tab(
        DMA_TAB_NAME,
        HEADER_TO_KEY_DMA,
        DMA_KEYS,
        business_name,
    )


def get_demand_check_rows(business_name: str | None = None) -> list[dict[str, str]]:
    """
    Read the Demand Check tab (maximum demand review invoice vs data).
    Filter by linked_business_name if provided.
    """
    return _read_tab(
        DEMAND_TAB_NAME,
        HEADER_TO_KEY_DEMAND,
        DEMAND_KEYS,
        business_name,
    )
