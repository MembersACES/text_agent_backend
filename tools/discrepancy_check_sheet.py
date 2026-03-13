"""
Discrepancy checks – read from Google Sheet (FILE_IDS spreadsheet).
Tabs: C&I Gas, C&I Electricity (Contract), DMA.
Uses the same Sheets service and sheet ID as business_info (FILE_IDS_SHEET_ID).
"""

import logging
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


def _normalize_header(h: Any) -> str:
    if h is None:
        return ""
    return str(h).strip().lower()


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

    range_str = f"'{tab_name}'!A1:Z1000"
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

    raw_headers = values[0]
    rows: list[dict[str, str]] = []
    for row in values[1:]:
        obj: dict[str, str] = {k: "" for k in normalized_keys}
        for i, raw in enumerate(row):
            if i >= len(raw_headers):
                break
            h = _normalize_header(raw_headers[i])
            key = header_to_key.get(h)
            if key:
                val = raw if raw is None else str(raw).strip()
                if key == "utility_identifier":
                    val = _normalize_identifier(raw)
                obj[key] = val

        if business_name:
            linked = (obj.get("linked_business_name") or "").strip()
            if linked.lower() != business_name.strip().lower():
                continue
        rows.append(obj)

    return rows


def get_discrepancy_rows(business_name: str | None = None) -> list[dict[str, str]]:
    """
    Read the C&I Gas Discrepancy Check tab and return a list of row objects
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
