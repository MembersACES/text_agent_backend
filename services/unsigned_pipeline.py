"""Unsigned utility pipeline: load-by-state + invoice PDF pack.

Joins Member ACES invoice tabs with FILE_IDS signed-via-ACES flags (per utility,
not per CRM stage) and optional Base 1 landing leads. Used by
GET /api/unsigned-pipeline and POST /api/unsigned-pipeline/drive-pack.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Optional

from services.climate_activity_etl import (
    UTILITY_ACTIVITY_MAP,
    _first_field,
    _parse_iso_date,
    _parse_period_range,
    _resolve_quantity,
)
from services.signed_contract_dry_run import (
    compute_signed_utilities,
    normalize_business_name,
    read_data_from_airtable_tab,
)
from services.waste_invoice_dump import (
    UTILITY_TAB_CONFIG,
    _norm,
    read_utility_tab_rows,
)
from tools.drive_file_metadata import extract_drive_file_id

logger = logging.getLogger(__name__)

UTILITY_GROUPS: dict[str, tuple[str, ...]] = {
    "gas": ("C&I Gas", "SME Gas"),
    "electricity": ("C&I Electricity", "SME Electricity"),
    "waste": ("Waste",),
    "oil": ("Oil",),
    "water": ("Water",),
    "cleaning": ("Cleaning", "Grease Trap", "Linen Cleaning"),
}

ALL_UTILITIES: tuple[str, ...] = tuple(
    ut for group in UTILITY_GROUPS.values() for ut in group
)
UTILITY_GROUPS["all"] = ALL_UTILITIES

FILE_IDS_TRACKED: frozenset[str] = frozenset(
    {
        "C&I Electricity",
        "SME Electricity",
        "C&I Gas",
        "SME Gas",
        "Waste",
        "Oil",
    }
)

AU_STATES: tuple[str, ...] = ("NSW", "VIC", "QLD", "SA", "WA", "TAS", "ACT", "NT")
UNKNOWN_STATE = "Unknown"
STATE_ORDER: tuple[str, ...] = AU_STATES + (UNKNOWN_STATE,)
MIN_ANNUALISE_DAYS = 90
GROUP_LABELS: dict[str, str] = {
    "gas": "gas",
    "electricity": "electricity",
    "waste": "waste",
    "oil": "oil",
    "water": "water",
    "cleaning": "cleaning",
    "all": "utilities",
}

NAME_FIELDS = (
    "Client Name",
    "client_name",
    "Account Name",
    "Business Name",
    "Customer Name",
)
ADDRESS_FIELDS = (
    "Site Address",
    "Site Address:",
    "Supply Address",
    "Address",
    "site_address",
    "client_address",
    "Client Address",
)
DAYS_FIELDS = (
    "Number of Days",
    "No of Days",
    "Days",
    "Billing Days",
    "Invoice Days",
    "Days in Period",
)
RETAILER_FIELDS = (
    "Retailer",
    "Provider",
    "supplier_name",
    "Supplier Name",
    "Retailer C&I Gas",
    "Retailer SME Gas",
    "Retailer SME Electricity",
)
LINK_HEADER_NORMS = frozenset(
    {
        "webview link",
        "web view link",
        "invoice pdf",
        "google drive link",
        "drive link",
        "pdf link",
    }
)
DISPLAY_UNIT: dict[str, str] = {
    "C&I Electricity": "kWh",
    "SME Electricity": "kWh",
    "C&I Gas": "GJ",
    "SME Gas": "GJ",
    "Waste": "t",
    "Oil": "L",
    "Water": "kL",
    "Cleaning": "AUD",
    "Grease Trap": "AUD",
    "Linen Cleaning": "AUD",
}
EXTRA_QUANTITY_MAP: dict[str, dict[str, Any]] = {
    "Water": {
        "unit": "kL",
        "quantity_fields": [
            "kL",
            "KL",
            "Total kL",
            "Usage (kL)",
            "Consumption (kL)",
            "Kilolitres",
            "Water Usage",
            "Quantity (kL)",
            "Total Consumption kL",
        ],
    },
    "Cleaning": {
        "unit": "AUD",
        "quantity_fields": [
            "Invoice Total Amount",
            "Invoice Total",
            "invoice_total",
            "Total Amount",
            "Amount",
            "Total",
        ],
    },
    "Grease Trap": {
        "unit": "AUD",
        "quantity_fields": [
            "Invoice Total Amount",
            "Invoice Total",
            "Total Amount",
            "Amount",
            "Total",
        ],
    },
    "Linen Cleaning": {
        "unit": "AUD",
        "quantity_fields": [
            "Invoice Total Amount",
            "invoice_total",
            "Invoice Total",
            "Total Amount",
            "Amount",
            "Total",
        ],
    },
}
BASE1_GROUP_TOKENS: dict[str, tuple[str, ...]] = {
    "gas": ("gas", "mrin", "mirn"),
    "electricity": ("electric", "nmi", "power"),
    "waste": ("waste", "bin"),
    "oil": ("oil", "uco"),
    "water": ("water",),
    "cleaning": ("clean", "grease", "linen"),
}

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9 _.-]+")
_POSTCODE_RE = re.compile(r"\b(\d{4})\b")
_STATE_RE = re.compile(r"\b(NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\b", re.I)

_FILE_IDS_CACHE: dict[str, Any] = {"ts": 0.0, "rows": None, "meta": None}
_FILE_IDS_TTL = 300.0


def resolve_utility_types(utility_group: str, segment: str = "all") -> list[str]:
    key = (utility_group or "gas").strip().lower()
    types = list(UTILITY_GROUPS.get(key) or UTILITY_GROUPS["gas"])
    seg = (segment or "all").strip().lower()
    if seg == "ci":
        filtered = [t for t in types if t.startswith("C&I")]
        return filtered or types
    if seg == "sme":
        filtered = [t for t in types if t.startswith("SME")]
        return filtered or types
    return types


def postcode_to_state(postcode: str) -> str:
    digits = re.sub(r"\D", "", postcode or "")
    if len(digits) < 4:
        return ""
    n = int(digits[:4])
    if 2000 <= n <= 2599 or 2619 <= n <= 2899 or 2921 <= n <= 2999:
        return "NSW"
    if 2600 <= n <= 2618 or 2900 <= n <= 2920:
        return "ACT"
    if 3000 <= n <= 3999:
        return "VIC"
    if 4000 <= n <= 4999:
        return "QLD"
    if 5000 <= n <= 5799:
        return "SA"
    if 6000 <= n <= 6797:
        return "WA"
    if 7000 <= n <= 7799:
        return "TAS"
    if 800 <= n <= 899:
        return "NT"
    return ""


def guess_state(address: str, explicit: str | None = None) -> str:
    raw = (explicit or "").strip().upper()
    if raw in AU_STATES:
        return raw
    aliases = {
        "NEW SOUTH WALES": "NSW",
        "VICTORIA": "VIC",
        "QUEENSLAND": "QLD",
        "SOUTH AUSTRALIA": "SA",
        "WESTERN AUSTRALIA": "WA",
        "TASMANIA": "TAS",
        "AUSTRALIAN CAPITAL TERRITORY": "ACT",
        "NORTHERN TERRITORY": "NT",
    }
    if raw in aliases:
        return aliases[raw]
    text = str(address or "").strip()
    m = _STATE_RE.search(text)
    if m:
        return m.group(1).upper()
    pc = _POSTCODE_RE.search(text)
    if pc:
        state = postcode_to_state(pc.group(1))
        if state:
            return state
    return UNKNOWN_STATE


def _row_text(row: dict[str, Any], names: tuple[str, ...]) -> str:
    val = _first_field(row, list(names))
    if val is None:
        return ""
    return str(val).strip()


def row_webview_link(row: dict[str, Any]) -> str:
    for key, val in (row or {}).items():
        if _norm(key) in LINK_HEADER_NORMS:
            link = str(val or "").strip()
            if link and link.lower() != "null":
                return link
    return ""


def identifier_from_row(row: dict[str, Any], key_cols: tuple[str, ...]) -> str:
    for col in key_cols:
        val = str(row.get(col) or "").strip()
        if val and val.lower() != "null":
            return val
    return ""


def _quantity_cfg(utility_type: str) -> dict[str, Any]:
    cfg = UTILITY_ACTIVITY_MAP.get(utility_type) or EXTRA_QUANTITY_MAP.get(utility_type)
    if cfg:
        return cfg
    return {"unit": DISPLAY_UNIT.get(utility_type, ""), "quantity_fields": []}


def row_quantity(row: dict[str, Any], utility_type: str) -> Optional[float]:
    cfg = _quantity_cfg(utility_type)
    return _resolve_quantity(row, cfg)


def row_period_days(row: dict[str, Any]) -> tuple[Optional[int], Optional[date], Optional[date], str]:
    """Return (days, start, end, label)."""
    label = _row_text(
        row,
        (
            "Invoice Review Period",
            "Review Period",
            "Billing Period",
            "Invoice Date",
            "invoice_date",
            "invoice_number",
            "Invoice Number",
        ),
    )
    explicit_days = _first_field(row, list(DAYS_FIELDS))
    if explicit_days is not None:
        try:
            days_n = int(float(str(explicit_days).replace(",", "").strip()))
        except ValueError:
            days_n = 0
        if days_n > 0:
            rng = _parse_period_range(label) if label else None
            start = rng[0] if rng else None
            end = rng[1] if rng else None
            return days_n, start, end, label

    rng = _parse_period_range(
        _first_field(row, ["Invoice Review Period", "Review Period", "Billing Period"])
    )
    if rng:
        days_n = (rng[1] - rng[0]).days + 1
        if days_n > 0:
            return days_n, rng[0], rng[1], label or f"{rng[0].isoformat()} – {rng[1].isoformat()}"

    single = None
    for field in ("Invoice Date Formatted", "Invoice Date", "invoice_date"):
        single = _parse_iso_date(_first_field(row, [field]))
        if single:
            break
    return None, single, single, label


def annualise_usage(billed_qty: float, billed_days: int) -> tuple[Optional[float], str]:
    if billed_qty <= 0:
        return None, "none"
    if billed_days > 0:
        return billed_qty / billed_days * 365.0, "annualised"
    return billed_qty, "sum_billed"


def mark_load_quality(site: dict[str, Any]) -> None:
    days = int(site.get("billed_days") or 0)
    load = site.get("annual_load")
    has_load = isinstance(load, (int, float))
    quoteable = (
        site.get("source") == "member_aces"
        and site.get("load_method") == "annualised"
        and days >= MIN_ANNUALISE_DAYS
        and has_load
    )
    site["quoteable"] = quoteable
    site["thin_data"] = bool(
        site.get("source") == "member_aces" and has_load and not quoteable
    )


def _invoice_sort_key(inv: dict[str, Any]) -> tuple[int, str]:
    end = inv.get("period_end") or ""
    return (0 if end else 1, str(end))


def group_invoice_rows(
    utility_type: str,
    rows: list[dict[str, Any]],
    key_cols: tuple[str, ...],
    label_col: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for row in rows:
        ident = identifier_from_row(row, key_cols)
        if not ident:
            continue
        grouped[_norm(ident)].append((ident, row))

    sites: list[dict[str, Any]] = []
    unit = DISPLAY_UNIT.get(utility_type) or _quantity_cfg(utility_type).get("unit") or ""
    for _key, items in grouped.items():
        invoices: list[dict[str, Any]] = []
        billed_qty = 0.0
        billed_days = 0
        business_name = ""
        address = ""
        retailer = ""
        display_ident = items[0][0]
        for ident, row in items:
            display_ident = ident or display_ident
            if not business_name:
                business_name = _row_text(row, NAME_FIELDS)
            if not address:
                address = _row_text(row, ADDRESS_FIELDS)
            if not retailer:
                retailer = _row_text(row, RETAILER_FIELDS)
            qty = row_quantity(row, utility_type)
            days, start, end, label = row_period_days(row)
            header_label = str(row.get(label_col) or "").strip()
            period_label = header_label or label
            link = row_webview_link(row)
            if qty and qty > 0:
                billed_qty += qty
                if days and days > 0:
                    billed_days += days
            invoices.append(
                {
                    "label": period_label,
                    "link": link,
                    "missing": not bool(link),
                    "quantity": qty,
                    "days": days,
                    "period_start": start.isoformat() if start else "",
                    "period_end": end.isoformat() if end else "",
                }
            )
        invoices.sort(key=_invoice_sort_key, reverse=True)
        annual, method = annualise_usage(billed_qty, billed_days)
        pdfs = [inv for inv in invoices if not inv["missing"]]
        latest = next((inv for inv in invoices if not inv["missing"]), invoices[0] if invoices else None)
        site = {
            "utility_type": utility_type,
            "identifier": display_ident,
            "business_name": business_name or display_ident,
            "site_address": address,
            "retailer": retailer,
            "state": guess_state(address),
            "unit": unit,
            "annual_load": round(annual, 3) if annual is not None else None,
            "load_method": method,
            "billed_quantity": round(billed_qty, 3) if billed_qty else None,
            "billed_days": billed_days or None,
            "invoice_count": len(invoices),
            "pdf_count": len(pdfs),
            "invoices": invoices,
            "latest_invoice": latest,
            "source": "member_aces",
        }
        mark_load_quality(site)
        sites.append(site)
    return sites


def _file_ids_indexes(
    sheet_rows: list[dict[str, str]],
) -> tuple[dict[str, dict[str, str]], dict[str, list[dict[str, str]]]]:
    by_id: dict[str, dict[str, str]] = {}
    by_name: dict[str, list[dict[str, str]]] = {}
    for row in sheet_rows:
        rid = (row.get("record_id") or "").strip()
        if rid:
            by_id[rid] = row
        name = normalize_business_name(row.get("business_name") or "")
        if name:
            by_name.setdefault(name, []).append(row)
    return by_id, by_name


def match_signed_rows(
    business_name: str,
    *,
    crm_by_name: dict[str, dict[str, Any]],
    by_record_id: dict[str, dict[str, str]],
    by_name: dict[str, list[dict[str, str]]],
) -> tuple[list[dict[str, str]], str]:
    name = normalize_business_name(business_name)
    client = crm_by_name.get(name)
    ext_id = str((client or {}).get("external_business_id") or "").strip()
    if ext_id and ext_id in by_record_id:
        return [by_record_id[ext_id]], "record_id"
    rows = by_name.get(name) or []
    if len(rows) == 1:
        return rows, "name"
    if len(rows) > 1:
        return rows, "name_collision"
    return [], "none"


def attach_signed_status(
    sites: list[dict[str, Any]],
    *,
    file_ids_rows: list[dict[str, str]],
    crm_clients: list[dict[str, Any]],
) -> None:
    by_id, by_name = _file_ids_indexes(file_ids_rows)
    crm_by_name: dict[str, dict[str, Any]] = {}
    for client in crm_clients:
        n = normalize_business_name(str(client.get("business_name") or ""))
        if n and n not in crm_by_name:
            crm_by_name[n] = client

    for site in sites:
        utility = site["utility_type"]
        matched, method = match_signed_rows(
            site.get("business_name") or "",
            crm_by_name=crm_by_name,
            by_record_id=by_id,
            by_name=by_name,
        )
        signed_utils: list[str] = []
        for row in matched:
            _has, labels = compute_signed_utilities(row)
            for label in labels:
                if label not in signed_utils:
                    signed_utils.append(label)
        has_flag = utility in FILE_IDS_TRACKED
        signed = has_flag and utility in signed_utils
        site["signed"] = signed
        site["signed_utilities"] = signed_utils
        site["has_contract_flag"] = has_flag
        site["match_method"] = method
        if client := crm_by_name.get(normalize_business_name(site.get("business_name") or "")):
            site["client_id"] = client.get("id")
            site["client_stage"] = client.get("stage")
        else:
            site["client_id"] = None
            site["client_stage"] = None


def base1_matches_group(utility_types_text: str, utility_group: str) -> bool:
    text = (utility_types_text or "").strip().lower()
    group = (utility_group or "gas").strip().lower()
    if not text:
        return group == "all"
    if group == "all":
        return True
    tokens = BASE1_GROUP_TOKENS.get(group) or (group,)
    return any(tok in text for tok in tokens)


def sites_from_base1(
    rows: list[dict[str, Any]],
    *,
    utility_group: str,
    existing_names: set[str],
    existing_emails: set[str],
) -> list[dict[str, Any]]:
    latest_by_company: dict[str, dict[str, Any]] = {}
    for row in rows:
        company = str(row.get("Company Name") or "").strip()
        if not company:
            continue
        email = str(row.get("Contact Email") or "").strip().lower()
        if normalize_business_name(company) in existing_names or (email and email in existing_emails):
            continue
        if not base1_matches_group(str(row.get("Utility Types") or ""), utility_group):
            continue
        key = company.lower()
        prev = latest_by_company.get(key)
        ts = str(row.get("Timestamp") or "")
        if prev is None or ts > str(prev.get("Timestamp") or ""):
            latest_by_company[key] = row

    sites: list[dict[str, Any]] = []
    for row in latest_by_company.values():
        company = str(row.get("Company Name") or "").strip()
        folder = str(row.get("Google Drive Folder") or "").strip()
        review = str(row.get("Base 1 Review") or "").strip()
        invoices: list[dict[str, Any]] = []
        if folder:
            invoices.append(
                {
                    "label": "Base 1 Drive folder",
                    "link": folder,
                    "missing": False,
                    "quantity": None,
                    "days": None,
                    "period_start": "",
                    "period_end": "",
                }
            )
        if review:
            invoices.append(
                {
                    "label": "Base 1 review",
                    "link": review,
                    "missing": False,
                    "quantity": None,
                    "days": None,
                    "period_start": "",
                    "period_end": "",
                }
            )
        sites.append(
            {
                "utility_type": "Base 1",
                "identifier": company,
                "business_name": company,
                "site_address": "",
                "retailer": "",
                "state": guess_state("", str(row.get("State") or "")),
                "unit": "",
                "annual_load": None,
                "load_method": "none",
                "billed_quantity": None,
                "billed_days": None,
                "invoice_count": len(invoices),
                "pdf_count": sum(1 for inv in invoices if not inv["missing"]),
                "invoices": invoices,
                "latest_invoice": invoices[0] if invoices else None,
                "source": "base1",
                "signed": False,
                "signed_utilities": [],
                "has_contract_flag": False,
                "match_method": "base1",
                "client_id": None,
                "client_stage": None,
                "utility_types": str(row.get("Utility Types") or "").strip() or None,
            }
        )
        mark_load_quality(sites[-1])
    return sites


def _load_for_pdfs(site: dict[str, Any], pdfs: str) -> list[dict[str, Any]]:
    invoices = list(site.get("invoices") or [])
    mode = (pdfs or "all").strip().lower()
    if mode == "latest":
        latest = site.get("latest_invoice")
        if latest and not latest.get("missing"):
            return [latest]
        return []
    return [inv for inv in invoices if not inv.get("missing")]


def aggregate_by_state(sites: list[dict[str, Any]], pdfs: str) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for site in sites:
        state = site.get("state") or UNKNOWN_STATE
        bucket = buckets.setdefault(
            state,
            {
                "state": state,
                "site_count": 0,
                "quoteable_site_count": 0,
                "thin_site_count": 0,
                "invoice_count": 0,
                "pdf_count": 0,
                "load_by_unit": defaultdict(float),
                "load_by_unit_all": defaultdict(float),
            },
        )
        bucket["site_count"] += 1
        if site.get("quoteable"):
            bucket["quoteable_site_count"] += 1
        if site.get("thin_data"):
            bucket["thin_site_count"] += 1
        bucket["invoice_count"] += int(site.get("invoice_count") or 0)
        selected = _load_for_pdfs(site, pdfs)
        bucket["pdf_count"] += len(selected)
        unit = str(site.get("unit") or "")
        load = site.get("annual_load")
        if unit and isinstance(load, (int, float)):
            bucket["load_by_unit_all"][unit] += float(load)
            if site.get("quoteable"):
                bucket["load_by_unit"][unit] += float(load)

    def _pack(bucket: dict[str, Any]) -> dict[str, Any]:
        return {
            "state": bucket["state"],
            "site_count": bucket["site_count"],
            "quoteable_site_count": bucket["quoteable_site_count"],
            "thin_site_count": bucket["thin_site_count"],
            "invoice_count": bucket["invoice_count"],
            "pdf_count": bucket["pdf_count"],
            "load_by_unit": {
                unit: round(val, 3)
                for unit, val in sorted(bucket["load_by_unit"].items())
                if val
            },
            "load_by_unit_all": {
                unit: round(val, 3)
                for unit, val in sorted(bucket["load_by_unit_all"].items())
                if val
            },
        }

    out: list[dict[str, Any]] = []
    for state in STATE_ORDER:
        bucket = buckets.get(state)
        if bucket:
            out.append(_pack(bucket))
    for state, bucket in buckets.items():
        if state not in STATE_ORDER:
            out.append(_pack(bucket))
    return out


def _totals(sites: list[dict[str, Any]], pdfs: str) -> dict[str, Any]:
    load_by_unit: dict[str, float] = defaultdict(float)
    load_by_unit_all: dict[str, float] = defaultdict(float)
    retailer_counts: dict[str, int] = defaultdict(int)
    pdf_count = 0
    invoice_count = 0
    for site in sites:
        invoice_count += int(site.get("invoice_count") or 0)
        pdf_count += len(_load_for_pdfs(site, pdfs))
        unit = str(site.get("unit") or "")
        load = site.get("annual_load")
        if unit and isinstance(load, (int, float)):
            load_by_unit_all[unit] += float(load)
            if site.get("quoteable"):
                load_by_unit[unit] += float(load)
        retailer = str(site.get("retailer") or "").strip() or "Unknown"
        retailer_counts[retailer] += 1
    retailers = [
        {"name": name, "site_count": count}
        for name, count in sorted(retailer_counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))
    ]
    return {
        "site_count": len(sites),
        "quoteable_site_count": sum(1 for s in sites if s.get("quoteable")),
        "thin_site_count": sum(1 for s in sites if s.get("thin_data")),
        "base1_site_count": sum(1 for s in sites if s.get("source") == "base1"),
        "unknown_state_count": sum(1 for s in sites if (s.get("state") or UNKNOWN_STATE) == UNKNOWN_STATE),
        "invoice_count": invoice_count,
        "pdf_count": pdf_count,
        "load_by_unit": {unit: round(val, 3) for unit, val in sorted(load_by_unit.items()) if val},
        "load_by_unit_all": {unit: round(val, 3) for unit, val in sorted(load_by_unit_all.items()) if val},
        "missing_pdf_sites": sum(1 for s in sites if int(s.get("pdf_count") or 0) == 0),
        "min_annualise_days": MIN_ANNUALISE_DAYS,
        "retailers": retailers,
    }


def _public_site(site: dict[str, Any], pdfs: str) -> dict[str, Any]:
    selected = _load_for_pdfs(site, pdfs)
    latest = site.get("latest_invoice")
    return {
        "utility_type": site.get("utility_type"),
        "identifier": site.get("identifier"),
        "business_name": site.get("business_name"),
        "site_address": site.get("site_address") or "",
        "retailer": site.get("retailer") or "",
        "state": site.get("state") or UNKNOWN_STATE,
        "unit": site.get("unit") or "",
        "annual_load": site.get("annual_load"),
        "load_method": site.get("load_method"),
        "quoteable": bool(site.get("quoteable")),
        "thin_data": bool(site.get("thin_data")),
        "billed_quantity": site.get("billed_quantity"),
        "billed_days": site.get("billed_days"),
        "invoice_count": site.get("invoice_count") or 0,
        "pdf_count": len(selected),
        "pdfs": selected,
        "latest_invoice": latest,
        "source": site.get("source"),
        "signed": bool(site.get("signed")),
        "signed_utilities": site.get("signed_utilities") or [],
        "has_contract_flag": bool(site.get("has_contract_flag")),
        "match_method": site.get("match_method"),
        "client_id": site.get("client_id"),
        "client_stage": site.get("client_stage"),
        "utility_types": site.get("utility_types"),
    }


def _fmt_load_map(load_by_unit: dict[str, Any]) -> str:
    parts: list[str] = []
    for unit, val in (load_by_unit or {}).items():
        try:
            num = float(val)
        except (TypeError, ValueError):
            continue
        if unit == "kWh" and num >= 1000:
            parts.append(f"{num / 1000:,.1f} MWh")
        elif num >= 10:
            parts.append(f"{num:,.0f} {unit}")
        else:
            parts.append(f"{num:,.1f} {unit}")
    return " · ".join(parts) if parts else "no quoteable load"


def build_summary(
    utility_group: str,
    totals: dict[str, Any],
    by_state: list[dict[str, Any]],
    pdfs: str,
) -> str:
    label = GROUP_LABELS.get(utility_group, utility_group)
    load = _fmt_load_map(totals.get("load_by_unit") or {})
    q = int(totals.get("quoteable_site_count") or 0)
    n = int(totals.get("site_count") or 0)
    lines = [
        f"Unsigned {label}: {n} sites ({q} with quoteable load, ≥{MIN_ANNUALISE_DAYS} bill days). Headline load: {load}."
    ]
    state_bits: list[str] = []
    for row in by_state:
        if row.get("state") == UNKNOWN_STATE:
            continue
        bit_load = _fmt_load_map(row.get("load_by_unit") or {})
        state_bits.append(f"{row.get('state')} {row.get('site_count')} sites / {bit_load}")
    if state_bits:
        lines.append("By state: " + " · ".join(state_bits) + ".")
    extras: list[str] = []
    thin = int(totals.get("thin_site_count") or 0)
    if thin:
        extras.append(f"{thin} thin-data sites excluded from headline load")
    base1 = int(totals.get("base1_site_count") or 0)
    if base1:
        extras.append(f"{base1} Base 1 leads (no extracted load)")
    unk = int(totals.get("unknown_state_count") or 0)
    if unk:
        extras.append(f"{unk} Unknown state")
    if extras:
        lines.append("; ".join(extras) + ".")
    pdf_word = "latest invoice PDFs" if pdfs == "latest" else "invoice PDFs"
    lines.append(f"{int(totals.get('pdf_count') or 0)} {pdf_word} in this view.")
    return " ".join(lines)


def cached_file_ids_rows() -> tuple[list[dict[str, str]], dict[str, Any]]:
    import time

    now = time.time()
    if (
        _FILE_IDS_CACHE["rows"] is not None
        and (now - float(_FILE_IDS_CACHE["ts"])) < _FILE_IDS_TTL
    ):
        return _FILE_IDS_CACHE["rows"], _FILE_IDS_CACHE["meta"]
    rows, meta = read_data_from_airtable_tab()
    _FILE_IDS_CACHE.update({"ts": now, "rows": rows, "meta": meta})
    return rows, meta


def build_pipeline(
    *,
    utility_group: str = "gas",
    unsigned_only: bool = True,
    pdfs: str = "all",
    include_base1: bool = True,
    segment: str = "all",
    states: list[str] | None = None,
    retailers: list[str] | None = None,
    exclude_retailers: list[str] | None = None,
    invoice_rows_by_utility: dict[str, list[dict[str, Any]]] | None = None,
    file_ids_rows: list[dict[str, str]] | None = None,
    crm_clients: list[dict[str, Any]] | None = None,
    base1_rows: list[dict[str, Any]] | None = None,
    existing_names: set[str] | None = None,
    existing_emails: set[str] | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    notes = list(warnings or [])
    types = resolve_utility_types(utility_group, segment)
    crm_clients = list(crm_clients or [])
    existing_names = existing_names or {
        normalize_business_name(str(c.get("business_name") or ""))
        for c in crm_clients
        if c.get("business_name")
    }
    existing_emails = existing_emails or {
        str(c.get("primary_contact_email") or "").strip().lower()
        for c in crm_clients
        if c.get("primary_contact_email")
    }

    sites: list[dict[str, Any]] = []
    for utility_type in types:
        cfg = UTILITY_TAB_CONFIG.get(utility_type)
        if not cfg:
            notes.append(f"No invoice tab configured for {utility_type}")
            continue
        _tab, key_cols, label_col = cfg
        if invoice_rows_by_utility is not None:
            rows = invoice_rows_by_utility.get(utility_type) or []
        else:
            try:
                rows = read_utility_tab_rows(utility_type)
            except Exception as e:
                logger.warning("[unsigned-pipeline] tab read failed %s: %s", utility_type, e)
                notes.append(f"Failed to read {utility_type} invoices: {e}")
                rows = []
        sites.extend(group_invoice_rows(utility_type, rows, key_cols, label_col))

    if file_ids_rows is None:
        try:
            file_ids_rows, _meta = cached_file_ids_rows()
        except Exception as e:
            logger.warning("[unsigned-pipeline] FILE_IDS read failed: %s", e)
            notes.append(f"FILE_IDS lookup failed: {e}")
            file_ids_rows = []

    attach_signed_status(sites, file_ids_rows=file_ids_rows, crm_clients=crm_clients)

    if include_base1:
        if base1_rows is None:
            try:
                from tools.business_info import get_base1_landing_responses

                base1_rows = get_base1_landing_responses() or []
            except Exception as e:
                logger.warning("[unsigned-pipeline] Base 1 read failed: %s", e)
                notes.append(f"Base 1 landing sheet failed: {e}")
                base1_rows = []
        sites.extend(
            sites_from_base1(
                base1_rows,
                utility_group=utility_group,
                existing_names=existing_names,
                existing_emails=existing_emails,
            )
        )

    if unsigned_only:
        sites = [s for s in sites if not s.get("signed")]

    if states:
        wanted = {str(s).strip().upper() for s in states if str(s).strip()}
        if wanted:
            sites = [s for s in sites if str(s.get("state") or "").upper() in wanted]

    if retailers:
        wanted_r = {str(r).strip().lower() for r in retailers if str(r).strip()}
        if wanted_r:
            def _ret_key(site: dict[str, Any]) -> str:
                return (str(site.get("retailer") or "").strip() or "unknown").lower()

            sites = [s for s in sites if _ret_key(s) in wanted_r]
    elif exclude_retailers:
        skip = {str(r).strip().lower() for r in exclude_retailers if str(r).strip()}
        if skip:
            sites = [
                s
                for s in sites
                if (str(s.get("retailer") or "").strip() or "unknown").lower() not in skip
            ]

    sites.sort(
        key=lambda s: (
            STATE_ORDER.index(s["state"]) if s.get("state") in STATE_ORDER else 99,
            s.get("utility_type") or "",
            (s.get("business_name") or "").lower(),
        )
    )
    pdfs_mode = "latest" if (pdfs or "").strip().lower() == "latest" else "all"
    public_sites = [_public_site(s, pdfs_mode) for s in sites]
    by_state = aggregate_by_state(sites, pdfs_mode)
    totals = _totals(sites, pdfs_mode)
    return {
        "utility_group": (utility_group or "gas").strip().lower(),
        "segment": (segment or "all").strip().lower(),
        "unsigned_only": bool(unsigned_only),
        "pdfs": pdfs_mode,
        "include_base1": bool(include_base1),
        "utility_types": types,
        "legend": (
            "Unsigned = this utility is not marked Signed via ACES on FILE_IDS. "
            "Members still appear if gas/power/etc. is unsigned. "
            f"Headline load only includes member invoices with ≥{MIN_ANNUALISE_DAYS} bill days. "
            "Thin data, Base 1, and as-billed rows stay in the table but are not quoted. "
            "Water/Cleaning/Grease/Linen have no ACES contract flag."
        ),
        "summary": build_summary(
            (utility_group or "gas").strip().lower(), totals, by_state, pdfs_mode
        ),
        "by_state": by_state,
        "totals": totals,
        "sites": public_sites,
        "warnings": notes,
    }


def _safe_filename(parts: list[str], suffix: str = ".pdf") -> str:
    raw = " - ".join(p.strip() for p in parts if p and str(p).strip())
    cleaned = _SAFE_NAME_RE.sub("", raw).strip() or "invoice"
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) > 120:
        cleaned = cleaned[:120].rstrip()
    if suffix and not cleaned.lower().endswith(suffix.lower()):
        cleaned = f"{cleaned}{suffix}"
    return cleaned


def summary_csv_bytes(payload: dict[str, Any]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "state",
            "utility_type",
            "business_name",
            "identifier",
            "retailer",
            "annual_load",
            "unit",
            "load_method",
            "quoteable",
            "thin_data",
            "billed_days",
            "invoice_count",
            "pdf_count",
            "signed",
            "has_contract_flag",
            "source",
            "match_method",
            "latest_period",
            "latest_link",
        ]
    )
    for site in payload.get("sites") or []:
        latest = site.get("latest_invoice") or {}
        writer.writerow(
            [
                site.get("state") or "",
                site.get("utility_type") or "",
                site.get("business_name") or "",
                site.get("identifier") or "",
                site.get("retailer") or "",
                site.get("annual_load") if site.get("annual_load") is not None else "",
                site.get("unit") or "",
                site.get("load_method") or "",
                "yes" if site.get("quoteable") else "no",
                "yes" if site.get("thin_data") else "no",
                site.get("billed_days") or "",
                site.get("invoice_count") or 0,
                site.get("pdf_count") or 0,
                "yes" if site.get("signed") else "no",
                "yes" if site.get("has_contract_flag") else "no",
                site.get("source") or "",
                site.get("match_method") or "",
                latest.get("label") or "",
                latest.get("link") or "",
            ]
        )
    writer.writerow([])
    writer.writerow(["state", "site_count", "invoice_count", "pdf_count", "load_by_unit"])
    for row in payload.get("by_state") or []:
        load = "; ".join(
            f"{val} {unit}" for unit, val in (row.get("load_by_unit") or {}).items()
        )
        writer.writerow(
            [
                row.get("state"),
                row.get("site_count"),
                row.get("invoice_count"),
                row.get("pdf_count"),
                load,
            ]
        )
    return buf.getvalue().encode("utf-8")


def _pack_parent_id() -> str:
    explicit = (os.getenv("RETAILER_PACKS_FOLDER_ID") or "").strip()
    if explicit:
        return explicit
    from tools.member_folder_drive import MEMBERS_B_FOLDER_ID

    return MEMBERS_B_FOLDER_ID


def create_drive_pack(
    payload: dict[str, Any],
    *,
    user_access_token: str | None = None,
    max_files: int = 250,
) -> dict[str, Any]:
    from tools.member_folder_drive import (
        MemberFolderDriveError,
        copy_file_into_folder,
        find_or_create_folder,
        upload_bytes_to_folder,
    )
    from tools.share_folder import drive_folder_url

    parent_id = _pack_parent_id()
    if not parent_id:
        raise RuntimeError("No Drive parent folder configured for retailer packs")

    stamp = datetime.now().strftime("%Y-%m-%d")
    group = payload.get("utility_group") or "pipeline"
    root_name = "ACES Retailer Packs"
    dated_name = f"{stamp} {group}"
    drive = None
    token = (user_access_token or "").strip()
    if token:
        from tools.member_folder_drive import _user_drive_service

        try:
            drive = _user_drive_service(token)
        except Exception as e:
            logger.warning("[unsigned-pipeline] user Drive client failed: %s", e)
            drive = None
    try:
        packs_id, _ = find_or_create_folder(parent_id, root_name, drive=drive)
        pack_id, created = find_or_create_folder(packs_id, dated_name, drive=drive)
    except MemberFolderDriveError:
        raise

    state_folders: dict[str, str] = {}
    copied = 0
    skipped_missing = 0
    skipped_cap = 0
    errors: list[str] = []
    seen_file_ids: set[str] = set()

    for site in payload.get("sites") or []:
        state = site.get("state") or UNKNOWN_STATE
        if state not in state_folders:
            folder_id, _ = find_or_create_folder(pack_id, state, drive=drive)
            state_folders[state] = folder_id
        dest = state_folders[state]
        for inv in site.get("pdfs") or []:
            link = str(inv.get("link") or "").strip()
            file_id = extract_drive_file_id(link)
            if not file_id:
                skipped_missing += 1
                continue
            if file_id in seen_file_ids:
                continue
            if copied >= max_files:
                skipped_cap += 1
                continue
            seen_file_ids.add(file_id)
            name = _safe_filename(
                [
                    str(site.get("business_name") or ""),
                    str(site.get("identifier") or ""),
                    str(inv.get("label") or ""),
                ]
            )
            try:
                copy_file_into_folder(
                    file_id,
                    dest,
                    name,
                    user_access_token=user_access_token,
                )
                copied += 1
            except Exception as e:
                errors.append(f"{site.get('business_name')} {site.get('identifier')}: {e}")
                logger.warning("[unsigned-pipeline] copy failed: %s", e)

    csv_bytes = summary_csv_bytes(payload)
    summary = upload_bytes_to_folder(
        csv_bytes,
        "_summary.csv",
        pack_id,
        mimetype="text/csv",
        user_access_token=user_access_token,
    )
    return {
        "folder_id": pack_id,
        "folder_url": drive_folder_url(pack_id),
        "folder_created": created,
        "copied": copied,
        "skipped_missing": skipped_missing,
        "skipped_cap": skipped_cap,
        "max_files": max_files,
        "errors": errors[:40],
        "error_count": len(errors),
        "summary_url": summary.get("url"),
        "states": sorted(state_folders.keys(), key=lambda s: STATE_ORDER.index(s) if s in STATE_ORDER else 99),
    }
