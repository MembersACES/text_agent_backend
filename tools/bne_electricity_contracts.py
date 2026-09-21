"""Signed C&I Electricity contract periods from Member ACES Data (17th Sheet).

Used by Alinta electricity EF lodgement: look up one or more NMIs and return
matching contract rows. Invoice NMIs can be one digit off the sheet (checksum).
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Optional

from tools.bne_gas_contracts import mrin_match_kind, parse_sheet_number
from tools.business_info import get_sheets_service
from tools.one_month_savings_calculation import MEMBER_ACES_DATA_SHEET_ID

logger = logging.getLogger(__name__)

SIGNED_CI_E_TAB = "17th Sheet - C&I E contracts"
_CACHE_TTL_SECONDS = 300.0
_CACHE: dict[str, Any] = {"ts": 0.0, "rows": None}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "nmi": ("nmi",),
    "company_name": ("company name", "business name", "linked business name"),
    "supply_address": ("supply address", "site address", "address"),
    "contract_start_date": ("contract start date", "start date"),
    "contract_end_date": ("contract end date", "end date"),
    "period_start_date": ("period start date",),
    "period_end_date": ("period end date",),
    "peak_rate": ("peak rate (c/kwh)", "peak rate", "peak contract rate (c/kwh)"),
    "off_peak_rate": (
        "off-peak rate (c/kwh)",
        "off peak rate (c/kwh)",
        "off-peak rate",
        "off peak rate",
        "off-peak contract rate (c/kwh)",
    ),
    "shoulder_rate": (
        "shoulder rate (c/kwh)",
        "shoulder rate",
        "shoulder contract rate (c/kwh)",
    ),
    "annual_kwh": (
        "annual kwh",
        "annual consumption (kwh)",
        "contract target consumption (kwh)",
        "target consumption (kwh)",
    ),
    "period_name": ("period name",),
    "retailer": ("retailer",),
    "webview_link": ("webview link",),
}

_NMI_KEEP_RE = re.compile(r"[^A-Z0-9]")
_PERIOD_NUM_RE = re.compile(r"(\d+)")


def normalize_nmi(raw: Any) -> str:
    """Uppercase NMI. Keeps Victorian letter prefixes (e.g. VEEE0WPKWT).

    Rejects heading words (ENGAGEMENT, ELECTRICITY, …): a real NMI always
    contains a digit. Numeric NMIs do not start with 0 (that is a phone number).
    """
    if raw is None:
        return ""
    s = str(raw).strip().replace(",", "").replace(" ", "").replace("\u00a0", "")
    if s.endswith(".0") and s[:-2].replace("-", "").isdigit():
        s = s[:-2]
    s = _NMI_KEEP_RE.sub("", s.upper())
    if not (10 <= len(s) <= 11):
        return ""
    if s.isalpha() or not any(ch.isdigit() for ch in s):
        return ""
    if s.isdigit():
        return "" if s.startswith("0") else s
    if s[0].isalpha():
        return s
    return ""


def nmi_match_kind(query: str, sheet: str) -> Optional[str]:
    q = normalize_nmi(query)
    s = normalize_nmi(sheet)
    if not q or not s:
        return None
    if q == s:
        return "exact"
    if q.isalpha() or s.isalpha() or not (q.isdigit() and s.isdigit()):
        return "exact" if q == s else None
    return mrin_match_kind(q, s)


def select_matched_nmis(query_nmi: str, sheet_nmis: list[str]) -> tuple[str, list[str]]:
    query = normalize_nmi(query_nmi)
    unique: list[str] = []
    seen: set[str] = set()
    for raw in sheet_nmis:
        nmi = normalize_nmi(raw)
        if not nmi or nmi in seen:
            continue
        seen.add(nmi)
        unique.append(nmi)
    if not query:
        return "none", []
    for kind in ("exact", "checksum", "one_digit"):
        matched = [n for n in unique if nmi_match_kind(query, n) == kind]
        if matched:
            return kind, matched
    return "none", []


def _norm_header(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def canonicalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    by_norm = {_norm_header(k): v for k, v in raw.items() if str(k).strip()}
    out: dict[str, Any] = {}
    for key, aliases in _HEADER_ALIASES.items():
        found = ""
        for alias in aliases:
            if alias in by_norm:
                found = by_norm[alias]
                break
        out[key] = found
    out["nmi"] = normalize_nmi(out.get("nmi"))
    return out


def _display(raw: Any) -> str:
    if raw is None:
        return ""
    text = str(raw).strip()
    if text.lower() == "null":
        return ""
    return text


def _period_sort_key(period: dict[str, Any]) -> tuple[int, str]:
    name = str(period.get("period_name") or "")
    match = _PERIOD_NUM_RE.search(name)
    number = int(match.group(1)) if match else 999
    return (number, str(period.get("period_start_date") or ""))


def _build_period(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "period_name": _display(row.get("period_name")),
        "period_start_date": _display(row.get("period_start_date")),
        "period_end_date": _display(row.get("period_end_date")),
        "peak_rate_c_kwh": parse_sheet_number(row.get("peak_rate")),
        "off_peak_rate_c_kwh": parse_sheet_number(row.get("off_peak_rate")),
        "shoulder_rate_c_kwh": parse_sheet_number(row.get("shoulder_rate")),
        "annual_kwh": parse_sheet_number(row.get("annual_kwh")),
    }


def _build_contract(nmi: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = rows[0]
    periods = [_build_period(row) for row in rows]
    periods.sort(key=_period_sort_key)
    return {
        "nmi": nmi,
        "company_name": _display(first.get("company_name")),
        "supply_address": _display(first.get("supply_address")),
        "contract_start_date": _display(first.get("contract_start_date")),
        "contract_end_date": _display(first.get("contract_end_date")),
        "retailer": _display(first.get("retailer")),
        "webview_link": _display(first.get("webview_link")),
        "row_count": len(rows),
        "periods": periods,
    }


def lookup_bne_electricity_contract_from_rows(
    query_nmi: str, rows: list[dict[str, Any]]
) -> dict[str, Any]:
    canonical = [canonicalize_row(row) for row in rows]
    kind, matched = select_matched_nmis(query_nmi, [row.get("nmi", "") for row in canonical])
    matched_set = set(matched)
    grouped: dict[str, list[dict[str, Any]]] = {nmi: [] for nmi in matched}
    for row in canonical:
        nmi = row.get("nmi") or ""
        if nmi in matched_set:
            grouped[nmi].append(row)
    contracts = [_build_contract(nmi, grouped[nmi]) for nmi in matched if grouped[nmi]]
    return {
        "query_nmi": str(query_nmi or "").strip(),
        "normalized_nmi": normalize_nmi(query_nmi),
        "match_kind": kind,
        "sheet_id": MEMBER_ACES_DATA_SHEET_ID,
        "sheet_tab": SIGNED_CI_E_TAB,
        "contracts": contracts,
    }


def _escape_sheet_title(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def _read_signed_ci_e_rows(force: bool = False) -> list[dict[str, Any]]:
    now = time.time()
    cached = _CACHE.get("rows")
    if (not force) and cached is not None and (now - float(_CACHE["ts"])) < _CACHE_TTL_SECONDS:
        return cached

    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service (check SERVICE_ACCOUNT_*)")

    tab = SIGNED_CI_E_TAB
    resp = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
            range=f"{_escape_sheet_title(tab)}!A1:AZ20000",
            valueRenderOption="FORMATTED_VALUE",
        )
        .execute()
    )
    values = resp.get("values", [])
    rows: list[dict[str, Any]] = []
    if values:
        headers = [str(h).strip() for h in values[0]]
        for raw in values[1:]:
            obj: dict[str, Any] = {}
            for i, header in enumerate(headers):
                if not header:
                    continue
                value = raw[i] if i < len(raw) else ""
                if isinstance(value, str) and value.strip().lower() == "null":
                    value = ""
                obj[header] = value
            rows.append(obj)

    _CACHE.update({"ts": now, "rows": rows})
    logger.info("[bne-electricity-contracts] loaded %d rows from %r", len(rows), tab)
    return rows


def lookup_bne_electricity_contract(nmi: str) -> dict[str, Any]:
    rows = _read_signed_ci_e_rows()
    return lookup_bne_electricity_contract_from_rows(nmi, rows)


def lookup_bne_electricity_contracts(nmis: list[str]) -> dict[str, Any]:
    """Look up several NMIs and merge contract hits. Retention if any NMI matches."""
    seen_nmi: set[str] = set()
    contracts: list[dict[str, Any]] = []
    kinds: list[str] = []
    normalized = [normalize_nmi(n) for n in nmis]
    normalized = [n for n in normalized if n]
    rows = _read_signed_ci_e_rows() if normalized else []
    for nmi in normalized:
        result = lookup_bne_electricity_contract_from_rows(nmi, rows)
        kinds.append(str(result.get("match_kind") or "none"))
        for contract in result.get("contracts") or []:
            key = str(contract.get("nmi") or "")
            if key and key in seen_nmi:
                continue
            if key:
                seen_nmi.add(key)
            contracts.append(contract)
    match_kind = "none"
    for preferred in ("exact", "checksum", "one_digit"):
        if preferred in kinds:
            match_kind = preferred
            break
    if not contracts:
        match_kind = "none"
    return {
        "query_nmis": normalized,
        "normalized_nmis": normalized,
        "match_kind": match_kind,
        "sheet_id": MEMBER_ACES_DATA_SHEET_ID,
        "sheet_tab": SIGNED_CI_E_TAB,
        "contracts": contracts,
    }
