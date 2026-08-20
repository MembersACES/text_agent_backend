"""Signed C&I Gas contract periods from Member ACES Data (13th Sheet).

Used by Base 2 B&E Gas: look up the clicked MRIN and return each contract period
and rate. Invoice MRINs are often one digit off the sheet (checksum / last digit).
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Optional

from tools.business_info import get_sheets_service
from tools.one_month_savings_calculation import MEMBER_ACES_DATA_SHEET_ID

logger = logging.getLogger(__name__)

SIGNED_CI_GAS_TAB = "13th Sheet - Signed C&I Gas"
SIGNED_CI_GAS_GID = 539274584
_CACHE_TTL_SECONDS = 300.0
_CACHE: dict[str, Any] = {"ts": 0.0, "rows": None}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "mrin": ("mrin",),
    "company_name": ("company name",),
    "supply_address": ("supply address",),
    "contract_start_date": ("contract start date",),
    "contract_end_date": ("contract end date",),
    "period_start_date": ("period start date",),
    "period_end_date": ("period end date",),
    "energy_rate": ("energy rate ($/gj)", "energy rate"),
    "cpq_gj": ("cpq (gj)", "cpq"),
    "maq_gj": ("maq (gj)",),
    "maq_pct": ("maq (%)", "maq %"),
    "mdq_gj_per_day": ("mdq (gj/day)", "mdq"),
    "mhq_gj_per_hour": ("mhq (gj/hour)", "mhq"),
    "overrun_rate": ("overrun rate ($/gj)", "overrun rate"),
    "excess_cpq_rate": ("excess cpq rate ($/gj)", "excess cpq rate"),
    "veec_rate": ("veec rate ($/certificate)", "veec rate"),
    "period_name": ("period name",),
    "retailer": ("retailer",),
    "webview_link": ("webview link",),
}

_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")
_PERIOD_NUM_RE = re.compile(r"(\d+)")


def normalize_mrin(raw: Any) -> str:
    """Digits-only MRIN. Strips spaces, commas, and trailing .0 from Sheets numbers."""
    if raw is None:
        return ""
    s = str(raw).strip().replace(",", "").replace(" ", "").replace("\u00a0", "")
    if s.endswith(".0") and s[:-2].replace("-", "").isdigit():
        s = s[:-2]
    return re.sub(r"\D", "", s)


def parse_sheet_number(raw: Any) -> Optional[float]:
    """Parse '17.80 $/GJ', '4,313 GJ', '80%' into a float. Empty -> None."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip().replace(",", "")
    if not text:
        return None
    match = _NUM_RE.search(text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def mrin_match_kind(query: str, sheet: str) -> Optional[str]:
    """How a sheet MRIN relates to the invoice MRIN.

    exact     — same digits
    checksum  — one trailing digit added or removed (common check-digit mismatch)
    one_digit — same length, only the last digit differs
    """
    if not query or not sheet:
        return None
    if query == sheet:
        return "exact"
    if len(sheet) == len(query) + 1 and sheet.startswith(query):
        return "checksum"
    if len(query) == len(sheet) + 1 and query.startswith(sheet):
        return "checksum"
    if len(query) == len(sheet) >= 2 and query[:-1] == sheet[:-1] and query[-1] != sheet[-1]:
        return "one_digit"
    return None


def select_matched_mrins(query_mrin: str, sheet_mrins: list[str]) -> tuple[str, list[str]]:
    """Pick sheet MRINs for a query. Exact wins, then checksum, then last-digit."""
    query = normalize_mrin(query_mrin)
    unique: list[str] = []
    seen: set[str] = set()
    for raw in sheet_mrins:
        mrin = normalize_mrin(raw)
        if not mrin or mrin in seen:
            continue
        seen.add(mrin)
        unique.append(mrin)
    if not query:
        return "none", []
    for kind in ("exact", "checksum", "one_digit"):
        matched = [m for m in unique if mrin_match_kind(query, m) == kind]
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
    out["mrin"] = normalize_mrin(out.get("mrin"))
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
    energy_display = _display(row.get("energy_rate"))
    return {
        "period_name": _display(row.get("period_name")),
        "period_start_date": _display(row.get("period_start_date")),
        "period_end_date": _display(row.get("period_end_date")),
        "energy_rate_per_gj": parse_sheet_number(row.get("energy_rate")),
        "energy_rate_display": energy_display,
        "cpq_gj": parse_sheet_number(row.get("cpq_gj")),
        "maq_gj": parse_sheet_number(row.get("maq_gj")),
        "maq_pct": parse_sheet_number(row.get("maq_pct")),
        "mdq_gj_per_day": parse_sheet_number(row.get("mdq_gj_per_day")),
        "mhq_gj_per_hour": parse_sheet_number(row.get("mhq_gj_per_hour")),
        "overrun_rate_per_gj": parse_sheet_number(row.get("overrun_rate")),
        "excess_cpq_rate_per_gj": parse_sheet_number(row.get("excess_cpq_rate")),
        "veec_rate": parse_sheet_number(row.get("veec_rate")),
    }


def _build_contract(mrin: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = rows[0]
    periods = [_build_period(row) for row in rows]
    periods.sort(key=_period_sort_key)
    return {
        "mrin": mrin,
        "company_name": _display(first.get("company_name")),
        "supply_address": _display(first.get("supply_address")),
        "contract_start_date": _display(first.get("contract_start_date")),
        "contract_end_date": _display(first.get("contract_end_date")),
        "retailer": _display(first.get("retailer")),
        "webview_link": _display(first.get("webview_link")),
        "row_count": len(rows),
        "periods": periods,
    }


def lookup_bne_gas_contract_from_rows(query_mrin: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Pure lookup against already-read sheet rows (header-keyed or canonical)."""
    canonical = [canonicalize_row(row) for row in rows]
    kind, matched = select_matched_mrins(query_mrin, [row.get("mrin", "") for row in canonical])
    matched_set = set(matched)
    grouped: dict[str, list[dict[str, Any]]] = {mrin: [] for mrin in matched}
    for row in canonical:
        mrin = row.get("mrin") or ""
        if mrin in matched_set:
            grouped[mrin].append(row)
    contracts = [_build_contract(mrin, grouped[mrin]) for mrin in matched if grouped[mrin]]
    return {
        "query_mrin": str(query_mrin or "").strip(),
        "normalized_mrin": normalize_mrin(query_mrin),
        "match_kind": kind,
        "sheet_id": MEMBER_ACES_DATA_SHEET_ID,
        "sheet_tab": SIGNED_CI_GAS_TAB,
        "sheet_gid": SIGNED_CI_GAS_GID,
        "contracts": contracts,
    }


def _escape_sheet_title(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def _read_signed_ci_gas_rows(force: bool = False) -> list[dict[str, Any]]:
    now = time.time()
    cached = _CACHE.get("rows")
    if (not force) and cached is not None and (now - float(_CACHE["ts"])) < _CACHE_TTL_SECONDS:
        return cached

    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service (check SERVICE_ACCOUNT_*)")

    tab = SIGNED_CI_GAS_TAB
    resp = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
            range=f"{_escape_sheet_title(tab)}!A1:Z20000",
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
    logger.info("[bne-gas-contracts] loaded %d rows from %r", len(rows), tab)
    return rows


def lookup_bne_gas_contract(mrin: str) -> dict[str, Any]:
    rows = _read_signed_ci_gas_rows()
    return lookup_bne_gas_contract_from_rows(mrin, rows)
