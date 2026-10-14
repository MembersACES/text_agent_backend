"""Read SME gas invoices straight from Member ACES Data, tab "5th Sheet - Small Gas".

Replaces the n8n `search-gas-sme-info` webhook. Only the MIRN and Client Name columns are
read to find matches; then just the header row and the matching rows are fetched, so the
whole tab is never pulled.

The response keeps the legacy `gas_sme_invoicedetails` shape the frontend already reads, and
adds `normalised` (services.sme_gas_invoice.summarise_mirn) with the checked figures.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Optional

from services.sme_gas_invoice import normalise_invoice, summarise_mirn
from tools.bne_gas_contracts import mrin_match_kind, normalize_mrin
from tools.business_info import get_sheets_service
from tools.one_month_savings_calculation import MEMBER_ACES_DATA_SHEET_ID

logger = logging.getLogger(__name__)

SME_GAS_TAB = os.getenv("SME_GAS_SHEET_TAB", "5th Sheet - Small Gas")
_LAST_COL = "BZ"
_INDEX_TTL_SECONDS = 60.0
_INDEX_CACHE: dict[str, Any] = {"ts": 0.0, "keys": None, "headers": None}


def _q(tab: str) -> str:
    return "'" + tab.replace("'", "''") + "'"


def _norm_name(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def _load_index(service, force: bool = False) -> tuple[list[str], list[list[str]]]:
    """Header row + [MRIN, Client Name] for every row. Cached briefly."""
    now = time.time()
    if (not force and _INDEX_CACHE["keys"] is not None
            and now - float(_INDEX_CACHE["ts"]) < _INDEX_TTL_SECONDS):
        return _INDEX_CACHE["headers"], _INDEX_CACHE["keys"]
    resp = (
        service.spreadsheets().values().batchGet(
            spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
            ranges=[f"{_q(SME_GAS_TAB)}!A1:{_LAST_COL}1", f"{_q(SME_GAS_TAB)}!A2:B"],
            valueRenderOption="FORMATTED_VALUE",
        ).execute()
    )
    ranges = resp.get("valueRanges", [])
    headers = [str(h).strip() for h in ((ranges[0].get("values") or [[]])[0] if ranges else [])]
    keys = (ranges[1].get("values") or []) if len(ranges) > 1 else []
    _INDEX_CACHE.update({"ts": now, "headers": headers, "keys": keys})
    return headers, keys


def match_rows(keys: list[list[str]], mrin: str = "", business_name: str = "") -> tuple[str, list[int]]:
    """Sheet row numbers (1-based) for an MRIN, or a business name when no MRIN is given.

    MRIN: exact first, then a one-digit checksum difference. A last-digit swap is NOT
    accepted, because on small sites that is usually a different meter.
    Name: exact (case/space insensitive) first, then a unique substring match.
    """
    query = normalize_mrin(mrin)
    if query:
        for kind in ("exact", "checksum"):
            hits = [i + 2 for i, k in enumerate(keys)
                    if k and mrin_match_kind(query, normalize_mrin(k[0])) == kind]
            if hits:
                return kind, hits
        return "none", []
    name = _norm_name(business_name)
    if not name:
        return "none", []
    names = [_norm_name(k[1]) if len(k) > 1 else "" for k in keys]
    exact = [i + 2 for i, n in enumerate(names) if n and n == name]
    if exact:
        return "name_exact", exact
    partial = [i + 2 for i, n in enumerate(names) if n and name in n]
    mrins = {normalize_mrin(keys[r - 2][0]) for r in partial}
    if partial and len(mrins) == 1:
        return "name_partial", partial
    return ("name_ambiguous" if partial else "none"), []


def _fetch_rows(service, headers: list[str], row_numbers: list[int]) -> list[dict[str, Any]]:
    resp = (
        service.spreadsheets().values().batchGet(
            spreadsheetId=MEMBER_ACES_DATA_SHEET_ID,
            ranges=[f"{_q(SME_GAS_TAB)}!A{r}:{_LAST_COL}{r}" for r in row_numbers],
            valueRenderOption="FORMATTED_VALUE",
        ).execute()
    )
    rows = []
    for r, vr in zip(row_numbers, resp.get("valueRanges", [])):
        values = (vr.get("values") or [[]])[0]
        row = {h: (values[i] if i < len(values) else "") for i, h in enumerate(headers) if h}
        row["_sheet_row"] = r
        rows.append(row)
    return rows


def _verified_rows(service, headers, keys, row_numbers, mrin, business_name):
    """Fetch rows and prove each one is the row the index pointed at. None if any row moved."""
    if not row_numbers:
        return []
    rows = _fetch_rows(service, headers, row_numbers)
    for row in rows:
        expected = keys[row["_sheet_row"] - 2]
        got_mrin = normalize_mrin(row.get("MRIN") or row.get("MIRN"))
        if got_mrin != normalize_mrin(expected[0] if expected else ""):
            logger.warning("[sme-gas-sheet] row %s moved: index MRIN %s, fetched %s",
                           row["_sheet_row"], expected[0] if expected else "", got_mrin)
            return None
        if mrin and not mrin_match_kind(normalize_mrin(mrin), got_mrin):
            logger.warning("[sme-gas-sheet] row %s MRIN %s does not match query %s", row["_sheet_row"], got_mrin, mrin)
            return None
    return rows


def legacy_details(inv: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    """The `gas_sme_invoicedetails` shape the portal and n8n already read.

    usage.general_usage_quantity and invoice_review_days are the SAME pair used for the annual
    figure (whole-bill MJ with invoice days, or latest-period MJ with rates-period days), so
    MJ/day is always right. Block rates are the latest price period.
    """
    basis = inv["annual_basis"]
    usage: dict[str, Any] = {"general_usage_quantity": _s(basis["mj"])}
    for b in inv["blocks"]:
        usage[f"block_{b['block']}"] = {
            "consumption": _s(b["mj"]),
            "rate": _s(b["rate_c_per_mj"]),
            "amount": _s(b["amount_aud"]),
            "threshold": b["threshold"]["label"],
        }
    return {
        "mrin": inv["mirn"],
        "client_name": inv["client_name"],
        "retailer": inv["retailer"],
        "site_address": inv["supply_address"],
        "invoice_review_period": raw.get("Invoice Review Period", ""),
        "invoice_review_days": _s(basis["days"]),
        "period_start": inv["invoice_period"]["start"],
        "period_end": inv["invoice_period"]["end"],
        "usage": usage,
        "supply_charge": {
            "rate": _s(inv["supply"]["rate_aud_per_day"]),
            "quantity_days": _s(inv["supply"]["days"]),
            "amount": _s(inv["supply"]["amount_aud"]),
        },
        "total_invoice_cost": _s(inv["invoice_total_ex_gst_aud"]),
        "total_invoice_cost_incl_gst": _s(inv["invoice_total_incl_gst_aud"]),
        "gst": _s(inv["gst_aud"]),
        "invoice_link": inv["webview_link"],
        "invoice_number": inv["invoice_number"],
        "plan_name": inv["plan_name"],
        "issue_date": raw.get("Issue Date", ""),
        "due_date": raw.get("Due Date", ""),
        "read_type": raw.get("Read Type", ""),
        "price_change_on_invoice": inv["price_change_on_invoice"],
    }


def _s(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def get_sme_gas_invoice_from_sheet(business_name: Optional[str] = None, mrin: Optional[str] = None,
                                   service=None) -> dict[str, Any]:
    if not business_name and not mrin:
        return {"error": "Please provide either a business name or mrin"}
    service = service or get_sheets_service()
    if not service:
        return {"error": "Could not create Google Sheets service (check SERVICE_ACCOUNT_*)"}

    headers, keys = _load_index(service)
    match, row_numbers = match_rows(keys, mrin or "", business_name or "")
    if not row_numbers:
        _, keys = _load_index(service, force=True)  # a row may have just been added
        match, row_numbers = match_rows(keys, mrin or "", business_name or "")
    if not row_numbers:
        detail = " (several sites match that name, search by MRIN)" if match == "name_ambiguous" else ""
        return {"error": f"Sorry but couldn't find gas SME invoice information for that business and MRIN{detail}"}

    rows = _verified_rows(service, headers, keys, row_numbers, mrin or "", business_name or "")
    if rows is None:
        # Rows moved between reading the index and fetching (new invoices are written to this tab).
        headers, keys = _load_index(service, force=True)
        match, row_numbers = match_rows(keys, mrin or "", business_name or "")
        rows = _verified_rows(service, headers, keys, row_numbers, mrin or "", business_name or "") or []
    if not rows:
        return {"error": "Sorry but couldn't find gas SME invoice information for that business and MRIN"}
    mrins = {normalize_mrin(r.get("MRIN") or r.get("MIRN")) for r in rows}
    query = normalize_mrin(mrin)
    wanted = query if query in mrins else sorted(mrins)[0]
    summary = summarise_mirn([r for r in rows if normalize_mrin(r.get("MRIN") or r.get("MIRN")) == wanted])
    latest = summary["latest"]
    latest_raw = next(r for r in rows if normalise_invoice(r)["invoice_number"] == latest["invoice_number"])
    logger.info("[sme-gas-sheet] %s match, rows %s, mrin %s", match, row_numbers, summary["mirn"])
    return {
        "gas_sme_invoicedetails": legacy_details(latest, latest_raw),
        "normalised": summary,
        "source": {"sheet_id": MEMBER_ACES_DATA_SHEET_ID, "tab": SME_GAS_TAB,
                   "rows": row_numbers, "match": match, "mirn_count": len(mrins)},
    }
