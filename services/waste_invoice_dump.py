"""Read-only reader for the RAW waste invoice dump sheet
('Member ACES Data' -> '7th Sheet - Waste').

Every waste invoice run is dumped here verbatim — NO cleaning, NO dedup. Each row carries the
per-bin schedule, Invoice Total Amount, and a Drive 'Webview Link' (the invoice PDF). A blank
Webview Link means no confirmed Drive file for that account+period -> a missing-invoice discrepancy.
Keyed by account / customer number. Rows are returned with the sheet's header names verbatim so the
frontend can compute the expected cost from the bins itself.
"""
from __future__ import annotations

import re
import time
import logging
from typing import Any

from tools.business_info import get_sheets_service

WASTE_DUMP_SHEET_ID = "1ozwxJjqBQE3fJeMHmsXzPFCsekupwXZ3A7F0jstOfVw"
WASTE_DUMP_TAB = "7th Sheet - Waste"
ACCOUNT_HEADER = "Account Number or Customer Number"


def _norm(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def read_waste_invoice_rows(account: str) -> dict:
    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service (check SERVICE_ACCOUNT_*)")

    resp = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=WASTE_DUMP_SHEET_ID,
            range=f"'{WASTE_DUMP_TAB}'!A1:BZ20000",
            valueRenderOption="FORMATTED_VALUE",  # dates as dd/mm/yyyy, numbers as display strings
        )
        .execute()
    )
    values = resp.get("values", [])
    if not values:
        return {"rows": [], "total_count": 0, "account": account, "sheet_id": WASTE_DUMP_SHEET_ID}

    headers = [str(h).strip() for h in values[0]]
    acc_idx = next((i for i, h in enumerate(headers) if _norm(h) == _norm(ACCOUNT_HEADER)), None)
    target = _norm(account)
    logging.info("[waste-invoice-rows] account=%r normalized=%r total_rows=%d account_col_idx=%s has_webview=%s",
                 account, target, len(values) - 1, acc_idx, any(_norm(h) == "webview link" for h in headers))

    rows: list[dict] = []
    for raw in values[1:]:
        if acc_idx is None or acc_idx >= len(raw):
            continue
        if _norm(raw[acc_idx]) != target:
            continue
        obj: dict = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            v = raw[i] if i < len(raw) else ""
            if isinstance(v, str) and v.strip().lower() == "null":
                v = ""
            obj[h] = v
        rows.append(obj)

    logging.info("[waste-invoice-rows] account=%r matched_rows=%d with_pdf=%d",
                 account, len(rows), sum(1 for r in rows if str(r.get("Webview Link", "")).strip()))
    return {
        "rows": rows,
        "total_count": len(rows),
        "account": account,
        "sheet_id": WASTE_DUMP_SHEET_ID,
        "sheet_tab": WASTE_DUMP_TAB,
    }


# ── Cached index for enrichment (build_entity_site_detail) ────────────────────
# The waste getSite enrichment runs per-meter; reading the whole 20k-row dump each
# time would be wasteful. Read the sheet ONCE, index by normalized account, cache
# for a short TTL. Slim output: just what the Disc Engine needs to render PDF links
# + the missing-invoice signal (NOT the full bin schedule — its rows already carry
# invoice data; it only lacks the Drive link).
_DUMP_CACHE: dict[str, Any] = {"ts": 0.0, "index": None}
_DUMP_TTL = 300.0  # seconds

# Slim field map (verbatim dump headers -> stable output keys).
_DOC_FIELDS = {
    "invoice_number": "Invoice Number",
    "invoice_date": "Invoice Date",
    "invoice_total": "Invoice Total Amount",
    "provider": "Provider",
    "review_period": "Review Period",
    "webview_link": "Webview Link",
}


def _load_dump_index(force: bool = False) -> dict[str, list[dict]]:
    """Read the whole waste dump ONCE and index slim docs by normalized account. Cached (TTL)."""
    now = time.time()
    cached = _DUMP_CACHE["index"]
    if (not force) and cached is not None and (now - float(_DUMP_CACHE["ts"])) < _DUMP_TTL:
        return cached

    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service (check SERVICE_ACCOUNT_*)")
    resp = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=WASTE_DUMP_SHEET_ID,
            range=f"'{WASTE_DUMP_TAB}'!A1:BZ20000",
            valueRenderOption="FORMATTED_VALUE",
        )
        .execute()
    )
    values = resp.get("values", [])
    index: dict[str, list[dict]] = {}
    if values:
        headers = [str(h).strip() for h in values[0]]
        col = {key: next((i for i, h in enumerate(headers) if _norm(h) == _norm(name)), None)
               for key, name in _DOC_FIELDS.items()}
        acc_idx = next((i for i, h in enumerate(headers) if _norm(h) == _norm(ACCOUNT_HEADER)), None)
        if acc_idx is not None:
            for raw in values[1:]:
                if acc_idx >= len(raw):
                    continue
                acct = _norm(raw[acc_idx])
                if not acct:
                    continue
                doc: dict[str, Any] = {}
                for key, idx in col.items():
                    v = raw[idx] if (idx is not None and idx < len(raw)) else ""
                    if isinstance(v, str) and v.strip().lower() == "null":
                        v = ""
                    doc[key] = v
                doc["missing"] = not str(doc.get("webview_link", "")).strip()
                index.setdefault(acct, []).append(doc)
    _DUMP_CACHE.update({"ts": now, "index": index})
    logging.info("[waste-dump-index] built: %d accounts (sheet=%s)", len(index), WASTE_DUMP_SHEET_ID)
    return index


def waste_documents_for_account(account: str) -> dict:
    """Slim per-account waste invoice documents (PDF links + missing flag) for payload enrichment.
    Returns {documents, total_count, with_pdf, missing_count, account, sheet_id}. {} on failure."""
    try:
        idx = _load_dump_index()
        docs = idx.get(_norm(account), [])
        with_pdf = sum(1 for d in docs if not d["missing"])
        return {
            "documents": docs,
            "total_count": len(docs),
            "with_pdf": with_pdf,
            "missing_count": len(docs) - with_pdf,
            "account": account,
            "sheet_id": WASTE_DUMP_SHEET_ID,
        }
    except Exception as e:  # pragma: no cover - defensive
        logging.info("[waste-documents] lookup failed for %r: %s", account, e)
        return {"documents": [], "total_count": 0, "with_pdf": 0, "missing_count": 0,
                "account": account, "sheet_id": WASTE_DUMP_SHEET_ID, "error": str(e)}


# ── Generic per-utility invoice links (all tabs of the Member ACES Data workbook) ─────────────────
# Same workbook as the waste dump; one tab per utility, each with a 'Webview Link' column (the invoice
# PDF). The KEY column (the linkage identifier) differs per tab — NMI for electricity, MRIN for gas,
# account number for waste/oil/water. Map: utility_type -> (tab, key column header, label column header).
MEMBER_DATA_SHEET_ID = WASTE_DUMP_SHEET_ID
UTILITY_TAB_CONFIG: dict[str, tuple[str, tuple[str, ...], str]] = {
    "C&I Electricity": ("2nd Sheet - Electricity details from the invoice", ("NMI",), "Invoice Review Period"),
    "SME Electricity": ("3rd Sheet - SME Electricity", ("NMI",), "Invoice Review Period"),
    "C&I Gas": ("4th Sheet - Large Gas", ("MRIN",), "Invoice Review Period"),
    "SME Gas": ("5th Sheet - Small Gas", ("MRIN",), "Invoice Review Period"),
    "Water": ("6th Sheet - Water", ("Account Number",), "Billing Period"),
    "Waste": ("7th Sheet - Waste", ("Account Number or Customer Number",), "Invoice Number"),
    # Oil / Grease Trap: account codes are inconsistent (change per invoice / often blank), so ACES
    # keys these by Client Name. We match either — try the name first, fall back to the account code.
    "Oil": ("8th Sheet - Oil", ("Client Name", "Account Number / Customer Code"), "Invoice Number"),
    "Grease Trap": ("9th Sheet - Grease Trap", ("Client Name", "Account Number / Customer Code"), "Invoice Number"),
    "Cleaning": ("14th Sheet - Cleaning Invoices", ("client_name",), "invoice_number"),
}

_TAB_CACHE: dict[str, dict] = {}  # tab name -> {ts, rows (list of header-keyed dicts)}
_TAB_TTL = 300.0


def _read_tab_rows(tab: str) -> list[dict]:
    """Read one tab of the Member ACES Data workbook into header-keyed row dicts. Cached per tab (TTL)."""
    now = time.time()
    cached = _TAB_CACHE.get(tab)
    if cached and (now - float(cached["ts"])) < _TAB_TTL:
        return cached["rows"]
    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service (check SERVICE_ACCOUNT_*)")
    resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=MEMBER_DATA_SHEET_ID, range=f"'{tab}'!A1:CZ20000", valueRenderOption="FORMATTED_VALUE")
        .execute()
    )
    values = resp.get("values", [])
    rows: list[dict] = []
    if values:
        headers = [str(h).strip() for h in values[0]]
        for raw in values[1:]:
            obj: dict = {}
            for i, h in enumerate(headers):
                if not h:
                    continue
                v = raw[i] if i < len(raw) else ""
                if isinstance(v, str) and v.strip().lower() == "null":
                    v = ""
                obj[h] = v
            rows.append(obj)
    _TAB_CACHE[tab] = {"ts": now, "rows": rows}
    return rows


def read_utility_invoice_links(utility_type: str, identifier: str) -> dict:
    """Invoice PDF links for ONE (utility_type, identifier) from that utility's tab. Read-only.
    Matches `identifier` against the tab's key column (NMI/MRIN/account). Returns
    {supported, documents:[{label,link,missing}], total_count, with_pdf, missing_count, ...}."""
    cfg = UTILITY_TAB_CONFIG.get(utility_type)
    if not cfg:
        return {"supported": False, "utility_type": utility_type, "identifier": identifier,
                "documents": [], "total_count": 0, "with_pdf": 0, "missing_count": 0}
    tab, key_cols, label_col = cfg
    try:
        rows = _read_tab_rows(tab)
    except Exception as e:  # pragma: no cover - defensive
        logging.info("[utility-invoice-links] read failed for %s: %s", tab, e)
        return {"supported": True, "utility_type": utility_type, "identifier": identifier, "tab": tab,
                "documents": [], "total_count": 0, "with_pdf": 0, "missing_count": 0, "error": str(e)}
    target = _norm(identifier)
    docs: list[dict] = []
    for r in rows:
        # Match the identifier against ANY of the candidate key columns (account / NMI / client name).
        if not any(_norm(r.get(kc, "")) == target for kc in key_cols):
            continue
        link = str(r.get("Webview Link", "")).strip()
        label = str(r.get(label_col, "")).strip() or str(r.get("Invoice Date", "")).strip()
        docs.append({"label": label, "link": link, "missing": not link})
    with_pdf = sum(1 for d in docs if not d["missing"])
    logging.info("[utility-invoice-links] %s id=%r tab=%r keys=%r matched=%d with_pdf=%d",
                 utility_type, identifier, tab, key_cols, len(docs), with_pdf)
    return {
        "supported": True,
        "utility_type": utility_type,
        "identifier": identifier,
        "tab": tab,
        "key_cols": list(key_cols),
        "documents": docs,
        "total_count": len(docs),
        "with_pdf": with_pdf,
        "missing_count": len(docs) - with_pdf,
        "sheet_id": MEMBER_DATA_SHEET_ID,
    }
