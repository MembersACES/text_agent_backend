"""Extract a signed Alinta C&I gas engagement form and draft an Agreement Request email."""

from __future__ import annotations

import io
import logging
import os
import re
from datetime import datetime
from typing import Any, Optional

import requests

from tools.bne_gas_contracts import lookup_bne_gas_contract, normalize_mrin, parse_sheet_number
from tools.pdf_scan_vision import collect_page_images, pdf_to_text, vision_extract_fields

logger = logging.getLogger(__name__)

N8N_QUOTE_WEBHOOK_URL = "https://membersaces.app.n8n.cloud/webhook/supplier-quote-request"
N8N_EMAIL_SUPPLIER_URL = "https://membersaces.app.n8n.cloud/webhook/email-supplier"
N8N_AGREEMENT_TYPE = "alinta_agreement_request"
TEST_RECIPIENT_EMAIL = "data.quote@fornrg.com"
DEFAULT_GAS_EF_FOLDER_ID = "1rSZIYdEsPviuyC4xmwOuPqI8gte8hpHA"
INVOICE_API_PROCESS_EF_URL = (
    os.getenv("ACES_INVOICE_API_PROCESS_EF_URL")
    or "https://aces-invoice-api-672026052958.australia-southeast2.run.app/v1/ef/process-ef"
)
DEFAULT_RETAIL_SERVICE_CHARGE = "1.99"
DEFAULT_MIN_CPQ_PCT = 80.0

EXTRACT_KEYS: tuple[str, ...] = (
    "company_name",
    "acn_abn",
    "address",
    "tel",
    "contact_name",
    "email",
    "mirn",
    "start_date",
    "end_date",
    "price_per_gj",
    "commission_per_gj",
    "cpq_gj",
    "min_cpq_gj",
    "min_cpq_pct",
    "mdq_gj",
    "retail_service_charge",
    "overrun_rate",
    "excess_cpq_price",
    "is_signed",
    "signed_date",
)

_VISION_PROMPT = """Extract fields from this Alinta Energy C&I gas engagement form / agreement.
The PDF is often a scan of a signed paper copy. Read printed text AND handwriting.

Return a JSON object with exactly these keys (use "" if not present; never invent):
company_name, acn_abn, address, tel, contact_name, email, mirn,
start_date, end_date, price_per_gj, commission_per_gj, cpq_gj, min_cpq_gj,
min_cpq_pct, mdq_gj, retail_service_charge, overrun_rate, excess_cpq_price,
is_signed, signed_date.

Rules:
- company_name is the customer / member, never Alinta Energy / ACES / Carbon Zero / EGB.
- mirn: digits only (Meter Installation Registration Number / MIRN / MRIN / DPI).
- acn_abn: digits only (11-digit ABN or 9-digit ACN).
- Dates: keep as written (prefer D/M/YYYY). If Start Date or End Date is TBC, TBA, TBD, or blank, return that exactly (e.g. "TBC"). Never invent a calendar date.
- price_per_gj is the energy / Period 1 Rate ($/GJ), numbers only (e.g. 14.70).
- commission_per_gj is the broker / ACES / EGB commission in $/GJ.
  On ACES Engagement Form - Gas Agreement this is usually labelled
  "Distributor Rebate: $X per GJ" (the rate is in the LABEL; the value cell may
  say "Included"). In that case commission_per_gj is X (e.g. 3), never "Included"
  and never the Period 1 Rate. Also accept Brokerage, Rebate, Commission.
- cpq_gj is Contract Period Quantity in GJ. min_cpq_gj is the minimum CPQ in GJ.
- min_cpq_pct is the minimum as a percent of CPQ (e.g. 80).
- mdq_gj is Maximum Daily Quantity in GJ.
- retail_service_charge is $/MIRN/day if shown.
- overrun_rate and excess_cpq_price are $/GJ.
- is_signed: "true" if there is a handwritten or digital signature, else "false".
- contact_name, email, tel: the customer contact, not Alinta / ACES / EGB.
"""

_MIRN_IN_NAME_RE = re.compile(r"(?:MIRN|MRIN)\s*[:#]?\s*(\d{8,13})", re.I)
_FILE_ID_RE = re.compile(r"/d/([a-zA-Z0-9_-]{20,})")
_FILE_ID_QUERY_RE = re.compile(r"[?&]id=([a-zA-Z0-9_-]{20,})")
_FOLDER_ID_RE = re.compile(r"/folders/([a-zA-Z0-9_-]{10,})")
_OPEN_DATE_RE = re.compile(r"^(tbc|tba|tbd|to be confirmed)$", re.I)
_REBATE_AMOUNT_RE = re.compile(
    r"(?:distributor\s+)?rebate[^.\n]{0,48}?\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:/\s*|per\s+)?gj",
    re.I,
)
_COMMISSION_AMOUNT_RE = re.compile(
    r"commission[^.\n]{0,24}?\$?\s*([0-9]+(?:\.[0-9]+)?)",
    re.I,
)
_NOT_A_RATE = frozenset({"included", "yes", "y", "true", "na", "n/a", "-"})
MAX_COMMISSION_PER_GJ = 50.0


def mirn_from_filename(filename: str) -> str:
    if not filename:
        return ""
    match = _MIRN_IN_NAME_RE.search(filename)
    if match:
        return normalize_mrin(match.group(1))
    digits = re.sub(r"\D", "", filename)
    if 10 <= len(digits) <= 12:
        return digits
    return ""


def drive_file_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    match = _FILE_ID_RE.search(text)
    if match:
        return match.group(1)
    match = _FILE_ID_QUERY_RE.search(text)
    if match:
        return match.group(1)
    token = text.split("/", 1)[0].strip()
    if re.fullmatch(r"[a-zA-Z0-9_-]{20,}", token):
        return token
    return ""


def drive_folder_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = _FOLDER_ID_RE.search(text)
    if match:
        return match.group(1)
    if re.fullmatch(r"[a-zA-Z0-9_-]{10,}", text):
        return text
    return ""


def _clean(value: Any) -> str:
    s = str(value or "").replace("\u00a0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    if s.lower() in {"", "null", "none", "n/a", "-", "—"}:
        return ""
    return s


def _plausible_commission(number: float) -> bool:
    return 0 < number <= MAX_COMMISSION_PER_GJ


def normalize_commission_per_gj(value: Any, extra_text: str = "") -> str:
    """ACES gas EFs often put the $/GJ in 'Distributor Rebate: $3 per GJ' with value 'Included'."""
    candidates: list[str] = []
    raw = _clean(value)
    if raw and raw.lower() not in _NOT_A_RATE:
        candidates.append(raw)
    blob = f"{raw}\n{extra_text or ''}"
    for rx in (_COMMISSION_AMOUNT_RE, _REBATE_AMOUNT_RE):
        for match in rx.finditer(blob):
            candidates.append(match.group(1))
    for item in candidates:
        number = parse_sheet_number(item)
        if number is not None and _plausible_commission(number):
            return str(number)
    return ""


def _digits(value: Any) -> str:
    return re.sub(r"\D", "", _clean(value))


def _parse_bool(value: Any) -> bool:
    s = _clean(value).lower()
    return s in {"true", "yes", "1", "signed"}


def _fmt_money(value: Any, prefix: str = "$") -> str:
    number = parse_sheet_number(value)
    if number is None:
        raw = _clean(value)
        return raw
    if abs(number - round(number)) < 1e-9:
        if abs(number) >= 1000:
            return f"{prefix}{number:,.0f}" if prefix else f"{number:,.0f}"
    return f"{prefix}{number:,.2f}" if prefix else f"{number:,.2f}"


def _fmt_qty(value: Any) -> str:
    number = parse_sheet_number(value)
    if number is None:
        return _clean(value)
    if abs(number - round(number)) < 1e-9:
        return f"{int(round(number)):,}"
    return f"{number:,.2f}"


def _fmt_pct(value: Any) -> str:
    number = parse_sheet_number(value)
    if number is None:
        raw = _clean(value)
        if raw and not raw.endswith("%"):
            return f"{raw}%"
        return raw
    if number <= 1 and number > 0:
        number = number * 100
    if abs(number - round(number)) < 1e-9:
        return f"{int(round(number))}%"
    return f"{number:.1f}%"


def _fmt_date(value: Any) -> str:
    s = _clean(value)
    if not s:
        return ""
    if _OPEN_DATE_RE.match(s):
        return s.upper() if len(s) <= 3 else s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        year, month, day = s.split("-")
        return f"{int(day)}/{int(month)}/{year}"
    match = re.fullmatch(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2,4})", s)
    if match:
        day, month, year = match.group(1), match.group(2), match.group(3)
        if len(year) == 2:
            year = f"20{year}"
        return f"{int(day)}/{int(month)}/{year}"
    return s


def _display(value: Any) -> str:
    return _clean(value)


def _empty_extract() -> dict[str, str]:
    return {key: "" for key in EXTRACT_KEYS}


def _parse_ef_text(text: str) -> dict[str, str]:
    out = _empty_extract()
    if not text or len(text.strip()) < 20:
        return out

    def after(label: str) -> str:
        match = re.search(rf"{label}\s*[:#]\s*(.+)", text, re.I)
        if not match:
            return ""
        return _clean(match.group(1).split("\n")[0])

    out["company_name"] = after("Company Name") or after("Customer Name") or after("Account Name")
    out["acn_abn"] = _digits(after("ABN") or after("ACN") or after("ACN/ABN"))
    out["address"] = after("Address") or after("Site Address") or after("Supply Address")
    out["tel"] = after("Tel") or after("Telephone") or after("Phone")
    out["contact_name"] = after("Contact Name") or after("Contact")
    out["email"] = after("Email")
    mirn_match = re.search(r"(?:MIRN|MRIN|DPI)\s*[:#]?\s*([0-9 ]{8,16})", text, re.I)
    if mirn_match:
        out["mirn"] = normalize_mrin(mirn_match.group(1))
    out["start_date"] = _fmt_date(after("Start date") or after("Start Date") or after("Supply Start"))
    out["end_date"] = _fmt_date(after("End date") or after("End Date") or after("Supply End"))
    out["price_per_gj"] = after("Price per GJ") or after("Energy Rate") or after("Period 1 Rate")
    out["commission_per_gj"] = normalize_commission_per_gj(
        after("Commission") or after("Distributor Rebate") or after("Rebate"),
        extra_text=text,
    )
    out["cpq_gj"] = after("Contract Period Quantity") or after("CPQ")
    out["mdq_gj"] = after("Maximum Daily Quantity") or after("MDQ")
    out["retail_service_charge"] = after("Retail Service Charge")
    out["overrun_rate"] = after("Overrun")
    out["excess_cpq_price"] = after("Excess CPQ")
    return out


def _merge_extract(base: dict[str, str], overlay: dict[str, str]) -> dict[str, str]:
    merged = dict(base)
    for key in EXTRACT_KEYS:
        if not (merged.get(key) or "").strip() and (overlay.get(key) or "").strip():
            merged[key] = overlay[key]
    return merged


def extract_alinta_gas_ef(pdf_bytes: bytes, filename: str = "") -> dict[str, Any]:
    warnings: list[str] = []
    extract = _empty_extract()
    try:
        text = pdf_to_text(pdf_bytes)
    except Exception as e:
        logger.warning("alinta EF PDF text extract failed: %s", e)
        text = ""
        warnings.append(f"Embedded PDF text could not be read: {e}")

    text = (text or "").replace("\u00a0", " ")
    if len(text.strip()) >= 40:
        extract = _parse_ef_text(text)

    hint_mirn = mirn_from_filename(filename)
    if hint_mirn and not extract.get("mirn"):
        extract["mirn"] = hint_mirn
        warnings.append("MIRN taken from the filename.")

    missing_core = not (
        extract.get("company_name") and extract.get("mirn") and extract.get("contact_name")
    )
    missing_commission = not extract.get("commission_per_gj")
    images = collect_page_images(pdf_bytes)
    need_vision = len(text.strip()) < 40 or missing_core or missing_commission
    if images and need_vision:
        scan = vision_extract_fields(
            images,
            _VISION_PROMPT,
            EXTRACT_KEYS,
            system="You extract structured fields from scanned ACES / Alinta gas engagement forms. JSON only.",
        )
        if scan:
            cleaned = _empty_extract()
            for key in EXTRACT_KEYS:
                cleaned[key] = _clean(scan.get(key))
            if cleaned.get("mirn"):
                cleaned["mirn"] = normalize_mrin(cleaned["mirn"])
            if cleaned.get("acn_abn"):
                cleaned["acn_abn"] = _digits(cleaned["acn_abn"])
            if cleaned.get("start_date"):
                cleaned["start_date"] = _fmt_date(cleaned["start_date"])
            if cleaned.get("end_date"):
                cleaned["end_date"] = _fmt_date(cleaned["end_date"])
            cleaned["commission_per_gj"] = normalize_commission_per_gj(
                cleaned.get("commission_per_gj"),
                extra_text=" ".join(
                    str(scan.get(key) or "") for key in EXTRACT_KEYS
                ),
            )
            extract = _merge_extract(extract, cleaned)
            warnings.append("Read from scanned pages, including handwriting.")
        elif not extract.get("company_name"):
            warnings.append(
                "This looks like a signed scan and the page images could not be read. "
                "Enter fields manually."
            )
    elif not images and len(text.strip()) < 40:
        warnings.append(
            "Could not read the engagement form (scanned image with no readable text). "
            "Enter fields manually."
        )

    if hint_mirn and not extract.get("mirn"):
        extract["mirn"] = hint_mirn

    extract["commission_per_gj"] = normalize_commission_per_gj(
        extract.get("commission_per_gj"),
        extra_text=text,
    )
    extract["is_signed"] = "true" if _parse_bool(extract.get("is_signed")) or extract.get("signed_date") else extract.get("is_signed") or ""
    extract["mirn"] = normalize_mrin(extract.get("mirn"))
    return {"extract": extract, "extraction_warnings": warnings}


def _biz_contact(business_info: Optional[dict[str, Any]]) -> dict[str, str]:
    if not business_info or not isinstance(business_info, dict):
        return {}
    details = business_info.get("business_details") or {}
    contact = business_info.get("contact_information") or {}
    rep = business_info.get("representative_details") or {}
    processed = business_info.get("_processed_file_ids") or {}
    gdrive = business_info.get("gdrive") or {}
    return {
        "company_name": _clean(details.get("name")),
        "acn_abn": _digits(details.get("abn")),
        "address": _clean(contact.get("site_address") or contact.get("postal_address")),
        "tel": _clean(contact.get("telephone")),
        "contact_name": _clean(rep.get("contact_name")),
        "email": _clean(contact.get("email")),
        "loa_link": _clean(processed.get("business_LOA")),
        "gdrive_folder_url": _clean(gdrive.get("folder_url")),
    }


def _latest_period(contract: dict[str, Any]) -> dict[str, Any]:
    periods = list(contract.get("periods") or [])
    if not periods:
        return {}
    return periods[-1]


def _field(value: Any, source: str, estimated: bool = False) -> dict[str, Any]:
    return {"value": _display(value), "source": source, "estimated": estimated}


def compose_alinta_gas_draft(
    extract: dict[str, str],
    contract_lookup: dict[str, Any],
    *,
    query_mrin: str = "",
    business_info: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    biz = _biz_contact(business_info)
    contracts = list((contract_lookup or {}).get("contracts") or [])
    match_kind = str((contract_lookup or {}).get("match_kind") or "none")
    found = bool(contracts) and match_kind != "none"
    contract = contracts[0] if contracts else {}
    period = _latest_period(contract) if found else {}

    def identity(extract_key: str, biz_key: str) -> dict[str, Any]:
        ef_val = _clean(extract.get(extract_key))
        if ef_val:
            return _field(ef_val, "ef")
        biz_val = _clean(biz.get(biz_key))
        if biz_val:
            return _field(biz_val, "crm")
        return _field("", "missing", estimated=True)

    mirn = (
        normalize_mrin(extract.get("mirn"))
        or normalize_mrin(query_mrin)
        or normalize_mrin(contract.get("mrin"))
    )
    mirn_source = "ef" if normalize_mrin(extract.get("mirn")) else ("query" if normalize_mrin(query_mrin) else ("sheet" if mirn else "missing"))

    def commercial(
        sheet_val: Any,
        ef_val: Any,
        *,
        money: bool = False,
        qty: bool = False,
        pct: bool = False,
        date: bool = False,
    ) -> dict[str, Any]:
        def fmt(raw: Any, source: str) -> dict[str, Any]:
            if qty:
                return _field(_fmt_qty(raw), source)
            if pct:
                return _field(_fmt_pct(raw), source)
            if money:
                return _field(_fmt_money(raw), source)
            if date:
                return _field(_fmt_date(raw), source)
            return _field(raw, source)

        if found and sheet_val not in (None, "") and not date:
            return fmt(sheet_val, "sheet")
        if _clean(ef_val):
            return fmt(ef_val, "ef")
        return _field("", "missing", estimated=True)

    start = commercial(
        None,
        extract.get("start_date"),
        date=True,
    )
    end = commercial(
        None,
        extract.get("end_date"),
        date=True,
    )

    price = commercial(
        period.get("energy_rate_per_gj") or period.get("energy_rate_display"),
        extract.get("price_per_gj"),
        money=True,
    )
    commission_raw = extract.get("commission_per_gj")
    commission = (
        _field(_fmt_money(commission_raw), "ef")
        if _clean(commission_raw)
        else _field("", "missing", estimated=True)
    )

    cpq = commercial(period.get("cpq_gj"), extract.get("cpq_gj"), qty=True)
    min_cpq = commercial(period.get("maq_gj"), extract.get("min_cpq_gj"), qty=True)
    min_pct = commercial(period.get("maq_pct"), extract.get("min_cpq_pct"), pct=True)
    if min_pct["source"] == "missing" and cpq["value"]:
        min_pct = _field(_fmt_pct(DEFAULT_MIN_CPQ_PCT), "estimated", estimated=True)
    if min_cpq["source"] == "missing" and cpq["value"]:
        cpq_num = parse_sheet_number(cpq["value"])
        pct_num = parse_sheet_number(min_pct["value"]) or DEFAULT_MIN_CPQ_PCT
        if cpq_num is not None:
            min_cpq = _field(_fmt_qty(cpq_num * (pct_num / 100.0)), "estimated", estimated=True)

    mdq = commercial(period.get("mdq_gj_per_day"), extract.get("mdq_gj"), qty=True)
    overrun = commercial(period.get("overrun_rate_per_gj"), extract.get("overrun_rate"), money=True)
    excess = commercial(period.get("excess_cpq_rate_per_gj"), extract.get("excess_cpq_price"), money=True)

    rsc_raw = extract.get("retail_service_charge")
    if _clean(rsc_raw):
        rsc = _field(_clean(rsc_raw).replace("$", ""), "ef")
    else:
        rsc = _field(DEFAULT_RETAIL_SERVICE_CHARGE, "default")

    request_kind = "Retention" if found else "Acquisition"
    loa_file_id = drive_file_id(biz.get("loa_link"))
    fields = {
        "company_name": identity("company_name", "company_name"),
        "acn_abn": identity("acn_abn", "acn_abn"),
        "address": identity("address", "address"),
        "tel": identity("tel", "tel"),
        "contact_name": identity("contact_name", "contact_name"),
        "email": identity("email", "email"),
        "mirn": _field(mirn, mirn_source or "missing", estimated=not bool(mirn)),
        "start_date": start,
        "end_date": end,
        "price_per_gj": price,
        "commission_per_gj": commission,
        "cpq_gj": cpq,
        "min_cpq_gj": min_cpq,
        "min_cpq_pct": min_pct,
        "mdq_gj": mdq,
        "retail_service_charge": rsc,
        "overrun_rate": overrun,
        "excess_cpq_price": excess,
    }
    estimated = any(item.get("estimated") for item in fields.values())
    return {
        "request_kind": request_kind,
        "match_kind": match_kind,
        "estimated": estimated and not found,
        "loa_file_id": loa_file_id,
        "loa_available": bool(loa_file_id),
        "gdrive_folder_url": biz.get("gdrive_folder_url") or "",
        "gdrive_folder_id": drive_folder_id(biz.get("gdrive_folder_url") or ""),
        "fields": fields,
    }


def _val(draft: dict[str, Any], key: str) -> str:
    fields = draft.get("fields") or {}
    item = fields.get(key) or {}
    if isinstance(item, dict):
        return _clean(item.get("value"))
    return _clean(item)


def flatten_draft_fields(draft: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, item in (draft.get("fields") or {}).items():
        if isinstance(item, dict):
            out[key] = _clean(item.get("value"))
        else:
            out[key] = _clean(item)
    out["request_kind"] = _clean(draft.get("request_kind") or "Retention")
    return out


def apply_flat_overrides(draft: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Copy operator edits onto the draft fields (source becomes 'manual')."""
    updated = dict(draft)
    fields = dict(draft.get("fields") or {})
    for key, value in (overrides or {}).items():
        if key in {"request_kind"}:
            updated["request_kind"] = _clean(value) or updated.get("request_kind")
            continue
        if key not in EXTRACT_KEYS and key not in fields:
            if key in {
                "company_name",
                "acn_abn",
                "address",
                "tel",
                "contact_name",
                "email",
                "mirn",
                "start_date",
                "end_date",
                "price_per_gj",
                "commission_per_gj",
                "cpq_gj",
                "min_cpq_gj",
                "min_cpq_pct",
                "mdq_gj",
                "retail_service_charge",
                "overrun_rate",
                "excess_cpq_price",
            }:
                fields[key] = _field(value, "manual")
            continue
        fields[key] = _field(value, "manual")
    updated["fields"] = fields
    kind = _clean(updated.get("request_kind"))
    if kind not in {"Retention", "Acquisition"}:
        updated["request_kind"] = "Retention" if updated.get("match_kind") not in {None, "", "none"} else "Acquisition"
    return updated


def build_email_subject(draft: dict[str, Any]) -> str:
    company = _val(draft, "company_name") or "Member"
    mirn = _val(draft, "mirn")
    cpq = _val(draft, "cpq_gj") or "—"
    kind = _clean(draft.get("request_kind") or "Retention")
    return f"Agreement Request: G-C&I (GJ) {cpq} {kind} {company} MIRN {mirn}".strip()


def build_email_html(draft: dict[str, Any]) -> str:
    company = _val(draft, "company_name")
    mirn = _val(draft, "mirn")
    kind = _clean(draft.get("request_kind") or "Retention")
    retention_line = (
        "<p>Please note this is a retention account.</p>"
        if kind.lower() == "retention"
        else "<p>Please note this is an acquisition account.</p>"
    )
    price = _val(draft, "price_per_gj")
    if price and not price.startswith("$"):
        price = f"${price}"
    commission = _val(draft, "commission_per_gj")
    if commission and not commission.startswith("$"):
        commission = f"${commission}"
    return f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team,</p>
  <p>I hope this email finds you well.</p>
  <p>This is an Agreement Request for our member, {company} (MIRN {mirn}).</p>
  {retention_line}
  <p>Company Name: {company}</p>
  <p>ACN/ABN:{_val(draft, "acn_abn")}<br>
  Address: {_val(draft, "address")}<br>
  Tel: {_val(draft, "tel")}<br>
  Contact Name: {_val(draft, "contact_name")}<br>
  Email: {_val(draft, "email")}</p>
  <p>Period</p>
  <p>Start date:{_val(draft, "start_date")}<br>
  End date: {_val(draft, "end_date")}<br>
  Price per GJ: {price}<br>
  Commission: {commission}</p>
  <p>Conditions:<br>
  Contract Period Quantity (GJ) {_val(draft, "cpq_gj")}<br>
  Minimum Contract Period Quantity (GJ) {_val(draft, "min_cpq_gj")}<br>
  Minimum Contract Period Quantity (%of CPQ) {_val(draft, "min_cpq_pct")}<br>
  Contract Maximum Daily Quantity (GJ) {_val(draft, "mdq_gj")}<br>
  Retail Service Charge ($/ MIRN/ Day) {_val(draft, "retail_service_charge")}<br>
  Overrun Rate ($/GJ) {_val(draft, "overrun_rate")}<br>
  Excess CPQ Price ($/GJ) {_val(draft, "excess_cpq_price")}</p>
  <p>Attached are both the LOA &amp; the signed engagement form.</p>
  <p>Kind regards,</p>
  <p>Alice</p>
  <p>FORNRG Pty Ltd<br>
  1300 938 638<br>
  W: <a href="http://www.fornrg.com/">http://www.fornrg.com/</a></p>
</body>
</html>"""


def required_send_errors(draft: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not _val(draft, "company_name"):
        errors.append("Company name is required.")
    if not _val(draft, "mirn"):
        errors.append("MIRN is required.")
    if not _val(draft, "start_date"):
        errors.append("Start date is required.")
    if not _val(draft, "end_date"):
        errors.append("End date is required.")
    if not _val(draft, "price_per_gj"):
        errors.append("Price per GJ is required.")
    if not _val(draft, "commission_per_gj"):
        errors.append("Commission is required.")
    if not _val(draft, "cpq_gj"):
        errors.append("Contract Period Quantity is required.")
    if not draft.get("loa_file_id"):
        errors.append("Letter of Authority was not found for this member. Upload the LOA first.")
    return errors


def gas_ef_folder_id() -> str:
    return (os.getenv("ALINTA_GAS_EF_FOLDER_ID") or DEFAULT_GAS_EF_FOLDER_ID).strip()


def gas_ef_folder_url() -> str:
    folder_id = gas_ef_folder_id()
    return f"https://drive.google.com/drive/folders/{folder_id}" if folder_id else ""


def lodge_signed_ef_on_member(pdf_bytes: bytes, filename: str, business_name: str) -> str:
    """Lodge the signed EF through invoice-api /v1/ef/process-ef (member Documents > Engagement)."""
    company = (business_name or "").strip()
    if not pdf_bytes or not company:
        return "Skipped member EF lodging (missing PDF or business name)."
    safe_name = filename or "Signed Alinta EF.pdf"
    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"
    try:
        response = requests.post(
            INVOICE_API_PROCESS_EF_URL,
            files={"file": (safe_name, io.BytesIO(pdf_bytes), "application/pdf")},
            data={"linked_business_name": company},
            timeout=90,
        )
    except requests.RequestException as e:
        logger.warning("alinta member EF lodging failed: %s", e)
        return f"Member EF lodging failed: {e}"
    if response.status_code >= 300:
        logger.warning(
            "alinta member EF lodging HTTP %s body=%s",
            response.status_code,
            (response.text or "")[:300],
        )
        return f"Member EF lodging failed HTTP {response.status_code}."
    return ""


def _download_loa_bytes(loa_file_id: str) -> tuple[bytes, str]:
    loa_id = drive_file_id(loa_file_id)
    if not loa_id:
        return b"", "No LOA file id."
    try:
        from tools.video_creation_pack import fetch_drive_file_bytes

        data = fetch_drive_file_bytes(loa_id) or b""
        if data:
            return data, ""
        return b"", f"Could not download LOA {loa_id} from Drive."
    except Exception as e:
        logger.warning("alinta LOA download failed: %s", e)
        return b"", f"LOA download failed: {e}"


def send_alinta_gas_agreement(
    draft: dict[str, Any],
    *,
    pdf_bytes: Optional[bytes] = None,
    filename: str = "",
    user_email: Optional[str] = None,
) -> dict[str, Any]:
    errors = required_send_errors(draft)
    if errors:
        return {"ok": False, "message": " ".join(errors), "errors": errors}
    if not pdf_bytes:
        return {"ok": False, "message": "The signed EF PDF is required to send.", "errors": ["missing pdf"]}

    subject = build_email_subject(draft)
    html = build_email_html(draft)
    company = _val(draft, "company_name")
    mirn = _val(draft, "mirn")
    uploaded_name = filename or f"Signed Alinta EF {company} MIRN {mirn}.pdf"
    if not uploaded_name.lower().endswith(".pdf"):
        uploaded_name = f"{uploaded_name}.pdf"

    loa_bytes, loa_dl_error = _download_loa_bytes(str(draft.get("loa_file_id") or ""))
    lodge_error = lodge_signed_ef_on_member(pdf_bytes, uploaded_name, company)
    client_folder_url = str(draft.get("gdrive_folder_url") or "").strip()
    client_folder_id = str(draft.get("gdrive_folder_id") or "").strip() or drive_folder_id(client_folder_url)

    files: dict[str, tuple[str, Any, str]] = {
        "file_0": (uploaded_name, io.BytesIO(pdf_bytes), "application/pdf"),
    }
    if loa_bytes:
        files["file_1"] = ("LOA.pdf", io.BytesIO(loa_bytes), "application/pdf")

    form = {
        "business_name": f"{company} MIRN: {mirn}",
        "contract_type": "Alinta C&I Gas",
        "agreement_type": N8N_AGREEMENT_TYPE,
        "supplier_email": TEST_RECIPIENT_EMAIL,
        "resolved_supplier_name": "Data Quote",
        "email_subject": subject,
        "email_html_content": html,
        "loa_file_id": str(draft.get("loa_file_id") or ""),
        "file_count": str(len(files)),
        "mirn": mirn,
        "user_email": user_email or "",
        "timestamp": datetime.now().isoformat(),
        "client_folder_url": client_folder_url,
        "client_folder_id": client_folder_id,
        "gdrive_folder_url": client_folder_url,
        "gdrive_folder_id": client_folder_id,
        "gas_ef_folder_id": gas_ef_folder_id(),
        "gas_ef_folder_url": gas_ef_folder_url(),
    }

    try:
        response = requests.post(
            N8N_EMAIL_SUPPLIER_URL,
            data=form,
            files=files,
            timeout=90,
        )
    except requests.RequestException as e:
        logger.error("alinta agreement email-supplier failed: %s", e)
        return {"ok": False, "message": f"Could not send email: {e}", "errors": [str(e)]}
    finally:
        for item in files.values():
            try:
                item[1].close()
            except Exception:
                pass

    logger.info(
        "alinta agreement email-supplier HTTP %s body=%s client_folder=%s loa_dl=%s lodge=%s",
        response.status_code,
        (response.text or "")[:500],
        client_folder_url,
        loa_dl_error,
        lodge_error,
    )
    if response.status_code != 200:
        return {
            "ok": False,
            "message": f"Email webhook returned HTTP {response.status_code}: {(response.text or '')[:300]}",
            "errors": [response.text[:300] if response.text else f"HTTP {response.status_code}"],
            "client_folder_url": client_folder_url or None,
        }

    notes: list[str] = [
        f"Agreement request sent to {TEST_RECIPIENT_EMAIL} with the EF"
        + (" and LOA attached." if loa_bytes else " attached (LOA download failed)."),
        f"Subject: {subject}",
    ]
    if lodge_error:
        notes.append(lodge_error)
    else:
        notes.append("Signed EF lodged on the member Documents > Engagement forms list.")
    if client_folder_url:
        notes.append(f"Member Drive folder: {client_folder_url}")
    else:
        notes.append("Member Drive folder was not on file — n8n will not have a client folder to file into.")
    if loa_dl_error and not loa_bytes:
        notes.append(f"LOA attachment: {loa_dl_error}")
    notes.append(f"LOA file ID: {draft.get('loa_file_id')}")

    return {
        "ok": True,
        "message": "\n".join(notes),
        "email_subject": subject,
        "recipient": TEST_RECIPIENT_EMAIL,
        "loa_file_id": draft.get("loa_file_id"),
        "client_folder_url": client_folder_url or None,
        "client_folder_id": client_folder_id or None,
        "drive_folder_url": client_folder_url or None,
        "lodge_error": lodge_error or None,
        "agreement_type": N8N_AGREEMENT_TYPE,
    }


def build_extract_response(
    pdf_bytes: bytes,
    *,
    filename: str = "",
    business_name: str = "",
    query_mrin: str = "",
) -> dict[str, Any]:
    extracted = extract_alinta_gas_ef(pdf_bytes, filename=filename)
    extract = extracted["extract"]
    mirn = normalize_mrin(extract.get("mirn")) or normalize_mrin(query_mrin)
    lookup: dict[str, Any] = {
        "query_mrin": query_mrin or mirn,
        "normalized_mrin": mirn,
        "match_kind": "none",
        "contracts": [],
    }
    if mirn:
        try:
            lookup = lookup_bne_gas_contract(mirn)
        except Exception as e:
            logger.warning("bne gas lookup failed for mirn=%s: %s", mirn, e)
            extracted["extraction_warnings"].append(f"Contract lookup failed: {e}")

    business_info = None
    name_for_lookup = business_name or extract.get("company_name") or ""
    if name_for_lookup:
        try:
            from tools.business_info import get_business_information

            info = get_business_information(name_for_lookup)
            if isinstance(info, dict) and not str(info.get("_formatted_output") or "").startswith("Sorry"):
                business_info = info
        except Exception as e:
            logger.warning("business info lookup failed for %r: %s", name_for_lookup, e)

    draft = compose_alinta_gas_draft(
        extract,
        lookup,
        query_mrin=query_mrin or mirn,
        business_info=business_info,
    )
    start_val = str((draft.get("fields") or {}).get("start_date", {}).get("value") or "")
    end_val = str((draft.get("fields") or {}).get("end_date", {}).get("value") or "")
    if _OPEN_DATE_RE.match(start_val) or _OPEN_DATE_RE.match(end_val):
        extracted["extraction_warnings"].append(
            "Period dates on the EF are TBC — they were not taken from the signed C&I gas sheet."
        )
    elif not start_val or not end_val:
        extracted["extraction_warnings"].append(
            "Period dates were not on the EF. Fill them from the PDF (sheet dates are the current contract, not this request)."
        )
    subject = build_email_subject(draft)
    html = build_email_html(draft)
    return {
        "extract": extract,
        "extraction_warnings": extracted["extraction_warnings"],
        "contract": lookup,
        "draft": draft,
        "email_subject": subject,
        "email_html_content": html,
        "recipient": TEST_RECIPIENT_EMAIL,
    }
