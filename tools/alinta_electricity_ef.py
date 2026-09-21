"""Extract a signed Alinta C&I electricity engagement form and draft an Agreement Request email."""

from __future__ import annotations

import io
import logging
import os
import re
from datetime import datetime
from typing import Any, Optional

import requests

from tools.alinta_gas_ef import (
    DEFAULT_GAS_EF_FOLDER_ID,
    N8N_EMAIL_SUPPLIER_URL,
    TEST_RECIPIENT_EMAIL,
    _biz_contact,
    _clean,
    _digits,
    _download_loa_bytes,
    _EMAIL_RE,
    _field,
    _fmt_date,
    _fmt_pct,
    _is_broker_company,
    _JUNK_FIELD_RE,
    _OPEN_DATE_RE,
    _parse_bool,
    drive_file_id,
    drive_folder_id,
    lodge_signed_ef_on_member,
)
from tools.bne_electricity_contracts import (
    lookup_bne_electricity_contracts,
    normalize_nmi,
)
from tools.bne_gas_contracts import parse_sheet_number
from tools.pdf_scan_vision import collect_page_images, pdf_to_text, vision_extract_fields

logger = logging.getLogger(__name__)

N8N_AGREEMENT_TYPE = "alinta_electricity_agreement_request"
MAX_COMMISSION_C_KWH = 10.0
_NMI_IN_NAME_RE = re.compile(r"(?:NMI)\s*[:#]?\s*([A-Z0-9]{10,11})", re.I)
_NMI_TOKEN_RE = re.compile(r"\b(?=[A-Z0-9]*\d)([A-Z0-9]{10,11})\b", re.I)
_NOT_A_RATE = frozenset({"included", "yes", "y", "true", "na", "n/a", "-"})
_REBATE_CENTS_RE = re.compile(
    r"(?:distributor\s+)?(?:rebate|brokerage|commission)[^.\n]{0,80}?"
    r"\$?\s*([0-9]+(?:\.[0-9]+)?)\s*"
    r"(?:c(?:ents)?\s*)?(?:/\s*|per\s+)?kwh",
    re.I,
)
_REBATE_MWH_RE = re.compile(
    r"(?:distributor\s+)?(?:rebate|brokerage|commission)[^.\n]{0,80}?"
    r"\$?\s*([0-9]+(?:\.[0-9]+)?)\s*"
    r"(?:\$\s*)?(?:/\s*|per\s+)mwh",
    re.I,
)
_COMMISSION_CENTS_RE = re.compile(
    r"(?:commission|rebate|brokerage)[^.\n]{0,40}?\$?\s*([0-9]+(?:\.[0-9]+)?)",
    re.I,
)

DEFAULT_TAKE_OR_PAY_PCT = 80.0
_TAKE_OR_PAY_RE = re.compile(
    r"(?:take[\s-]*or[\s-]*pay|t\s*/\s*p|minimum\s+offtake)[^.\n%]{0,48}?(\d{2,3}(?:\.\d+)?)\s*%",
    re.I,
)
EXTRACT_KEYS: tuple[str, ...] = (
    "company_name",
    "acn_abn",
    "address",
    "tel",
    "contact_name",
    "email",
    "nmis",
    "start_date",
    "end_date",
    "peak_rate_c_kwh",
    "off_peak_rate_c_kwh",
    "shoulder_rate_c_kwh",
    "commission_c_kwh",
    "annual_kwh",
    "take_or_pay_pct",
    "is_signed",
    "signed_date",
)

_VISION_PROMPT = """Extract fields from this Alinta Energy C&I electricity engagement form / agreement.
The PDF is often a scan of a signed paper copy. Read printed text AND handwriting.

This ACES "Engagement Form - Electricity Agreement" is typically a two-column form:
LEFT / label column is field names; RIGHT / value column is the answers.
Do not read down the label column as if it were values.

Return a JSON object with exactly these keys (use "" if not present; never invent):
company_name, acn_abn, address, tel, contact_name, email, nmis,
start_date, end_date, peak_rate_c_kwh, off_peak_rate_c_kwh, shoulder_rate_c_kwh,
commission_c_kwh, annual_kwh, take_or_pay_pct, is_signed, signed_date.

Rules:
- company_name is the MEMBER on the Company Name row (the customer).
  NEVER use Environmental Global Benefits, EGB, EGB Executive, ACES, Carbon Zero,
  FORNRG, or Alinta Energy — those are us / the retailer, usually in the header
  or the "Distributed By" / "Contact" rows above Company Name.
- nmis: ONLY the National Meter Identifier value(s) from the NMI field — never heading
  words. Most forms have one NMI; some have two. Return them comma-separated.
  Valid NMIs are 10–11 characters and ALWAYS contain a digit:
  all digits (e.g. 6203800971) or Victorian mixed (e.g. VEEE0WPKWT).
  NEVER return ENGAGEMENT, ELECTRICITY, DISTRIBUTED, EXTRUSIONS, DISTRIBUTOR,
  DISCLOSURE, AGREEMENT, FORM, COMPANY, or the ABN.
- acn_abn: digits only from the ACN/ABN row (11-digit ABN or 9-digit ACN).
- email must contain @.
- contact_name is the member Contact Name row, not "Distributed By" / EGB Executive.
- Dates: keep as written (prefer D/M/YYYY). If Start Date or End Date is TBC, TBA, TBD, or blank, return that exactly. Never invent a calendar date.
- Rates are in c/kWh (cents per kWh), numbers only (e.g. 8.02).
- shoulder_rate_c_kwh: only if the form actually has a Shoulder rate. Peak + Off-peak
  only is common — then return "".
- commission_c_kwh is the broker / ACES / EGB / Distributor Rebate, numbers only.
  On ACES forms this is often labelled "Distributor Rebate: X c/kWh" or "X $/MWh"
  with the value cell "Included". In that case commission_c_kwh is X (convert
  $/MWh to c/kWh by dividing by 10). Never return "Included".
- annual_kwh is estimated / contract annual consumption in kWh if present.
- take_or_pay_pct is the take-or-pay / minimum offtake percent if present (e.g. 70
  or 80). Number only. Leave "" if the form does not state one — do not invent 80.
- is_signed: "true" if there is a handwritten or digital signature, else "false".
- contact_name, email, tel: the customer contact, not Alinta / ACES / EGB.
"""


def alinta_electricity_recipient_email() -> str:
    try:
        from services.operational_emails import emails_csv, get_recipient_by_key, with_db

        row = with_db(lambda db: get_recipient_by_key(db, "alinta_electricity", "default"))
        if row:
            return emails_csv(row)
        gas_row = with_db(lambda db: get_recipient_by_key(db, "alinta_gas", "default"))
        if gas_row:
            return emails_csv(gas_row)
    except Exception as e:
        logger.warning("alinta electricity recipient lookup failed: %s", e)
    return TEST_RECIPIENT_EMAIL


def split_nmis(value: Any, exclude: set[str] | None = None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    blocked = {normalize_nmi(item) for item in (exclude or set()) if item}
    found: list[str] = []

    def add(raw: str) -> None:
        nmi = normalize_nmi(raw)
        if nmi and nmi not in found and nmi not in blocked:
            found.append(nmi)

    parts = re.split(r"[,;/&+]|\band\b", text, flags=re.I)
    for part in parts:
        add(part)
    if found:
        return found
    for match in _NMI_TOKEN_RE.finditer(text.upper()):
        add(match.group(1) or match.group(0))
    return found


def join_nmis(nmis: list[str]) -> str:
    return ", ".join(nmi for nmi in nmis if nmi)


def nmis_from_filename(filename: str) -> list[str]:
    if not filename:
        return []
    found: list[str] = []
    for match in _NMI_IN_NAME_RE.finditer(filename):
        nmi = normalize_nmi(match.group(1))
        if nmi and nmi not in found:
            found.append(nmi)
    return found


def format_nmis_for_subject(nmis: list[str]) -> str:
    if not nmis:
        return ""
    if len(nmis) == 1:
        return nmis[0]
    if len(nmis) == 2:
        return f"{nmis[0]} & {nmis[1]}"
    return ", ".join(nmis[:-1]) + f" & {nmis[-1]}"


def _empty_extract() -> dict[str, str]:
    return {key: "" for key in EXTRACT_KEYS}


def _plausible_commission(number: float) -> bool:
    return 0 < number <= MAX_COMMISSION_C_KWH


def normalize_commission_c_kwh(value: Any, extra_text: str = "", avoid: Optional[list[str]] = None) -> str:
    avoid_nums = {
        n
        for n in (parse_sheet_number(item) for item in (avoid or []))
        if n is not None
    }
    candidates: list[tuple[float, str]] = []
    raw = _clean(value)
    if raw and raw.lower() not in _NOT_A_RATE:
        number = parse_sheet_number(raw)
        if number is not None:
            candidates.append((number, "raw"))
    blob = f"{raw}\n{extra_text or ''}"
    for match in _REBATE_CENTS_RE.finditer(blob):
        number = parse_sheet_number(match.group(1))
        if number is not None:
            candidates.append((number, "cents"))
    for match in _REBATE_MWH_RE.finditer(blob):
        number = parse_sheet_number(match.group(1))
        if number is not None:
            candidates.append((number / 10.0, "mwh"))
    for match in _COMMISSION_CENTS_RE.finditer(blob):
        number = parse_sheet_number(match.group(1))
        if number is not None:
            candidates.append((number, "generic"))
    for number, kind in candidates:
        if number in avoid_nums and kind == "generic":
            continue
        if _plausible_commission(number):
            if abs(number - round(number, 2)) < 1e-9:
                text = f"{number:.2f}".rstrip("0").rstrip(".")
                return text
            return str(number)
    return ""


def _fmt_cents(value: Any) -> str:
    number = parse_sheet_number(value)
    if number is None:
        return _clean(value)
    if abs(number - round(number)) < 1e-9:
        return f"{int(round(number))} c/kWh"
    return f"{number:.2f} c/kWh"


def _fmt_kwh(value: Any) -> str:
    number = parse_sheet_number(value)
    if number is None:
        return _clean(value)
    if abs(number - round(number)) < 1e-9:
        return f"{int(round(number)):,} kWh"
    return f"{number:,.2f} kWh"


def normalize_take_or_pay_pct(value: Any) -> str:
    number = parse_sheet_number(value)
    if number is None:
        raw = _clean(value)
        match = re.search(r"(\d{1,3}(?:\.\d+)?)\s*%?", raw)
        if match:
            number = parse_sheet_number(match.group(1))
    if number is None:
        return ""
    if 0 < number <= 1:
        number = number * 100
    if 50 <= number <= 100:
        if abs(number - round(number)) < 1e-9:
            return str(int(round(number)))
        return f"{number:.1f}".rstrip("0").rstrip(".")
    return ""


def _take_or_pay_from_text(text: str) -> str:
    match = _TAKE_OR_PAY_RE.search(text or "")
    if match:
        return match.group(1)
    return ""


def _nmis_from_text(text: str) -> list[str]:
    found: list[str] = []
    for match in re.finditer(r"(?:NMI)\s*[:#]?\s*([A-Z0-9]{10,11})", text or "", re.I):
        nmi = normalize_nmi(match.group(1))
        if nmi and nmi not in found:
            found.append(nmi)
    if found:
        return found
    return split_nmis(text)


def _parse_ef_text(text: str) -> dict[str, str]:
    out = _empty_extract()
    if not text or len(text.strip()) < 20:
        return out

    def after(label: str) -> str:
        match = re.search(rf"{re.escape(label)}[ \t]*[:#][ \t]*(.+)", text, re.I)
        if not match:
            return ""
        return _clean(match.group(1).split("\n")[0])

    out["company_name"] = after("Company Name") or after("Customer Name") or after("Account Name")
    out["acn_abn"] = _digits(after("ABN") or after("ACN") or after("ACN/ABN"))
    out["address"] = after("Address") or after("Site Address") or after("Supply Address")
    out["tel"] = after("Tel") or after("Telephone") or after("Phone")
    out["contact_name"] = after("Contact Name")
    out["email"] = after("Email")
    out["nmis"] = join_nmis(_nmis_from_text(text))
    out["start_date"] = _fmt_date(after("Start date") or after("Start Date") or after("Supply Start"))
    out["end_date"] = _fmt_date(after("End date") or after("End Date") or after("Supply End"))
    out["peak_rate_c_kwh"] = after("Peak Rate") or after("Peak")
    out["off_peak_rate_c_kwh"] = after("Off-Peak Rate") or after("Off Peak Rate") or after("Off-peak")
    out["shoulder_rate_c_kwh"] = after("Shoulder Rate") or after("Shoulder")
    out["commission_c_kwh"] = normalize_commission_c_kwh(
        after("Commission") or after("Distributor Rebate") or after("Rebate"),
        extra_text=text,
        avoid=[out["peak_rate_c_kwh"], out["off_peak_rate_c_kwh"], out["shoulder_rate_c_kwh"]],
    )
    out["annual_kwh"] = after("Annual Consumption") or after("Annual kWh") or after("Estimated Annual")
    out["take_or_pay_pct"] = normalize_take_or_pay_pct(
        after("Take or Pay")
        or after("Take-or-Pay")
        or after("Take or pay")
        or after("Minimum offtake")
        or _take_or_pay_from_text(text)
    )
    return out


def _sanitize_extract(extract: dict[str, str]) -> dict[str, str]:
    out = dict(extract)
    if _is_broker_company(out.get("company_name")):
        out["company_name"] = ""
    if _JUNK_FIELD_RE.match(_clean(out.get("company_name"))):
        out["company_name"] = ""
    if _JUNK_FIELD_RE.match(_clean(out.get("address"))):
        out["address"] = ""
    if _JUNK_FIELD_RE.match(_clean(out.get("tel"))):
        out["tel"] = ""
    if _JUNK_FIELD_RE.match(_clean(out.get("contact_name"))) or _is_broker_company(
        out.get("contact_name")
    ):
        out["contact_name"] = ""
    email = _clean(out.get("email"))
    if email and not _EMAIL_RE.match(email):
        out["email"] = ""
    out["nmis"] = join_nmis(split_nmis(out.get("nmis"), exclude={_digits(out.get("acn_abn"))}))
    out["take_or_pay_pct"] = normalize_take_or_pay_pct(out.get("take_or_pay_pct"))
    return out


def _merge_extract(base: dict[str, str], overlay: dict[str, str]) -> dict[str, str]:
    merged = dict(base)
    for key in EXTRACT_KEYS:
        if not (merged.get(key) or "").strip() and (overlay.get(key) or "").strip():
            merged[key] = overlay[key]
    return merged


def extract_alinta_electricity_ef(pdf_bytes: bytes, filename: str = "") -> dict[str, Any]:
    warnings: list[str] = []
    extract = _empty_extract()
    try:
        text = pdf_to_text(pdf_bytes)
    except Exception as e:
        logger.warning("alinta electricity EF PDF text extract failed: %s", e)
        text = ""
        warnings.append(f"Embedded PDF text could not be read: {e}")

    text = (text or "").replace("\u00a0", " ")
    if len(text.strip()) >= 40:
        extract = _merge_extract(extract, _parse_ef_text(text))
    extract = _sanitize_extract(extract)

    hint_nmis = nmis_from_filename(filename)
    if hint_nmis and not extract.get("nmis"):
        extract["nmis"] = join_nmis(hint_nmis)
        warnings.append("NMI taken from the filename.")

    missing_core = not (
        extract.get("company_name") and extract.get("nmis") and extract.get("contact_name")
    )
    missing_commission = not extract.get("commission_c_kwh")
    images = collect_page_images(pdf_bytes)
    need_vision = len(text.strip()) < 40 or missing_core or missing_commission
    if images and need_vision:
        scan = vision_extract_fields(
            images,
            _VISION_PROMPT,
            EXTRACT_KEYS,
            system="You extract structured fields from scanned ACES / Alinta electricity engagement forms. JSON only.",
        )
        if scan:
            cleaned = _empty_extract()
            for key in EXTRACT_KEYS:
                cleaned[key] = _clean(scan.get(key))
            if cleaned.get("nmis"):
                cleaned["nmis"] = join_nmis(
                    split_nmis(cleaned["nmis"], exclude={_digits(cleaned.get("acn_abn"))})
                )
            if cleaned.get("acn_abn"):
                cleaned["acn_abn"] = _digits(cleaned["acn_abn"])
            if cleaned.get("start_date"):
                cleaned["start_date"] = _fmt_date(cleaned["start_date"])
            if cleaned.get("end_date"):
                cleaned["end_date"] = _fmt_date(cleaned["end_date"])
            cleaned["commission_c_kwh"] = normalize_commission_c_kwh(
                cleaned.get("commission_c_kwh"),
                extra_text=" ".join(str(scan.get(key) or "") for key in EXTRACT_KEYS) + "\n" + text,
                avoid=[
                    cleaned.get("peak_rate_c_kwh") or "",
                    cleaned.get("off_peak_rate_c_kwh") or "",
                    cleaned.get("shoulder_rate_c_kwh") or "",
                ],
            )
            cleaned = _sanitize_extract(cleaned)
            extract = _merge_extract(extract, cleaned)
            extract = _sanitize_extract(extract)
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

    if hint_nmis and not extract.get("nmis"):
        extract["nmis"] = join_nmis(hint_nmis)
    extract["commission_c_kwh"] = normalize_commission_c_kwh(
        extract.get("commission_c_kwh"),
        extra_text=text,
        avoid=[
            extract.get("peak_rate_c_kwh") or "",
            extract.get("off_peak_rate_c_kwh") or "",
            extract.get("shoulder_rate_c_kwh") or "",
        ],
    )
    extract["is_signed"] = (
        "true"
        if _parse_bool(extract.get("is_signed")) or extract.get("signed_date")
        else extract.get("is_signed") or ""
    )
    extract["nmis"] = join_nmis(
        split_nmis(extract.get("nmis"), exclude={_digits(extract.get("acn_abn"))})
    )
    extract = _sanitize_extract(extract)
    return {"extract": extract, "extraction_warnings": warnings}


def _linked_nmis(business_info: Optional[dict[str, Any]]) -> list[str]:
    if not business_info or not isinstance(business_info, dict):
        return []
    linked = business_info.get("Linked_Details") or {}
    raw = (linked.get("linked_utilities") or {}).get("C&I Electricity")
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
    elif isinstance(raw, list):
        parts = [str(p).strip() for p in raw if str(p).strip()]
    else:
        parts = []
    found: list[str] = []
    for part in parts:
        nmi = normalize_nmi(part)
        if nmi and nmi not in found:
            found.append(nmi)
    return found


def _latest_period(contract: dict[str, Any]) -> dict[str, Any]:
    periods = list(contract.get("periods") or [])
    if not periods:
        return {}
    return periods[-1]


def compose_alinta_electricity_draft(
    extract: dict[str, str],
    contract_lookup: dict[str, Any],
    *,
    query_nmis: Optional[list[str]] = None,
    business_info: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    biz = _biz_contact(business_info)
    contracts = list((contract_lookup or {}).get("contracts") or [])
    match_kind = str((contract_lookup or {}).get("match_kind") or "none")
    found = bool(contracts) and match_kind != "none"
    period = _latest_period(contracts[0]) if found else {}

    def identity(extract_key: str, biz_key: str) -> dict[str, Any]:
        ef_val = _clean(extract.get(extract_key))
        if ef_val:
            return _field(ef_val, "ef")
        biz_val = _clean(biz.get(biz_key))
        if biz_val:
            return _field(biz_val, "crm")
        return _field("", "missing", estimated=True)

    nmis = split_nmis(extract.get("nmis"), exclude={_digits(extract.get("acn_abn"))}) or list(
        query_nmis or []
    ) or _linked_nmis(business_info)
    nmis = [normalize_nmi(n) for n in nmis]
    nmis = [n for n in nmis if n]
    nmi_source = (
        "ef"
        if split_nmis(extract.get("nmis"))
        else ("query" if query_nmis else ("crm" if nmis else "missing"))
    )

    def commercial(
        sheet_val: Any,
        ef_val: Any,
        *,
        cents: bool = False,
        kwh: bool = False,
        date: bool = False,
        optional: bool = False,
    ) -> dict[str, Any]:
        def fmt(raw: Any, source: str) -> dict[str, Any]:
            if cents:
                return _field(_fmt_cents(raw), source)
            if kwh:
                return _field(_fmt_kwh(raw), source)
            if date:
                return _field(_fmt_date(raw), source)
            return _field(raw, source)

        if _clean(ef_val):
            return fmt(ef_val, "ef")
        if found and sheet_val not in (None, ""):
            return fmt(sheet_val, "sheet")
        return _field("", "missing", estimated=not optional)

    start = commercial(None, extract.get("start_date"), date=True)
    end = commercial(None, extract.get("end_date"), date=True)
    peak = commercial(period.get("peak_rate_c_kwh"), extract.get("peak_rate_c_kwh"), cents=True)
    off_peak = commercial(period.get("off_peak_rate_c_kwh"), extract.get("off_peak_rate_c_kwh"), cents=True)
    shoulder = commercial(
        period.get("shoulder_rate_c_kwh"),
        extract.get("shoulder_rate_c_kwh"),
        cents=True,
        optional=True,
    )
    commission_raw = extract.get("commission_c_kwh")
    commission = (
        _field(_fmt_cents(commission_raw), "ef")
        if _clean(commission_raw)
        else _field("", "missing", estimated=True)
    )
    annual = commercial(period.get("annual_kwh"), extract.get("annual_kwh"), kwh=True)
    ef_take_or_pay = normalize_take_or_pay_pct(extract.get("take_or_pay_pct"))
    take_or_pay = (
        _field(_fmt_pct(ef_take_or_pay), "ef")
        if ef_take_or_pay
        else _field(_fmt_pct(DEFAULT_TAKE_OR_PAY_PCT), "estimated", estimated=True)
    )

    request_kind = "Retention" if found else "Acquisition"
    loa_file_id = drive_file_id(biz.get("loa_link"))
    fields = {
        "company_name": identity("company_name", "company_name"),
        "acn_abn": identity("acn_abn", "acn_abn"),
        "address": identity("address", "address"),
        "tel": identity("tel", "tel"),
        "contact_name": identity("contact_name", "contact_name"),
        "email": identity("email", "email"),
        "nmis": _field(join_nmis(nmis), nmi_source or "missing", estimated=not bool(nmis)),
        "start_date": start,
        "end_date": end,
        "peak_rate_c_kwh": peak,
        "off_peak_rate_c_kwh": off_peak,
        "shoulder_rate_c_kwh": shoulder,
        "commission_c_kwh": commission,
        "annual_kwh": annual,
        "take_or_pay_pct": take_or_pay,
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


def flatten_electricity_draft_fields(draft: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, item in (draft.get("fields") or {}).items():
        if isinstance(item, dict):
            out[key] = _clean(item.get("value"))
        else:
            out[key] = _clean(item)
    out["request_kind"] = _clean(draft.get("request_kind") or "Retention")
    return out


def apply_electricity_overrides(draft: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    updated = dict(draft)
    fields = dict(draft.get("fields") or {})
    for key, value in (overrides or {}).items():
        if key in {"request_kind"}:
            updated["request_kind"] = _clean(value) or updated.get("request_kind")
            continue
        if key == "nmis":
            fields[key] = _field(join_nmis(split_nmis(value)), "manual")
            continue
        if key == "take_or_pay_pct":
            pct = normalize_take_or_pay_pct(value)
            fields[key] = _field(_fmt_pct(pct) if pct else _clean(value), "manual")
            continue
        if key not in EXTRACT_KEYS and key not in fields:
            continue
        fields[key] = _field(value, "manual")
    updated["fields"] = fields
    kind = _clean(updated.get("request_kind"))
    if kind not in {"Retention", "Acquisition"}:
        updated["request_kind"] = "Retention" if updated.get("match_kind") not in {None, "", "none"} else "Acquisition"
    return updated


def _elec_merge_values(draft: dict[str, Any]) -> dict[str, str]:
    company = _val(draft, "company_name")
    nmis = split_nmis(_val(draft, "nmis"))
    nmi_display = format_nmis_for_subject(nmis)
    kind = _clean(draft.get("request_kind") or "Retention")
    request_kind_html = (
        "<p>Please note this is a retention account.</p>"
        if kind.lower() == "retention"
        else "<p>Please note this is an acquisition account.</p>"
    )
    nmi_lines = "<br>\n  ".join(f"NMI: {nmi}" for nmi in nmis) if nmis else "NMI:"
    return {
        "company_name": company,
        "nmis": join_nmis(nmis),
        "nmi_display": nmi_display,
        "nmi_lines": nmi_lines,
        "request_kind": kind,
        "request_kind_html": request_kind_html,
        "acn_abn": _val(draft, "acn_abn"),
        "address": _val(draft, "address"),
        "tel": _val(draft, "tel"),
        "contact_name": _val(draft, "contact_name"),
        "email": _val(draft, "email"),
        "start_date": _val(draft, "start_date"),
        "end_date": _val(draft, "end_date"),
        "peak_rate_c_kwh": _val(draft, "peak_rate_c_kwh"),
        "off_peak_rate_c_kwh": _val(draft, "off_peak_rate_c_kwh"),
        "shoulder_rate_c_kwh": _val(draft, "shoulder_rate_c_kwh"),
        "commission_c_kwh": _val(draft, "commission_c_kwh"),
        "annual_kwh": _val(draft, "annual_kwh"),
        "take_or_pay_pct": _val(draft, "take_or_pay_pct"),
    }


def _alinta_elec_db_template() -> tuple[str, str] | None:
    try:
        from services.operational_emails import load_template_content, with_db

        return with_db(lambda db: load_template_content(db, "alinta_electricity.default"))
    except Exception as e:
        logger.warning("alinta electricity template lookup failed: %s", e)
        return None


def build_electricity_email_subject(draft: dict[str, Any]) -> str:
    values = _elec_merge_values(draft)
    loaded = _alinta_elec_db_template()
    if loaded:
        from services.operational_emails import render_tokens

        return render_tokens(loaded[0], values).strip()
    company = values["company_name"] or "Member"
    kind = values["request_kind"]
    nmi_display = values["nmi_display"]
    return f"Agreement Request: E-C&I {kind} {company} NMI {nmi_display}".strip()


def build_electricity_email_html(draft: dict[str, Any]) -> str:
    values = _elec_merge_values(draft)
    loaded = _alinta_elec_db_template()
    if loaded:
        from services.operational_emails import render_tokens

        html = render_tokens(loaded[1], values)
    else:
        company = values["company_name"]
        nmi_display = values["nmi_display"]
        shoulder_line = (
            f"  Shoulder: {values['shoulder_rate_c_kwh']}<br>\n"
            if values["shoulder_rate_c_kwh"]
            else ""
        )
        html = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team,</p>
  <p>I hope this email finds you well.</p>
  <p>This is an Agreement Request for our member, {company} (NMI {nmi_display}).</p>
  {values["request_kind_html"]}
  <p>Company Name: {company}</p>
  <p>ACN/ABN:{values["acn_abn"]}<br>
  Address: {values["address"]}<br>
  Tel: {values["tel"]}<br>
  Contact Name: {values["contact_name"]}<br>
  Email: {values["email"]}</p>
  <p>{values["nmi_lines"]}</p>
  <p>Period</p>
  <p>Start date:{values["start_date"]}<br>
  End date: {values["end_date"]}<br>
  Peak: {values["peak_rate_c_kwh"]}<br>
  Off-peak: {values["off_peak_rate_c_kwh"]}<br>
{shoulder_line}  Commission: {values["commission_c_kwh"]}</p>
  <p>Annual consumption: {values["annual_kwh"]}</p>
  <p>Take or Pay: {values["take_or_pay_pct"]}</p>
  <p>Attached are both the LOA &amp; the signed engagement form.</p>
  <p>Kind regards,</p>
  <p>Alice</p>
  <p>FORNRG Pty Ltd<br>
  1300 938 638<br>
  W: <a href="http://www.fornrg.com/">http://www.fornrg.com/</a></p>
</body>
</html>"""
    if not values["shoulder_rate_c_kwh"]:
        html = re.sub(r"[ \t]*Shoulder:\s*<br>\s*", "", html, flags=re.I)
    if values["take_or_pay_pct"] and not re.search(r"take[\s-]*or[\s-]*pay", html, re.I):
        insertion = f'  <p>Take or Pay: {values["take_or_pay_pct"]}</p>\n'
        html, replaced = re.subn(
            r"(<p>Annual consumption:[^<]*</p>)",
            lambda match: match.group(1) + "\n" + insertion,
            html,
            count=1,
            flags=re.I,
        )
        if not replaced:
            html = html.replace(
                "<p>Attached are both",
                f"{insertion}  <p>Attached are both",
                1,
            )
    return html


def required_electricity_send_errors(draft: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not _val(draft, "company_name"):
        errors.append("Company name is required.")
    if not split_nmis(_val(draft, "nmis")):
        errors.append("At least one NMI is required.")
    if not _val(draft, "start_date"):
        errors.append("Start date is required.")
    if not _val(draft, "end_date"):
        errors.append("End date is required.")
    if not _val(draft, "peak_rate_c_kwh"):
        errors.append("Peak rate is required.")
    if not _val(draft, "commission_c_kwh"):
        errors.append("Commission is required.")
    if not draft.get("loa_file_id"):
        errors.append("Letter of Authority was not found for this member. Upload the LOA first.")
    return errors


def electricity_ef_folder_id() -> str:
    return (
        os.getenv("ALINTA_ELEC_EF_FOLDER_ID")
        or os.getenv("ALINTA_GAS_EF_FOLDER_ID")
        or DEFAULT_GAS_EF_FOLDER_ID
    ).strip()


def electricity_ef_folder_url() -> str:
    folder_id = electricity_ef_folder_id()
    return f"https://drive.google.com/drive/folders/{folder_id}" if folder_id else ""


def send_alinta_electricity_agreement(
    draft: dict[str, Any],
    *,
    pdf_bytes: Optional[bytes] = None,
    filename: str = "",
    user_email: Optional[str] = None,
) -> dict[str, Any]:
    errors = required_electricity_send_errors(draft)
    if errors:
        return {"ok": False, "message": " ".join(errors), "errors": errors}
    if not pdf_bytes:
        return {"ok": False, "message": "The signed EF PDF is required to send.", "errors": ["missing pdf"]}

    subject = build_electricity_email_subject(draft)
    html = build_electricity_email_html(draft)
    recipient = alinta_electricity_recipient_email()
    company = _val(draft, "company_name")
    nmis = split_nmis(_val(draft, "nmis"))
    nmi_display = format_nmis_for_subject(nmis)
    uploaded_name = filename or f"Signed Alinta EF {company} NMI {nmi_display}.pdf"
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
        "business_name": f"{company} NMI: {nmi_display}",
        "contract_type": "Alinta C&I Electricity",
        "agreement_type": N8N_AGREEMENT_TYPE,
        "supplier_email": recipient,
        "resolved_supplier_name": "Data Quote",
        "email_subject": subject,
        "email_html_content": html,
        "loa_file_id": str(draft.get("loa_file_id") or ""),
        "file_count": str(len(files)),
        "nmi": join_nmis(nmis),
        "user_email": user_email or "",
        "timestamp": datetime.now().isoformat(),
        "client_folder_url": client_folder_url,
        "client_folder_id": client_folder_id,
        "gdrive_folder_url": client_folder_url,
        "gdrive_folder_id": client_folder_id,
        "gas_ef_folder_id": electricity_ef_folder_id(),
        "gas_ef_folder_url": electricity_ef_folder_url(),
    }

    try:
        response = requests.post(
            N8N_EMAIL_SUPPLIER_URL,
            data=form,
            files=files,
            timeout=90,
        )
    except requests.RequestException as e:
        logger.error("alinta electricity agreement email-supplier failed: %s", e)
        return {"ok": False, "message": f"Could not send email: {e}", "errors": [str(e)]}
    finally:
        for item in files.values():
            try:
                item[1].close()
            except Exception:
                pass

    logger.info(
        "alinta electricity agreement email-supplier HTTP %s body=%s client_folder=%s loa_dl=%s lodge=%s",
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
        f"Agreement request sent to {recipient} with the EF"
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
        "recipient": recipient,
        "loa_file_id": draft.get("loa_file_id"),
        "client_folder_url": client_folder_url or None,
        "client_folder_id": client_folder_id or None,
        "drive_folder_url": client_folder_url or None,
        "lodge_error": lodge_error or None,
        "agreement_type": N8N_AGREEMENT_TYPE,
    }


def build_electricity_extract_response(
    pdf_bytes: bytes,
    *,
    filename: str = "",
    business_name: str = "",
    query_nmis: Optional[list[str]] = None,
) -> dict[str, Any]:
    extracted = extract_alinta_electricity_ef(pdf_bytes, filename=filename)
    extract = extracted["extract"]
    nmis = split_nmis(extract.get("nmis")) or list(query_nmis or [])
    lookup: dict[str, Any] = {
        "query_nmis": nmis,
        "normalized_nmis": nmis,
        "match_kind": "none",
        "contracts": [],
    }
    if nmis:
        try:
            lookup = lookup_bne_electricity_contracts(nmis)
        except Exception as e:
            logger.warning("bne electricity lookup failed for nmis=%s: %s", nmis, e)
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

    draft = compose_alinta_electricity_draft(
        extract,
        lookup,
        query_nmis=query_nmis or nmis,
        business_info=business_info,
    )
    start_val = str((draft.get("fields") or {}).get("start_date", {}).get("value") or "")
    end_val = str((draft.get("fields") or {}).get("end_date", {}).get("value") or "")
    if _OPEN_DATE_RE.match(start_val) or _OPEN_DATE_RE.match(end_val):
        extracted["extraction_warnings"].append(
            "Period dates on the EF are TBC — they were not taken from the signed C&I electricity sheet."
        )
    elif not start_val or not end_val:
        extracted["extraction_warnings"].append(
            "Period dates were not on the EF. Fill them from the PDF (sheet dates are the current contract, not this request)."
        )
    subject = build_electricity_email_subject(draft)
    html = build_electricity_email_html(draft)
    return {
        "extract": extract,
        "extraction_warnings": extracted["extraction_warnings"],
        "contract": lookup,
        "draft": draft,
        "email_subject": subject,
        "email_html_content": html,
        "recipient": alinta_electricity_recipient_email(),
    }
