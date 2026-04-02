"""
Solar cleaning quote generation via Google Docs API (service account).

1. Copy the master quote template in Drive.
2. batchUpdate replaceAllText for each placeholder.
3. Export the filled doc as PDF and upload beside it in the client folder.

The Google Doc master must use UNIQUE amount tokens per row, e.g. $[AMT_CLEAN] (or $$[AMT_CLEAN]),
  -$[AMT_DISC] or −$$[AMT_DISC] (Unicode minus OK), $[AMT_SUB], $[AMT_GST], $[AMT_TOT].

Other placeholders match the EasyNRG template (square brackets), including a single [STREET ADDRESS]
line for the full site address (no separate suburb line).

Master Google Doc IDs are constants below (SOLAR_QUOTE_TEMPLATE_DOC_ID,
SOLAR_QUOTE_TCS_MASTER_DOC_ID); share those files with the service account.

Destination for new quotes must be inside a Google Shared Drive (Team Drive): service accounts have
no personal Drive quota. Set ONE_MONTH_SAVINGS_DRIVE_FOLDER_ID (or client_folder_url) to a folder
that lives in a Shared Drive where the service account is a member.
"""

from __future__ import annotations

import io
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from tools.one_month_savings import (
    INVOICE_STORAGE_FOLDER_ID,
    extract_folder_id_from_url,
    get_or_create_subfolder,
    upload_file_to_drive,
)

logger = logging.getLogger(__name__)


def _drive_storage_quota_user_hint(http_error: HttpError) -> Optional[str]:
    """If error is storageQuotaExceeded, return a short setup hint for service accounts + Shared Drives."""
    try:
        import json

        raw = getattr(http_error, "content", None) or b""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        data = json.loads(raw) if raw.strip().startswith("{") else {}
        for err in (data.get("error") or {}).get("errors") or []:
            if err.get("reason") == "storageQuotaExceeded":
                return (
                    "Google service accounts have no personal Drive storage. Put the parent folder inside "
                    "a Shared Drive (Team Drive), add the service account as a member (e.g. Content manager), "
                    "then set ONE_MONTH_SAVINGS_DRIVE_FOLDER_ID or the client folder URL to that Shared Drive "
                    "folder. Share the quote template Google Doc with the service account so it can copy it."
                )
    except Exception:
        pass
    return None


backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(dotenv_path=os.path.join(backend_root, ".env"), override=False)

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service-account-key.json")

DRIVE_DOCS_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

# EasyNRG_Quote_TEMPLATE
SOLAR_QUOTE_TEMPLATE_DOC_ID = "1-gX0UUAZW1PnQPahffVsRDqYWWGNpoZa"
# EasyNRG_Solar_Cleaning_TCs (master for n8n / send flow)
SOLAR_QUOTE_TCS_MASTER_DOC_ID = "1yAVB8xyd1IvgC2uCFqtZi9tEOjmxX9Eb"


def _load_sa_credentials() -> Optional[Credentials]:
    service_account_info = os.getenv("SERVICE_ACCOUNT_JSON")
    file_exists = os.path.exists(SERVICE_ACCOUNT_FILE)
    try:
        if file_exists:
            return Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=DRIVE_DOCS_SCOPES,
            )
        if service_account_info:
            import json

            data = (
                json.loads(service_account_info)
                if isinstance(service_account_info, str)
                else service_account_info
            )
            return Credentials.from_service_account_info(
                data,
                scopes=DRIVE_DOCS_SCOPES,
            )
    except Exception as e:
        logger.exception("Failed to load service account for solar quote: %s", e)
    return None


def get_drive_and_docs_services() -> Tuple[Any, Any]:
    creds = _load_sa_credentials()
    if not creds:
        raise RuntimeError("Service account not configured for solar cleaning quote")
    drive = build("drive", "v3", credentials=creds)
    docs = build("docs", "v1", credentials=creds)
    return drive, docs


def _format_aud(n: float) -> str:
    """Format as $1,234.56 for template amounts."""
    neg = n < 0
    n = abs(n)
    s = f"{n:,.2f}"
    return f"-${s}" if neg else f"${s}"


def _pdf_to_text(pdf_bytes: bytes) -> str:
    """Extract plain text from a PDF (best-effort)."""
    try:
        from pypdf import PdfReader
    except ImportError as e:
        raise RuntimeError(
            "pypdf is required for vendor quote extraction. Install with: pip install pypdf"
        ) from e
    reader = PdfReader(io.BytesIO(pdf_bytes))
    parts: list[str] = []
    for page in reader.pages:
        t = page.extract_text()
        if t:
            parts.append(t)
    return "\n".join(parts)


def _parse_money_token(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    s = raw.replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _money_after_label(text: str, label_regex: str) -> Optional[float]:
    """Find first $x.xx after a label (same line or shortly after).

    Wrap label_regex in a non-capturing group so alternation (|) in the label
    cannot match without the trailing amount (which would leave group(1) unset).
    """
    pattern = r"(?:" + label_regex + r")[^\$]{0,120}\$\s*([\d,]+\.\d{2})"
    m = re.search(pattern, text, re.I | re.DOTALL)
    if not m:
        return None
    g1 = m.group(1)
    if not g1:
        return None
    return _parse_money_token(g1)


def _money_after_label_last(text: str, label_regex: str) -> Optional[float]:
    """Like _money_after_label but use the last match (e.g. footer totals vs mid-page duplicate)."""
    pattern = r"(?:" + label_regex + r")[^\$]{0,120}\$\s*([\d,]+\.\d{2})"
    matches = list(re.finditer(pattern, text, re.I | re.DOTALL))
    if not matches:
        return None
    g1 = matches[-1].group(1)
    if not g1:
        return None
    return _parse_money_token(g1)


def extract_vendor_quote_from_pdf(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    Heuristic extraction from supplier quote PDF (e.g. EasyNRG layout).
    Returns merge keys plus extraction_warnings when guesses were used.
    """
    warnings: list[str] = []
    text = _pdf_to_text(pdf_bytes)
    # Normalise spaces so lookbehinds (e.g. "ex GST") match PDFs that use NBSP/thin space.
    text = (
        text.replace("\u00a0", " ")
        .replace("\u2009", " ")
        .replace("\u202f", " ")
    )
    if not text or len(text.strip()) < 20:
        return {
            "extraction_warnings": ["Could not read meaningful text from the PDF (scanned image?). Use Manual entry or OCR."]
        }

    out: Dict[str, Any] = {}

    qn_m = re.search(
        r"(?:CUSTOMER\s+)?QUOTATION\s*No\.?\s*(\d+)|Quote\s*No\.?\s*[:\s#-]*(\d+)|Quotation\s*No\.?\s*[:\s#-]*(\d+)",
        text,
        re.I,
    )
    if qn_m:
        qn_groups = [g for g in qn_m.groups() if g]
        if qn_groups:
            out["quote_number"] = qn_groups[0]
    else:
        warnings.append("Quote number not found in PDF text.")

    date_m = re.search(
        r"\b(\d{1,2})[/.](\d{1,2})[/.](\d{2,4})\b",
        text,
    )
    if date_m:
        d, mo, y = date_m.group(1), date_m.group(2), date_m.group(3)
        if len(y) == 2:
            y = "20" + y
        out["quote_date"] = f"{int(d):02d}/{int(mo):02d}/{y}"

    pk = re.search(
        r"(\d+)\s*panels?\s*[/\s]+\s*(\d+(?:\.\d+)?)\s*kW",
        text,
        re.I,
    )
    if pk:
        out["panel_qty"] = pk.group(1)
        out["system_kw"] = pk.group(2).rstrip("0").rstrip(".") if "." in pk.group(2) else pk.group(2)
    else:
        pk2 = re.search(
            r"(\d+(?:\.\d+)?)\s*kW[^\n]{0,40}?(\d+)\s*p(?:anel)?s?",
            text,
            re.I,
        )
        if pk2:
            out["system_kw"] = pk2.group(1).rstrip("0").rstrip(".")
            out["panel_qty"] = pk2.group(2)
        else:
            warnings.append("Panel count / kW not found in PDF text.")

    amt_clean = _money_after_label(
        text,
        r"(?:Commercial\s+)?Solar\s+Panel\s+Cleaning|Panel\s+Cleaning",
    )
    amt_disc = _money_after_label(
        text,
        r"(?:Member\s+)?Discount|Group\s+Member\s+Discount",
    )
    if amt_disc is None:
        dm = re.search(
            r"Discount[^\n]*?([−\-])\s*\$\s*([\d,]+\.\d{2})",
            text,
            re.I,
        )
        if dm:
            amt_disc = _parse_money_token(dm.group(2))
    # Subtotal / total: use last match — quotes often repeat "Sub Total ex GST" (line item + summary).
    amt_sub = _money_after_label_last(text, r"Sub\s*Total\s*ex\s*GST")
    amt_tot = _money_after_label_last(text, r"TOTAL\s*INC\s*GST")
    # Do not match the trailing "GST" in "Sub Total ex GST" (would capture the subtotal as GST).
    # (?<!ex\s) = not immediately after "ex" + space (fixed-width lookbehind).
    amt_gst = _money_after_label(
        text,
        r"(?<!ex\s)GST\s*(?:\(10%\))?",
    )
    # If still equal to subtotal (mis-parse), try GST amount after the last subtotal block.
    if (
        amt_gst is not None
        and amt_sub is not None
        and abs(amt_gst - amt_sub) < 0.02
    ):
        amt_gst = None
        last_sub = None
        for m in re.finditer(r"Sub\s*Total\s*ex\s*GST", text, re.I):
            last_sub = m
        if last_sub is not None:
            tail = text[last_sub.end() : last_sub.end() + 600]
            tail_gst = _money_after_label(
                tail,
                r"(?<!ex\s)GST\s*(?:\(10%\))?",
            )
            if tail_gst is not None and abs(tail_gst - amt_sub) >= 0.02:
                amt_gst = tail_gst
    if (
        amt_gst is None
        and amt_sub is not None
        and amt_tot is not None
        and amt_tot > amt_sub
    ):
        inferred = round(amt_tot - amt_sub, 2)
        expected_gst = round(0.1 * amt_sub, 2)
        if inferred > 0 and abs(inferred - expected_gst) <= max(2.0, 0.02 * amt_sub):
            amt_gst = inferred
            warnings.append("GST inferred from Total inc GST minus Subtotal ex GST (verify).")

    if amt_clean is not None:
        out["amount_cleaning_ex_gst"] = amt_clean
    if amt_disc is not None:
        out["amount_discount"] = amt_disc
    if amt_sub is not None:
        out["amount_subtotal_ex_gst"] = amt_sub
    if amt_gst is not None:
        out["amount_gst"] = amt_gst
    if amt_tot is not None:
        out["amount_total_inc_gst"] = amt_tot

    # Fallback: first five dollar amounts in order of appearance
    if (
        amt_clean is None
        or amt_sub is None
        or amt_tot is None
    ):
        ordered = re.findall(r"\$\s*([\d,]+\.\d{2})", text)
        floats = []
        seen: set[float] = set()
        for o in ordered:
            v = _parse_money_token(o)
            if v is not None and v not in seen:
                seen.add(v)
                floats.append(v)
        if len(floats) >= 5 and amt_clean is None:
            out.setdefault("amount_cleaning_ex_gst", floats[0])
            out.setdefault("amount_discount", floats[1])
            out.setdefault("amount_subtotal_ex_gst", floats[2])
            out.setdefault("amount_gst", floats[3])
            out.setdefault("amount_total_inc_gst", floats[4])
            warnings.append("Amounts inferred from first five currency values in the PDF (verify).")

    # "Prepared for" client lines
    client_block = re.search(
        r"Prepared\s+for:?\s*\n+([^\n]+)(?:\n+([^\n]+))?(?:\n+([^\n]+))?",
        text,
        re.I,
    )
    if client_block:
        name_l = client_block.group(1).strip()
        if name_l and not re.match(r"^\$|^\d", name_l):
            out["client_name"] = name_l
            out.setdefault("site_name", name_l)
        if client_block.group(2):
            line2 = client_block.group(2).strip()
            if line2 and not re.match(r"^Date:|^Valid|^Sales", line2, re.I):
                out.setdefault("street_address", line2)
        if client_block.group(3):
            line3 = client_block.group(3).strip()
            if line3 and not re.match(r"^Date:|^Valid|^Sales", line3, re.I):
                out.setdefault("suburb_state_postcode", line3)

    attn = re.search(
        r"(?:Contact|Attention)\s*:?\s*([A-Za-z][^\n]{2,80})",
        text,
        re.I,
    )
    if attn:
        out["contact_name"] = attn.group(1).strip()

    out["extraction_warnings"] = warnings
    return out


def _coerce_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    return _parse_money_token(str(v)) or 0.0


def merge_quote_payload(
    payload: Dict[str, Any],
    *,
    preview_only: bool = False,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    If manual_entry is False, require PDF and merge extraction with CRM prefills
    (payload non-empty strings win for identity fields; extracted wins for amounts if not overridden).

    preview_only: merge and return best-effort fields for UI prefill without requiring
    quote number / pricing (still requires a decodable PDF).
    """
    manual = bool(payload.get("manual_entry"))
    notes: list[str] = []

    src_b64 = payload.get("source_pdf_base64")
    pdf_bytes: Optional[bytes] = None
    if src_b64:
        import base64

        try:
            pdf_bytes = base64.b64decode(src_b64)
        except Exception:
            return {}, ["Invalid vendor PDF data (base64)."]

    if not manual:
        if not pdf_bytes:
            return {}, [
                "Vendor quote PDF is required, or enable Manual entry to type fields instead."
            ]
        try:
            extracted = extract_vendor_quote_from_pdf(pdf_bytes)
        except RuntimeError as e:
            return {}, [str(e)]
        ex_warn = extracted.pop("extraction_warnings", None) or []
        notes.extend(ex_warn)

        merged: Dict[str, Any] = {**payload}

        # Identity: CRM / URL prefill beats empty extraction
        for key in (
            "client_name",
            "street_address",
            "suburb_state_postcode",
            "contact_name",
            "site_name",
            "business_name",
        ):
            cur = merged.get(key)
            cur_s = str(cur).strip() if cur is not None else ""
            ex_val = extracted.get(key)
            if cur_s:
                merged[key] = cur_s
            elif ex_val:
                merged[key] = ex_val

        # Quote meta: extraction first, allow explicit overrides from form
        for key in ("quote_number", "quote_date", "panel_qty", "system_kw", "pv_cleaning_note"):
            ex_val = extracted.get(key)
            cur = merged.get(key)
            cur_s = (str(cur).strip() if cur is not None else "")
            if cur_s:
                merged[key] = cur_s
            elif ex_val:
                merged[key] = ex_val

        # Amounts: extraction fills; manual form can override if user typed in (only when manual off we normally hide fields — still merge)
        for key in (
            "amount_cleaning_ex_gst",
            "amount_discount",
            "amount_subtotal_ex_gst",
            "amount_gst",
            "amount_total_inc_gst",
        ):
            ex_val = extracted.get(key)
            cur_f = _coerce_float(merged.get(key))
            if cur_f != 0:
                merged[key] = cur_f
            elif ex_val is not None:
                merged[key] = float(ex_val)
            else:
                merged[key] = 0.0

        if not preview_only:
            if not (merged.get("quote_number") or "").strip():
                return {}, notes + ["Could not read quote number from the PDF. Use Manual entry."]
            if not (merged.get("client_name") or merged.get("business_name") or "").strip():
                return {}, notes + ["Could not determine client name from PDF or CRM. Use Manual entry."]
            tot = _coerce_float(merged.get("amount_total_inc_gst"))
            if tot <= 0 and _coerce_float(merged.get("amount_subtotal_ex_gst")) <= 0:
                return {}, notes + ["Could not read pricing from the PDF. Use Manual entry or check PDF text."]
        else:
            if not (merged.get("quote_number") or "").strip():
                notes.append(
                    "Quote number was not detected; enter it manually or check the PDF."
                )
            if not (merged.get("client_name") or merged.get("business_name") or "").strip():
                notes.append(
                    "Client name was not detected; CRM or manual entry may be needed."
                )
            tot = _coerce_float(merged.get("amount_total_inc_gst"))
            if tot <= 0 and _coerce_float(merged.get("amount_subtotal_ex_gst")) <= 0:
                notes.append(
                    "Pricing may be incomplete; verify amounts before generating."
                )
    else:
        merged = {**payload}
        if not (merged.get("quote_number") or "").strip():
            return {}, ["Quote number is required (Manual entry)."]
        if not (merged.get("client_name") or "").strip():
            return {}, ["Client name is required (Manual entry)."]
        for _amt_key in (
            "amount_cleaning_ex_gst",
            "amount_discount",
            "amount_subtotal_ex_gst",
            "amount_gst",
            "amount_total_inc_gst",
        ):
            merged[_amt_key] = _coerce_float(merged.get(_amt_key))

    # Defaults
    if not (merged.get("quote_date") or "").strip():
        merged["quote_date"] = _today_dd_mm_yyyy()
    merged["site_name"] = (merged.get("site_name") or merged.get("client_name") or "").strip()
    merged["business_name"] = (
        merged.get("business_name") or merged.get("client_name") or ""
    ).strip()
    merged["pv_cleaning_note"] = (
        merged.get("pv_cleaning_note") or "TBC — confirmed upon acceptance"
    ).strip()

    return merged, notes


def _clean_address_token(v: Any) -> str:
    """Avoid str(None) -> \"None\" and other junk in joined addresses."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() in ("none", "null"):
        return ""
    return s


def _full_street_address_line(payload: Dict[str, Any]) -> str:
    """Single template line [STREET ADDRESS]: combine legacy suburb field if present."""
    s1 = _clean_address_token(payload.get("street_address"))
    s2 = _clean_address_token(payload.get("suburb_state_postcode"))
    return ", ".join(p for p in (s1, s2) if p)


def applied_fields_from_merged(merged: Dict[str, Any]) -> Dict[str, Any]:
    """Serializable snapshot of merged quote fields for the UI."""
    full_addr = _full_street_address_line(merged)
    return {
        "quote_number": str(merged.get("quote_number") or ""),
        "quote_date": str(merged.get("quote_date") or ""),
        "client_name": str(merged.get("client_name") or ""),
        "street_address": full_addr,
        "suburb_state_postcode": "",
        "panel_qty": str(merged.get("panel_qty") or ""),
        "system_kw": str(merged.get("system_kw") or ""),
        "site_name": str(merged.get("site_name") or ""),
        "contact_name": str(merged.get("contact_name") or ""),
        "amount_cleaning_ex_gst": _coerce_float(merged.get("amount_cleaning_ex_gst")),
        "amount_discount": _coerce_float(merged.get("amount_discount")),
        "amount_subtotal_ex_gst": _coerce_float(merged.get("amount_subtotal_ex_gst")),
        "amount_gst": _coerce_float(merged.get("amount_gst")),
        "amount_total_inc_gst": _coerce_float(merged.get("amount_total_inc_gst")),
    }


def preview_solar_quote_extract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Parse vendor PDF + CRM prefill for UI only (no Google Doc generation)."""
    body = {**payload, "manual_entry": False}
    merged, notes = merge_quote_payload(body, preview_only=True)
    if not merged:
        return {
            "success": False,
            "error": notes[0] if notes else "Could not read PDF",
            "extraction_warnings": notes,
        }
    return {
        "success": True,
        "extraction_warnings": notes,
        "applied_fields": applied_fields_from_merged(merged),
    }


def _today_dd_mm_yyyy() -> str:
    import datetime

    d = datetime.datetime.now()
    return f"{d.day:02d}/{d.month:02d}/{d.year}"


def _slug(name: str, max_len: int = 40) -> str:
    s = re.sub(r"[^\w\s-]", "", name or "").strip()
    s = re.sub(r"[-\s]+", "_", s)
    return (s or "Client")[:max_len]


def build_replacements(payload: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Ordered (find, replace) pairs. Longer / more specific strings first where relevant."""
    qn = str(payload.get("quote_number", "")).strip()
    client_name = str(payload.get("client_name", "")).strip()
    address_line = _full_street_address_line(payload)
    date_q = str(payload.get("quote_date", "")).strip()
    tbc = str(
        payload.get("pv_cleaning_note") or "TBC — confirmed upon acceptance"
    ).strip()
    qty = str(payload.get("panel_qty", "")).strip()
    kw = str(payload.get("system_kw", "")).strip()
    site_name = str(payload.get("site_name") or client_name).strip()
    contact_name = str(payload.get("contact_name", "")).strip()

    try:
        amt_clean = float(payload.get("amount_cleaning_ex_gst", 0))
    except (TypeError, ValueError):
        amt_clean = 0.0
    try:
        amt_disc = float(payload.get("amount_discount", 0))
    except (TypeError, ValueError):
        amt_disc = 0.0
    try:
        amt_sub = float(payload.get("amount_subtotal_ex_gst", 0))
    except (TypeError, ValueError):
        amt_sub = 0.0
    try:
        amt_gst = float(payload.get("amount_gst", 0))
    except (TypeError, ValueError):
        amt_gst = 0.0
    try:
        amt_tot = float(payload.get("amount_total_inc_gst", 0))
    except (TypeError, ValueError):
        amt_tot = 0.0

    # Unicode minus (U+2212) for discount row in template
    disc_display = _format_aud(-abs(amt_disc)) if amt_disc else _format_aud(0.0)

    # Order matters: replace lines that still contain [CLIENT NAME] before replacing [CLIENT NAME] alone.
    pairs: List[Tuple[str, str]] = [
        ("CUSTOMER QUOTATION No. XXXX", f"CUSTOMER QUOTATION No. {qn}"),
        ("SOLAR CLEANING QUOTATION No. XXXX", f"SOLAR CLEANING QUOTATION No. {qn}"),
        (
            "EasyNRG Quote No: XXXX [CLIENT NAME]",
            f"EasyNRG Quote No. {qn} {client_name}",
        ),
        ("Quote No. XXXX", f"Quote No. {qn}"),
        ("[CLIENT NAME]", client_name),
        ("[STREET ADDRESS]", address_line),
        ("[DD/MM/YYYY]", date_q),
        ("[TBC — confirmed upon acceptance]", tbc),
        ("[QTY]", qty),
        ("[kW]", kw),
        ("[SITE NAME]", site_name),
        ("[CLIENT CONTACT NAME]", contact_name),
        # Amount tokens: support $$[…] (legacy) and $[…] (current EasyNRG template).
        ("$$[AMT_CLEAN]", _format_aud(amt_clean)),
        ("$[AMT_CLEAN]", _format_aud(amt_clean)),
        ("−$$[AMT_DISC]", disc_display),
        ("-$[AMT_DISC]", disc_display),
        ("−$[AMT_DISC]", disc_display),
        ("$$[AMT_SUB]", _format_aud(amt_sub)),
        ("$[AMT_SUB]", _format_aud(amt_sub)),
        ("$$[AMT_GST]", _format_aud(amt_gst)),
        ("$[AMT_GST]", _format_aud(amt_gst)),
        ("$$[AMT_TOT]", _format_aud(amt_tot)),
        ("$[AMT_TOT]", _format_aud(amt_tot)),
    ]
    return pairs


def generate_solar_cleaning_quote(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge + validate payload (PDF extraction unless manual_entry), then copy template,
    replace text, export PDF to Drive.

    When manual_entry is False, source_pdf_base64 is required and quote fields are read from the PDF.
    """
    merged, merge_notes = merge_quote_payload(payload, preview_only=False)
    if not merged:
        return {
            "success": False,
            "error": merge_notes[0] if merge_notes else "Invalid request",
            "extraction_warnings": merge_notes,
        }
    payload = merged

    template_id = SOLAR_QUOTE_TEMPLATE_DOC_ID
    drive, docs = get_drive_and_docs_services()

    folder_url = (payload.get("client_folder_url") or "").strip()
    parent_folder_id: Optional[str] = None
    if folder_url:
        parent_folder_id = extract_folder_id_from_url(folder_url)
    if not parent_folder_id:
        parent_folder_id = INVOICE_STORAGE_FOLDER_ID or None
    if not parent_folder_id:
        return {
            "success": False,
            "error": "No Drive folder: set client_folder_url or INVOICE_STORAGE_FOLDER_ID",
        }

    work_folder_id = get_or_create_subfolder(
        drive, parent_folder_id, "Solar Cleaning Quotes"
    )
    if not work_folder_id:
        return {"success": False, "error": "Could not create or open Solar Cleaning Quotes folder"}

    business = str(payload.get("business_name") or payload.get("client_name") or "Client").strip()
    qn = str(payload.get("quote_number", "")).strip()
    copy_title = f"EasyNRG_Quote_{qn}_{_slug(business)}"

    try:
        copied = (
            drive.files()
            .copy(
                fileId=template_id,
                body={"name": copy_title, "parents": [work_folder_id]},
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as e:
        logger.exception("Drive copy failed")
        hint = _drive_storage_quota_user_hint(e)
        msg = hint or f"Drive copy failed: {e.reason or e}"
        return {"success": False, "error": msg}

    new_doc_id = copied.get("id")
    if not new_doc_id:
        return {"success": False, "error": "Copy succeeded but no document id returned"}

    requests_body: List[Dict[str, Any]] = []
    for find, repl in build_replacements(payload):
        if not find:
            continue
        requests_body.append(
            {
                "replaceAllText": {
                    "containsText": {"text": find, "matchCase": True},
                    "replaceText": repl,
                }
            }
        )

    if requests_body:
        try:
            docs.documents().batchUpdate(
                documentId=new_doc_id, body={"requests": requests_body}
            ).execute()
        except HttpError as e:
            logger.exception("Docs batchUpdate failed")
            return {
                "success": False,
                "error": f"Docs update failed: {e.reason or e}",
                "quote_google_doc_id": new_doc_id,
            }

    # Export PDF
    pdf_bytes: Optional[bytes] = None
    try:
        req = drive.files().export_media(
            fileId=new_doc_id, mimeType="application/pdf"
        )
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        pdf_bytes = fh.read()
    except HttpError as e:
        logger.exception("Export PDF failed")
        return {
            "success": False,
            "error": f"PDF export failed: {e.reason or e}",
            "quote_google_doc_id": new_doc_id,
            "quote_doc_url": f"https://docs.google.com/document/d/{new_doc_id}/edit",
        }

    pdf_name = f"EasyNRG_Quote_{qn}_{_slug(business)}.pdf"
    pdf_file_id = upload_file_to_drive(
        pdf_bytes, pdf_name, work_folder_id, "application/pdf", drive_service=drive
    )

    source_file_id: Optional[str] = None
    src_b64 = payload.get("source_pdf_base64")
    src_fn = payload.get("source_pdf_filename") or "vendor_quote.pdf"
    if src_b64:
        import base64

        try:
            raw = base64.b64decode(src_b64)
            safe_fn = re.sub(r"[^\w.\-]+", "_", str(src_fn))[:180]
            source_file_id = upload_file_to_drive(
                raw,
                f"vendor_{qn}_{safe_fn}",
                work_folder_id,
                "application/pdf",
                drive_service=drive,
            )
        except Exception as e:
            logger.warning("Optional vendor PDF upload failed: %s", e)

    return {
        "success": True,
        "quote_google_doc_id": new_doc_id,
        "quote_doc_url": f"https://docs.google.com/document/d/{new_doc_id}/edit",
        "quote_pdf_file_id": pdf_file_id,
        "quote_pdf_url": (
            f"https://drive.google.com/file/d/{pdf_file_id}/view" if pdf_file_id else None
        ),
        "folder_id": work_folder_id,
        "vendor_quote_file_id": source_file_id,
        "tcs_master_doc_id": SOLAR_QUOTE_TCS_MASTER_DOC_ID,
        "extraction_warnings": merge_notes,
        "applied_fields": applied_fields_from_merged(payload),
    }
