"""Extract Specialist Energy Management Distribution Agreement fields and write the master list."""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
from typing import Any, Optional

from googleapiclient.errors import HttpError

from tools.member_folder_drive import _user_drive_service

logger = logging.getLogger(__name__)

DISTRIBUTOR_MASTER_SHEET_ID = os.getenv(
    "DISTRIBUTOR_MASTER_SHEET_ID",
    "1kj9K6XG7477gdIGfZf3BY6Ozm9K6ImVVFAYOpjXgMkwU",
)
DISTRIBUTOR_MASTER_TAB = os.getenv("DISTRIBUTOR_MASTER_TAB", "Sheet1")

FOLDER_NAME_PREFIX = "A - "
_SCAN_IMAGE_MIN_BYTES = 80_000
_SCAN_MAX_PAGES = 6
_VISION_KEYS = (
    "distributor_business",
    "trading_as",
    "abn",
    "acn",
    "contact_name",
    "contact_position",
    "email",
    "phone",
    "mobile",
    "address",
    "state",
    "postcode",
    "start_date",
    "signed_date",
    "initial_term_months",
    "territory",
    "exclusivity",
    "notes",
)
_VISION_PROMPT = """Extract fields from this Carbon Zero Australasia (CZAS) Distribution Agreement.
The PDF is often a scan of a signed paper copy. Read printed text AND handwriting, including
carets / insertions (e.g. handwritten "P.O. Box" in the address, handwritten ABN).

Return a JSON object with exactly these keys (use "" if not present; never invent):
distributor_business, trading_as, abn, acn, contact_name, contact_position, email, phone,
mobile, address, state, postcode, start_date, signed_date, initial_term_months, territory,
exclusivity, notes.

Rules:
- distributor_business is the other party, never Carbon Zero Australasia / CZAS / the Supplier.
- abn and acn: digits only.
- address: the distributor address after applying handwritten corrections.
- state: NSW/VIC/QLD/SA/WA/TAS/ACT/NT. postcode: 4 digits.
- territory: the Area field (e.g. Australia).
- initial_term_months: number of months only.
- exclusivity: "Y" if they are exclusive to CZAS, otherwise as printed.
- contact_name, contact_position, signed_date: from the Distributor signature block
  (the party that is not CZAS), including handwritten name/date.
- notes: only if something important cannot fit another field.
"""

LEGAL_SUFFIX_RE = re.compile(
    r"\s+(pty\.?\s*ltd\.?|pty|limited|ltd|inc\.?|incorporated)\s*$",
    re.I,
)

SHEET_COLUMNS: list[str] = [
    "Distributor Business",
    "Trading As",
    "ABN",
    "ACN",
    "Contact Name",
    "Contact Position",
    "Email",
    "Phone",
    "Mobile",
    "Address",
    "State",
    "Postcode",
    "Start Date",
    "Signed Date",
    "Initial Term (months)",
    "Territory / Area",
    "Exclusivity",
    "Status",
    "Folder Name",
    "Drive Folder ID",
    "Drive Folder URL",
    "Agreement File URL",
    "Notes",
]


def _pdf_to_text(pdf_bytes: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(pdf_bytes))
    parts: list[str] = []
    for page in reader.pages:
        t = page.extract_text()
        if t:
            parts.append(t)
    return "\n".join(parts)


def _pdf_page_images(pdf_bytes: bytes) -> list[bytes]:
    """Full-page scan JPEGs/PNGs embedded in the PDF (typical of signed scans)."""
    from pypdf import PdfReader

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
    except Exception:
        return []
    out: list[bytes] = []
    for page in reader.pages:
        try:
            images = list(page.images)
        except Exception:
            images = []
        for im in images:
            data = getattr(im, "data", None) or b""
            if len(data) >= _SCAN_IMAGE_MIN_BYTES:
                out.append(data)
        if len(out) >= _SCAN_MAX_PAGES:
            break
    return out[:_SCAN_MAX_PAGES]


def _jpeg_for_vision(image_bytes: bytes) -> bytes:
    from PIL import Image

    im = Image.open(io.BytesIO(image_bytes))
    if im.mode != "RGB":
        im = im.convert("RGB")
    width, height = im.size
    max_edge = 2048
    longest = max(width, height)
    if longest > max_edge:
        scale = max_edge / longest
        im = im.resize((int(width * scale), int(height * scale)), Image.Resampling.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=90, optimize=True)
    return buf.getvalue()


def _extract_from_page_images(pdf_bytes: bytes) -> dict[str, str]:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    images = _pdf_page_images(pdf_bytes)
    if not api_key or not images:
        return {}
    import httpx

    parts: list[dict[str, Any]] = [{"type": "text", "text": _VISION_PROMPT}]
    for raw in images:
        try:
            jpeg = _jpeg_for_vision(raw)
        except Exception as e:
            logger.warning("distributor scan image could not be prepared: %s", e)
            continue
        b64 = base64.b64encode(jpeg).decode("ascii")
        parts.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "high"},
            }
        )
    if len(parts) < 2:
        return {}
    try:
        response = httpx.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o",
                "temperature": 0,
                "response_format": {"type": "json_object"},
                "messages": [
                    {
                        "role": "system",
                        "content": "You extract structured fields from scanned contracts. JSON only.",
                    },
                    {"role": "user", "content": parts},
                ],
            },
            timeout=90.0,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except Exception as e:
        logger.warning("distributor scan vision extract failed: %s", e)
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, str] = {}
    for key in _VISION_KEYS:
        value = parsed.get(key)
        out[key] = _clean("" if value is None else str(value))
    if out.get("abn"):
        out["abn"] = re.sub(r"\D", "", out["abn"])
    if out.get("acn"):
        out["acn"] = re.sub(r"\D", "", out["acn"])
    if out.get("initial_term_months"):
        tm = re.search(r"(\d+)", out["initial_term_months"])
        out["initial_term_months"] = tm.group(1) if tm else ""
    return out


def _parse_agreement_text(text: str) -> dict[str, str]:
    business = _field_after(text, r"Distributor")
    if business.lower().startswith("carbon zero"):
        m = re.search(r"Distributor:\s*(.+)", text)
        business = _clean(m.group(1)) if m else business

    abn = _field_after(text, r"ABN")
    acn = _field_after(text, r"ACN")
    address = _field_after(text, r"Adress") or _field_after(text, r"Address")
    email = _field_after(text, r"Email")
    phone = _field_after(text, r"Phone")
    mobile = _field_after(text, r"Mobile")
    start_date = _field_after(text, r"Start date")
    area = _field_after(text, r"Area")
    term_raw = _field_after(text, r"Initial term")
    term_months = ""
    tm = re.search(r"(\d+)", term_raw)
    if tm:
        term_months = tm.group(1)

    names = re.findall(r"Name:\s*(.+)", text, re.I)
    positions = re.findall(r"Position:\s*(.+)", text, re.I)
    dates = re.findall(r"Date:\s*(.+)", text, re.I)
    contact_name = _clean(names[-1]) if names else ""
    contact_position = _clean(positions[-1]) if positions else ""
    signed_date = ""
    if dates:
        signed_date = re.sub(r"^_+|_+$", "", _clean(dates[-1])).strip()

    exclusivity = "Y" if re.search(r"You are exclusive to us", text, re.I) else ""

    return {
        "distributor_business": business,
        "trading_as": "",
        "abn": re.sub(r"\D", "", abn) if abn else "",
        "acn": re.sub(r"\D", "", acn) if acn else "",
        "contact_name": contact_name,
        "contact_position": contact_position,
        "email": email,
        "phone": phone,
        "mobile": mobile,
        "address": address,
        "state": "",
        "postcode": "",
        "start_date": start_date,
        "signed_date": signed_date,
        "initial_term_months": term_months,
        "territory": area.rstrip("."),
        "exclusivity": exclusivity,
        "notes": "",
    }


def _merge_details(base: dict[str, str], overlay: dict[str, str]) -> dict[str, str]:
    merged = dict(base)
    for key in _VISION_KEYS:
        if not (merged.get(key) or "").strip() and (overlay.get(key) or "").strip():
            merged[key] = overlay[key]
    return merged


def _finalize_details(parsed: dict[str, str], warnings: list[str]) -> dict[str, Any]:
    business = parsed.get("distributor_business") or ""
    address = parsed.get("address") or ""
    state = parsed.get("state") or ""
    postcode = parsed.get("postcode") or ""
    if not state or not postcode:
        guessed_state, guessed_pc = _guess_state_postcode(address)
        state = state or guessed_state
        postcode = postcode or guessed_pc
    folder_name = distributor_folder_name(business, parsed.get("trading_as") or "")
    if not business:
        warnings.append("Distributor business name not found.")
    if not parsed.get("abn"):
        warnings.append("ABN not found.")
    if not folder_name:
        warnings.append("Could not derive folder name.")
    return {
        "distributor_business": business,
        "trading_as": parsed.get("trading_as") or "",
        "abn": parsed.get("abn") or "",
        "acn": parsed.get("acn") or "",
        "contact_name": parsed.get("contact_name") or "",
        "contact_position": parsed.get("contact_position") or "",
        "email": parsed.get("email") or "",
        "phone": parsed.get("phone") or "",
        "mobile": parsed.get("mobile") or "",
        "address": address,
        "state": state,
        "postcode": postcode,
        "start_date": parsed.get("start_date") or "",
        "signed_date": parsed.get("signed_date") or "",
        "initial_term_months": parsed.get("initial_term_months") or "",
        "territory": parsed.get("territory") or "",
        "exclusivity": parsed.get("exclusivity") or "Y",
        "status": "Active",
        "folder_name": folder_name,
        "notes": parsed.get("notes") or "",
        "extraction_warnings": warnings,
    }


def extract_distribution_agreement(pdf_bytes: bytes) -> dict[str, Any]:
    """Best-effort field extraction from the CZAS distribution agreement PDF."""
    warnings: list[str] = []
    try:
        text = _pdf_to_text(pdf_bytes)
    except Exception as e:
        logger.warning("distributor PDF text extract failed: %s", e)
        text = ""
        warnings.append(f"Embedded PDF text could not be read: {e}")

    text = (text or "").replace("\u00a0", " ")
    parsed: dict[str, str] = {}
    if len(text.strip()) >= 40:
        parsed = _parse_agreement_text(text)

    images = _pdf_page_images(pdf_bytes)
    missing_handwritten = not (
        parsed.get("abn") and parsed.get("contact_name") and parsed.get("signed_date")
    )
    if images and (len(text.strip()) < 40 or missing_handwritten):
        scan = _extract_from_page_images(pdf_bytes)
        if scan:
            parsed = _merge_details(parsed, scan)
            warnings.append("Read from scanned pages, including handwriting.")
        elif not parsed.get("distributor_business"):
            warnings.append(
                "This looks like a signed scan and the page images could not be read. "
                "Enter fields manually."
            )
            return {"extraction_warnings": warnings}

    if not parsed.get("distributor_business") and len(text.strip()) < 40:
        return {
            "extraction_warnings": warnings
            or [
                "Could not read the agreement (scanned image with no readable text). "
                "Enter fields manually."
            ]
        }

    return _finalize_details(parsed, warnings)


def _clean(value: str) -> str:
    s = (value or "").replace("\u00a0", " ").strip()
    s = s.strip("].;,:")
    s = re.sub(r"\s+", " ", s)
    if s in {"[•]", "•", "-", "n/a", "N/A"}:
        return ""
    return s


def _field_after(text: str, label: str) -> str:
    pattern = rf"{label}\s*:\s*(.+)"
    m = re.search(pattern, text, re.I)
    if not m:
        return ""
    return _clean(m.group(1).split("\n")[0])


def distributor_folder_name(business_name: str, trading_as: str = "") -> str:
    source = (trading_as or "").strip() or (business_name or "").strip()
    source = LEGAL_SUFFIX_RE.sub("", source).strip()
    source = re.sub(r"\s+", " ", source)
    if not source:
        return ""
    if source.startswith(FOLDER_NAME_PREFIX):
        return source
    return f"{FOLDER_NAME_PREFIX}{source}"


def _guess_state_postcode(address: str) -> tuple[str, str]:
    m = re.search(r"\b(NSW|VIC|QLD|SA|WA|TAS|ACT|NT)\b\s*(\d{4})?", address, re.I)
    if not m:
        pc = re.search(r"\b(\d{4})\b", address)
        return "", pc.group(1) if pc else ""
    return m.group(1).upper(), m.group(2) or ""


def details_to_sheet_row(details: dict[str, Any]) -> list[str]:
    mapping = {
        "Distributor Business": details.get("distributor_business") or "",
        "Trading As": details.get("trading_as") or "",
        "ABN": details.get("abn") or "",
        "ACN": details.get("acn") or "",
        "Contact Name": details.get("contact_name") or "",
        "Contact Position": details.get("contact_position") or "",
        "Email": details.get("email") or "",
        "Phone": details.get("phone") or "",
        "Mobile": details.get("mobile") or "",
        "Address": details.get("address") or "",
        "State": details.get("state") or "",
        "Postcode": details.get("postcode") or "",
        "Start Date": details.get("start_date") or "",
        "Signed Date": details.get("signed_date") or "",
        "Initial Term (months)": details.get("initial_term_months") or "",
        "Territory / Area": details.get("territory") or "",
        "Exclusivity": details.get("exclusivity") or "",
        "Status": details.get("status") or "Active",
        "Folder Name": details.get("folder_name") or "",
        "Drive Folder ID": details.get("drive_folder_id") or "",
        "Drive Folder URL": details.get("drive_folder_url") or "",
        "Agreement File URL": details.get("agreement_file_url") or "",
        "Notes": details.get("notes") or "",
    }
    return [str(mapping[col]) for col in SHEET_COLUMNS]


def _a1_tab(tab: str) -> str:
    name = (tab or "Sheet1").replace("'", "''")
    return f"'{name}'"


def _ensure_header(service: Any, spreadsheet_id: str, tab: str) -> None:
    header_resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{_a1_tab(tab)}!A1:AZ1")
        .execute()
    )
    existing = [str(h).strip() for h in (header_resp.get("values") or [[]])[0]]
    if existing[:3] == SHEET_COLUMNS[:3] and len(existing) >= len(SHEET_COLUMNS):
        return
    if existing and existing[0] and existing != SHEET_COLUMNS:
        logger.warning(
            "Distributor master list header mismatch (have %s). Writing canonical header to row 1.",
            existing[:8],
        )
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{_a1_tab(tab)}!A1",
        valueInputOption="RAW",
        body={"values": [SHEET_COLUMNS]},
    ).execute()


def _user_sheets_service(access_token: str) -> Any:
    from google.oauth2.credentials import Credentials as UserCredentials
    from googleapiclient.discovery import build

    return build(
        "sheets",
        "v4",
        credentials=UserCredentials(token=(access_token or "").strip()),
        cache_discovery=False,
    )


def _drive_file_meta(drive: Any, file_id: str) -> Optional[dict[str, Any]]:
    try:
        return (
            drive.files()
            .get(
                fileId=file_id,
                fields="id,name,mimeType,capabilities",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as e:
        status = int(
            getattr(e, "status_code", None)
            or getattr(getattr(e, "resp", None), "status", 0)
            or 0
        )
        if status in {403, 404}:
            return None
        raise


def _find_master_list_in_drive(drive: Any) -> Optional[dict[str, Any]]:
    query = (
        "name contains 'Distributor Master' and trashed=false and "
        "mimeType='application/vnd.google-apps.spreadsheet'"
    )
    result = (
        drive.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id,name,mimeType)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = result.get("files") or []
    if not files:
        result = (
            drive.files()
            .list(
                q="name contains 'Distributor Master' and trashed=false",
                spaces="drive",
                fields="files(id,name,mimeType)",
                pageSize=10,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = result.get("files") or []
    return files[0] if files else None


def resolve_distributor_master_sheet(access_token: str) -> tuple[str, str]:
    """Return (spreadsheet_id, tab_title) visible to the signed-in user."""
    drive = _user_drive_service(access_token)
    configured = (DISTRIBUTOR_MASTER_SHEET_ID or "").strip()
    meta = _drive_file_meta(drive, configured) if configured else None
    if not meta:
        meta = _find_master_list_in_drive(drive)
    if not meta:
        raise RuntimeError(
            "Drive cannot see Distributor Master List with this login, so the row "
            "could not be written. The folder was still created."
        )
    mime = str(meta.get("mimeType") or "")
    sheet_id = str(meta.get("id") or "").strip()
    if mime and mime != "application/vnd.google-apps.spreadsheet":
        raise RuntimeError(
            f"Distributor Master List is {mime}, not a native Google Sheet. "
            "In Drive: File → Save as Google Sheets, then retry Confirm."
        )
    sheets = _user_sheets_service(access_token)
    try:
        info = (
            sheets.spreadsheets()
            .get(spreadsheetId=sheet_id, fields="sheets.properties.title")
            .execute()
        )
    except HttpError as e:
        raise RuntimeError(
            f"Drive can see {meta.get('name')!r} ({sheet_id}) but Sheets API cannot "
            f"open it ({getattr(e, 'status_code', '?')}: {getattr(e, 'reason', e)})."
        ) from e
    titles = [
        str(s.get("properties", {}).get("title") or "")
        for s in (info.get("sheets") or [])
    ]
    titles = [t for t in titles if t]
    if not titles:
        raise RuntimeError("Distributor Master List has no tabs.")
    preferred = (DISTRIBUTOR_MASTER_TAB or "Sheet1").strip()
    tab = preferred if preferred in titles else titles[0]
    logger.info(
        "[distributor] writing master list id=%s name=%r tab=%r tabs=%s",
        sheet_id,
        meta.get("name"),
        tab,
        titles,
    )
    return sheet_id, tab


def _write_distributor_row(service: Any, spreadsheet_id: str, tab: str, details: dict[str, Any]) -> None:
    _ensure_header(service, spreadsheet_id, tab)
    row = details_to_sheet_row(details)
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{_a1_tab(tab)}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


def append_distributor_row(
    details: dict[str, Any],
    *,
    user_access_token: Optional[str] = None,
) -> dict[str, Any]:
    token = (user_access_token or "").strip()
    if not token:
        raise RuntimeError(
            "Distributor Master List was not updated because the portal did not send "
            "your Google access token."
        )
    sheet_id, tab = resolve_distributor_master_sheet(token)
    try:
        _write_distributor_row(_user_sheets_service(token), sheet_id, tab, details)
    except HttpError as e:
        raise RuntimeError(
            f"Could not write Distributor Master List row ({getattr(e, 'status_code', '?')}: "
            f"{getattr(e, 'reason', e)})."
        ) from e
    return {
        "spreadsheet_id": sheet_id,
        "spreadsheet_url": f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit",
        "tab": tab,
    }


def update_distributor_drive_cells(
    folder_name: str,
    *,
    folder_id: str,
    folder_url: str,
    agreement_file_url: str = "",
    user_access_token: Optional[str] = None,
) -> None:
    """Fill Drive columns on the last matching Folder Name row."""
    token = (user_access_token or "").strip()
    if not token or not folder_name:
        return
    try:
        sheet_id, tab = resolve_distributor_master_sheet(token)
        service = _user_sheets_service(token)
    except Exception as e:
        logger.warning("Failed to resolve distributor sheet for Drive cells: %s", e)
        return
    try:
        resp = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=f"{_a1_tab(tab)}!A1:W500")
            .execute()
        )
        rows = resp.get("values") or []
        if len(rows) < 2:
            return
        headers = [str(h).strip() for h in rows[0]]
        try:
            name_idx = headers.index("Folder Name")
            id_idx = headers.index("Drive Folder ID")
            url_idx = headers.index("Drive Folder URL")
            file_idx = headers.index("Agreement File URL")
        except ValueError:
            return
        target = (folder_name or "").strip()
        row_number = None
        for i, row in enumerate(rows[1:], start=2):
            cell = row[name_idx].strip() if name_idx < len(row) else ""
            if cell == target:
                row_number = i
        if row_number is None:
            return
        updates = [
            {"range": f"{_a1_tab(tab)}!{chr(65 + id_idx)}{row_number}", "values": [[folder_id]]},
            {"range": f"{_a1_tab(tab)}!{chr(65 + url_idx)}{row_number}", "values": [[folder_url]]},
            {
                "range": f"{_a1_tab(tab)}!{chr(65 + file_idx)}{row_number}",
                "values": [[agreement_file_url]],
            },
        ]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "RAW", "data": updates},
        ).execute()
    except Exception as e:
        logger.warning("Failed to back-fill distributor Drive cells: %s", e)


def list_distributor_master_rows(
    user_access_token: Optional[str] = None,
) -> dict[str, Any]:
    token = (user_access_token or "").strip()
    if not token:
        raise RuntimeError(
            "Distributor Master List could not be loaded because the portal did not send "
            "your Google access token."
        )
    sheet_id, tab = resolve_distributor_master_sheet(token)
    try:
        resp = (
            _user_sheets_service(token)
            .spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=f"{_a1_tab(tab)}!A1:AZ500")
            .execute()
        )
    except HttpError as e:
        raise RuntimeError(
            f"Could not read Distributor Master List ({getattr(e, 'status_code', '?')}: "
            f"{getattr(e, 'reason', e)})."
        ) from e
    values = resp.get("values") or []
    headers = [str(h).strip() for h in (values[0] if values else SHEET_COLUMNS)]
    headers = [h or f"Column {i + 1}" for i, h in enumerate(headers)]
    rows: list[dict[str, Any]] = []
    for i, row in enumerate(values[1:], start=2):
        obj: dict[str, Any] = {"_row_number": i}
        for j, key in enumerate(headers):
            obj[key] = row[j] if j < len(row) else ""
        if any(str(obj.get(key) or "").strip() for key in headers):
            rows.append(obj)
    rows.reverse()
    return {
        "spreadsheet_id": sheet_id,
        "spreadsheet_url": f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit",
        "tab": tab,
        "columns": headers,
        "rows": rows,
    }
