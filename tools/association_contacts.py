"""Contacts for an association, stored in a Google Sheet inside its Drive folder."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import uuid4

from googleapiclient.errors import HttpError

from tools.association_folders import _candidate_drives
from tools.member_folder_drive import MemberFolderDriveError
from tools.one_month_savings import get_sheets_service
from tools.supplier_folders import _list_children

logger = logging.getLogger(__name__)

CONTACTS_SHEET_NAME = "Contacts"
SPREADSHEET_MIME = "application/vnd.google-apps.spreadsheet"
CONTACT_HEADERS = ("Id", "Name", "Role", "Email", "Phone", "Mobile", "Primary", "Notes")

_PRIMARY_YES = {"yes", "y", "true", "1", "primary"}
_HEADER_NAMES = {"name", "contact name", "email"}


def _norm_header(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _sheet_url(file_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{file_id}/edit"


def _a1(title: str, cells: str) -> str:
    escaped = (title or "Sheet1").replace("'", "''")
    return f"'{escaped}'!{cells}"


def sheet_uses_our_header(values: List[Any]) -> bool:
    if not values:
        return True
    first = values[0] if isinstance(values[0], (list, tuple)) else []
    if not first:
        return False
    return _norm_header(first[0]) == "id"


def new_contact(
    *,
    name: str,
    role: str = "",
    email: str = "",
    phone: str = "",
    mobile: str = "",
    primary: bool = False,
    notes: str = "",
    contact_id: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "id": contact_id or str(uuid4()),
        "name": (name or "").strip(),
        "role": (role or "").strip(),
        "email": (email or "").strip(),
        "phone": (phone or "").strip(),
        "mobile": (mobile or "").strip(),
        "primary": bool(primary),
        "notes": (notes or "").strip(),
    }


def with_single_primary(contacts: List[Dict[str, Any]], primary_id: str) -> List[Dict[str, Any]]:
    return [{**item, "primary": item["id"] == primary_id} for item in contacts]


def contacts_to_rows(contacts: List[Dict[str, Any]]) -> List[List[str]]:
    return [
        [
            str(item.get("id") or ""),
            str(item.get("name") or ""),
            str(item.get("role") or ""),
            str(item.get("email") or ""),
            str(item.get("phone") or ""),
            str(item.get("mobile") or ""),
            "Yes" if item.get("primary") else "",
            str(item.get("notes") or ""),
        ]
        for item in contacts
    ]


def parse_contact_values(values: List[Any]) -> Tuple[List[Dict[str, Any]], bool]:
    """Read sheet rows. The second value is true when ids or primary flags should be written back."""
    rows = [list(row) if isinstance(row, (list, tuple)) else [] for row in (values or [])]
    if not rows:
        return [], False

    header_cells = [_norm_header(cell) for cell in rows[0]]
    has_header = bool(set(header_cells) & _HEADER_NAMES)
    data_rows = rows[1:] if has_header else rows

    def header_index(*names: str) -> Optional[int]:
        for name in names:
            if name in header_cells:
                return header_cells.index(name)
        return None

    if has_header:
        indexes: Dict[str, Optional[int]] = {
            "id": header_index("id"),
            "name": header_index("name", "contact name"),
            "role": header_index("role"),
            "email": header_index("email", "e-mail"),
            "phone": header_index("phone", "number"),
            "mobile": header_index("mobile"),
            "primary": header_index("primary"),
            "notes": header_index("notes", "note"),
        }
    else:
        indexes = {
            "id": 0,
            "name": 1,
            "role": 2,
            "email": 3,
            "phone": 4,
            "mobile": 5,
            "primary": 6,
            "notes": 7,
        }

    def cell(raw: List[Any], key: str) -> str:
        index = indexes[key]
        if index is None or index >= len(raw):
            return ""
        return str(raw[index] or "").strip()

    contacts: List[Dict[str, Any]] = []
    seen = set()
    rewrite = False
    for raw in data_rows:
        if not any(str(item or "").strip() for item in raw):
            continue
        contact_id = cell(raw, "id")
        name = cell(raw, "name")
        role = cell(raw, "role")
        email = cell(raw, "email")
        phone = cell(raw, "phone")
        mobile = cell(raw, "mobile")
        notes = cell(raw, "notes")
        if not any([contact_id, name, role, email, phone, mobile, notes]):
            continue
        if not contact_id or contact_id in seen:
            contact_id = str(uuid4())
            rewrite = True
        seen.add(contact_id)
        contacts.append(
            new_contact(
                contact_id=contact_id,
                name=name,
                role=role,
                email=email,
                phone=phone,
                mobile=mobile,
                primary=cell(raw, "primary").lower() in _PRIMARY_YES,
                notes=notes,
            )
        )

    seen_primary = False
    for contact in contacts:
        if not contact["primary"]:
            continue
        if seen_primary:
            contact["primary"] = False
            rewrite = True
        else:
            seen_primary = True
    return contacts, rewrite


def contacts_from_sheet_values(values: List[Any]) -> Tuple[List[Dict[str, Any]], bool]:
    contacts, rewrite = parse_contact_values(values)
    return contacts, bool(rewrite and sheet_uses_our_header(values))


def _user_sheets_service(access_token: str) -> Any:
    from google.oauth2.credentials import Credentials as UserCredentials
    from googleapiclient.discovery import build

    return build(
        "sheets",
        "v4",
        credentials=UserCredentials(token=(access_token or "").strip()),
        cache_discovery=False,
    )


def _candidate_sheets(user_access_token: Optional[str]) -> List[Any]:
    services: List[Any] = []
    service = get_sheets_service()
    if service:
        services.append(service)
    token = (user_access_token or "").strip()
    if token:
        services.append(_user_sheets_service(token))
    return services


def _with_sheets(user_access_token: Optional[str], action: Callable[[Any], Any]) -> Any:
    services = _candidate_sheets(user_access_token)
    if not services:
        raise MemberFolderDriveError(
            "Google Sheets is not configured. Set SERVICE_ACCOUNT_FILE or SERVICE_ACCOUNT_JSON.",
            status_code=503,
        )
    last: Optional[Exception] = None
    for service in services:
        try:
            return action(service)
        except HttpError as exc:
            last = exc
            logger.info("Contacts sheet retry: %s", exc)
    if isinstance(last, HttpError):
        raise MemberFolderDriveError(
            f"Google Sheets error: {getattr(last, 'reason', last)}",
            status_code=502,
        ) from last
    raise MemberFolderDriveError("Could not update the Contacts sheet.", status_code=502)


def _first_sheet(service: Any, spreadsheet_id: str) -> Tuple[str, int]:
    meta = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets.properties(sheetId,title)")
        .execute()
    )
    sheets = meta.get("sheets") or []
    if not sheets:
        raise MemberFolderDriveError("Contacts spreadsheet has no tabs.", status_code=502)
    props = sheets[0].get("properties") or {}
    return str(props.get("title") or "Sheet1"), int(props.get("sheetId") or 0)


def _file_meta(drive: Any, file_id: str) -> Optional[Dict[str, Any]]:
    try:
        meta = (
            drive.files()
            .get(
                fileId=file_id,
                fields="id,name,mimeType,webViewLink,trashed",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError:
        return None
    if meta.get("trashed"):
        return None
    return meta


def _find_named_sheet(drive: Any, folder_id: str) -> Optional[Dict[str, Any]]:
    children, err = _list_children(drive, folder_id, files_only=True)
    if err:
        message = err.replace("drive_error:", "Google Drive error: ")
        if message == "folder_not_found":
            message = "The association folder was not found, or Drive cannot access it."
        raise MemberFolderDriveError(message, status_code=502)
    for item in children:
        if (
            item.get("name") == CONTACTS_SHEET_NAME
            and item.get("mimeType") == SPREADSHEET_MIME
            and item.get("id")
        ):
            return item
    return None


def _create_spreadsheet(drive: Any, folder_id: str) -> Dict[str, Any]:
    try:
        return (
            drive.files()
            .create(
                body={
                    "name": CONTACTS_SHEET_NAME,
                    "mimeType": SPREADSHEET_MIME,
                    "parents": [folder_id],
                },
                fields="id,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as exc:
        raise MemberFolderDriveError(
            f"Google Drive error: {getattr(exc, 'reason', exc)}",
            status_code=502,
        ) from exc


def format_new_contacts_sheet(spreadsheet_id: str, user_access_token: Optional[str] = None) -> None:
    def action(service: Any) -> None:
        _title, sheet_id = _first_sheet(service, spreadsheet_id)
        (
            service.spreadsheets()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": sheet_id,
                                    "title": CONTACTS_SHEET_NAME,
                                    "gridProperties": {"frozenRowCount": 1},
                                },
                                "fields": "title,gridProperties.frozenRowCount",
                            }
                        },
                        {
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "dimension": "COLUMNS",
                                    "startIndex": 0,
                                    "endIndex": 1,
                                },
                                "properties": {"hiddenByUser": True},
                                "fields": "hiddenByUser",
                            }
                        },
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": len(CONTACT_HEADERS),
                                },
                                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                                "fields": "userEnteredFormat.textFormat.bold",
                            }
                        },
                    ]
                },
            )
            .execute()
        )

    try:
        _with_sheets(user_access_token, action)
    except MemberFolderDriveError as exc:
        logger.warning("Contacts sheet format skipped for %s: %s", spreadsheet_id, exc.message)


def locate_or_create_contacts_sheet(
    folder_id: str,
    existing_id: Optional[str],
    user_access_token: Optional[str] = None,
) -> Tuple[str, str, bool]:
    """Return spreadsheet id, url, and whether this call created the file."""
    drives, err = _candidate_drives(user_access_token)
    if not drives:
        raise MemberFolderDriveError(err or "Google Drive is not configured.", status_code=503)

    stored = (existing_id or "").strip()
    if stored:
        for drive in drives:
            meta = _file_meta(drive, stored)
            if meta and meta.get("mimeType") == SPREADSHEET_MIME:
                return stored, meta.get("webViewLink") or _sheet_url(stored), False

    found: Optional[Dict[str, Any]] = None
    saw_folder = False
    last: Optional[MemberFolderDriveError] = None
    for drive in drives:
        try:
            match = _find_named_sheet(drive, folder_id)
            saw_folder = True
            if match:
                found = match
                break
        except MemberFolderDriveError as exc:
            last = exc
    if found and found.get("id"):
        file_id = str(found["id"])
        return file_id, found.get("webViewLink") or _sheet_url(file_id), False
    if not saw_folder:
        raise last or MemberFolderDriveError("Could not read the association folder.", status_code=502)

    created: Optional[Dict[str, Any]] = None
    for drive in drives:
        try:
            created = _create_spreadsheet(drive, folder_id)
            break
        except MemberFolderDriveError as exc:
            last = exc
    if not created or not created.get("id"):
        raise last or MemberFolderDriveError("Could not create the Contacts sheet.", status_code=502)

    file_id = str(created["id"])
    logger.info("Created association contacts sheet %s in folder %s", file_id, folder_id)
    format_new_contacts_sheet(file_id, user_access_token)
    return file_id, created.get("webViewLink") or _sheet_url(file_id), True


def load_sheet_contacts(
    spreadsheet_id: str,
    user_access_token: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    def action(service: Any) -> Tuple[List[Dict[str, Any]], bool]:
        title, _sheet_id = _first_sheet(service, spreadsheet_id)
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=_a1(title, "A1:H"))
            .execute()
        )
        return contacts_from_sheet_values(result.get("values") or [])

    return _with_sheets(user_access_token, action)


def save_sheet_contacts(
    spreadsheet_id: str,
    contacts: List[Dict[str, Any]],
    user_access_token: Optional[str] = None,
) -> None:
    def action(service: Any) -> None:
        title, _sheet_id = _first_sheet(service, spreadsheet_id)
        (
            service.spreadsheets()
            .values()
            .clear(spreadsheetId=spreadsheet_id, range=_a1(title, "A2:H"))
            .execute()
        )
        (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=_a1(title, "A1:H"),
                valueInputOption="RAW",
                body={"values": [list(CONTACT_HEADERS), *contacts_to_rows(contacts)]},
            )
            .execute()
        )

    _with_sheets(user_access_token, action)
