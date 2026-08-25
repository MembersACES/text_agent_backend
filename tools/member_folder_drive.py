"""Drive helpers for member / distributor folder creation (replaces Apps Script googleDriveRequest)."""

from __future__ import annotations

import logging
import os
from io import BytesIO
from typing import Any, Optional

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from tools.one_month_savings import get_drive_service
from tools.share_folder import drive_file_url, drive_folder_url, is_sa_quota_error

logger = logging.getLogger(__name__)

FOLDER_MIME = "application/vnd.google-apps.folder"

MEMBERS_B_FOLDER_ID = os.getenv(
    "MEMBERS_B_FOLDER_ID",
    "1QeUpiWaBdNGa9DBjqWoBNfvMo9vd1uLy",
)
DISTRIBUTORS_FOLDER_ID = os.getenv(
    "DISTRIBUTORS_FOLDER_ID",
    "16fJN23di6zJxa_eK5bfpPUQsMYPbb5Fv",
)
MEMBER_TEMPLATE_FOLDER_ID = os.getenv(
    "MEMBER_TEMPLATE_FOLDER_ID",
    "1sCeC8zGfS739zoLzL-nbopfeCFuaxnZH",
)
LOA_STAGING_FOLDER_ID = os.getenv(
    "LOA_STAGING_FOLDER_ID",
    "1eqlc0R_4ZC9LTH4c6CJlrs5mjc5eY41E",
)
WIP_TEMPLATE_FILE_ID = os.getenv(
    "WIP_TEMPLATE_FILE_ID",
    "1Dik_0Ndwl06YNv_WIkjRAnR0CwAoo7Ge0leF18to5ok",
)


class MemberFolderDriveError(Exception):
    def __init__(self, message: str, status_code: int = 502):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _escape_drive_query(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def _user_drive_service(access_token: str) -> Any:
    from google.oauth2.credentials import Credentials as UserCredentials
    from googleapiclient.discovery import build

    creds = UserCredentials(token=(access_token or "").strip())
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _require_drive(drive: Any | None = None) -> Any:
    service = drive or get_drive_service()
    if not service:
        raise MemberFolderDriveError(
            "Google Drive is not configured. Check SERVICE_ACCOUNT_JSON / SERVICE_ACCOUNT_FILE "
            "and share 003-Members-B with the service account as Editor.",
            status_code=503,
        )
    return service


def _drive_error_message(exc: HttpError) -> str:
    reason = getattr(exc, "reason", None) or str(exc)
    code = getattr(exc, "status_code", None)
    blob = str(exc).lower()
    if code == 403 and (
        "storagequotaexceeded" in blob
        or "storage quota" in blob
        or "service accounts do not have storage quota" in blob
    ):
        return (
            "Google blocked the file upload: service accounts have no My Drive storage. "
            "003-Distributors / 003-Members-B are in My Drive, not a Shared Drive. "
            "The folder can still be created; files must be uploaded as your Google user "
            "(same as invoice PDFs), or those folders moved into a Shared Drive."
        )
    if code:
        return f"Drive API {code}: {reason}"
    return f"Drive API error: {reason}"


def list_child_folders(
    parent_id: str,
    *,
    name_prefix: Optional[str] = None,
    drive: Any | None = None,
) -> list[dict[str, str]]:
    """List non-trashed child folders of parent_id, optionally filtered by name prefix."""
    service = _require_drive(drive)
    parent_id = (parent_id or "").strip()
    if not parent_id:
        raise MemberFolderDriveError("parent_id is required", status_code=400)

    prefix = (name_prefix or "").strip()
    query = f"'{parent_id}' in parents and mimeType='{FOLDER_MIME}' and trashed=false"
    out: list[dict[str, str]] = []
    page_token: Optional[str] = None
    try:
        while True:
            kwargs: dict[str, Any] = {
                "q": query,
                "spaces": "drive",
                "fields": "nextPageToken, files(id, name, mimeType, webViewLink)",
                "pageSize": 100,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
                "orderBy": "name",
            }
            if page_token:
                kwargs["pageToken"] = page_token
            result = service.files().list(**kwargs).execute()
            for item in result.get("files") or []:
                name = str(item.get("name") or "").strip()
                fid = str(item.get("id") or "").strip()
                if not name or not fid:
                    continue
                if prefix and not name.startswith(prefix):
                    continue
                out.append(
                    {
                        "id": fid,
                        "name": name,
                        "url": item.get("webViewLink") or drive_folder_url(fid),
                    }
                )
            page_token = result.get("nextPageToken")
            if not page_token:
                break
    except HttpError as e:
        raise MemberFolderDriveError(_drive_error_message(e), status_code=502) from e

    out.sort(key=_folder_sort_key)
    return out


def _folder_sort_key(item: dict[str, str]) -> tuple[int, str]:
    name = item.get("name") or ""
    group = 0 if name.startswith("A - ") else 1
    return (group, name.lower())


def find_child_folder_id(
    parent_id: str,
    name: str,
    *,
    drive: Any | None = None,
) -> Optional[str]:
    service = _require_drive(drive)
    safe = _escape_drive_query(name)
    query = (
        f"name='{safe}' and '{parent_id}' in parents "
        f"and mimeType='{FOLDER_MIME}' and trashed=false"
    )
    try:
        result = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id,name)",
                pageSize=5,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
    except HttpError as e:
        raise MemberFolderDriveError(_drive_error_message(e), status_code=502) from e
    files = result.get("files") or []
    if not files:
        return None
    fid = files[0].get("id")
    return str(fid) if fid else None


def find_or_create_folder(
    parent_id: str,
    name: str,
    *,
    drive: Any | None = None,
) -> tuple[str, bool]:
    """Return (folder_id, created)."""
    service = _require_drive(drive)
    name = (name or "").strip()
    if not name:
        raise MemberFolderDriveError("Folder name is required", status_code=400)
    existing = find_child_folder_id(parent_id, name, drive=service)
    if existing:
        return existing, False
    try:
        created = (
            service.files()
            .create(
                body={
                    "name": name,
                    "mimeType": FOLDER_MIME,
                    "parents": [parent_id],
                },
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as e:
        raise MemberFolderDriveError(_drive_error_message(e), status_code=502) from e
    folder_id = str(created.get("id") or "").strip()
    if not folder_id:
        raise MemberFolderDriveError("Drive created a folder but returned no id.", status_code=502)
    logger.info("[member_folder] created folder %r id=%s parent=%s", name, folder_id, parent_id)
    return folder_id, True


def member_wip_spreadsheet_name(business_name: str) -> str:
    return f"{(business_name or '').strip()} Work in Progress"


def member_site_folder_name(business_name: str, trading_as: str) -> str:
    site = (business_name or "").strip()
    trading = (trading_as or "").strip() or "N/A"
    return f"{site} - {trading}"


def _iter_children(service: Any, parent_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page_token: Optional[str] = None
    query = f"'{parent_id}' in parents and trashed=false"
    while True:
        kwargs: dict[str, Any] = {
            "q": query,
            "spaces": "drive",
            "fields": "nextPageToken, files(id, name, mimeType, modifiedTime)",
            "pageSize": 100,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }
        if page_token:
            kwargs["pageToken"] = page_token
        result = service.files().list(**kwargs).execute()
        items.extend(result.get("files") or [])
        page_token = result.get("nextPageToken")
        if not page_token:
            break
    return items


def copy_folder_contents(
    template_folder_id: str,
    dest_folder_id: str,
    *,
    drive: Any | None = None,
) -> int:
    """Recursively copy files and subfolders from template into dest. Returns file copy count."""
    service = _require_drive(drive)
    return _copy_folder_contents(service, template_folder_id, dest_folder_id)


def _copy_folder_contents(service: Any, source_id: str, dest_id: str) -> int:
    copied = 0
    try:
        children = _iter_children(service, source_id)
    except HttpError as e:
        raise MemberFolderDriveError(_drive_error_message(e), status_code=502) from e
    for item in children:
        name = str(item.get("name") or "Untitled")
        mime = str(item.get("mimeType") or "")
        src_id = str(item.get("id") or "")
        if not src_id:
            continue
        if mime == FOLDER_MIME:
            new_id, _created = find_or_create_folder(dest_id, name, drive=service)
            copied += _copy_folder_contents(service, src_id, new_id)
            continue
        try:
            service.files().copy(
                fileId=src_id,
                body={"name": name, "parents": [dest_id]},
                fields="id",
                supportsAllDrives=True,
            ).execute()
            copied += 1
        except HttpError as e:
            logger.warning(
                "[member_folder] skip copy %r (%s): %s",
                name,
                src_id,
                _drive_error_message(e),
            )
    return copied


def invoice_api_loa_name_prefix(business_name: str) -> str:
    """Match aces-invoice-api loa/router.py: keep alnum, space, hyphen, underscore."""
    return "".join(
        c for c in (business_name or "") if c.isalnum() or c in (" ", "-", "_")
    ).strip()


def staging_loa_filename_matches(filename: str, business_name: str) -> bool:
    """True if a staging Drive file is the LOA for this business.

    Invoice-api uploads `{cleaned}_LOA_{date}.pdf`, so a startswith on the raw
    business name misses files once parentheses or punctuation are stripped.
    """
    name = (filename or "").strip()
    raw = (business_name or "").strip()
    if not name or not raw:
        return False
    name_l = name.lower()
    if name_l.startswith(raw.lower()):
        return True
    cleaned = invoice_api_loa_name_prefix(raw)
    if cleaned and name_l.startswith(cleaned.lower()):
        return True
    stem = name_l.split("_loa_")[0].strip()
    if not stem:
        return False
    cleaned_l = cleaned.lower()
    if cleaned_l and stem == cleaned_l:
        return True
    return bool(cleaned_l and invoice_api_loa_name_prefix(stem).lower() == cleaned_l)


def _relocate_file_to_folder(
    service: Any,
    file_id: str,
    dest_folder_id: str,
    name: str,
) -> dict[str, Any]:
    meta = (
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, parents, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    parents = [p for p in (meta.get("parents") or []) if p]
    try:
        kwargs: dict[str, Any] = {
            "fileId": file_id,
            "addParents": dest_folder_id,
            "fields": "id, webViewLink",
            "supportsAllDrives": True,
        }
        if parents:
            kwargs["removeParents"] = ",".join(parents)
        return service.files().update(**kwargs).execute()
    except HttpError as move_err:
        logger.warning(
            "[member_folder] move LOA failed, copying instead: %s",
            _drive_error_message(move_err),
        )
        copied = (
            service.files()
            .copy(
                fileId=file_id,
                body={"name": name or meta.get("name") or "LOA.pdf", "parents": [dest_folder_id]},
                fields="id, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        try:
            service.files().update(
                fileId=file_id,
                body={"trashed": True},
                supportsAllDrives=True,
            ).execute()
        except HttpError as trash_err:
            logger.warning(
                "[member_folder] copied LOA but could not trash original: %s",
                _drive_error_message(trash_err),
            )
        return copied


def move_loa_from_staging(
    business_name: str,
    dest_folder_id: str,
    *,
    staging_folder_id: Optional[str] = None,
    drive: Any | None = None,
) -> Optional[dict[str, str]]:
    """Move the matching staging LOA into dest. Apps Script checkAndMoveFile."""
    service = _require_drive(drive)
    staging = (staging_folder_id or LOA_STAGING_FOLDER_ID).strip()
    prefix = (business_name or "").strip()
    if not prefix or not staging:
        return None
    try:
        children = _iter_children(service, staging)
    except HttpError as e:
        logger.warning("[member_folder] list LOA staging failed: %s", _drive_error_message(e))
        return None
    matches: list[dict[str, Any]] = []
    for item in children:
        if str(item.get("mimeType") or "") == FOLDER_MIME:
            continue
        name = str(item.get("name") or "")
        if staging_loa_filename_matches(name, prefix):
            matches.append(item)
    if not matches:
        sample = [
            str(item.get("name") or "")
            for item in children
            if str(item.get("mimeType") or "") != FOLDER_MIME
        ][:12]
        logger.info(
            "[member_folder] no staging LOA for %r (cleaned %r); %s files in staging, sample=%s",
            prefix,
            invoice_api_loa_name_prefix(prefix),
            len(children),
            sample,
        )
        return None
    matches.sort(key=lambda item: str(item.get("modifiedTime") or ""), reverse=True)
    match = matches[0]
    file_id = str(match.get("id") or "")
    if not file_id:
        return None
    try:
        updated = _relocate_file_to_folder(
            service, file_id, dest_folder_id, str(match.get("name") or "")
        )
    except HttpError as e:
        logger.warning("[member_folder] move/copy LOA failed: %s", _drive_error_message(e))
        return None
    fid = str(updated.get("id") or file_id)
    url = str(updated.get("webViewLink") or drive_file_url(fid))
    logger.info("[member_folder] moved LOA %s into %s", fid, dest_folder_id)
    return {"id": fid, "url": url, "name": str(match.get("name") or "")}


def upload_bytes_to_folder(
    file_bytes: bytes,
    filename: str,
    folder_id: str,
    *,
    mimetype: str = "application/pdf",
    drive: Any | None = None,
    user_access_token: Optional[str] = None,
) -> dict[str, str]:
    service = _require_drive(drive)

    def _create(svc: Any) -> dict[str, Any]:
        media = MediaIoBaseUpload(BytesIO(file_bytes), mimetype=mimetype, resumable=True)
        return (
            svc.files()
            .create(
                body={"name": filename, "parents": [folder_id]},
                media_body=media,
                fields="id, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )

    try:
        created = _create(service)
    except HttpError as e:
        token = (user_access_token or "").strip()
        if token and is_sa_quota_error(e):
            logger.info("[member_folder] SA quota on upload; retrying as signed-in user")
            try:
                created = _create(_user_drive_service(token))
            except HttpError as user_err:
                raise MemberFolderDriveError(_drive_error_message(user_err), status_code=502) from user_err
        else:
            raise MemberFolderDriveError(_drive_error_message(e), status_code=502) from e
    fid = str(created.get("id") or "").strip()
    if not fid:
        raise MemberFolderDriveError("Drive upload returned no file id.", status_code=502)
    return {"id": fid, "url": created.get("webViewLink") or drive_file_url(fid)}


def find_named_file_in_folder(
    parent_id: str,
    name: str,
    *,
    drive: Any | None = None,
) -> Optional[dict[str, str]]:
    service = _require_drive(drive)
    wanted = (name or "").strip().lower()
    if not wanted or not (parent_id or "").strip():
        return None
    try:
        children = _iter_children(service, parent_id)
    except HttpError as e:
        logger.warning("[member_folder] list folder %s failed: %s", parent_id, _drive_error_message(e))
        return None
    for item in children:
        if str(item.get("mimeType") or "") == FOLDER_MIME:
            continue
        if str(item.get("name") or "").strip().lower() != wanted:
            continue
        fid = str(item.get("id") or "").strip()
        if not fid:
            continue
        return {
            "id": fid,
            "name": str(item.get("name") or name),
            "url": drive_file_url(fid),
        }
    return None


def copy_file_into_folder(
    source_file_id: str,
    dest_folder_id: str,
    name: str,
    *,
    drive: Any | None = None,
    user_access_token: Optional[str] = None,
) -> dict[str, str]:
    source = (source_file_id or "").strip()
    dest = (dest_folder_id or "").strip()
    if not source or not dest:
        raise MemberFolderDriveError("WIP template id and member folder id are required", status_code=400)
    token = (user_access_token or "").strip()

    def _copy(svc: Any) -> dict[str, Any]:
        return (
            svc.files()
            .copy(
                fileId=source,
                body={"name": name, "parents": [dest]},
                fields="id, webViewLink, name",
                supportsAllDrives=True,
            )
            .execute()
        )

    user_err_msg = ""
    if token:
        try:
            copied = _copy(_user_drive_service(token))
        except HttpError as user_err:
            user_err_msg = _drive_error_message(user_err)
            logger.warning("[member_folder] user WIP copy failed: %s", user_err_msg)
            copied = None
        else:
            fid = str(copied.get("id") or "").strip()
            if fid:
                return {
                    "id": fid,
                    "name": str(copied.get("name") or name),
                    "url": copied.get("webViewLink") or drive_file_url(fid),
                }
    else:
        copied = None

    try:
        copied = _copy(_require_drive(drive))
    except HttpError as e:
        sa_msg = _drive_error_message(e)
        if is_sa_quota_error(e):
            raise MemberFolderDriveError(
                "WIP could not be copied into the member folder. Service accounts have no "
                "My Drive storage, so this copy has to run as your Google user. Sign out of "
                "the portal, sign back in (accept Drive access), then retry folder creation. "
                + (f"User copy error: {user_err_msg}" if user_err_msg else ""),
                status_code=502,
            ) from e
        if user_err_msg:
            raise MemberFolderDriveError(
                f"WIP copy failed as your Google user ({user_err_msg}). "
                f"Service account fallback: {sa_msg}",
                status_code=502,
            ) from e
        raise MemberFolderDriveError(sa_msg, status_code=502) from e
    fid = str(copied.get("id") or "").strip()
    if not fid:
        raise MemberFolderDriveError("WIP copy returned no file id.", status_code=502)
    return {
        "id": fid,
        "name": str(copied.get("name") or name),
        "url": copied.get("webViewLink") or drive_file_url(fid),
    }


def create_member_drive_folder(
    *,
    classification: str,
    state: str,
    business_name: str,
    trading_as: str,
    classification_folder_id: Optional[str] = None,
    state_folder_id: Optional[str] = None,
    copy_template: bool = True,
    move_loa: bool = True,
    drive: Any | None = None,
) -> dict[str, Any]:
    """
    Members-B / {classification} / {state} / {Business Name} - {Trading As}
    Copies the member template only when the site folder is newly created.
    """
    service = _require_drive(drive)
    classification = (classification or "").strip()
    state = (state or "").strip()
    business_name = (business_name or "").strip()
    if not classification or not state or not business_name:
        raise MemberFolderDriveError(
            "classification, state, and business_name are required",
            status_code=400,
        )

    class_id = (classification_folder_id or "").strip()
    class_created = False
    if not class_id:
        class_id, class_created = find_or_create_folder(
            MEMBERS_B_FOLDER_ID, classification, drive=service
        )

    state_id = (state_folder_id or "").strip()
    state_created = False
    if not state_id:
        state_id, state_created = find_or_create_folder(class_id, state, drive=service)

    site_name = member_site_folder_name(business_name, trading_as)
    site_id, site_created = find_or_create_folder(state_id, site_name, drive=service)

    copied = 0
    if copy_template and site_created and MEMBER_TEMPLATE_FOLDER_ID:
        copied = copy_folder_contents(MEMBER_TEMPLATE_FOLDER_ID, site_id, drive=service)

    loa_file = None
    if move_loa:
        loa_file = move_loa_from_staging(business_name, site_id, drive=service)

    return {
        "classification": classification,
        "classification_folder_id": class_id,
        "classification_folder_url": drive_folder_url(class_id),
        "classification_created": class_created,
        "state": state,
        "state_folder_id": state_id,
        "state_folder_url": drive_folder_url(state_id),
        "state_created": state_created,
        "folder_name": site_name,
        "folder_id": site_id,
        "folder_url": drive_folder_url(site_id),
        "folder_created": site_created,
        "template_files_copied": copied,
        "loa_file": loa_file,
    }


def create_empty_named_folder(
    parent_id: str,
    name: str,
    *,
    drive: Any | None = None,
) -> dict[str, Any]:
    folder_id, created = find_or_create_folder(parent_id, name, drive=drive)
    return {
        "folder_id": folder_id,
        "folder_url": drive_folder_url(folder_id),
        "folder_name": name,
        "folder_created": created,
    }
