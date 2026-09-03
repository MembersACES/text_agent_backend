"""List and upload member site photos into a Drive 'Site Photos' subfolder."""

from __future__ import annotations

import logging
import re
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from tools.one_month_savings import extract_folder_id_from_url, get_drive_service
from tools.share_folder import drive_file_url, drive_folder_url

logger = logging.getLogger(__name__)

CANONICAL_FOLDER_NAME = "Site Photos"
FOLDER_MIME = "application/vnd.google-apps.folder"
MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_FILES_PER_REQUEST = 50

FOLDER_NAME_ALIASES = frozenset(
    {
        "site photos",
        "site photo",
        "site-photos",
        "site_photos",
    }
)

EXT_TO_MIME = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".heic": "image/heic",
    ".heif": "image/heif",
}

ALLOWED_MIME_TYPES = frozenset(
    {
        "image/jpeg",
        "image/jpg",
        "image/png",
        "image/webp",
        "image/gif",
        "image/heic",
        "image/heif",
        "image/heic-sequence",
    }
)


class SitePhotosError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def normalize_folder_name(name: str) -> str:
    return re.sub(r"[\s_\-]+", " ", (name or "").strip().lower())


def is_site_photos_folder_name(name: str) -> bool:
    return normalize_folder_name(name) in FOLDER_NAME_ALIASES


def safe_filename(name: str) -> str:
    base = Path(name or "").name.strip() or "photo.jpg"
    return base.replace("/", "_").replace("\\", "_")


def prefixed_filename(business_name: str, original_name: str) -> str:
    filename = safe_filename(original_name)
    prefix = (business_name or "").strip()
    if not prefix:
        return filename
    expected = f"{prefix} - "
    if filename.lower().startswith(expected.lower()):
        return filename
    return f"{expected}{filename}"


def resolve_image_mime(filename: str, content_type: str) -> str:
    mime = (content_type or "").split(";")[0].strip().lower()
    if mime in ALLOWED_MIME_TYPES:
        if mime == "image/jpg":
            return "image/jpeg"
        return mime
    ext = Path(filename or "").suffix.lower()
    mapped = EXT_TO_MIME.get(ext, "")
    return mapped


def is_allowed_image(filename: str, content_type: str) -> bool:
    return bool(resolve_image_mime(filename, content_type))


def is_image_file(item: dict[str, Any]) -> bool:
    mime = str(item.get("mimeType") or item.get("mime_type") or "").lower()
    if mime.startswith("image/"):
        return True
    name = str(item.get("name") or "")
    return Path(name).suffix.lower() in EXT_TO_MIME


def _escape_drive_query(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def _drive_error_message(exc: HttpError) -> str:
    reason = getattr(exc, "reason", None) or str(exc)
    code = getattr(exc, "status_code", None)
    if code:
        return f"Drive API {code}: {reason}"
    return f"Drive API error: {reason}"


def _require_drive(drive: Any | None) -> Any:
    service = drive or get_drive_service()
    if not service:
        raise SitePhotosError(
            "Google Drive is not configured. Check SERVICE_ACCOUNT_JSON.",
            status_code=503,
        )
    return service


def _require_parent_id(gdrive_url: str) -> str:
    parent_id = extract_folder_id_from_url((gdrive_url or "").strip())
    if not parent_id:
        raise SitePhotosError(
            "This member has no Google Drive folder, or the folder URL could not be parsed.",
            status_code=400,
        )
    return parent_id


def _list_child_folders(drive: Any, parent_id: str) -> list[dict[str, Any]]:
    folders: list[dict[str, Any]] = []
    page_token: Optional[str] = None
    query = (
        f"'{parent_id}' in parents and mimeType='{FOLDER_MIME}' and trashed=false"
    )
    while True:
        result = (
            drive.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id,name)",
                pageSize=100,
                pageToken=page_token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        folders.extend(result.get("files") or [])
        page_token = result.get("nextPageToken")
        if not page_token:
            break
    return folders


def find_site_photos_folder_id(drive: Any, parent_id: str) -> Optional[str]:
    query = (
        f"name='{_escape_drive_query(CANONICAL_FOLDER_NAME)}' "
        f"and '{parent_id}' in parents "
        f"and mimeType='{FOLDER_MIME}' and trashed=false"
    )
    result = (
        drive.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id,name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        .execute()
    )
    folders = result.get("files") or []
    if folders:
        folder_id = folders[0].get("id")
        if isinstance(folder_id, str) and folder_id.strip():
            return folder_id

    for folder in _list_child_folders(drive, parent_id):
        if not is_site_photos_folder_name(str(folder.get("name") or "")):
            continue
        folder_id = folder.get("id")
        if isinstance(folder_id, str) and folder_id.strip():
            return folder_id
    return None


def get_or_create_site_photos_folder(drive: Any, parent_id: str) -> tuple[str, bool]:
    existing = find_site_photos_folder_id(drive, parent_id)
    if existing:
        return existing, False
    created = (
        drive.files()
        .create(
            body={
                "name": CANONICAL_FOLDER_NAME,
                "mimeType": FOLDER_MIME,
                "parents": [parent_id],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    folder_id = created.get("id")
    if not isinstance(folder_id, str) or not folder_id.strip():
        raise SitePhotosError("Drive created a folder but returned no id.", status_code=502)
    logger.info("[site_photos] created folder id=%s parent=%s", folder_id, parent_id)
    return folder_id, True


def _serialize_photo(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    fid = item.get("id")
    if not isinstance(fid, str) or not fid.strip():
        return None
    if not is_image_file(item):
        return None
    link = item.get("webViewLink") or drive_file_url(fid)
    thumbnail = item.get("thumbnailLink") or f"https://drive.google.com/thumbnail?id={fid}&sz=w400"
    return {
        "id": fid,
        "name": str(item.get("name") or "Untitled"),
        "mime_type": str(item.get("mimeType") or ""),
        "web_view_link": link,
        "thumbnail_link": thumbnail,
        "created_time": item.get("createdTime"),
        "modified_time": item.get("modifiedTime"),
    }


def _list_photos(drive: Any, folder_id: str) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    page_token: Optional[str] = None
    query = f"'{folder_id}' in parents and trashed=false"
    while True:
        list_kwargs: dict[str, Any] = {
            "q": query,
            "spaces": "drive",
            "fields": (
                "nextPageToken, files(id,name,mimeType,webViewLink,"
                "thumbnailLink,createdTime,modifiedTime)"
            ),
            "pageSize": 100,
            "includeItemsFromAllDrives": True,
            "supportsAllDrives": True,
        }
        if page_token:
            list_kwargs["pageToken"] = page_token
        try:
            result = drive.files().list(**list_kwargs, orderBy="createdTime desc").execute()
        except HttpError:
            result = drive.files().list(**list_kwargs).execute()
        files.extend(result.get("files") or [])
        page_token = result.get("nextPageToken")
        if not page_token:
            break
    photos: list[dict[str, Any]] = []
    for item in files:
        serialized = _serialize_photo(item)
        if serialized:
            photos.append(serialized)
    return photos


def _empty_list_payload() -> dict[str, Any]:
    return {
        "ok": True,
        "exists": False,
        "created": False,
        "folder_id": None,
        "folder_url": None,
        "folder_name": CANONICAL_FOLDER_NAME,
        "files": [],
    }


def list_site_photos(gdrive_url: str, drive: Any | None = None) -> dict[str, Any]:
    parent_id = _require_parent_id(gdrive_url)
    service = _require_drive(drive)
    try:
        folder_id = find_site_photos_folder_id(service, parent_id)
    except HttpError as e:
        raise SitePhotosError(_drive_error_message(e), status_code=502) from e
    if not folder_id:
        return _empty_list_payload()
    try:
        photos = _list_photos(service, folder_id)
    except HttpError as e:
        raise SitePhotosError(_drive_error_message(e), status_code=502) from e
    return {
        "ok": True,
        "exists": True,
        "created": False,
        "folder_id": folder_id,
        "folder_url": drive_folder_url(folder_id),
        "folder_name": CANONICAL_FOLDER_NAME,
        "files": photos,
    }


def _folder_drive_id(drive: Any, folder_id: str) -> Optional[str]:
    try:
        info = (
            drive.files()
            .get(fileId=folder_id, fields="id,driveId", supportsAllDrives=True)
            .execute()
        )
    except HttpError:
        return None
    drive_id = info.get("driveId")
    return drive_id if isinstance(drive_id, str) and drive_id.strip() else None


def _upload_image(
    drive: Any,
    folder_id: str,
    filename: str,
    content: bytes,
    mime_type: str,
    drive_id: Optional[str],
) -> dict[str, Any]:
    media = MediaIoBaseUpload(BytesIO(content), mimetype=mime_type, resumable=True)
    create_params: dict[str, Any] = {
        "body": {"name": filename, "parents": [folder_id]},
        "media_body": media,
        "fields": "id,name,mimeType,webViewLink,thumbnailLink,createdTime,modifiedTime",
        "supportsAllDrives": True,
    }
    if drive_id:
        create_params["driveId"] = drive_id
    created = drive.files().create(**create_params).execute()
    serialized = _serialize_photo(created)
    if not serialized:
        fid = created.get("id")
        if not isinstance(fid, str) or not fid.strip():
            raise SitePhotosError("Drive uploaded a file but returned no id.", status_code=502)
        serialized = {
            "id": fid,
            "name": filename,
            "mime_type": mime_type,
            "web_view_link": created.get("webViewLink") or drive_file_url(fid),
            "thumbnail_link": created.get("thumbnailLink"),
            "created_time": created.get("createdTime"),
            "modified_time": created.get("modifiedTime"),
        }
    return serialized


def upload_site_photos(
    gdrive_url: str,
    files: list[tuple[str, str, bytes]],
    business_name: str = "",
    drive: Any | None = None,
) -> dict[str, Any]:
    if not files:
        raise SitePhotosError("No files were provided.", status_code=400)
    if len(files) > MAX_FILES_PER_REQUEST:
        raise SitePhotosError(
            f"Please upload at most {MAX_FILES_PER_REQUEST} photos at a time.",
            status_code=400,
        )

    parent_id = _require_parent_id(gdrive_url)
    service = _require_drive(drive)
    try:
        folder_id, created = get_or_create_site_photos_folder(service, parent_id)
    except HttpError as e:
        raise SitePhotosError(_drive_error_message(e), status_code=502) from e

    drive_id = _folder_drive_id(service, folder_id)
    uploaded: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for original_name, content_type, content in files:
        filename = prefixed_filename(business_name, original_name)
        mime_type = resolve_image_mime(original_name, content_type)
        if not mime_type:
            errors.append(
                {
                    "name": original_name or filename,
                    "error": "Only image files (JPG, PNG, WebP, GIF, HEIC) are allowed.",
                }
            )
            continue
        if not content:
            errors.append({"name": filename, "error": "File is empty."})
            continue
        if len(content) > MAX_FILE_BYTES:
            errors.append(
                {
                    "name": filename,
                    "error": "File is larger than 20 MB.",
                }
            )
            continue
        try:
            uploaded.append(
                _upload_image(service, folder_id, filename, content, mime_type, drive_id)
            )
        except HttpError as e:
            errors.append({"name": filename, "error": _drive_error_message(e)})
        except Exception as e:
            logger.exception("[site_photos] upload failed name=%s", filename)
            errors.append({"name": filename, "error": str(e) or "Upload failed."})

    if not uploaded and errors:
        raise SitePhotosError(
            errors[0]["error"] if len(errors) == 1 else "None of the photos could be uploaded.",
            status_code=400,
        )

    return {
        "ok": True,
        "exists": True,
        "created": created,
        "folder_id": folder_id,
        "folder_url": drive_folder_url(folder_id),
        "folder_name": CANONICAL_FOLDER_NAME,
        "files": uploaded,
        "errors": errors,
    }
