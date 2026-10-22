"""LinkedIn testimonial drafts stored as Word files in a shared Drive folder.

Drive stays the source of the wording. This module lists the folder, reads each
draft, and matches it to a testimonial so the content page can show the post.
"""

from __future__ import annotations

import io
import logging
import os
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Any

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)

DEFAULT_FOLDER_ID = "1ykyVXKW0rMTtaInuMTBQa_QEn_wB3aMT"
DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
GOOGLE_DOC_MIME = "application/vnd.google-apps.document"
FOLDER_MIME = "application/vnd.google-apps.folder"
_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_MAX_TEXT = 20000
_STICKY_SCORE = 200

_SUFFIXES = (
    ("proprietary", "limited"),
    ("pty", "ltd"),
    ("pty", "limited"),
    ("sub", "branch"),
    ("limited",),
    ("ltd",),
    ("inc",),
    ("incorporated",),
    ("plc",),
    ("subbranch",),
)


def drafts_folder_id() -> str:
    return (os.getenv("LINKEDIN_SOCIAL_DRAFTS_FOLDER_ID") or DEFAULT_FOLDER_ID).strip()


def folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def file_url(file_id: str, web_view_link: str | None = None) -> str:
    link = (web_view_link or "").strip()
    if link:
        return link
    return f"https://drive.google.com/file/d/{file_id}/view"


def is_post_guide(name: str) -> bool:
    lower = (name or "").lower()
    if lower.startswith("testimonial"):
        return False
    return "guide" in lower


def is_social_draft(name: str, mime: str) -> bool:
    if is_post_guide(name) or mime == FOLDER_MIME:
        return False
    lower = (name or "").lower()
    if lower.startswith("testimonial"):
        return True
    return mime in (DOCX_MIME, GOOGLE_DOC_MIME) or lower.endswith(".docx")


def normalise_party_name(value: str) -> str:
    """Drop the Testimonial_ prefix, file extension, and legal suffixes."""
    text = (value or "").lower().strip()
    text = re.sub(r"^testimo(?:nial|nal)[_\s\-]+", "", text)
    text = re.sub(r"\.(docx|doc)$", "", text)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = text.split()
    changed = True
    while changed and tokens:
        changed = False
        for suffix in _SUFFIXES:
            if len(tokens) > len(suffix) and tuple(tokens[-len(suffix) :]) == suffix:
                tokens = tokens[: -len(suffix)]
                changed = True
                break
    return " ".join(tokens)


def name_score(file_name: str, business_name: str) -> int:
    file_key = normalise_party_name(file_name)
    business_key = normalise_party_name(business_name)
    if not file_key or not business_key:
        return 0
    if file_key == business_key:
        return 100
    file_tokens = file_key.split()
    business_tokens = business_key.split()
    if len(file_tokens) >= 2 and all(token in business_tokens for token in file_tokens):
        return 90
    if len(business_tokens) >= 2 and all(token in file_tokens for token in business_tokens):
        return 85
    if len(file_key) >= 8 and (file_key in business_key or business_key in file_key):
        return 80
    return 0


def assign_draft_files(
    files: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Pair each draft file with at most one testimonial row.

    A stored social_draft_file_id wins over a name match, so a later rename
    in Drive does not move the draft onto a different member.
    """
    pairs: list[tuple[int, int, int, int, dict[str, Any], dict[str, Any]]] = []
    for file in files:
        file_id = str(file.get("id") or "")
        if not file_id:
            continue
        for row in rows:
            sticky = str(row.get("social_draft_file_id") or "") == file_id
            score = (
                _STICKY_SCORE
                if sticky
                else name_score(str(file.get("name") or ""), str(row.get("business_name") or ""))
            )
            if score < 80:
                continue
            drafted = 1 if row.get("social_status") == "Drafted" else 0
            approved = 1 if row.get("status") == "Approved" else 0
            pairs.append((score, drafted, approved, int(row.get("id") or 0), file, row))

    pairs.sort(key=lambda item: (item[0], item[1], item[2], item[3]), reverse=True)
    used_files: set[str] = set()
    used_rows: set[int] = set()
    matches: list[dict[str, Any]] = []
    for _score, _drafted, _approved, row_id, file, row in pairs:
        file_id = str(file["id"])
        if file_id in used_files or row_id in used_rows:
            continue
        used_files.add(file_id)
        used_rows.add(row_id)
        matches.append(
            {
                "testimonial_id": row_id,
                "business_name": row.get("business_name") or "",
                "file": file,
            }
        )
    unlinked = [file for file in files if str(file.get("id") or "") not in used_files]
    return matches, unlinked


def docx_bytes_to_text(data: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        try:
            xml_bytes = archive.read("word/document.xml")
        except KeyError as exc:
            raise ValueError("That Word file has no document body.") from exc
    root = ET.fromstring(xml_bytes)
    paragraphs: list[str] = []
    for para in root.iter(f"{_W}p"):
        parts: list[str] = []
        for node in para.iter():
            if node.tag == f"{_W}t" and node.text:
                parts.append(node.text)
            elif node.tag == f"{_W}tab":
                parts.append("\t")
            elif node.tag == f"{_W}br":
                parts.append("\n")
        line = "".join(parts).strip()
        if line:
            paragraphs.append(line)
    return "\n\n".join(paragraphs).strip()


def _download_bytes(drive: Any, file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


def _read_draft_text(drive: Any, file: dict[str, Any]) -> tuple[str, str | None]:
    mime = str(file.get("mimeType") or "")
    name = str(file.get("name") or "")
    file_id = str(file.get("id") or "")
    try:
        if mime == GOOGLE_DOC_MIME:
            raw = drive.files().export(fileId=file_id, mimeType="text/plain").execute()
            text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw or "")
        elif mime == DOCX_MIME or name.lower().endswith(".docx"):
            text = docx_bytes_to_text(_download_bytes(drive, file_id))
        else:
            return "", "This file isn't a Word document or Google Doc, so the wording can't be shown here."
    except Exception:
        logger.exception("Failed to read LinkedIn draft %s", file_id)
        return "", "Couldn't read the wording from Drive. Open the file to review it there."
    text = text.replace("\r\n", "\n").strip()
    if len(text) > _MAX_TEXT:
        text = text[:_MAX_TEXT].rstrip() + "…"
    return text, None


def _file_payload(file: dict[str, Any], text: str, read_error: str | None) -> dict[str, Any]:
    file_id = str(file.get("id") or "")
    return {
        "file_id": file_id,
        "file_name": str(file.get("name") or ""),
        "file_url": file_url(file_id, file.get("webViewLink")),
        "modified_time": file.get("modifiedTime"),
        "text": text,
        "read_error": read_error,
    }


def _list_folder_files(drive: Any, folder_id: str) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    page_token = None
    escaped = folder_id.replace("\\", "\\\\").replace("'", "\\'")
    query = f"'{escaped}' in parents and trashed = false"
    while True:
        kwargs: dict[str, Any] = {
            "q": query,
            "spaces": "drive",
            "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, webViewLink)",
            "pageSize": 100,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }
        if page_token:
            kwargs["pageToken"] = page_token
        result = drive.files().list(**kwargs).execute()
        files.extend(result.get("files") or [])
        page_token = result.get("nextPageToken")
        if not page_token:
            return files


def _remember_file_ids(db: Any, matches: list[dict[str, Any]]) -> None:
    from models import Testimonial

    wanted = {
        int(match["testimonial_id"]): str(match["file"]["id"])
        for match in matches
        if int(match["testimonial_id"]) > 0 and match.get("file", {}).get("id")
    }
    if not wanted:
        return
    rows = db.query(Testimonial).filter(Testimonial.id.in_(list(wanted))).all()
    changed = False
    for row in rows:
        file_id = wanted.get(row.id)
        if file_id and row.social_draft_file_id != file_id:
            row.social_draft_file_id = file_id
            changed = True
    if changed:
        db.commit()


def _empty(folder: str, error: str | None = None) -> dict[str, Any]:
    return {
        "folder_url": folder_url(folder),
        "guide": None,
        "drafts": [],
        "unlinked": [],
        "error": error,
    }


def load_social_drafts(db: Any) -> dict[str, Any]:
    folder = drafts_folder_id()
    if not folder:
        return _empty("", "LinkedIn drafts folder is not configured.")

    from tools.one_month_savings import get_drive_service
    from tools.testimonial_examples import get_testimonials_for_solution_type

    drive = get_drive_service()
    if not drive:
        return _empty(
            folder,
            "Google Drive is not configured. Set SERVICE_ACCOUNT_FILE or SERVICE_ACCOUNT_JSON.",
        )

    try:
        listed = _list_folder_files(drive, folder)
    except HttpError as exc:
        logger.exception("Failed to list LinkedIn drafts folder %s", folder)
        status = getattr(getattr(exc, "resp", None), "status", None)
        if status in (403, 404):
            return _empty(
                folder,
                "The service account cannot read the LinkedIn drafts folder. Share the folder with it, then refresh.",
            )
        return _empty(folder, "Couldn't read the LinkedIn drafts folder.")
    except Exception:
        logger.exception("Failed to list LinkedIn drafts folder %s", folder)
        return _empty(folder, "Couldn't read the LinkedIn drafts folder.")

    guide_file = next((item for item in listed if is_post_guide(str(item.get("name") or ""))), None)
    draft_files = [
        item
        for item in listed
        if is_social_draft(str(item.get("name") or ""), str(item.get("mimeType") or ""))
    ]
    try:
        rows = get_testimonials_for_solution_type(db, solution_type_id=None, limit=500)
    except Exception:
        logger.exception("Failed to load testimonials while matching LinkedIn drafts")
        rows = []

    matches, unlinked_files = assign_draft_files(draft_files, rows)
    try:
        _remember_file_ids(db, matches)
    except Exception:
        logger.exception("Could not remember LinkedIn draft file ids")
        try:
            db.rollback()
        except Exception:
            logger.exception("Rollback after LinkedIn draft save failed")

    drafts = []
    for match in matches:
        text, read_error = _read_draft_text(drive, match["file"])
        payload = _file_payload(match["file"], text, read_error)
        payload["testimonial_id"] = match["testimonial_id"]
        payload["business_name"] = match["business_name"]
        drafts.append(payload)

    unlinked = []
    for file in unlinked_files:
        text, read_error = _read_draft_text(drive, file)
        unlinked.append(_file_payload(file, text, read_error))

    guide = _file_payload(guide_file, "", None) if guide_file else None
    logger.info(
        "LinkedIn drafts: %s matched, %s unlinked, guide=%s",
        len(drafts),
        len(unlinked),
        bool(guide),
    )
    return {
        "folder_url": folder_url(folder),
        "guide": guide,
        "drafts": drafts,
        "unlinked": unlinked,
        "error": None,
    }
