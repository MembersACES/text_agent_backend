"""Create Interface Video Creation Pack folders on Google Drive.

Video-only Drive helpers live in this module — do not modify one_month_savings.py.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, Iterator, List, Optional, Tuple

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from tools.one_month_savings import get_drive_service

logger = logging.getLogger(__name__)

# Interface Video Creation Packs — set VIDEO_CREATION_PACKS_FOLDER_ID to a folder inside a Shared Drive
DEFAULT_VIDEO_CREATION_PACKS_FOLDER_ID = "1nb0ZM5wT9FDfnzTzRJEQOBTI5BZHZCtA"

SUBFOLDERS = ("qa", "scripts", "slides", "renders")


def get_video_creation_packs_parent_id() -> str:
    raw = (
        os.getenv("VIDEO_CREATION_PACKS_FOLDER_ID")
        or os.getenv("INTERFACE_VIDEO_CREATION_PACKS_FOLDER_ID")
        or DEFAULT_VIDEO_CREATION_PACKS_FOLDER_ID
    ).strip()
    return raw


def build_pack_folder_name(slug: str, business_name: Optional[str] = None) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    slug_part = re.sub(r"[^a-z0-9-]+", "-", (slug or "video").strip().lower()).strip("-")[:64]
    return f"{slug_part}-pack-{stamp}"


def _probe_pack_parent(drive, folder_id: str) -> Dict[str, Any]:
    """Lightweight Shared Drive check for the video packs parent folder."""
    summary: Dict[str, Any] = {
        "folder_id": folder_id,
        "folder_visible": False,
        "folder_name": None,
        "drive_id": None,
        "can_add_children": None,
        "errors": [],
    }
    try:
        info = drive.files().get(
            fileId=folder_id,
            fields="id,name,driveId,capabilities/canAddChildren",
            supportsAllDrives=True,
        ).execute()
        summary["folder_visible"] = True
        summary["folder_name"] = info.get("name")
        summary["drive_id"] = info.get("driveId")
        summary["can_add_children"] = (info.get("capabilities") or {}).get("canAddChildren")
    except HttpError as e:
        summary["errors"].append(f"{e.status_code} {e.reason}")
        logger.error("[video_pack] parent probe failed for %s: %s", folder_id, e.reason)
    except Exception as e:
        summary["errors"].append(str(e))
        logger.exception("[video_pack] parent probe error")
    return summary


def _video_get_or_create_subfolder(drive, parent_folder_id: str, subfolder_name: str) -> Optional[str]:
    """Shared-Drive aware subfolder create — video packs only."""
    try:
        safe_name = subfolder_name.replace("'", "\\'")
        query = (
            f"name='{safe_name}' and '{parent_folder_id}' in parents "
            "and mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        results = drive.files().list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        existing = results.get("files") or []
        if existing:
            return existing[0]["id"]

        folder = drive.files().create(
            body={
                "name": subfolder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id],
            },
            fields="id",
            supportsAllDrives=True,
            supportsTeamDrives=True,
        ).execute()
        return folder.get("id")
    except HttpError as e:
        logger.error("[video_pack] subfolder '%s' failed: %s - %s", subfolder_name, e.status_code, e.reason)
        return None
    except Exception:
        logger.exception("[video_pack] subfolder '%s' error", subfolder_name)
        return None


def _video_upload_bytes(
    drive,
    *,
    folder_id: str,
    filename: str,
    file_bytes: bytes,
    mimetype: str,
) -> Optional[str]:
    """Upload a small file into a video pack folder (Shared Drive)."""
    try:
        media = MediaIoBaseUpload(BytesIO(file_bytes), mimetype=mimetype, resumable=True)
        created = drive.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
            supportsTeamDrives=True,
        ).execute()
        return created.get("id")
    except HttpError as e:
        logger.error("[video_pack] upload '%s' failed: %s - %s", filename, e.status_code, e.reason)
        return None
    except Exception:
        logger.exception("[video_pack] upload '%s' error", filename)
        return None


def create_video_creation_pack(
    *,
    slug: str,
    kind: str,
    business_name: Optional[str] = None,
    testimonial_id: Optional[int] = None,
    client_id: Optional[int] = None,
    source_doc_file_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a named pack folder under Interface Video Creation Packs.
    Returns folder_id, folder_url, folder_name, subfolders map, or error.
    """
    parent_id = get_video_creation_packs_parent_id()
    if not parent_id:
        return {"error": "VIDEO_CREATION_PACKS_FOLDER_ID is not configured"}

    folder_name = build_pack_folder_name(slug, business_name)
    drive = get_drive_service()
    if not drive:
        return {"error": "Google Drive service unavailable — check SERVICE_ACCOUNT_JSON"}

    probe = _probe_pack_parent(drive, parent_id)
    if not probe.get("folder_visible"):
        err = probe.get("errors") or []
        detail = err[0] if err else "Parent folder not visible to service account"
        return {
            "error": (
                f"Cannot access Interface Video Creation Packs folder ({parent_id}): {detail}. "
                "Add the service account to the Shared Drive as Content manager and set "
                "VIDEO_CREATION_PACKS_FOLDER_ID to a folder inside that Shared Drive."
            )
        }
    if probe.get("can_add_children") is False:
        return {
            "error": (
                "Service account cannot create folders in Interface Video Creation Packs — "
                "add it to the Shared Drive as Content manager."
            )
        }
    if not probe.get("drive_id"):
        return {
            "error": (
                "VIDEO_CREATION_PACKS_FOLDER_ID must point to a folder inside a Google Shared Drive "
                "(Team Drive), not My Drive. Service accounts cannot store files in My Drive. "
                "Set VIDEO_CREATION_PACKS_FOLDER_ID to your Shared Drive folder "
                "(e.g. the Interface Video Creation Packs folder you created under Shared drives)."
            ),
            "parent_folder_url": f"https://drive.google.com/drive/folders/{parent_id}",
        }
    if probe.get("folder_id") == probe.get("drive_id"):
        return {
            "error": (
                "VIDEO_CREATION_PACKS_FOLDER_ID is set to the Shared Drive root ID, not a folder inside it. "
                "Open Shared drives → Interface Videos → Interface Video Creation Packs, copy that folder's "
                "URL (…/folders/XXXXXXXX), and set VIDEO_CREATION_PACKS_FOLDER_ID to that folder id — "
                "not the shared-drive root (0AO…)."
            ),
            "parent_folder_url": f"https://drive.google.com/drive/folders/{parent_id}",
        }

    pack_folder_id = _video_get_or_create_subfolder(drive, parent_id, folder_name)
    if not pack_folder_id:
        return {"error": f"Could not create pack folder '{folder_name}' on Drive"}

    subfolder_ids: Dict[str, str] = {}
    for sub in SUBFOLDERS:
        sub_id = _video_get_or_create_subfolder(drive, pack_folder_id, sub)
        if sub_id:
            subfolder_ids[sub] = sub_id

    manifest = {
        "slug": slug,
        "kind": kind,
        "business_name": business_name,
        "testimonial_id": testimonial_id,
        "client_id": client_id,
        "source_doc_file_id": source_doc_file_id,
        "status": "awaiting_local_render",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "subfolders": list(SUBFOLDERS),
        "notes": "QA-Review.html, scripts, and slides land here after claude-videos postrender + publish.",
    }
    readme = (
        f"CZA Video Creation Pack\n"
        f"=======================\n\n"
        f"Slug: {slug}\n"
        f"Kind: {kind}\n"
        f"Business: {business_name or '—'}\n"
        f"Status: awaiting local render\n\n"
        f"Subfolders:\n"
        f"  qa/       — QA-Review.html after postrender\n"
        f"  scripts/  — narration / slide plans\n"
        f"  slides/   — presentation HTML for review\n"
        f"  renders/  — MP4 long + 30s cuts\n\n"
        f"Source testimonial doc: https://drive.google.com/file/d/{source_doc_file_id}/view\n"
        if source_doc_file_id
        else ""
    )

    upload_warnings: list[str] = []
    if not _video_upload_bytes(
        drive,
        folder_id=pack_folder_id,
        filename="pack-manifest.json",
        file_bytes=json.dumps(manifest, indent=2).encode("utf-8"),
        mimetype="application/json",
    ):
        upload_warnings.append("pack-manifest.json could not be uploaded")
    if not _video_upload_bytes(
        drive,
        folder_id=pack_folder_id,
        filename="README.txt",
        file_bytes=readme.encode("utf-8"),
        mimetype="text/plain",
    ):
        upload_warnings.append("README.txt could not be uploaded")

    folder_url = f"https://drive.google.com/drive/folders/{pack_folder_id}"
    parent_url = f"https://drive.google.com/drive/folders/{parent_id}"

    result: Dict[str, Any] = {
        "folder_id": pack_folder_id,
        "folder_name": folder_name,
        "folder_url": folder_url,
        "parent_folder_url": parent_url,
        "subfolders": subfolder_ids,
        "shared_drive_id": probe.get("drive_id"),
    }
    if upload_warnings:
        result["warnings"] = upload_warnings
    return result


def _folder_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None


def upload_pack_artifacts(
    *,
    pack_folder_url: str,
    qa_html_bytes: Optional[bytes] = None,
    long_mp4_bytes: Optional[bytes] = None,
    short_mp4_bytes: Optional[bytes] = None,
    long_filename: Optional[str] = None,
    short_filename: Optional[str] = None,
    script_bytes: Optional[bytes] = None,
    script_filename: str = "understand.md",
    script_files: Optional[Dict[str, bytes]] = None,
    slide_files: Optional[Dict[str, bytes]] = None,
) -> Dict[str, Any]:
    """
    Upload render outputs into an existing creation pack folder subfolders.
    Returns Drive URLs for qa review and updated manifest notes.
    """
    pack_folder_id = _folder_id_from_url(pack_folder_url)
    if not pack_folder_id:
        return {"error": f"Invalid pack folder URL: {pack_folder_url}"}

    drive = get_drive_service()
    if not drive:
        return {"error": "Google Drive service unavailable"}

    subfolder_ids: Dict[str, str] = {}
    for sub in SUBFOLDERS:
        sub_id = _video_get_or_create_subfolder(drive, pack_folder_id, sub)
        if sub_id:
            subfolder_ids[sub] = sub_id

    result: Dict[str, Any] = {"subfolders": subfolder_ids, "uploads": {}}

    if qa_html_bytes and subfolder_ids.get("qa"):
        qa_id = _video_upload_bytes(
            drive,
            folder_id=subfolder_ids["qa"],
            filename="QA-Review.html",
            file_bytes=qa_html_bytes,
            mimetype="text/html",
        )
        if qa_id:
            result["uploads"]["qa_review"] = qa_id
            result["qa_review_url"] = f"https://drive.google.com/file/d/{qa_id}/view"
            result["qa_review_file_id"] = qa_id

    renders_id = subfolder_ids.get("renders")
    if renders_id:
        if long_mp4_bytes:
            lid = _video_upload_bytes(
                drive,
                folder_id=renders_id,
                filename=long_filename or "long.mp4",
                file_bytes=long_mp4_bytes,
                mimetype="video/mp4",
            )
            if lid:
                result["uploads"]["long_mp4"] = lid
                result["long_mp4_file_id"] = lid
                result["long_mp4_preview_url"] = f"https://drive.google.com/file/d/{lid}/preview"
                result["long_mp4_web_view_link"] = f"https://drive.google.com/file/d/{lid}/view"
        if short_mp4_bytes:
            sid = _video_upload_bytes(
                drive,
                folder_id=renders_id,
                filename=short_filename or "30s.mp4",
                file_bytes=short_mp4_bytes,
                mimetype="video/mp4",
            )
            if sid:
                result["uploads"]["short_mp4"] = sid
                result["short_mp4_file_id"] = sid
                result["short_mp4_preview_url"] = f"https://drive.google.com/file/d/{sid}/preview"
                result["short_mp4_web_view_link"] = f"https://drive.google.com/file/d/{sid}/view"

    scripts_id = subfolder_ids.get("scripts")
    script_mimes = {
        ".md": "text/markdown",
        ".json": "application/json",
        ".txt": "text/plain",
    }
    if scripts_id:
        uploaded_scripts: Dict[str, str] = {}
        for fname, data in (script_files or {}).items():
            ext = os.path.splitext(fname)[1].lower()
            sid = _video_upload_bytes(
                drive,
                folder_id=scripts_id,
                filename=fname,
                file_bytes=data,
                mimetype=script_mimes.get(ext, "application/octet-stream"),
            )
            if sid:
                uploaded_scripts[fname] = sid
        if script_bytes:
            sid = _video_upload_bytes(
                drive,
                folder_id=scripts_id,
                filename=script_filename,
                file_bytes=script_bytes,
                mimetype="text/markdown",
            )
            if sid:
                uploaded_scripts[script_filename] = sid
        if uploaded_scripts:
            result["uploads"]["scripts"] = uploaded_scripts
            first = next(iter(uploaded_scripts.values()))
            result["script_url"] = f"https://drive.google.com/file/d/{first}/view"

    slides_id = subfolder_ids.get("slides")
    if slides_id and slide_files:
        uploaded_slides: Dict[str, str] = {}
        for fname, data in slide_files.items():
            ext = os.path.splitext(fname)[1].lower()
            mime = "application/pdf" if ext == ".pdf" else "text/html" if ext in (".html", ".htm") else "application/octet-stream"
            sid = _video_upload_bytes(
                drive,
                folder_id=slides_id,
                filename=fname,
                file_bytes=data,
                mimetype=mime,
            )
            if sid:
                uploaded_slides[fname] = sid
        if uploaded_slides:
            result["uploads"]["slides"] = uploaded_slides
            first = next(iter(uploaded_slides.values()))
            result["slides_url"] = f"https://drive.google.com/file/d/{first}/view"

    result["pack_folder_url"] = pack_folder_url
    return result


def drive_folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def list_pack_subfolder_urls(pack_folder_url: str) -> Dict[str, Any]:
    """Resolve direct Drive URLs for qa/, renders/, scripts/, slides/ under a pack folder."""
    pack_folder_id = _folder_id_from_url(pack_folder_url)
    if not pack_folder_id:
        return {"error": f"Invalid pack folder URL: {pack_folder_url}"}

    drive = get_drive_service()
    if not drive:
        return {"error": "Google Drive service unavailable"}

    subfolders: Dict[str, Dict[str, str]] = {}
    for name in SUBFOLDERS:
        safe_name = name.replace("'", "\\'")
        query = (
            f"name='{safe_name}' and '{pack_folder_id}' in parents "
            "and mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        try:
            results = drive.files().list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()
            files = results.get("files") or []
            if files:
                fid = files[0]["id"]
                entry: Dict[str, Any] = {
                    "folder_id": fid,
                    "folder_url": drive_folder_url(fid),
                }
                try:
                    child_q = f"'{fid}' in parents and trashed=false"
                    child_results = drive.files().list(
                        q=child_q,
                        spaces="drive",
                        fields="files(id, name, mimeType)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    ).execute()
                    child_files = child_results.get("files") or []
                    entry["files"] = [
                        {
                            "id": f["id"],
                            "name": f["name"],
                            "view_url": f"https://drive.google.com/file/d/{f['id']}/view",
                        }
                        for f in child_files
                    ]
                    entry["file_count"] = len(child_files)
                except HttpError as e:
                    logger.error("[video_pack] list files in '%s' failed: %s", name, e.reason)
                    entry["files"] = []
                    entry["file_count"] = 0
                subfolders[name] = entry
        except HttpError as e:
            logger.error("[video_pack] list subfolder '%s' failed: %s", name, e.reason)

    return {
        "pack_folder_id": pack_folder_id,
        "pack_folder_url": pack_folder_url,
        "subfolders": subfolders,
    }


def get_drive_file_meta(file_id: str) -> Optional[Dict[str, Any]]:
    drive = get_drive_service()
    if not drive or not file_id:
        return None
    try:
        return drive.files().get(
            fileId=file_id,
            fields="id,name,mimeType,size",
            supportsAllDrives=True,
        ).execute()
    except HttpError as e:
        logger.error("[video_pack] meta for %s failed: %s", file_id, e.reason)
        return None
    except Exception:
        logger.exception("[video_pack] meta for %s error", file_id)
        return None


def _parse_range_header(range_header: Optional[str], total_size: int) -> Tuple[int, int]:
    """Return (start, end) inclusive byte range. Defaults to full file."""
    if not range_header or not range_header.strip().lower().startswith("bytes="):
        return 0, max(0, total_size - 1)
    spec = range_header.strip()[6:].split(",", 1)[0].strip()
    if "-" not in spec:
        return 0, max(0, total_size - 1)
    start_s, end_s = spec.split("-", 1)
    if start_s:
        start = int(start_s)
        end = int(end_s) if end_s else total_size - 1
    else:
        suffix = int(end_s)
        start = max(0, total_size - suffix)
        end = total_size - 1
    end = min(end, total_size - 1)
    start = max(0, min(start, end))
    return start, end


def iter_drive_file_chunks(
    file_id: str,
    *,
    start: int = 0,
    end: Optional[int] = None,
    partial: bool = False,
    chunk_size: int = 256 * 1024,
) -> Iterator[bytes]:
    """Stream a Drive file via the service account. Set partial=True for HTTP Range requests."""
    drive = get_drive_service()
    if not drive or not file_id:
        return
    try:
        request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)

        if partial:
            headers = dict(getattr(request, "headers", None) or {})
            range_end = str(end) if end is not None else ""
            headers["Range"] = f"bytes={start}-{range_end}"
            _, content = request.http.request(request.uri, method="GET", headers=headers)
            if content:
                for i in range(0, len(content), chunk_size):
                    yield content[i : i + chunk_size]
            return

        import io

        from googleapiclient.http import MediaIoBaseDownload

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request, chunksize=chunk_size)
        done = False
        last_pos = 0
        while not done:
            _, done = downloader.next_chunk()
            fh.seek(last_pos)
            chunk = fh.read()
            last_pos = fh.tell()
            if chunk:
                yield chunk
    except HttpError as e:
        logger.error("[video_pack] stream file %s failed: %s", file_id, e.reason)
    except Exception:
        logger.exception("[video_pack] stream file %s error", file_id)


def fetch_drive_file_bytes(file_id: str) -> Optional[bytes]:
    drive = get_drive_service()
    if not drive or not file_id:
        return None
    try:
        from googleapiclient.http import MediaIoBaseDownload
        import io

        request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()
    except HttpError as e:
        logger.error("[video_pack] fetch file %s failed: %s", file_id, e.reason)
        return None
    except Exception:
        logger.exception("[video_pack] fetch file %s error", file_id)
        return None


def load_qa_review_html(*, qa_review_path: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Load QA-Review.html from a Drive file URL/id or local path (dev publish).
    Returns (html_text, error_message).
    """
    raw = (qa_review_path or "").strip()
    if not raw:
        return None, "No QA review path configured"

    if raw.startswith("http"):
        file_id = _folder_id_from_url(raw.replace("/view", "").replace("/preview", ""))
        # file URLs use /file/d/ not /folders/
        m = re.search(r"/file/d/([^/]+)", raw)
        file_id = m.group(1) if m else file_id
        if not file_id:
            return None, "Could not parse Drive file id from QA URL"
        data = fetch_drive_file_bytes(file_id)
        if not data:
            return None, "Could not download QA-Review.html from Drive"
        try:
            return data.decode("utf-8"), None
        except UnicodeDecodeError:
            return data.decode("utf-8", errors="replace"), None

    if os.path.isfile(raw):
        try:
            with open(raw, "r", encoding="utf-8") as f:
                return f.read(), None
        except OSError as e:
            return None, str(e)

    return None, "QA path is not a reachable Drive URL or local file"
