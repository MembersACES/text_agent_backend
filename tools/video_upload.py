"""Upload and register CZA marketing / testimonial videos on Google Drive."""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Optional, Tuple

from tools.one_month_savings import extract_folder_id_from_url, upload_file_to_drive
from tools.resources_drive_videos import get_resources_videos_folder_id
from tools.video_naming import VideoKind, VideoVariant, build_drive_filename, parse_drive_filename, validate_drive_filename
from tools.video_registry_loader import load_video_registry, lookup_slug, solution_type_for_slug

logger = logging.getLogger(__name__)

VIDEO_STATUSES = ("draft", "qa_pending", "approved", "published")
DEFAULT_MIMETYPE = "video/mp4"
DOCX_MIMETYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


def resolve_testimonial_drive_folder(
    *,
    drive_folder_url: Optional[str] = None,
    client_gdrive_url: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    """
    Resolve a Drive folder id for testimonial doc upload.
    Returns (folder_id, error_message).
    """
    for raw in (drive_folder_url, client_gdrive_url):
        if raw and str(raw).strip():
            fid = extract_folder_id_from_url(str(raw).strip())
            if fid:
                return fid, None
            if re.match(r"^[a-zA-Z0-9_-]{10,}$", str(raw).strip()):
                return str(raw).strip(), None

    for env_key in (
        "TESTIMONIAL_STORAGE_FOLDER_ID",
        "RESOURCES_VIDEOS_FOLDER_ID",
        "ONE_MONTH_SAVINGS_DRIVE_FOLDER_ID",
    ):
        val = (os.getenv(env_key) or "").strip()
        if val:
            fid = extract_folder_id_from_url(val) or val
            return fid, None

    default = get_resources_videos_folder_id()
    if default:
        return default, None
    return "", "No Drive folder configured. Set TESTIMONIAL_STORAGE_FOLDER_ID or pass client gdrive_folder_url."


def upload_testimonial_document(
    *,
    file_bytes: bytes,
    filename: str,
    business_name: str,
    drive_folder_id: str,
    content_type: str = DOCX_MIMETYPE,
    testimonial_type: Optional[str] = None,
    testimonial_solution_type_id: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Upload testimonial doc via n8n; fall back to service-account Drive upload for local dev.
    Returns (file_id, error_message).
    """
    from tools.n8n_file_upload import UPLOAD_TYPE_TESTIMONIAL, upload_file_via_n8n

    extra: Dict[str, str] = {}
    if testimonial_type:
        extra["testimonial_type"] = testimonial_type
    if testimonial_solution_type_id:
        extra["testimonial_solution_type_id"] = testimonial_solution_type_id

    n8n_result, n8n_ok, _status = upload_file_via_n8n(
        file_bytes=file_bytes,
        filename=filename,
        upload_type=UPLOAD_TYPE_TESTIMONIAL,
        business_name=business_name,
        drive_folder=drive_folder_id,
        content_type=content_type or DOCX_MIMETYPE,
        extra_form=extra or None,
    )
    file_id = n8n_result.get("file_id") or n8n_result.get("fileId")
    if n8n_ok and file_id:
        return str(file_id), None

    n8n_err = n8n_result.get("message") or n8n_result.get("error_code") or "n8n upload failed"
    logger.warning("n8n testimonial upload failed (%s); trying direct Drive upload", n8n_err)

    ct = content_type or DOCX_MIMETYPE
    if filename.lower().endswith(".doc"):
        ct = "application/msword"
    direct_id = upload_file_to_drive(
        file_bytes=file_bytes,
        filename=filename,
        folder_id=drive_folder_id,
        mimetype=ct,
    )
    if direct_id:
        return direct_id, None
    return None, f"{n8n_err}. Direct Drive upload also failed — check SERVICE_ACCOUNT_JSON and folder access."


def upload_custom_source_document(
    *,
    file_bytes: bytes,
    filename: str,
    drive_folder_id: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Upload custom video source material directly to Drive (no n8n)."""
    ct = DOCX_MIMETYPE
    lower = filename.lower()
    if lower.endswith(".pdf"):
        ct = "application/pdf"
    elif lower.endswith(".txt"):
        ct = "text/plain"
    elif lower.endswith(".md"):
        ct = "text/markdown"
    elif lower.endswith(".doc"):
        ct = "application/msword"

    direct_id = upload_file_to_drive(
        file_bytes=file_bytes,
        filename=filename,
        folder_id=drive_folder_id,
        mimetype=ct,
    )
    if direct_id:
        return direct_id, None
    return None, "Direct Drive upload failed — check SERVICE_ACCOUNT_JSON and folder access."


def suggest_testimonial_slug(
    business_name: str,
    crm_solution_type_id: Optional[str] = None,
) -> str:
    """Best-effort slug from registry, else generate from member + solution type."""
    from tools.video_brief import generate_testimonial_slug

    name = business_name.strip().lower()
    if not name:
        return generate_testimonial_slug(business_name, crm_solution_type_id)

    reg = load_video_registry()
    entries = [e for e in (reg.get("entries") or []) if e.get("kind") == "testimonial"]
    if crm_solution_type_id:
        typed = [e for e in entries if e.get("crm_solution_type_id") == crm_solution_type_id]
        for e in typed:
            slug = (e.get("slug") or "").lower()
            slug_token = slug.replace("-", " ")
            if slug and (slug.replace("-", " ") in name or name in slug_token or slug.split("-")[0] in name):
                return slug
        if len(typed) == 1:
            only = (typed[0].get("slug") or "").lower()
            if only:
                return only
    tokens = [t for t in re.split(r"[^a-z0-9]+", name) if len(t) > 3]
    best: Optional[str] = None
    best_score = 0
    for e in entries:
        slug = (e.get("slug") or "").lower()
        if not slug:
            continue
        slug_parts = slug.split("-")
        score = sum(1 for t in tokens if t in slug_parts or t in slug)
        if score > best_score:
            best_score = score
            best = slug
    if best and best_score > 0:
        return best
    return generate_testimonial_slug(business_name, crm_solution_type_id)


def drive_links_for_file(file_id: str) -> Dict[str, str]:
    return {
        "preview_url": f"https://drive.google.com/file/d/{file_id}/preview",
        "web_view_link": f"https://drive.google.com/file/d/{file_id}/view",
    }


def upload_video_to_library(
    file_bytes: bytes,
    slug: str,
    variant: VideoVariant,
    kind: VideoKind = "marketing",
    filename: Optional[str] = None,
    folder_id: Optional[str] = None,
) -> Tuple[Optional[str], str, Optional[str]]:
    """
    Upload MP4 to Interface Videos folder with standard naming.
    Returns (file_id, file_name, error_message).
    """
    folder_id = (folder_id or get_resources_videos_folder_id()).strip()
    if not folder_id:
        return None, "", "RESOURCES_VIDEOS_FOLDER_ID is not set."

    file_name = filename or build_drive_filename(slug, variant, kind)
    if not validate_drive_filename(file_name):
        return None, file_name, f"Invalid video filename (expected standard pattern): {file_name}"

    file_id = upload_file_to_drive(
        file_bytes=file_bytes,
        filename=file_name,
        folder_id=folder_id,
        mimetype=DEFAULT_MIMETYPE,
    )
    if not file_id:
        return None, file_name, "Drive upload failed."
    return file_id, file_name, None


def resolve_crm_solution_type_id(slug: str, kind: VideoKind, explicit: Optional[str] = None) -> Optional[str]:
    if explicit and explicit.strip():
        return explicit.strip()
    entry = lookup_slug(slug, kind=kind)
    if entry and entry.get("crm_solution_type_id"):
        return entry["crm_solution_type_id"]
    return solution_type_for_slug(slug)
