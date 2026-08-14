"""Batch-fetch Google Drive created/modified times for member document file IDs."""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from tools.one_month_savings import get_drive_service

logger = logging.getLogger(__name__)

MAX_FILE_IDS = 100
_DRIVE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{10,}$")


def extract_drive_file_id(value: object) -> str:
    """Extract a Drive/Sheets file id from a URL or raw cell value."""
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    for pattern in (
        r"/spreadsheets/d/([a-zA-Z0-9_-]+)",
        r"/file/d/([a-zA-Z0-9_-]+)",
        r"/folders/([a-zA-Z0-9_-]+)",
        r"[?&]id=([a-zA-Z0-9_-]+)",
    ):
        m = re.search(pattern, s)
        if m:
            return m.group(1)
    token = s.split("/", 1)[0].strip()
    if _DRIVE_ID_RE.match(token):
        return token
    return ""


def _unique_file_ids(file_ids: list[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in file_ids:
        fid = extract_drive_file_id(raw)
        if not fid or fid in seen:
            continue
        seen.add(fid)
        out.append(fid)
        if len(out) >= MAX_FILE_IDS:
            break
    return out


def get_drive_file_times(file_ids: list[Any]) -> dict[str, dict[str, Optional[str]]]:
    """
    Return {file_id: {created_time, modified_time}} for the given Drive IDs/URLs.
    Missing or inaccessible files are omitted.
    """
    unique = _unique_file_ids(file_ids)
    if not unique:
        return {}

    drive = get_drive_service()
    if not drive:
        logger.warning("Drive service unavailable; skipping file metadata")
        return {}

    out: dict[str, dict[str, Optional[str]]] = {}
    req_to_id: dict[str, str] = {}

    def callback(request_id: str, response: Any, exception: Exception | None) -> None:
        fid = req_to_id.get(request_id)
        if not fid:
            return
        if exception is not None:
            logger.debug("Drive metadata failed for %s: %s", fid, exception)
            return
        if not isinstance(response, dict):
            return
        out[fid] = {
            "created_time": response.get("createdTime"),
            "modified_time": response.get("modifiedTime"),
        }

    try:
        batch = drive.new_batch_http_request(callback=callback)
        for i, fid in enumerate(unique):
            rid = str(i)
            req_to_id[rid] = fid
            batch.add(
                drive.files().get(
                    fileId=fid,
                    fields="id,createdTime,modifiedTime",
                    supportsAllDrives=True,
                ),
                request_id=rid,
            )
        batch.execute()
    except Exception as e:
        logger.warning("Drive batch metadata failed: %s", e)
    return out
