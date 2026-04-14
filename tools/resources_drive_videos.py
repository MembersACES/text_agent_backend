"""List video files in a Drive folder for the ACES resources page (service account)."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError

from tools.one_month_savings import get_drive_service

logger = logging.getLogger(__name__)

# Default: ACES resources video folder (override with RESOURCES_VIDEOS_FOLDER_ID)
DEFAULT_RESOURCES_VIDEOS_FOLDER_ID = "1VmTut-4mztUiz95g2BqnZTMoeCVMhsb9"


def get_resources_videos_folder_id() -> str:
    return (os.getenv("RESOURCES_VIDEOS_FOLDER_ID") or DEFAULT_RESOURCES_VIDEOS_FOLDER_ID).strip()


def list_resources_folder_videos() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    List direct children of the folder that have a video/* MIME type.

    Returns (videos, error_message). error_message is None on success.
    Each video dict: id, name, mimeType, webViewLink, previewUrl, createdTime.
    """
    folder_id = get_resources_videos_folder_id()
    if not folder_id:
        return [], "RESOURCES_VIDEOS_FOLDER_ID is not set."

    drive = get_drive_service()
    if not drive:
        return [], (
            "Google Drive is not configured. Set SERVICE_ACCOUNT_FILE or SERVICE_ACCOUNT_JSON, "
            "enable the Drive API for the project, and share the videos folder with the service "
            "account email (Viewer is enough)."
        )

    q = f"'{folder_id}' in parents and trashed = false and mimeType contains 'video/'"
    videos: List[Dict[str, Any]] = []
    page_token: Optional[str] = None

    try:
        while True:
            kwargs: Dict[str, Any] = {
                "q": q,
                "spaces": "drive",
                "fields": "nextPageToken, files(id, name, mimeType, webViewLink, createdTime)",
                "pageSize": 100,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
            }
            if page_token:
                kwargs["pageToken"] = page_token
            result = drive.files().list(**kwargs).execute()
            for f in result.get("files", []):
                fid = f.get("id")
                if not fid:
                    continue
                videos.append(
                    {
                        "id": fid,
                        "name": f.get("name") or "Video",
                        "mimeType": f.get("mimeType") or "",
                        "webViewLink": f.get("webViewLink")
                        or f"https://drive.google.com/file/d/{fid}/view",
                        "previewUrl": f"https://drive.google.com/file/d/{fid}/preview",
                        "createdTime": f.get("createdTime"),
                    }
                )
            page_token = result.get("nextPageToken")
            if not page_token:
                break
    except HttpError as e:
        logger.exception("Drive API list videos failed: %s", e)
        status = getattr(e.resp, "status", None)
        if status == 404:
            return [], (
                "Folder not found or the service account cannot access it. "
                "Share the folder (or parent Shared drive) with the service account."
            )
        return [], f"Google Drive error: {e.reason}"

    videos.sort(key=lambda x: (x.get("name") or "").lower())
    return videos, None
