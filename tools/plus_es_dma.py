"""List Plus ES DMA PDFs from a Google Drive folder (service account)."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError

from tools.one_month_savings import get_drive_service

logger = logging.getLogger(__name__)

DEFAULT_PLUS_ES_DMA_FOLDER_ID = "1Y4GEr3ZVmvrfM9Jb3ZHeYFAH6WFKpQqO"


def get_plus_es_dma_folder_id() -> str:
    return (os.getenv("PLUS_ES_DMA_FOLDER_ID") or DEFAULT_PLUS_ES_DMA_FOLDER_ID).strip()


def list_plus_es_dma_pdfs() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    List PDF files in the Plus ES DMA Drive folder.

    Returns (pdfs, error_message). error_message is None on success.
    Each pdf dict: id, name, mimeType, webViewLink, previewUrl, createdTime.
    """
    folder_id = get_plus_es_dma_folder_id()
    if not folder_id:
        return [], "PLUS_ES_DMA_FOLDER_ID is not set."

    drive = get_drive_service()
    if not drive:
        return [], (
            "Google Drive is not configured. Set SERVICE_ACCOUNT_FILE or SERVICE_ACCOUNT_JSON, "
            "enable the Drive API for the project, and share the DMA folder with the service "
            "account email (Viewer is enough)."
        )

    q = f"'{folder_id}' in parents and trashed = false and mimeType = 'application/pdf'"
    pdfs: List[Dict[str, Any]] = []
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
                mime = f.get("mimeType") or ""
                name = f.get("name") or "Document"
                pdfs.append(
                    {
                        "id": fid,
                        "name": name,
                        "mimeType": mime,
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
        logger.exception("Drive API list Plus ES DMA PDFs failed: %s", e)
        status = getattr(e.resp, "status", None)
        if status == 404:
            return [], (
                "Folder not found or the service account cannot access it. "
                "Share the folder (or parent Shared drive) with the service account."
            )
        return [], f"Google Drive error: {e.reason}"

    pdfs.sort(key=lambda x: (x.get("name") or "").lower())
    return pdfs, None
