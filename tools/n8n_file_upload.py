"""
Unified n8n webhook for Drive file uploads (testimonials + 1st month savings invoices).

n8n Switch node should branch on `upload_type`:
  - testimonial
  - one_month_savings_invoice
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

N8N_FILE_UPLOAD_WEBHOOK = os.getenv(
    "N8N_FILE_UPLOAD_WEBHOOK",
    "https://membersaces.app.n8n.cloud/webhook-test/file-upload",
)

UPLOAD_TYPE_TESTIMONIAL = "testimonial"
UPLOAD_TYPE_ONE_MONTH_SAVINGS = "one_month_savings_invoice"

FILE_UPLOAD_LOG_PREFIX = "[FILE_UPLOAD]"


def file_upload_log(
    level: int,
    message: str,
    *,
    upload_type: Optional[str] = None,
    request_id: Optional[str] = None,
    business_name: Optional[str] = None,
    **fields: Any,
) -> None:
    parts = [FILE_UPLOAD_LOG_PREFIX]
    if request_id:
        parts.append(f"request_id={request_id}")
    if upload_type:
        parts.append(f"upload_type={upload_type}")
    if business_name:
        parts.append(f"business={business_name!r}")
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).replace("\n", " ")
        if len(text) > 400:
            text = text[:397] + "..."
        parts.append(f"{key}={text}")
    parts.append(message)
    logger.log(level, " | ".join(parts))


def _parse_n8n_response(text: str) -> Dict[str, Any]:
    if not (text or "").strip():
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {"message": text.strip()}
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        return parsed[0]
    if isinstance(parsed, dict):
        return parsed
    return {"message": str(parsed)}


def upload_file_via_n8n(
    *,
    file_bytes: bytes,
    filename: str,
    upload_type: str,
    business_name: str,
    drive_folder: str,
    content_type: str = "application/octet-stream",
    request_id: Optional[str] = None,
    requested_by: Optional[str] = None,
    extra_form: Optional[Dict[str, str]] = None,
    timeout: int = 120,
) -> Tuple[Dict[str, Any], bool, int]:
    """
    POST multipart file + metadata to the unified n8n file-upload webhook.

    Returns (parsed_json, http_ok, status_code).
  On success, parsed_json should contain file_id (or fileId).
    """
    webhook_url = N8N_FILE_UPLOAD_WEBHOOK
    if not drive_folder or not str(drive_folder).strip():
        file_upload_log(
            logging.ERROR,
            "FAIL missing drive_folder",
            upload_type=upload_type,
            request_id=request_id,
            business_name=business_name,
            error_code="MISSING_DRIVE_FOLDER",
        )
        return (
            {
                "success": False,
                "error_code": "MISSING_DRIVE_FOLDER",
                "message": "drive_folder is required for n8n upload",
            },
            False,
            400,
        )

    form: Dict[str, str] = {
        "upload_type": upload_type,
        "business_name": business_name.strip(),
        "drive_folder": str(drive_folder).strip(),
        "filename": filename,
    }
    if request_id:
        form["request_id"] = request_id
    if requested_by:
        form["requested_by"] = requested_by
    if extra_form:
        for key, value in extra_form.items():
            if value is not None and str(value).strip():
                form[key] = str(value).strip()

    files = {
        "file": (filename, file_bytes, content_type),
    }

    file_upload_log(
        logging.INFO,
        "n8n request start",
        upload_type=upload_type,
        request_id=request_id,
        business_name=business_name,
        webhook=webhook_url,
        filename=filename,
        bytes=len(file_bytes or b""),
        drive_folder=drive_folder,
        form_keys=",".join(sorted(form.keys())),
    )

    try:
        resp = requests.post(
            webhook_url,
            data=form,
            files=files,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        file_upload_log(
            logging.ERROR,
            "FAIL n8n request exception",
            upload_type=upload_type,
            request_id=request_id,
            business_name=business_name,
            error_code="N8N_REQUEST_FAILED",
            message=str(exc),
        )
        return (
            {
                "success": False,
                "error_code": "N8N_REQUEST_FAILED",
                "message": str(exc),
            },
            False,
            502,
        )

    text = resp.text or ""
    parsed = _parse_n8n_response(text)
    http_ok = resp.ok

    file_id = (parsed.get("file_id") or parsed.get("fileId") or "").strip() or None
    if http_ok and not file_id:
        parsed["success"] = False
        parsed.setdefault("error_code", "N8N_NO_FILE_ID")
        parsed.setdefault(
            "message",
            "n8n returned HTTP 200 but no file_id in response",
        )
        http_ok = False

    if http_ok:
        file_upload_log(
            logging.INFO,
            "n8n request ok",
            upload_type=upload_type,
            request_id=request_id,
            business_name=business_name,
            http=resp.status_code,
            file_id=file_id,
        )
        parsed.setdefault("success", True)
        parsed["file_id"] = file_id
        if file_id and not parsed.get("file_url"):
            parsed["file_url"] = f"https://drive.google.com/file/d/{file_id}/view"
    else:
        err_code = parsed.get("error_code") or "N8N_UPLOAD_FAILED"
        err_msg = (
            parsed.get("message")
            or parsed.get("detail")
            or text.strip()[:300]
            or f"n8n upload failed with HTTP {resp.status_code}"
        )
        file_upload_log(
            logging.ERROR,
            "FAIL n8n response",
            upload_type=upload_type,
            request_id=request_id,
            business_name=business_name,
            http=resp.status_code,
            error_code=err_code,
            message=err_msg,
            raw_preview=text.strip().replace("\n", " ")[:200],
        )
        parsed.setdefault("success", False)
        parsed.setdefault("error_code", err_code)
        parsed.setdefault("message", err_msg)

    return parsed, http_ok, int(resp.status_code)
