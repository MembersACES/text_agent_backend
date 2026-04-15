"""
Signed GET requests to Pudu Open Platform (HMAC-SHA1).
Spec: PUDU_API_INTEGRATION.md — match query ordering, quote on values, unquote sorted query in signing path.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote, unquote

import httpx

DEFAULT_HOST = "css-open-platform.pudutech.com"
PUDU_ENTRY_PREFIX = "/pudu-entry"


def logical_path_to_url_path(logical_path: str) -> str:
    p = (logical_path or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    if not p.startswith(PUDU_ENTRY_PREFIX):
        p = PUDU_ENTRY_PREFIX + p
    return p


def _build_query_string(params: Dict[str, Any]) -> str:
    segments: list[str] = []
    for k in sorted(params.keys()):
        v = params[k]
        if v is None:
            continue
        segments.append(f"{k}={quote(str(v), safe='')}")
    return "&".join(segments)


def canonical_path_for_signing(url_path: str, query_string: str) -> str:
    if not query_string:
        return url_path
    parts = [p for p in query_string.split("&") if p]
    sorted_q = "&".join(sorted(parts))
    return f"{url_path}?{unquote(sorted_q)}"


def signed_pudu_get(
    app_key: str,
    app_secret: str,
    logical_path: str,
    query_params: Optional[Dict[str, Any]] = None,
    *,
    host: Optional[str] = None,
    timeout: float = 30.0,
) -> Tuple[Optional[Dict[str, Any]], Optional[httpx.Response], Optional[str]]:
    """
    Returns (json_dict_or_none, httpx_response_or_none, error_message_or_none).
    On HTTP 200 with valid JSON, json is returned even if message is not success (caller checks).
    """
    query_params = dict(query_params or {})
    path = logical_path_to_url_path(logical_path)
    qs = _build_query_string(query_params)
    sign_path = canonical_path_for_signing(path, qs)

    x_date = format_datetime(datetime.now(timezone.utc), usegmt=True)
    signing_str = (
        f"x-date: {x_date}\n"
        "GET\n"
        "application/json\n"
        "application/json\n"
        "\n"
        f"{sign_path}"
    )
    sig = base64.b64encode(
        hmac.new(
            app_secret.encode("utf-8"),
            signing_str.encode("utf-8"),
            hashlib.sha1,
        ).digest()
    ).decode("ascii")
    auth = (
        f'hmac id="{app_key}", algorithm="hmac-sha1", '
        f'headers="x-date", signature="{sig}"'
    )

    h = (host or os.environ.get("PUDU_API_HOST") or DEFAULT_HOST).strip().rstrip("/")
    url = f"https://{h}{path}"
    if qs:
        url = f"{url}?{qs}"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-date": x_date,
        "Authorization": auth,
    }
    try:
        r = httpx.get(url, headers=headers, timeout=timeout)
    except Exception as e:
        logging.exception("Pudu HTTP request failed: %s", e)
        return None, None, str(e)

    if r.status_code != 200:
        return None, r, f"HTTP {r.status_code}: {(r.text or '')[:500]}"

    try:
        body = r.json()
    except Exception as e:
        logging.warning("Pudu non-JSON body: %s", e)
        return None, r, "Invalid JSON from Pudu"

    if not isinstance(body, dict):
        return None, r, "Unexpected JSON type from Pudu"

    return body, r, None


def pudu_message_ok(body: Optional[Dict[str, Any]]) -> bool:
    if not body:
        return False
    msg = body.get("message")
    if msg in ("ok", "SUCCESS", "success"):
        return True
    if isinstance(msg, str) and msg.lower() == "success":
        return True
    return False
