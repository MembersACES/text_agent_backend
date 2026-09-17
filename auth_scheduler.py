"""Cloud Scheduler OIDC for the two unauthenticated-cron routes.

Audience is the Cloud Run service URL (or job URI), not GOOGLE_CLIENT_ID.
Do not run the staff email-domain policy on these tokens.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from fastapi import Header, HTTPException
from google.auth.transport import requests as grequests
from google.oauth2 import id_token

from auth_domain import current_request_origin, current_request_url

logger = logging.getLogger(__name__)

DEFAULT_SCHEDULER_SA_EMAIL = "672026052958-compute@developer.gserviceaccount.com"


def _audience_values(aud: Any) -> list[str]:
    if aud is None:
        return []
    if isinstance(aud, str):
        return [aud]
    if isinstance(aud, (list, tuple)):
        return [str(item) for item in aud if item]
    return [str(aud)]


def _add_audience(allowed: set[str], url: str) -> None:
    value = (url or "").strip().rstrip("/")
    if not value:
        return
    allowed.add(value)
    if value.startswith("http://"):
        allowed.add("https://" + value[len("http://") :])
    elif value.startswith("https://"):
        allowed.add("http://" + value[len("https://") :])


def _allowed_audiences() -> set[str]:
    allowed: set[str] = set()
    raw = (os.getenv("SCHEDULER_OIDC_AUDIENCE") or "").strip()
    if raw:
        for part in raw.split(","):
            _add_audience(allowed, part)
    _add_audience(allowed, current_request_origin())
    _add_audience(allowed, current_request_url())
    return allowed


def _email_verified(idinfo: dict[str, Any]) -> bool:
    verified = idinfo.get("email_verified")
    if verified is True:
        return True
    return str(verified).strip().lower() == "true"


def verify_cloud_scheduler_oidc(
    authorization: Optional[str] = Header(None),
) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Scheduler authorization required")
    token = authorization.split("Bearer ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Scheduler authorization required")

    try:
        idinfo = id_token.verify_oauth2_token(token, grequests.Request())
    except Exception:
        logger.warning("ACES_SCHEDULER_OIDC_DENY reason=invalid_token")
        raise HTTPException(status_code=401, detail="Invalid scheduler token") from None

    expected_sa = (
        os.getenv("SCHEDULER_OIDC_SA_EMAIL") or DEFAULT_SCHEDULER_SA_EMAIL
    ).strip().lower()
    email = str(idinfo.get("email") or "").strip().lower()
    if email != expected_sa or not _email_verified(idinfo):
        logger.warning(
            "ACES_SCHEDULER_OIDC_DENY reason=sa_mismatch email=%s",
            email or "-",
        )
        raise HTTPException(status_code=401, detail="Invalid scheduler token")

    token_auds = {value.rstrip("/") for value in _audience_values(idinfo.get("aud")) if value}
    allowed = _allowed_audiences()
    if not token_auds.intersection(allowed):
        logger.warning(
            "ACES_SCHEDULER_OIDC_DENY reason=audience token_aud=%s allowed=%s",
            ",".join(sorted(token_auds)) or "-",
            ",".join(sorted(allowed)) or "-",
        )
        raise HTTPException(status_code=401, detail="Invalid scheduler token")
    return idinfo
