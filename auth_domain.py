"""Staff Google ID-token domain policy.

Used by verify_google_token and verify_roster_access. Key/HMAC/unsubscribe
paths must not call this. Default AUTH_DOMAIN_MODE=log (deny lines only);
set AUTH_DOMAIN_MODE=enforce to 403.
"""

from __future__ import annotations

import logging
import os
from contextvars import ContextVar, Token
from typing import Any

from fastapi import HTTPException, Request

logger = logging.getLogger(__name__)

DEFAULT_ALLOWED_EMAIL_DOMAINS = ("acesolutions.com.au", "czeroanz.com")
DEFAULT_AUTH_DOMAIN_MODE = "log"

LOG_DENY = "ACES_AUTH_DOMAIN_DENY"
LOG_EMPTY_ENV = "ACES_AUTH_DOMAIN_EMPTY_ENV"
LOG_MODE_INVALID = "ACES_AUTH_DOMAIN_MODE_INVALID"

_request_path: ContextVar[str] = ContextVar("aces_auth_request_path", default="-")
_request_origin: ContextVar[str] = ContextVar("aces_auth_request_origin", default="")
_request_url: ContextVar[str] = ContextVar("aces_auth_request_url", default="")

_empty_env_logged = False
_invalid_mode_logged = False


def bind_request_context(request: Request) -> tuple[Token, Token, Token]:
    path = request.url.path or "-"
    host = (request.headers.get("host") or "").split(",")[0].strip()
    proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip()
    if not proto:
        proto = request.url.scheme or "https"
    origin = f"{proto}://{host}".rstrip("/") if host else str(request.base_url).rstrip("/")
    full_url = f"{origin}{path}"
    return (
        _request_path.set(path),
        _request_origin.set(origin),
        _request_url.set(full_url),
    )


def reset_request_context(tokens: tuple[Token, Token, Token]) -> None:
    _request_path.reset(tokens[0])
    _request_origin.reset(tokens[1])
    _request_url.reset(tokens[2])


def current_request_path() -> str:
    return _request_path.get()


def current_request_origin() -> str:
    return _request_origin.get()


def current_request_url() -> str:
    return _request_url.get()


def parse_allowed_email_domains(raw: str | None) -> list[str]:
    global _empty_env_logged
    if raw is None:
        return list(DEFAULT_ALLOWED_EMAIL_DOMAINS)
    if not raw.strip():
        if not _empty_env_logged:
            logger.warning(
                "%s AUTH_ALLOWED_EMAIL_DOMAINS is set but empty; "
                "using default acesolutions.com.au,czeroanz.com",
                LOG_EMPTY_ENV,
            )
            _empty_env_logged = True
        return list(DEFAULT_ALLOWED_EMAIL_DOMAINS)
    domains = [
        part.strip().lower().lstrip("@")
        for part in raw.split(",")
        if part.strip()
    ]
    return domains or list(DEFAULT_ALLOWED_EMAIL_DOMAINS)


def get_allowed_email_domains() -> list[str]:
    return parse_allowed_email_domains(os.getenv("AUTH_ALLOWED_EMAIL_DOMAINS"))


def get_auth_domain_mode() -> str:
    global _invalid_mode_logged
    raw = (os.getenv("AUTH_DOMAIN_MODE") or "").strip().lower()
    if raw in ("", DEFAULT_AUTH_DOMAIN_MODE):
        return DEFAULT_AUTH_DOMAIN_MODE
    if raw == "enforce":
        return "enforce"
    if not _invalid_mode_logged:
        logger.warning(
            "%s AUTH_DOMAIN_MODE=%r is not log or enforce; using log",
            LOG_MODE_INVALID,
            raw,
        )
        _invalid_mode_logged = True
    return DEFAULT_AUTH_DOMAIN_MODE


def email_domain_allowed(email: str | None) -> bool:
    if not email or "@" not in email:
        return False
    domain = email.rsplit("@", 1)[1].strip().lower()
    if not domain:
        return False
    return domain in get_allowed_email_domains()


def apply_email_domain_policy(idinfo: dict[str, Any], auth_path: str) -> dict[str, Any]:
    """Log or 403 off-domain Google ID tokens. Returns idinfo unchanged on allow or log-only."""
    email = str(idinfo.get("email") or "").strip()
    if email_domain_allowed(email):
        return idinfo

    mode = get_auth_domain_mode()
    path = current_request_path()
    logger.warning(
        "%s mode=%s auth_path=%s path=%s email=%s",
        LOG_DENY,
        mode,
        auth_path,
        path,
        email or "-",
    )
    if mode == "enforce":
        raise HTTPException(status_code=403, detail="Forbidden")
    return idinfo
