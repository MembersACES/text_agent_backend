"""Invoicing page allowlist — mirror of frontend INVOICING_ALLOWED_EMAILS semantics."""

from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

from fastapi import HTTPException

logger = logging.getLogger(__name__)


def normalize_email(email: str) -> str:
    return email.strip().lower()


def get_invoicing_allowlist_raw() -> Tuple[str, str]:
    """
    Returns (raw_value, source_env_name).
    source_env_name is "" when neither var is set.
    """
    primary = os.getenv("INVOICING_ALLOWED_EMAILS")
    if primary is not None and primary.strip() != "":
        return primary, "INVOICING_ALLOWED_EMAILS"
    fallback = os.getenv("NEXT_PUBLIC_INVOICING_ALLOWED_EMAILS")
    if fallback is not None and fallback.strip() != "":
        return fallback, "NEXT_PUBLIC_INVOICING_ALLOWED_EMAILS"
    # Distinguish unset vs explicitly empty
    if primary is not None:
        return primary, "INVOICING_ALLOWED_EMAILS"
    if fallback is not None:
        return fallback, "NEXT_PUBLIC_INVOICING_ALLOWED_EMAILS"
    return "", ""


def parse_invoicing_allowlist(raw: str) -> list[str]:
    return [normalize_email(part) for part in raw.split(",") if normalize_email(part)]


def is_email_in_invoicing_allowlist(email: Optional[str]) -> bool:
    if not email:
        return False
    raw, _source = get_invoicing_allowlist_raw()
    allowed = parse_invoicing_allowlist(raw)
    if not allowed:
        return False
    return normalize_email(email) in allowed


def require_invoicing_user(user_info: dict) -> dict:
    """
    Raise 403 if the authenticated user is not on the invoicing allowlist.
    Empty allowlist denies everyone (same as frontend).
    """
    email = user_info.get("email") if isinstance(user_info, dict) else None
    raw, source = get_invoicing_allowlist_raw()
    allowed = parse_invoicing_allowlist(raw)
    normalized = normalize_email(email) if email else ""

    if not email:
        logger.warning(
            "[invoicing_access] DENY | reason=no_email_on_token | "
            "allowlist_source=%s | allowlist_count=%s",
            source or "(unset)",
            len(allowed),
        )
        raise HTTPException(status_code=403, detail="Invoicing access denied")

    if not source:
        logger.warning(
            "[invoicing_access] DENY | reason=allowlist_env_unset | email=%s | "
            "hint=Set INVOICING_ALLOWED_EMAILS in text_agent_backend/.env and fully restart uvicorn "
            "(reload does not pick up .env changes)",
            normalized,
        )
        raise HTTPException(
            status_code=403,
            detail=(
                "Invoicing access denied: INVOICING_ALLOWED_EMAILS is not set on the backend. "
                "Add it to text_agent_backend/.env and fully restart the API server."
            ),
        )

    if not allowed:
        logger.warning(
            "[invoicing_access] DENY | reason=allowlist_empty | email=%s | "
            "allowlist_source=%s | raw_length=%s",
            normalized,
            source,
            len(raw),
        )
        raise HTTPException(
            status_code=403,
            detail=(
                "Invoicing access denied: INVOICING_ALLOWED_EMAILS is empty on the backend. "
                "Add comma-separated emails and fully restart the API server."
            ),
        )

    if normalized not in allowed:
        # Log only other allowlisted local-parts domains for debugging typo/mismatch —
        # not full addresses beyond the caller.
        logger.warning(
            "[invoicing_access] DENY | reason=email_not_in_allowlist | email=%s | "
            "allowlist_source=%s | allowlist_count=%s | allowlist_emails=%s",
            normalized,
            source,
            len(allowed),
            ",".join(allowed),
        )
        raise HTTPException(
            status_code=403,
            detail=(
                f"Invoicing access denied for {normalized}. "
                "This email is authenticated but not in the backend INVOICING_ALLOWED_EMAILS list."
            ),
        )

    logger.info(
        "[invoicing_access] ALLOW | email=%s | allowlist_source=%s | allowlist_count=%s",
        normalized,
        source,
        len(allowed),
    )
    return user_info
