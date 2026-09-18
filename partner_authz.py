"""Partner identity, tool flags, lead-collision guard, and write audit.

Identity decisions happen in auth_domain.apply_email_domain_policy:
staff domain returns unchanged; only then is lookup_active_partner_principal
consulted. Route handlers must not re-implement staff vs partner.

Empty partners / partner_users is a no-op: lookup returns None, staff never
calls lookup. Do not add partner domains to AUTH_ALLOWED_EMAIL_DOMAINS.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from database import SessionLocal
from models import Client, Partner, PartnerAuditEvent, PartnerLeadCollision, PartnerUser

logger = logging.getLogger(__name__)

ACES_AUTH_KEY = "aces_auth"
ROLE_PARTNER = "partner"

COLLISION_PUBLIC_ACK = {"status": "received"}
LOG_PARTNER_ALLOW = "ACES_AUTH_PARTNER_ALLOW"


class PartnerTool(str, Enum):
    BASE1 = "base1"
    BASE2 = "base2"


@dataclass(frozen=True)
class PartnerPrincipal:
    partner_id: int
    partner_user_id: int
    email: str
    tools: tuple[str, ...]


@dataclass(frozen=True)
class PartnerLeadAdmitResult:
    outcome: str
    public: dict[str, Any]
    client_id: Optional[int] = None
    collision_id: Optional[int] = None


def parse_enabled_tools(raw: Any) -> tuple[str, ...]:
    if raw is None or raw == "":
        return ()
    if isinstance(raw, (list, tuple)):
        values = raw
    else:
        try:
            values = json.loads(raw)
        except (TypeError, ValueError):
            return ()
        if not isinstance(values, list):
            return ()
    known = {item.value for item in PartnerTool}
    return tuple(item for item in values if isinstance(item, str) and item in known)


def lookup_active_partner_principal(
    email: str,
    db: Optional[Session] = None,
) -> Optional[PartnerPrincipal]:
    """Active partner_users row on an active partner. Fail closed on DB errors."""
    normalised = (email or "").strip().lower()
    if not normalised or "@" not in normalised:
        return None
    owns_session = db is None
    session = db if db is not None else SessionLocal()
    try:
        row = (
            session.query(PartnerUser, Partner)
            .join(Partner, PartnerUser.partner_id == Partner.id)
            .filter(
                func.lower(PartnerUser.email) == normalised,
                PartnerUser.active == 1,
                Partner.active == 1,
            )
            .first()
        )
        if row is None:
            return None
        user, partner = row
        return PartnerPrincipal(
            partner_id=int(partner.id),
            partner_user_id=int(user.id),
            email=str(user.email),
            tools=parse_enabled_tools(partner.enabled_tools),
        )
    except Exception:
        logger.exception("ACES_AUTH_PARTNER_LOOKUP_FAIL email=%s", normalised)
        return None
    finally:
        if owns_session:
            session.close()


def tag_idinfo(idinfo: dict[str, Any], principal: PartnerPrincipal) -> dict[str, Any]:
    tagged = dict(idinfo)
    tagged[ACES_AUTH_KEY] = {
        "role": ROLE_PARTNER,
        "partner_id": principal.partner_id,
        "partner_user_id": principal.partner_user_id,
        "tools": list(principal.tools),
    }
    return tagged


def partner_principal_from_idinfo(idinfo: dict[str, Any] | None) -> Optional[PartnerPrincipal]:
    if not idinfo:
        return None
    blob = idinfo.get(ACES_AUTH_KEY)
    if not isinstance(blob, dict) or blob.get("role") != ROLE_PARTNER:
        return None
    try:
        partner_id = int(blob["partner_id"])
        partner_user_id = int(blob["partner_user_id"])
    except (KeyError, TypeError, ValueError):
        return None
    email = str(idinfo.get("email") or "").strip()
    tools = parse_enabled_tools(blob.get("tools"))
    return PartnerPrincipal(
        partner_id=partner_id,
        partner_user_id=partner_user_id,
        email=email,
        tools=tools,
    )


def partner_has_tool(principal: PartnerPrincipal, tool: PartnerTool) -> bool:
    return tool.value in principal.tools


def require_partner_tool(principal: PartnerPrincipal, tool: PartnerTool) -> None:
    """Tool flags are authorisation. Call on every partner route with partner_id scoping."""
    if not partner_has_tool(principal, tool):
        raise HTTPException(status_code=403, detail="Forbidden")


def _clients_matching_business_name(db: Session, business_name: str) -> list[Client]:
    needle = (business_name or "").strip().lower()
    if not needle:
        return []
    return (
        db.query(Client)
        .filter(func.lower(func.trim(Client.business_name)) == needle)
        .all()
    )


def admit_partner_lead(
    db: Session,
    principal: PartnerPrincipal,
    business_name: str,
    payload: dict[str, Any] | None = None,
) -> PartnerLeadAdmitResult:
    """Never stamp partner_id onto a pre-existing clients row.

    create: no matching business_name (caller may insert later).
    own: every match already belongs to this partner; partner_id is left as-is.
    collision: any ACES-owned or other-partner match; write a staff referral row
    and return only an acknowledgement.
    """
    matches = _clients_matching_business_name(db, business_name)
    if not matches:
        return PartnerLeadAdmitResult(outcome="create", public={"status": "ok"})

    if matches and all(row.partner_id == principal.partner_id for row in matches):
        return PartnerLeadAdmitResult(
            outcome="own",
            public={"status": "ok"},
            client_id=matches[0].id,
        )

    existing = next(
        (row for row in matches if row.partner_id != principal.partner_id),
        matches[0],
    )
    collision = PartnerLeadCollision(
        partner_id=principal.partner_id,
        submitted_by_email=principal.email,
        submitted_business_name=(business_name or "").strip(),
        existing_client_id=existing.id,
        payload_json=json.dumps(payload) if payload is not None else None,
        status="pending",
    )
    db.add(collision)
    db.flush()
    log_partner_write(
        db,
        principal,
        action="lead_collision",
        target_type="partner_lead_collision",
        target_id=collision.id,
    )
    return PartnerLeadAdmitResult(
        outcome="collision",
        public=dict(COLLISION_PUBLIC_ACK),
        collision_id=collision.id,
    )


def log_partner_write(
    db: Session,
    principal: PartnerPrincipal,
    action: str,
    target_type: str,
    target_id: str | int | None = None,
    path: str | None = None,
) -> PartnerAuditEvent:
    event = PartnerAuditEvent(
        partner_id=principal.partner_id,
        email=principal.email,
        action=action,
        target_type=target_type,
        target_id=None if target_id is None else str(target_id),
        path=path,
    )
    db.add(event)
    db.flush()
    return event
