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
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from database import SessionLocal
from models import Client, Partner, PartnerAuditEvent, PartnerLeadCollision, PartnerUser
from schemas import PartnerClientResponse

logger = logging.getLogger(__name__)

ACES_AUTH_KEY = "aces_auth"
ROLE_PARTNER = "partner"

COLLISION_PUBLIC_ACK = {"status": "received"}
LOG_PARTNER_ALLOW = "ACES_AUTH_PARTNER_ALLOW"

PARTNER_CLIENT_KEYS = frozenset(PartnerClientResponse.model_fields)

BASE1_ALLOWED_EXTENSIONS = frozenset({".pdf", ".jpg", ".jpeg", ".png", ".heic", ".heif"})
BASE1_ALLOWED_MIME_TYPES = frozenset(
    {
        "application/pdf",
        "image/jpeg",
        "image/jpg",
        "image/png",
        "image/heic",
        "image/heif",
        "image/heic-sequence",
        "application/octet-stream",
    }
)
BASE1_MAX_FILES = 15
BASE1_MAX_FILE_BYTES = 8 * 1024 * 1024
BASE1_MAX_TOTAL_BYTES = 30 * 1024 * 1024
BASE1_MAX_SUBMISSIONS_PER_HOUR = 10
BASE1_RATE_ACTION = "base1_submit_attempt"
BASE1_RATE_LIMIT_MESSAGE = (
    "Too many submissions in a short window. Please try again shortly."
)
BASE1_TYPE_MESSAGE = "PDF, JPG, PNG, or HEIC only"
COLLISION_UPLOAD_FOLDER = "Lead collisions"
HEIC_FTYP_BRANDS = frozenset(
    {b"heic", b"heix", b"hevc", b"hevx", b"heim", b"heis", b"mif1", b"msf1"}
)


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


def refuse_staff_route_if_partner(idinfo: dict[str, Any] | None) -> None:
    if partner_principal_from_idinfo(idinfo):
        raise HTTPException(status_code=403, detail="Forbidden")


def partner_client_public(client: Client) -> dict[str, Any]:
    payload = PartnerClientResponse.model_validate(client).model_dump(mode="json")
    if set(payload) != PARTNER_CLIENT_KEYS:
        raise RuntimeError("partner client payload keys drifted")
    return payload


def _file_ext(name: str) -> str:
    raw = (name or "").rsplit(".", 1)
    if len(raw) != 2:
        return ""
    return f".{raw[1].strip().lower()}"


def _is_heic(data: bytes) -> bool:
    if len(data) < 12:
        return False
    if data[4:8] != b"ftyp":
        return False
    return data[8:12] in HEIC_FTYP_BRANDS


def _magic_ok(filename: str, data: bytes) -> bool:
    ext = _file_ext(filename)
    if ext == ".pdf":
        return data.startswith(b"%PDF")
    if ext in {".jpg", ".jpeg"}:
        return data.startswith(b"\xff\xd8\xff")
    if ext == ".png":
        return data.startswith(b"\x89PNG\r\n\x1a\n")
    if ext in {".heic", ".heif"}:
        return _is_heic(data)
    return False


def validate_base1_files(files: list[tuple[str, bytes, str | None]]) -> None:
    if not files:
        raise HTTPException(status_code=400, detail="Upload at least one utility invoice")
    if len(files) > BASE1_MAX_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"At most {BASE1_MAX_FILES} files per submission",
        )
    total = 0
    for name, data, content_type in files:
        ext = _file_ext(name)
        if ext not in BASE1_ALLOWED_EXTENSIONS:
            raise HTTPException(status_code=400, detail=f"{name}: {BASE1_TYPE_MESSAGE}")
        mime = (content_type or "").split(";")[0].strip().lower()
        if mime and mime not in BASE1_ALLOWED_MIME_TYPES:
            raise HTTPException(status_code=400, detail=f"{name}: {BASE1_TYPE_MESSAGE}")
        size = len(data)
        if size <= 0:
            raise HTTPException(status_code=400, detail=f"{name}: empty file")
        if size > BASE1_MAX_FILE_BYTES:
            raise HTTPException(status_code=400, detail=f"{name}: max size is 8MB")
        if not _magic_ok(name, data):
            raise HTTPException(status_code=400, detail=f"{name}: file contents do not match type")
        total += size
    if total > BASE1_MAX_TOTAL_BYTES:
        raise HTTPException(status_code=400, detail="Total upload exceeds 30MB")


def assert_base1_rate_limit(db: Session, partner_id: int) -> None:
    cutoff = datetime.utcnow() - timedelta(hours=1)
    count = (
        db.query(PartnerAuditEvent)
        .filter(
            PartnerAuditEvent.partner_id == partner_id,
            PartnerAuditEvent.action == BASE1_RATE_ACTION,
            PartnerAuditEvent.created_at >= cutoff,
        )
        .count()
    )
    if count >= BASE1_MAX_SUBMISSIONS_PER_HOUR:
        raise HTTPException(
            status_code=429,
            detail={
                "code": "rate_limited",
                "message": BASE1_RATE_LIMIT_MESSAGE,
            },
        )


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
    return PartnerLeadAdmitResult(
        outcome="collision",
        public=dict(COLLISION_PUBLIC_ACK),
        collision_id=collision.id,
    )


def persist_collision_files(
    partner: Partner,
    collision: PartnerLeadCollision,
    files: list[tuple[str, bytes, str | None]],
) -> list[dict[str, Any]]:
    """Write collision uploads into the partner Drive folder. No CRM client is created."""
    folder_id = (partner.drive_folder_id or "").strip()
    if not folder_id:
        logger.error("ACES_PARTNER_COLLISION_NO_DRIVE partner_id=%s", partner.id)
        raise HTTPException(
            status_code=503,
            detail={
                "code": "drive_unavailable",
                "message": "We could not save your documents. Please try again shortly.",
            },
        )
    from tools.member_folder_drive import (
        MemberFolderDriveError,
        find_or_create_folder,
        upload_bytes_to_folder,
    )
    from tools.share_folder import drive_file_url, drive_folder_url

    try:
        root_id, _created = find_or_create_folder(folder_id, COLLISION_UPLOAD_FOLDER)
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        safe = "".join(
            ch if ch.isalnum() or ch in {" ", "-", "_"} else "-"
            for ch in (collision.submitted_business_name or "submission")
        ).strip()[:60] or "submission"
        dest_id, _created = find_or_create_folder(
            root_id, f"{stamp}-{collision.id}-{safe}"
        )
        refs: list[dict[str, Any]] = []
        for name, data, content_type in files:
            mime = (content_type or "").split(";")[0].strip() or "application/octet-stream"
            uploaded = upload_bytes_to_folder(data, name, dest_id, mimetype=mime)
            file_id = uploaded.get("id") or ""
            refs.append(
                {
                    "id": file_id,
                    "name": name,
                    "url": uploaded.get("url") or drive_file_url(file_id),
                    "folder_id": dest_id,
                    "folder_url": drive_folder_url(dest_id),
                }
            )
        return refs
    except HTTPException:
        raise
    except MemberFolderDriveError:
        logger.exception(
            "ACES_PARTNER_COLLISION_DRIVE_FAIL partner_id=%s collision_id=%s",
            partner.id,
            collision.id,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "code": "drive_unavailable",
                "message": "We could not save your documents. Please try again shortly.",
            },
        ) from None
    except Exception:
        logger.exception(
            "ACES_PARTNER_COLLISION_DRIVE_FAIL partner_id=%s collision_id=%s",
            partner.id,
            collision.id,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "code": "drive_unavailable",
                "message": "We could not save your documents. Please try again shortly.",
            },
        ) from None


def log_partner_write(
    db: Session,
    principal: PartnerPrincipal,
    action: str,
    target_type: str,
    target_id: str | int | None = None,
    path: str | None = None,
    detail: dict[str, Any] | None = None,
) -> PartnerAuditEvent:
    event = PartnerAuditEvent(
        partner_id=principal.partner_id,
        email=principal.email,
        action=action,
        target_type=target_type,
        target_id=None if target_id is None else str(target_id),
        path=path,
        detail_json=json.dumps(detail) if detail is not None else None,
    )
    db.add(event)
    db.flush()
    return event
