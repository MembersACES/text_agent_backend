"""Staff distributor-partner admin. Partner tokens are refused by verify_google_token."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from auth_domain import email_domain_allowed
from database import get_db
from models import Partner, PartnerUser
from partner_authz import PartnerTool, ensure_partner_drive_folder, parse_enabled_tools
from tools.distributor_folders import confirm_distributor_entity
from tools.share_folder import drive_folder_url

logger = logging.getLogger(__name__)

_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_TOOL_LABELS = {
    PartnerTool.BASE1: "Base 1",
    PartnerTool.BASE2: "Base 2",
}


class PartnerCreateBody(BaseModel):
    name: str
    slug: str
    enabled_tools: list[str] = Field(default_factory=list)
    drive_folder_id: Optional[str] = None


class PartnerPatchBody(BaseModel):
    enabled_tools: Optional[list[str]] = None
    drive_folder_id: Optional[str] = None
    provision_drive_folder: bool = False


class PartnerInviteBody(BaseModel):
    email: str


def _name(value: str) -> str:
    name = (value or "").strip()
    if not name or len(name) > 255:
        raise HTTPException(status_code=400, detail="Enter a distributor name.")
    return name


def _slug(value: str) -> str:
    slug = (value or "").strip().lower()
    if not slug or len(slug) > 128 or not _SLUG.match(slug):
        raise HTTPException(
            status_code=400,
            detail="Slug must be lowercase letters, numbers, and hyphens.",
        )
    return slug


def _tools(values: list[str]) -> list[str]:
    known = {item.value for item in PartnerTool}
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in values:
        tool = (item or "").strip().lower()
        if tool not in known:
            raise HTTPException(status_code=400, detail=f"Unknown tool: {tool or '(blank)'}")
        if tool not in seen:
            seen.add(tool)
            cleaned.append(tool)
    return cleaned


def _email(value: str) -> str:
    email = (value or "").strip().lower()
    if "@" not in email or email.startswith("@") or email.endswith("@"):
        raise HTTPException(status_code=400, detail="Enter a valid email address.")
    local, domain = email.rsplit("@", 1)
    if not local or "." not in domain or " " in email:
        raise HTTPException(status_code=400, detail="Enter a valid email address.")
    if email_domain_allowed(email):
        raise HTTPException(
            status_code=400,
            detail="Invite the distributor's Google account. Staff addresses already sign in to the ACES app.",
        )
    return email


def _partner_or_404(db: Session, partner_id: int) -> Partner:
    row = db.query(Partner).filter(Partner.id == partner_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Distributor not found.")
    return row


def _store_tools(tools: list[str]) -> str:
    return json.dumps(tools)


def _folder_taken(db: Session, folder_id: str, partner_id: int | None) -> Partner | None:
    query = db.query(Partner).filter(Partner.drive_folder_id == folder_id)
    if partner_id is not None:
        query = query.filter(Partner.id != partner_id)
    return query.first()


def _require_entity_folder(folder_id: str) -> str:
    _payload, err, status = confirm_distributor_entity(folder_id)
    if err == "not_distributor_entity":
        raise HTTPException(
            status_code=400,
            detail="Choose the distributor folder, not a folder inside it.",
        )
    if err in {"distributor_not_found", "missing_folder_id"}:
        raise HTTPException(
            status_code=404,
            detail="That folder is not a distributor under 003-Distributors.",
        )
    if err:
        raise HTTPException(status_code=status if status >= 400 else 503, detail=err)
    return (folder_id or "").strip()


def _attach_folder(db: Session, partner: Partner, folder_id: str) -> None:
    taken = _folder_taken(db, folder_id, partner.id)
    if taken is not None:
        raise HTTPException(
            status_code=409,
            detail=f"That Drive folder is already linked to {taken.name}.",
        )
    partner.drive_folder_id = folder_id
    ensure_partner_drive_folder(partner)


def _provision_folder(partner: Partner) -> None:
    partner.drive_folder_id = None
    ensure_partner_drive_folder(partner)


def _user_payload(row: PartnerUser) -> dict:
    return {
        "id": row.id,
        "email": row.email,
        "active": bool(row.active),
    }


def _partner_payload(partner: Partner, users: list[PartnerUser]) -> dict:
    folder_id = (partner.drive_folder_id or "").strip() or None
    deactivated = partner.deactivated_at
    return {
        "id": partner.id,
        "name": partner.name,
        "slug": partner.slug,
        "drive_folder_id": folder_id,
        "drive_folder_url": drive_folder_url(folder_id) if folder_id else None,
        "enabled_tools": list(parse_enabled_tools(partner.enabled_tools)),
        "active": bool(partner.active),
        "deactivated_at": deactivated.isoformat() if deactivated else None,
        "users": [_user_payload(row) for row in users],
    }


def _users_for(db: Session, partner_id: int) -> list[PartnerUser]:
    return (
        db.query(PartnerUser)
        .filter(PartnerUser.partner_id == partner_id)
        .order_by(func.lower(PartnerUser.email))
        .all()
    )


def _tool_catalogue() -> list[dict[str, str]]:
    return [{"id": tool.value, "label": label} for tool, label in _TOOL_LABELS.items()]


def register_partner_admin_routes(app, verify_google_token):
    @app.get("/api/distributors/partners")
    def list_partners(
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        _ = user_info
        partners = (
            db.query(Partner)
            .order_by(Partner.active.desc(), func.lower(Partner.name))
            .all()
        )
        users = db.query(PartnerUser).order_by(func.lower(PartnerUser.email)).all()
        grouped: dict[int, list[PartnerUser]] = {}
        for row in users:
            grouped.setdefault(row.partner_id, []).append(row)
        return {
            "tools": _tool_catalogue(),
            "partners": [_partner_payload(row, grouped.get(row.id, [])) for row in partners],
        }

    @app.post("/api/distributors/partners", status_code=201)
    def create_partner(
        body: PartnerCreateBody,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        name = _name(body.name)
        slug = _slug(body.slug)
        tools = _tools(body.enabled_tools)
        folder_id = (body.drive_folder_id or "").strip()
        if folder_id:
            folder_id = _require_entity_folder(folder_id)
            taken = _folder_taken(db, folder_id, None)
            if taken is not None:
                raise HTTPException(
                    status_code=409,
                    detail=f"That Drive folder is already linked to {taken.name}.",
                )
        if (
            db.query(Partner).filter(func.lower(Partner.slug) == slug).first()
            is not None
        ):
            raise HTTPException(status_code=409, detail="That slug is already in use.")

        now = datetime.utcnow()
        partner = Partner(
            name=name,
            slug=slug,
            enabled_tools=_store_tools(tools),
            drive_folder_id=folder_id or None,
            active=1,
            created_at=now,
            updated_at=now,
        )
        db.add(partner)
        try:
            db.flush()
            if folder_id:
                _attach_folder(db, partner, folder_id)
            else:
                _provision_folder(partner)
            if not (partner.drive_folder_id or "").strip():
                raise HTTPException(
                    status_code=503,
                    detail="Drive folder was not created.",
                )
            db.commit()
            db.refresh(partner)
        except HTTPException:
            db.rollback()
            raise
        except IntegrityError:
            db.rollback()
            raise HTTPException(status_code=409, detail="That slug is already in use.") from None
        logger.info(
            "distributors/partners create id=%s slug=%s user=%s linked=%s",
            partner.id,
            partner.slug,
            user_info.get("email"),
            bool(folder_id),
        )
        return _partner_payload(partner, [])

    @app.patch("/api/distributors/partners/{partner_id}")
    def update_partner(
        partner_id: int,
        body: PartnerPatchBody,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        _ = user_info
        fields = body.model_fields_set
        if not fields & {"enabled_tools", "drive_folder_id", "provision_drive_folder"}:
            raise HTTPException(status_code=400, detail="Nothing to update.")
        partner = _partner_or_404(db, partner_id)
        try:
            if "enabled_tools" in fields:
                partner.enabled_tools = _store_tools(_tools(body.enabled_tools or []))
            folder_id = (body.drive_folder_id or "").strip()
            if "drive_folder_id" in fields:
                if not folder_id:
                    raise HTTPException(status_code=400, detail="Choose a Drive folder to link.")
                confirmed = _require_entity_folder(folder_id)
                if (partner.drive_folder_id or "").strip() != confirmed:
                    _attach_folder(db, partner, confirmed)
                else:
                    ensure_partner_drive_folder(partner)
            elif body.provision_drive_folder and not (partner.drive_folder_id or "").strip():
                _provision_folder(partner)
            partner.updated_at = datetime.utcnow()
            db.commit()
            db.refresh(partner)
        except HTTPException:
            db.rollback()
            raise
        return _partner_payload(partner, _users_for(db, partner.id))

    @app.post("/api/distributors/partners/{partner_id}/deactivate")
    def deactivate_partner(
        partner_id: int,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        _ = user_info
        partner = _partner_or_404(db, partner_id)
        if partner.active:
            partner.active = 0
            partner.deactivated_at = datetime.utcnow()
            partner.updated_at = partner.deactivated_at
            db.commit()
            db.refresh(partner)
        return _partner_payload(partner, _users_for(db, partner.id))

    @app.post("/api/distributors/partners/{partner_id}/users", status_code=201)
    def invite_user(
        partner_id: int,
        body: PartnerInviteBody,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        _ = user_info
        partner = _partner_or_404(db, partner_id)
        if not partner.active:
            raise HTTPException(
                status_code=400,
                detail="This distributor is deactivated. Logins cannot be added.",
            )
        email = _email(body.email)
        existing = (
            db.query(PartnerUser)
            .filter(func.lower(PartnerUser.email) == email)
            .first()
        )
        if existing is not None:
            if existing.partner_id == partner.id:
                raise HTTPException(
                    status_code=409,
                    detail="This email is already a login for this distributor.",
                )
            raise HTTPException(
                status_code=409,
                detail="This email is already a login for another distributor.",
            )
        now = datetime.utcnow()
        db.add(
            PartnerUser(
                partner_id=partner.id,
                email=email,
                active=1,
                created_at=now,
                updated_at=now,
            )
        )
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            raise HTTPException(
                status_code=409,
                detail="This email is already a login for a distributor.",
            ) from None
        db.refresh(partner)
        return _partner_payload(partner, _users_for(db, partner.id))

    @app.post("/api/distributors/partners/{partner_id}/users/{user_id}/deactivate")
    def deactivate_user(
        partner_id: int,
        user_id: int,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        _ = user_info
        partner = _partner_or_404(db, partner_id)
        row = (
            db.query(PartnerUser)
            .filter(PartnerUser.id == user_id, PartnerUser.partner_id == partner.id)
            .first()
        )
        if row is None:
            raise HTTPException(status_code=404, detail="Login not found.")
        if row.active:
            row.active = 0
            row.updated_at = datetime.utcnow()
            db.commit()
        return _partner_payload(partner, _users_for(db, partner.id))
