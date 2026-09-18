"""Partner portal HTTP surface. Staff routes must not import these handlers."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Optional

import httpx
from fastapi import BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session

from crm_enums import ClientStage
from models import Client, Partner, PartnerLeadCollision
from partner_authz import (
    BASE1_RATE_ACTION,
    PartnerTool,
    admit_partner_lead,
    assert_base1_rate_limit,
    log_partner_write,
    partner_client_public,
    partner_principal_from_idinfo,
    persist_collision_files,
    require_partner_tool,
    validate_base1_files,
)
from schemas import PartnerClientResponse

logger = logging.getLogger(__name__)

N8N_BASE1_WEBHOOK_URL = os.getenv(
    "N8N_BASE1_WEBHOOK_URL",
    "https://membersaces.app.n8n.cloud/webhook/interface_form_base1_dev",
)


def _principal(idinfo: dict):
    principal = partner_principal_from_idinfo(idinfo)
    if principal is None:
        raise HTTPException(status_code=403, detail="Forbidden")
    return principal


def _owned_client(db: Session, principal, client_id: int) -> Client:
    row = (
        db.query(Client)
        .filter(Client.id == client_id, Client.partner_id == principal.partner_id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    return row


def _forward_base1_to_n8n(form_fields: dict[str, str], files: list[tuple[str, bytes, str]]) -> None:
    if os.getenv("PARTNER_BASE1_SKIP_N8N") == "1":
        return
    try:
        multipart = []
        for key, value in form_fields.items():
            multipart.append((key, (None, value)))
        for index, (name, data, content_type) in enumerate(files):
            multipart.append(
                (f"file_{index}", (name, data, content_type or "application/octet-stream"))
            )
        with httpx.Client(timeout=30.0) as client:
            client.post(N8N_BASE1_WEBHOOK_URL, files=multipart)
    except Exception:
        logger.exception("ACES_PARTNER_BASE1_N8N_FAIL")


def register_partner_routes(app, verify_partner_token, get_db):
    @app.get("/api/partner/me")
    def partner_me(idinfo: dict = Depends(verify_partner_token)):
        principal = _principal(idinfo)
        return {
            "email": principal.email,
            "partner_id": principal.partner_id,
            "tools": list(principal.tools),
        }

    @app.get("/api/partner/clients", response_model=list[PartnerClientResponse])
    def partner_list_clients(
        idinfo: dict = Depends(verify_partner_token),
        db: Session = Depends(get_db),
    ):
        principal = _principal(idinfo)
        rows = (
            db.query(Client)
            .filter(Client.partner_id == principal.partner_id)
            .order_by(Client.id.desc())
            .all()
        )
        return [partner_client_public(row) for row in rows]

    @app.get("/api/partner/clients/{client_id}", response_model=PartnerClientResponse)
    def partner_get_client(
        client_id: int,
        idinfo: dict = Depends(verify_partner_token),
        db: Session = Depends(get_db),
    ):
        principal = _principal(idinfo)
        row = _owned_client(db, principal, client_id)
        return partner_client_public(row)

    @app.post("/api/partner/base1")
    async def partner_base1(
        background_tasks: BackgroundTasks,
        idinfo: dict = Depends(verify_partner_token),
        db: Session = Depends(get_db),
        companyName: str = Form(...),
        fullName: str = Form(""),
        email: str = Form(""),
        phone: str = Form(""),
        state: str = Form(""),
        additionalInfo: str = Form(""),
        files: Optional[list[UploadFile]] = File(None),
    ):
        principal = _principal(idinfo)
        require_partner_tool(principal, PartnerTool.BASE1)
        assert_base1_rate_limit(db, principal.partner_id)
        log_partner_write(
            db,
            principal,
            action=BASE1_RATE_ACTION,
            target_type="partner",
            target_id=principal.partner_id,
            path="/api/partner/base1",
        )
        db.commit()

        uploads = files or []
        parsed: list[tuple[str, bytes, str | None]] = []
        for item in uploads:
            data = await item.read()
            parsed.append((item.filename or "upload.bin", data, item.content_type))
        validate_base1_files(parsed)

        business_name = (companyName or "").strip()
        if not business_name:
            raise HTTPException(status_code=400, detail="companyName is required")
        contact_email = (email or "").strip() or principal.email

        admit = admit_partner_lead(
            db,
            principal,
            business_name,
            payload={"fullName": fullName, "email": contact_email, "phone": phone, "state": state},
        )
        client_id = admit.client_id
        now = datetime.utcnow()
        if admit.outcome == "create":
            row = Client(
                business_name=business_name,
                primary_contact_email=contact_email,
                stage=ClientStage.LEAD.value,
                partner_id=principal.partner_id,
                created_at=now,
                updated_at=now,
            )
            db.add(row)
            db.flush()
            client_id = row.id
        if admit.outcome == "collision":
            collision = (
                db.query(PartnerLeadCollision)
                .filter(PartnerLeadCollision.id == admit.collision_id)
                .one()
            )
            partner_row = (
                db.query(Partner).filter(Partner.id == principal.partner_id).one()
            )
            refs = persist_collision_files(partner_row, collision, parsed)
            collision.files_json = json.dumps(refs)
            log_partner_write(
                db,
                principal,
                action="lead_collision",
                target_type="partner_lead_collision",
                target_id=collision.id,
                path="/api/partner/base1",
                detail={
                    "file_count": len(refs),
                    "files": refs,
                    "folder_id": refs[0]["folder_id"] if refs else None,
                },
            )
        else:
            log_partner_write(
                db,
                principal,
                action="base1_submit",
                target_type="client",
                target_id=client_id,
                path="/api/partner/base1",
            )
        db.commit()

        if admit.outcome != "collision":
            form_fields = {
                "fullName": fullName,
                "companyName": business_name,
                "email": contact_email,
                "phone": phone,
                "state": state,
                "additionalInfo": additionalInfo,
                "partner_id": str(principal.partner_id),
                "client_id": str(client_id or ""),
            }
            background_tasks.add_task(
                _forward_base1_to_n8n,
                form_fields,
                [(name, data, ctype or "application/octet-stream") for name, data, ctype in parsed],
            )

        public = dict(admit.public)
        if admit.outcome != "collision" and client_id is not None:
            public["client_id"] = client_id
        return public
