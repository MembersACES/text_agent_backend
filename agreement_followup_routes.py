"""Agreement Follow Up HTTP routes."""

from __future__ import annotations

import logging

from fastapi import Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import get_db
from services.agreement_followup import (
    UTILITY_TYPES,
    AgreementFollowupError,
    create_agreement_type,
    list_agreement_types,
    loa_contact_for_business,
    purge_agreement_followup_test_stubs,
    render_first_touch,
    resolve_agreement_type,
    start_agreement_followup,
    update_agreement_type,
)
from models import Client

logger = logging.getLogger(__name__)


class AgreementTypeCreateBody(BaseModel):
    label: str
    utility_type: str
    retailer: str = ""
    default_subject: str = ""
    default_body: str = ""


class AgreementTypeUpdateBody(BaseModel):
    label: str | None = None
    utility_type: str | None = None
    retailer: str | None = None
    default_subject: str | None = None
    default_body: str | None = None
    is_active: bool | None = None


def register_agreement_followup_routes(app, get_current_user):
    @app.get("/api/autonomous/agreement-followup/types")
    def list_agreement_types_route(
        include_inactive: bool = False,
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user),
    ):
        return {
            "items": list_agreement_types(db, include_inactive=include_inactive),
            "utility_types": list(UTILITY_TYPES),
        }

    @app.post("/api/autonomous/agreement-followup/types")
    def create_agreement_type_route(
        body: AgreementTypeCreateBody,
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user),
    ):
        created_by = ((user_data or {}).get("idinfo") or {}).get("email")
        try:
            return create_agreement_type(
                db,
                label=body.label,
                utility_type=body.utility_type,
                retailer=body.retailer,
                default_subject=body.default_subject,
                default_body=body.default_body,
                created_by=created_by,
            )
        except AgreementFollowupError as exc:
            logger.warning(
                "agreement-followup type create rejected status=%s label=%r utility=%r: %s",
                exc.status_code,
                body.label,
                body.utility_type,
                exc,
            )
            raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    @app.patch("/api/autonomous/agreement-followup/types/{type_id}")
    def update_agreement_type_route(
        type_id: str,
        body: AgreementTypeUpdateBody,
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user),
    ):
        try:
            return update_agreement_type(
                db,
                type_id,
                label=body.label,
                utility_type=body.utility_type,
                retailer=body.retailer,
                default_subject=body.default_subject,
                default_body=body.default_body,
                is_active=body.is_active,
            )
        except AgreementFollowupError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    @app.get("/api/clients/{client_id}/contact-defaults")
    def client_contact_defaults(
        client_id: int,
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user),
    ):
        client = db.query(Client).filter(Client.id == client_id).first()
        if not client:
            raise HTTPException(status_code=404, detail="Member not found")
        loa = loa_contact_for_business(client.business_name)
        return {
            "client_id": client.id,
            "business_name": client.business_name,
            "contact_name": loa["contact_name"],
            "contact_email": loa["contact_email"] or (client.primary_contact_email or ""),
            "contact_phone": loa["contact_phone"],
        }

    @app.get("/api/autonomous/agreement-followup/preview")
    def preview_agreement_first_touch(
        agreement_type: str,
        business_name: str = "",
        contact_name: str = "",
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user),
    ):
        try:
            agreement = resolve_agreement_type(db, agreement_type)
        except AgreementFollowupError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
        subject, body_text, html, text = render_first_touch(
            agreement_label=agreement["label"],
            business_name=business_name,
            contact_name=contact_name,
            template_subject=str(agreement.get("default_subject") or ""),
            template_body=str(agreement.get("default_body") or ""),
        )
        return {
            "agreement_type": agreement["id"],
            "agreement_label": agreement["label"],
            "subject": subject,
            "body_text": body_text,
            "html": html,
            "text": text,
        }

    @app.post("/api/autonomous/agreement-followup/start")
    async def start_agreement_followup_route(
        client_id: int = Form(...),
        agreement_type: str = Form(...),
        contact_email: str = Form(...),
        file: UploadFile = File(...),
        offer_id: str | None = Form(None),
        contact_name: str = Form(""),
        contact_phone: str = Form(""),
        subject: str = Form(""),
        body_text: str = Form(""),
        test_mode: str = Form("false"),
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user),
    ):
        created_by = ((user_data or {}).get("idinfo") or {}).get("email")
        is_test = str(test_mode or "").strip().lower() in {"1", "true", "yes", "on"}
        parsed_offer_id: int | None = None
        if not is_test and offer_id is not None and str(offer_id).strip():
            try:
                parsed_offer_id = int(str(offer_id).strip())
            except ValueError:
                raise HTTPException(status_code=400, detail="offer_id must be a number")
        pdf_bytes = await file.read()
        logger.info(
            "agreement_followup start route client_id=%s offer_id=%s test_mode=%s "
            "filename=%s pdf_bytes=%s by=%s",
            client_id,
            parsed_offer_id,
            is_test,
            file.filename,
            len(pdf_bytes or b""),
            created_by,
        )
        try:
            return start_agreement_followup(
                db,
                client_id=client_id,
                agreement_type_raw=agreement_type,
                contact_email=contact_email,
                pdf_bytes=pdf_bytes,
                filename=file.filename or "",
                content_type=file.content_type or "",
                offer_id=parsed_offer_id,
                contact_name=contact_name or "",
                contact_phone=contact_phone or "",
                subject=subject or "",
                body_text=body_text or "",
                created_by=created_by,
                test_mode=is_test,
            )
        except AgreementFollowupError as exc:
            logger.exception(
                "agreement_followup start route failed client_id=%s status=%s detail=%s",
                client_id,
                exc.status_code,
                exc,
            )
            raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    @app.post("/api/autonomous/agreement-followup/test-stubs/purge")
    def purge_agreement_followup_test_stubs_route(
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user),
    ):
        return {"ok": True, **purge_agreement_followup_test_stubs(db)}
