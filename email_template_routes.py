"""Dashboard APIs for operational email templates and recipient lists."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database import get_db
from services.operational_emails import (
    OperationalEmailError,
    assert_editor_allowed,
    create_recipient,
    deactivate_recipient,
    get_template,
    list_recipients,
    list_templates,
    recipient_to_dict,
    render_tokens,
    seed_operational_emails,
    template_to_dict,
    update_recipient,
    update_template,
)


class TemplatePatchBody(BaseModel):
    subject: Optional[str] = None
    html_body: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None


class TemplatePreviewBody(BaseModel):
    subject: Optional[str] = None
    html_body: Optional[str] = None
    values: Optional[dict[str, Any]] = None


class RecipientCreateBody(BaseModel):
    flow: str
    key: str
    display_name: str = ""
    emails: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    group_name: Optional[str] = None
    extra_groups: list[str] = Field(default_factory=list)
    is_placeholder: bool = False


class RecipientPatchBody(BaseModel):
    key: Optional[str] = None
    display_name: Optional[str] = None
    emails: Optional[list[str]] = None
    aliases: Optional[list[str]] = None
    group_name: Optional[str] = None
    extra_groups: Optional[list[str]] = None
    is_placeholder: Optional[bool] = None
    is_active: Optional[bool] = None


def _raise(exc: OperationalEmailError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


def _actor(user_info: dict) -> str:
    return str(user_info.get("email") or "").strip()


def register_email_template_routes(app, verify_google_token):
    @app.get("/api/email-templates")
    def list_all(
        category: Optional[str] = None,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        seed_operational_emails(db)
        rows = list_templates(db, category)
        return {"templates": [template_to_dict(row) for row in rows], "user_email": user_info.get("email")}

    @app.get("/api/email-templates/{key}")
    def get_one(key: str, user_info: dict = Depends(verify_google_token), db: Session = Depends(get_db)):
        try:
            row = get_template(db, key)
        except OperationalEmailError as exc:
            _raise(exc)
        return template_to_dict(row)

    @app.patch("/api/email-templates/{key}")
    def patch_one(
        key: str,
        body: TemplatePatchBody,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        try:
            actor = assert_editor_allowed(_actor(user_info))
            row = update_template(
                db,
                key,
                subject=body.subject,
                html_body=body.html_body,
                name=body.name,
                description=body.description,
                updated_by=actor,
            )
        except OperationalEmailError as exc:
            _raise(exc)
        return template_to_dict(row)

    @app.post("/api/email-templates/{key}/preview")
    def preview(
        key: str,
        body: TemplatePreviewBody,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        try:
            row = get_template(db, key)
        except OperationalEmailError as exc:
            _raise(exc)
        payload = template_to_dict(row)
        values = dict(payload.get("sample_values") or {})
        if body.values:
            values.update({str(k): "" if v is None else str(v) for k, v in body.values.items()})
        subject = body.subject if body.subject is not None else row.subject
        html_body = body.html_body if body.html_body is not None else row.html_body
        return {
            "subject": render_tokens(subject, values),
            "html_body": render_tokens(html_body, values),
        }

    @app.get("/api/email-recipients")
    def list_all_recipients(
        flow: Optional[str] = None,
        include_inactive: bool = False,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        seed_operational_emails(db)
        rows = list_recipients(db, flow, include_inactive=include_inactive)
        return {"recipients": [recipient_to_dict(row) for row in rows], "user_email": user_info.get("email")}

    @app.post("/api/email-recipients")
    def create_one(
        body: RecipientCreateBody,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        try:
            actor = assert_editor_allowed(_actor(user_info))
            row = create_recipient(
                db,
                flow=body.flow,
                key=body.key,
                display_name=body.display_name,
                emails=body.emails,
                group_name=body.group_name,
                aliases=body.aliases,
                extra_groups=body.extra_groups,
                is_placeholder=body.is_placeholder,
                updated_by=actor,
            )
        except OperationalEmailError as exc:
            _raise(exc)
        return recipient_to_dict(row)

    @app.patch("/api/email-recipients/{recipient_id}")
    def patch_recipient(
        recipient_id: int,
        body: RecipientPatchBody,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        try:
            actor = assert_editor_allowed(_actor(user_info))
            row = update_recipient(
                db,
                recipient_id,
                display_name=body.display_name,
                emails=body.emails,
                aliases=body.aliases,
                group_name=body.group_name,
                extra_groups=body.extra_groups,
                is_placeholder=body.is_placeholder,
                is_active=body.is_active,
                key=body.key,
                updated_by=actor,
            )
        except OperationalEmailError as exc:
            _raise(exc)
        return recipient_to_dict(row)

    @app.delete("/api/email-recipients/{recipient_id}")
    def delete_one(
        recipient_id: int,
        user_info: dict = Depends(verify_google_token),
        db: Session = Depends(get_db),
    ):
        try:
            actor = assert_editor_allowed(_actor(user_info))
            row = deactivate_recipient(db, recipient_id, updated_by=actor)
        except OperationalEmailError as exc:
            _raise(exc)
        return recipient_to_dict(row)
