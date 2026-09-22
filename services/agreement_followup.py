"""Agreement Follow Up: send a retailer agreement PDF now, then chase signature."""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from html import escape
from typing import Any

import httpx
from sqlalchemy import or_

from crm_enums import OfferActivityType, OfferStatus
from models import AgreementFollowupType, Client, Offer
from services.autonomous_sequence import (
    AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
    ACES_TEAM_FOLLOWUP_SIGNATURE_HTML,
    start_gas_base2_sequence,
)
from services.campaigns import extract_email_id_from_webhook_response
from services.crm import create_offer_activity
from services.merge_template import html_to_plain_text, looks_like_email, sanitize_html

logger = logging.getLogger(__name__)

N8N_AGREEMENT_FOLLOWUP_URL = os.getenv("N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL", "").strip()
MAX_PDF_BYTES = 15 * 1024 * 1024

UTILITY_TYPES: tuple[str, ...] = (
    "C&I Gas",
    "C&I Electricity",
    "SME Gas",
    "SME Electricity",
    "Waste",
    "Oil",
    "DMA",
    "Other",
)

SEED_AGREEMENT_TYPES: tuple[dict[str, str], ...] = (
    {
        "id": "alinta_ci_gas",
        "label": "Alinta C&I Gas",
        "utility_type": "C&I Gas",
        "retailer": "Alinta",
    },
    {
        "id": "alinta_ci_electricity",
        "label": "Alinta C&I Electricity",
        "utility_type": "C&I Electricity",
        "retailer": "Alinta",
    },
    {
        "id": "alinta_sme_gas",
        "label": "Alinta SME Gas",
        "utility_type": "SME Gas",
        "retailer": "Alinta",
    },
    {
        "id": "alinta_sme_electricity",
        "label": "Alinta SME Electricity",
        "utility_type": "SME Electricity",
        "retailer": "Alinta",
    },
)

AGREEMENT_TYPES = SEED_AGREEMENT_TYPES


class AgreementFollowupError(Exception):
    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def slugify_agreement_type(label: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", (label or "").strip().lower()).strip("_")
    if not text:
        raise AgreementFollowupError("Type name must include at least one letter or number")
    if not text[0].isalpha():
        text = f"type_{text}"
    return text[:80]


def _type_to_dict(row: AgreementFollowupType) -> dict[str, Any]:
    return {
        "id": row.id,
        "label": row.label,
        "utility_type": row.utility_type,
        "retailer": (row.retailer or "").strip(),
        "default_subject": row.default_subject or "",
        "default_body": row.default_body or "",
        "is_active": bool(row.is_active),
        "sort_order": int(row.sort_order or 0),
    }


def ensure_agreement_followup_types(db: Session) -> None:
    existing = {row.id for row in db.query(AgreementFollowupType).all()}
    added = False
    for index, seed in enumerate(SEED_AGREEMENT_TYPES):
        if seed["id"] in existing:
            continue
        db.add(
            AgreementFollowupType(
                id=seed["id"],
                label=seed["label"],
                utility_type=seed["utility_type"],
                retailer=seed.get("retailer") or "",
                is_active=1,
                sort_order=index,
            )
        )
        added = True
    if added:
        db.commit()


def list_agreement_types(db: Session, include_inactive: bool = False) -> list[dict[str, Any]]:
    ensure_agreement_followup_types(db)
    q = db.query(AgreementFollowupType)
    if not include_inactive:
        q = q.filter(AgreementFollowupType.is_active == 1)
    rows = q.order_by(AgreementFollowupType.sort_order, AgreementFollowupType.label).all()
    return [_type_to_dict(row) for row in rows]


def resolve_agreement_type(db: Session, raw: str) -> dict[str, Any]:
    ensure_agreement_followup_types(db)
    key = (raw or "").strip()
    if not key:
        raise AgreementFollowupError("agreement_type is required")
    row = (
        db.query(AgreementFollowupType)
        .filter(AgreementFollowupType.id == key)
        .first()
    )
    if row is None:
        row = (
            db.query(AgreementFollowupType)
            .filter(AgreementFollowupType.label.ilike(key))
            .first()
        )
    if row is None:
        allowed = ", ".join(item["label"] for item in list_agreement_types(db))
        raise AgreementFollowupError(f"Unknown agreement type. Use one of: {allowed}")
    return _type_to_dict(row)


def create_agreement_type(
    db: Session,
    *,
    label: str,
    utility_type: str,
    retailer: str = "",
    default_subject: str = "",
    default_body: str = "",
    created_by: str | None = None,
) -> dict[str, Any]:
    ensure_agreement_followup_types(db)
    name = (label or "").strip()
    if not name:
        raise AgreementFollowupError("Type name is required")
    utility = (utility_type or "").strip()
    if utility not in UTILITY_TYPES:
        raise AgreementFollowupError(f"utility_type must be one of: {', '.join(UTILITY_TYPES)}")
    slug = slugify_agreement_type(name)
    clash = (
        db.query(AgreementFollowupType)
        .filter(
            or_(
                AgreementFollowupType.id == slug,
                AgreementFollowupType.label.ilike(name),
            )
        )
        .first()
    )
    if clash:
        raise AgreementFollowupError(f"A type named {clash.label} already exists")
    max_order = db.query(AgreementFollowupType.sort_order).order_by(
        AgreementFollowupType.sort_order.desc()
    ).first()
    next_order = int((max_order[0] if max_order else 0) or 0) + 1
    row = AgreementFollowupType(
        id=slug,
        label=name,
        utility_type=utility,
        retailer=(retailer or "").strip(),
        default_subject=(default_subject or "").strip() or None,
        default_body=(default_body or "").strip() or None,
        is_active=1,
        sort_order=next_order,
        created_by=created_by,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _type_to_dict(row)


def update_agreement_type(
    db: Session,
    type_id: str,
    *,
    label: str | None = None,
    utility_type: str | None = None,
    retailer: str | None = None,
    default_subject: str | None = None,
    default_body: str | None = None,
    is_active: bool | None = None,
) -> dict[str, Any]:
    ensure_agreement_followup_types(db)
    row = db.query(AgreementFollowupType).filter(AgreementFollowupType.id == type_id).first()
    if not row:
        raise AgreementFollowupError("Agreement type not found", 404)
    if label is not None:
        name = label.strip()
        if not name:
            raise AgreementFollowupError("Type name is required")
        clash = (
            db.query(AgreementFollowupType)
            .filter(
                AgreementFollowupType.label.ilike(name),
                AgreementFollowupType.id != type_id,
            )
            .first()
        )
        if clash:
            raise AgreementFollowupError(f"A type named {clash.label} already exists")
        row.label = name
    if utility_type is not None:
        utility = utility_type.strip()
        if utility not in UTILITY_TYPES:
            raise AgreementFollowupError(f"utility_type must be one of: {', '.join(UTILITY_TYPES)}")
        row.utility_type = utility
    if retailer is not None:
        row.retailer = retailer.strip()
    if default_subject is not None:
        row.default_subject = default_subject.strip() or None
    if default_body is not None:
        row.default_body = default_body.strip() or None
    if is_active is not None:
        row.is_active = 1 if is_active else 0
    db.commit()
    db.refresh(row)
    return _type_to_dict(row)


def greeting_first_name(contact_name: str) -> str:
    raw = (contact_name or "").strip()
    if not raw:
        return "there"
    return raw.split()[0] or "there"


def fill_copy_template(
    template: str,
    *,
    first_name: str,
    business_name: str,
    agreement_label: str,
) -> str:
    return (
        (template or "")
        .replace("{{first_name}}", first_name)
        .replace("{{contact_name}}", first_name)
        .replace("{{business_name}}", business_name)
        .replace("{{agreement_label}}", agreement_label)
        .replace("{{label}}", agreement_label)
    )
    company = (business_name or "").strip() or "your business"
    return f"{agreement_label} ready for signing — {company}"


def first_touch_subject(*, agreement_label: str, business_name: str) -> str:
    company = (business_name or "").strip() or "your business"
    return f"{agreement_label} ready for signing — {company}"


def first_touch_body_text(
    *,
    agreement_label: str,
    business_name: str,
    contact_name: str,
) -> str:
    greeting = greeting_first_name(contact_name)
    company = (business_name or "").strip() or "your business"
    label = (agreement_label or "").strip() or "agreement"
    return (
        f"Hi {greeting},\n\n"
        f"Please find attached the {label} for {company}.\n\n"
        "Could you review and return the signed agreement at your earliest convenience? "
        "Reply to this email with the signed PDF, or let us know if you have any questions.\n\n"
        "Kind regards,"
    )


def wrap_first_touch_html(body_text: str, signature_html: str | None = None) -> str:
    raw = (body_text or "").strip()
    if not raw:
        raise AgreementFollowupError("Email body cannot be empty")
    if re.search(r"<[a-zA-Z][^>]*>", raw):
        body_html = sanitize_html(raw).strip()
        if not body_html:
            raise AgreementFollowupError("Email body cannot be empty")
    else:
        chunks = [p.strip() for p in re.split(r"\n\s*\n", raw) if p.strip()]
        parts: list[str] = []
        for chunk in chunks:
            inner = "<br>".join(escape(line) for line in chunk.split("\n"))
            parts.append(f"<p>{inner}</p>")
        body_html = "\n".join(parts)
    signature = (signature_html or "").strip() or ACES_TEAM_FOLLOWUP_SIGNATURE_HTML
    return f"{body_html}\n{signature}"


def render_first_touch(
    *,
    agreement_label: str,
    business_name: str,
    contact_name: str,
    subject: str | None = None,
    body_text: str | None = None,
    signature_html: str | None = None,
    template_subject: str | None = None,
    template_body: str | None = None,
) -> tuple[str, str, str, str]:
    sig = (signature_html or "").strip() or ACES_TEAM_FOLLOWUP_SIGNATURE_HTML
    first_name = greeting_first_name(contact_name)
    company = (business_name or "").strip() or "your business"
    filled_subject = fill_copy_template(
        template_subject or "",
        first_name=first_name,
        business_name=company,
        agreement_label=agreement_label,
    )
    filled_body = fill_copy_template(
        template_body or "",
        first_name=first_name,
        business_name=company,
        agreement_label=agreement_label,
    )
    resolved_subject = (subject or "").strip() or filled_subject or first_touch_subject(
        agreement_label=agreement_label,
        business_name=business_name,
    )
    resolved_body = (body_text or "").strip() or filled_body or first_touch_body_text(
        agreement_label=agreement_label,
        business_name=business_name,
        contact_name=contact_name,
    )
    html = wrap_first_touch_html(resolved_body, sig)
    return resolved_subject, resolved_body, html, html_to_plain_text(html)


def _str_nested(data: dict[str, Any], *keys: str) -> str:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(key)
    return str(cur or "").strip()


def loa_contact_for_business(business_name: str) -> dict[str, str]:
    """Contact name / phone / email live on the LOA record, not the CRM client row."""
    empty = {"contact_name": "", "contact_email": "", "contact_phone": ""}
    name = (business_name or "").strip()
    if not name:
        return empty
    try:
        from services.airtable_client import (
            build_business_info_from_loa,
            get_loa_record_by_business_name,
        )

        rec = get_loa_record_by_business_name(name)
        if not rec:
            return empty
        info = build_business_info_from_loa(rec)
    except Exception:
        logger.exception("LOA contact lookup failed for %s", name)
        return empty
    if not isinstance(info, dict):
        return empty
    return {
        "contact_name": _str_nested(info, "representative_details", "contact_name"),
        "contact_email": _str_nested(info, "contact_information", "email"),
        "contact_phone": _str_nested(info, "contact_information", "telephone"),
    }


def extract_thread_id_from_webhook_response(webhook_response: Any) -> str | None:
    def _read(obj: dict[str, Any]) -> str | None:
        for key in ("gmail_thread_id", "thread_id", "threadId", "gmail_threadId"):
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in obj.values():
            if isinstance(value, dict):
                found = _read(value)
                if found:
                    return found
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        found = _read(item)
                        if found:
                            return found
        return None

    if isinstance(webhook_response, list):
        for item in webhook_response:
            if isinstance(item, dict):
                found = _read(item)
                if found:
                    return found
        return None
    if isinstance(webhook_response, dict):
        return _read(webhook_response)
    return None


def _filename_is_pdf(filename: str, content_type: str, pdf_bytes: bytes) -> bool:
    name = (filename or "").strip().lower()
    ctype = (content_type or "").strip().lower()
    if name.endswith(".pdf") or "pdf" in ctype:
        return True
    return pdf_bytes[:4] == b"%PDF"


def send_agreement_first_touch_email(
    *,
    to: str,
    subject: str,
    html: str,
    text: str,
    pdf_bytes: bytes,
    filename: str,
    agreement_type: str,
    agreement_label: str,
    business_name: str,
    offer_id: int | None,
    client_id: int | None,
) -> dict[str, Any]:
    if not N8N_AGREEMENT_FOLLOWUP_URL:
        logger.info(
            "[agreement-followup] webhook not set; placeholder to=%s file=%s",
            to,
            filename,
        )
        return {
            "ok": True,
            "mode": "placeholder",
            "payload": {
                "to": to,
                "subject": subject,
                "filename": filename,
                "agreement_type": agreement_type,
            },
        }

    form = {
        "channel": "email",
        "to": to,
        "subject": subject,
        "body_html": html,
        "body_text": text,
        "sequence_type": AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
        "agreement_type": agreement_type,
        "agreement_label": agreement_label,
        "business_name": business_name,
        "offer_id": "" if offer_id is None else str(offer_id),
        "client_id": "" if client_id is None else str(client_id),
        "reply_in_thread": "false",
        "use_html_signature": "false",
        "first_touch": "true",
        "attach_pdf": "true",
    }
    files = {
        "file": (
            filename or "agreement.pdf",
            pdf_bytes,
            "application/pdf",
        )
    }
    with httpx.Client(timeout=90.0) as client:
        response = client.post(N8N_AGREEMENT_FOLLOWUP_URL, data=form, files=files)
        response.raise_for_status()
        try:
            return {"ok": True, "response": response.json()}
        except Exception:
            return {"ok": True, "response_text": response.text[:2000]}


def _complete_first_email_step(db: Session, run_id: int) -> None:
    from models import AutonomousSequenceStep

    step = (
        db.query(AutonomousSequenceStep)
        .filter(
            AutonomousSequenceStep.run_id == run_id,
            AutonomousSequenceStep.step_index == 0,
        )
        .first()
    )
    if not step or (step.channel or "").strip().lower() != "email":
        return
    if step.step_status in {"ready", "to_start"}:
        step.step_status = "completed"
        step.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        step.last_outcome_summary = (
            "agreement follow-up first-touch sent with PDF attachment (not LLM-drafted)"
        )
        db.commit()


def _safe_pdf_filename(filename: str, agreement_label: str, business_name: str) -> str:
    raw = (filename or "").strip()
    if not raw.lower().endswith(".pdf"):
        slug = re.sub(r"[^A-Za-z0-9]+", "-", f"{agreement_label}-{business_name}").strip("-")
        raw = f"{slug or 'agreement'}.pdf"
    return os.path.basename(raw)[:180]


def start_agreement_followup(
    db: Session,
    *,
    client_id: int,
    agreement_type_raw: str,
    contact_email: str,
    pdf_bytes: bytes,
    filename: str,
    content_type: str = "",
    offer_id: int | None = None,
    contact_name: str = "",
    contact_phone: str = "",
    subject: str = "",
    body_text: str = "",
    created_by: str | None = None,
) -> dict[str, Any]:
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise AgreementFollowupError("Member not found", 404)

    agreement = resolve_agreement_type(db, agreement_type_raw)
    loa = loa_contact_for_business(client.business_name)
    display_name = (contact_name or "").strip() or loa["contact_name"]
    phone = (contact_phone or "").strip() or loa["contact_phone"]
    to = (contact_email or client.primary_contact_email or loa["contact_email"] or "").strip()
    if not looks_like_email(to):
        raise AgreementFollowupError("A valid member email is required")
    if not pdf_bytes:
        raise AgreementFollowupError("Please upload the agreement PDF")
    if len(pdf_bytes) > MAX_PDF_BYTES:
        raise AgreementFollowupError("PDF must be 15 MB or smaller")
    if not _filename_is_pdf(filename, content_type, pdf_bytes):
        raise AgreementFollowupError("Upload must be a PDF")

    offer: Offer | None = None
    created_offer = False
    if offer_id is not None:
        offer = db.query(Offer).filter(Offer.id == offer_id, Offer.client_id == client_id).first()
        if not offer:
            raise AgreementFollowupError("Offer not found for this member", 404)
    else:
        offer = Offer(
            client_id=client.id,
            business_name=client.business_name,
            utility_type=agreement["utility_type"],
            utility_type_identifier=agreement["label"],
            status=OfferStatus.AUTONOMOUS_AGENT_TRIGGER.value,
            pipeline_stage="contract_received",
            created_by=created_by,
        )
        db.add(offer)
        db.flush()
        created_offer = True

    from models import AutonomousSequenceRun

    existing = (
        db.query(AutonomousSequenceRun)
        .filter(
            AutonomousSequenceRun.offer_id == offer.id,
            AutonomousSequenceRun.sequence_type == AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
            AutonomousSequenceRun.run_status == "running",
        )
        .first()
    )
    if existing:
        raise AgreementFollowupError(
            f"Offer #{offer.id} already has a running agreement follow-up (run #{existing.id}). "
            "Stop that run first, or pick another offer.",
            409,
        )

    if offer.status not in {OfferStatus.ACCEPTED.value, OfferStatus.LOST.value}:
        offer.status = OfferStatus.AUTONOMOUS_AGENT_TRIGGER.value

    business_name = (offer.business_name or client.business_name or "").strip()
    first_name = greeting_first_name(display_name)
    subject, _body, html, text = render_first_touch(
        agreement_label=agreement["label"],
        business_name=business_name,
        contact_name=display_name,
        subject=subject,
        body_text=body_text,
        template_subject=str(agreement.get("default_subject") or ""),
        template_body=str(agreement.get("default_body") or ""),
    )
    upload_name = _safe_pdf_filename(filename, agreement["label"], business_name)

    try:
        n8n_result = send_agreement_first_touch_email(
            to=to,
            subject=subject,
            html=html,
            text=text,
            pdf_bytes=pdf_bytes,
            filename=upload_name,
            agreement_type=agreement["id"],
            agreement_label=agreement["label"],
            business_name=business_name,
            offer_id=offer.id,
            client_id=client.id,
        )
    except httpx.HTTPError as exc:
        logger.exception("agreement follow-up first-touch n8n failed client_id=%s", client.id)
        raise AgreementFollowupError(
            "Could not send the agreement email via n8n. Check N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL.",
            502,
        ) from exc

    webhook_body = n8n_result.get("response") if isinstance(n8n_result, dict) else n8n_result
    email_id = extract_email_id_from_webhook_response(webhook_body)
    thread_id = extract_thread_id_from_webhook_response(webhook_body)
    if not email_id:
        logger.warning(
            "[agreement-followup] n8n returned no email_id client_id=%s offer_id=%s to=%s",
            client.id,
            offer.id,
            to,
        )

    create_offer_activity(
        db,
        offer=offer,
        client=client,
        activity_type=OfferActivityType.CONTRACT_RECEIVED,
        metadata={
            "agreement_type": agreement["id"],
            "agreement_label": agreement["label"],
            "filename": upload_name,
        },
        created_by=created_by,
    )
    activity = create_offer_activity(
        db,
        offer=offer,
        client=client,
        activity_type=OfferActivityType.CONTRACT_SENT_FOR_SIGNING,
        metadata={
            "agreement_type": agreement["id"],
            "agreement_label": agreement["label"],
            "filename": upload_name,
            "to": to,
        },
        created_by=created_by,
    )

    context: dict[str, Any] = {
        "business_name": business_name,
        "contact_name": display_name or to.split("@")[0],
        "first_name": first_name,
        "contact_email": to,
        "contact_phone": phone,
        "agreement_type": agreement["id"],
        "agreement_label": agreement["label"],
        "utility_type": agreement["utility_type"],
        "filename": upload_name,
        "agreement_first_touch_sent": True,
        "campaign_first_touch_sent": True,
        "reply_in_thread": True,
        "omit_validity": True,
        "omit_document_links": True,
        "initial_email_subject": subject,
        "signature_html": ACES_TEAM_FOLLOWUP_SIGNATURE_HTML,
    }
    if email_id:
        context["email_ID"] = email_id
        context["email_id"] = email_id
        context["gmail_message_id"] = email_id
    if thread_id:
        context["gmail_thread_id"] = thread_id
        context["thread_id"] = thread_id

    run = start_gas_base2_sequence(
        db,
        sequence_type=AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
        offer_id=offer.id,
        client_id=client.id,
        crm_activity_id=activity.id if activity else None,
        anchor_at=datetime.now(timezone.utc),
        tz=None,
        context=context,
    )
    _complete_first_email_step(db, run.id)
    db.refresh(run)

    return {
        "ok": True,
        "run_id": run.id,
        "offer_id": offer.id,
        "client_id": client.id,
        "created_offer": created_offer,
        "sequence_type": AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
        "agreement_type": agreement["id"],
        "agreement_label": agreement["label"],
        "to": to,
        "subject": subject,
        "email_id": email_id,
        "thread_id": thread_id,
        "n8n_mode": n8n_result.get("mode") if isinstance(n8n_result, dict) else None,
        "filename": upload_name,
    }
