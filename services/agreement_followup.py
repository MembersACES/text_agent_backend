"""Agreement Follow Up: send a retailer agreement PDF now, then chase signature."""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from html import escape
from typing import Any, NoReturn
from urllib.parse import urlparse

import httpx
from sqlalchemy import or_
from sqlalchemy.orm import Session

from crm_enums import OfferActivityType, OfferStatus
from models import (
    AgreementFollowupType,
    AutonomousSequenceEvent,
    AutonomousSequenceRun,
    AutonomousSequenceStep,
    Campaign,
    Client,
    Offer,
    OfferActivity,
    StrategyItem,
)
from services.autonomous_sequence import (
    AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
    ACES_TEAM_FOLLOWUP_SIGNATURE_HTML,
    start_gas_base2_sequence,
)
from services.campaigns import extract_email_id_from_webhook_response
from services.crm import create_offer_activity
from services.merge_template import html_to_plain_text, looks_like_email, sanitize_html

logger = logging.getLogger(__name__)

N8N_AGREEMENT_FOLLOWUP_HARDCODED_URL = (
    "https://membersaces.app.n8n.cloud/webhook/aces-autonomous-agent/agreement-followup-email"
)
N8N_AGREEMENT_FOLLOWUP_ENV_KEYS: tuple[str, ...] = (
    "N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL",
    "N8N_AGREEMENT_FOLLOWUP_WEBHOOK_URL",
    "AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL",
)
N8N_AGREEMENT_FOLLOWUP_URL = os.getenv("N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL", "").strip()
MAX_PDF_BYTES = 15 * 1024 * 1024


def _safe_url_for_log(url: str) -> str:
    parsed = urlparse((url or "").strip())
    host = parsed.netloc or "?"
    path = parsed.path or "/"
    return f"{host}{path}"


def _process_identity() -> dict[str, str]:
    backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = os.path.join(backend_root, ".env")
    return {
        "k_service": (os.getenv("K_SERVICE") or "").strip() or "-",
        "k_revision": (os.getenv("K_REVISION") or "").strip() or "-",
        "k_configuration": (os.getenv("K_CONFIGURATION") or "").strip() or "-",
        "dotenv_present": "yes" if os.path.isfile(dotenv_path) else "no",
    }


def _webhook_env_snapshot() -> list[str]:
    rows: list[str] = []
    seen: set[str] = set()
    for key in N8N_AGREEMENT_FOLLOWUP_ENV_KEYS:
        seen.add(key)
        raw = os.getenv(key)
        if raw is None:
            rows.append(f"{key}=missing")
        elif not raw.strip():
            rows.append(f"{key}=empty")
        else:
            rows.append(f"{key}=set:{len(raw.strip())}:{_safe_url_for_log(raw)}")
    for key in sorted(os.environ):
        upper = key.upper()
        if key in seen:
            continue
        if "N8N" not in upper and "WEBHOOK" not in upper and "AGREEMENT_FOLLOWUP" not in upper:
            continue
        raw = os.environ.get(key) or ""
        rows.append(f"{key}={'set:' + str(len(raw.strip())) if raw.strip() else 'empty'}")
    return rows


def resolve_agreement_followup_webhook() -> tuple[str, str]:
    for key in N8N_AGREEMENT_FOLLOWUP_ENV_KEYS:
        raw = (os.getenv(key) or "").strip()
        if raw:
            return raw, f"env:{key}"
    import_time = (N8N_AGREEMENT_FOLLOWUP_URL or "").strip()
    if import_time:
        return import_time, "import_time_env"
    return N8N_AGREEMENT_FOLLOWUP_HARDCODED_URL, "hardcoded"


def agreement_followup_webhook_url() -> str:
    url, _source = resolve_agreement_followup_webhook()
    return url


def _log_webhook_resolution(url: str, source: str) -> None:
    identity = _process_identity()
    logger.info(
        "agreement_followup webhook resolve source=%s url=%s k_service=%s k_revision=%s "
        "k_configuration=%s import_time_env=%s dotenv_present=%s related_env=%s",
        source,
        _safe_url_for_log(url),
        identity["k_service"],
        identity["k_revision"],
        identity["k_configuration"],
        "set" if N8N_AGREEMENT_FOLLOWUP_URL else "empty",
        identity["dotenv_present"],
        _webhook_env_snapshot(),
    )
    if source == "hardcoded":
        logger.warning(
            "agreement_followup webhook env was empty on this process; using hardcoded n8n URL. "
            "If you set N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL, it is not on this Cloud Run "
            "service/revision (k_service=%s k_revision=%s).",
            identity["k_service"],
            identity["k_revision"],
        )


AGREEMENT_FOLLOWUP_TEST_CAMPAIGN_NAME = "Agreement Follow Up — test stubs"
AGREEMENT_FOLLOWUP_TEST_OFFER_IDENTIFIER = "agreement_followup_test_stub"

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
        if not bool(clash.is_active):
            raise AgreementFollowupError(
                f"“{clash.label}” already exists but is hidden. Use Show on that row "
                "instead of creating a duplicate."
            )
        raise AgreementFollowupError(
            f"A type named {clash.label} already exists. Pick a different name, or use the existing tile."
        )
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
    webhook_url, source = resolve_agreement_followup_webhook()
    _log_webhook_resolution(webhook_url, source)
    if not webhook_url:
        raise AgreementFollowupError(
            "The agreement email was not sent: no n8n webhook URL (env empty and hardcoded "
            "fallback missing). No sequence was started.",
            502,
        )

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
    identity = _process_identity()
    logger.info(
        "agreement_followup first-touch POST start source=%s url=%s to=%s subject=%s "
        "filename=%s pdf_bytes=%s offer_id=%s client_id=%s agreement_type=%s "
        "k_service=%s k_revision=%s dotenv_present=%s",
        source,
        _safe_url_for_log(webhook_url),
        to,
        subject,
        filename,
        len(pdf_bytes or b""),
        offer_id,
        client_id,
        agreement_type,
        identity["k_service"],
        identity["k_revision"],
        identity["dotenv_present"],
    )
    started = datetime.now(timezone.utc)
    try:
        with httpx.Client(timeout=90.0) as client:
            response = client.post(webhook_url, data=form, files=files)
    except httpx.TimeoutException as exc:
        logger.exception(
            "agreement_followup first-touch TIMEOUT source=%s url=%s to=%s",
            source,
            _safe_url_for_log(webhook_url),
            to,
        )
        raise AgreementFollowupError(
            f"n8n webhook timed out after 90s ({_safe_url_for_log(webhook_url)}, source={source}). "
            "No sequence was started.",
            502,
        ) from exc
    except httpx.RequestError as exc:
        logger.exception(
            "agreement_followup first-touch REQUEST_ERROR source=%s url=%s to=%s err=%s",
            source,
            _safe_url_for_log(webhook_url),
            to,
            exc,
        )
        raise AgreementFollowupError(
            f"n8n webhook could not be reached ({_safe_url_for_log(webhook_url)}, source={source}): "
            f"{exc}. No sequence was started.",
            502,
        ) from exc

    elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
    snippet = (response.text or "")[:500]
    content_type = (response.headers.get("content-type") or "").split(";")[0]
    logger.info(
        "agreement_followup first-touch POST done source=%s url=%s status=%s "
        "elapsed_ms=%s content_type=%s body=%s",
        source,
        _safe_url_for_log(webhook_url),
        response.status_code,
        elapsed_ms,
        content_type or "-",
        snippet,
    )
    if response.status_code >= 400:
        raise AgreementFollowupError(
            f"n8n webhook HTTP {response.status_code} from {_safe_url_for_log(webhook_url)} "
            f"(source={source}). Response: {snippet or '(empty)'}. No sequence was started.",
            502,
        )
    try:
        body = response.json()
    except Exception as exc:
        logger.exception(
            "agreement_followup first-touch non-JSON status=%s content_type=%s body=%s",
            response.status_code,
            content_type,
            snippet,
        )
        raise AgreementFollowupError(
            "The agreement webhook returned HTTP "
            f"{response.status_code} ({content_type or 'no content-type'}) but not JSON, "
            f"so Gmail ids could not be confirmed. Body: {snippet or '(empty)'}. "
            "No sequence was started.",
            502,
        ) from exc
    if not isinstance(body, (dict, list)):
        raise AgreementFollowupError(
            "The agreement webhook JSON was empty, so the send was not confirmed. "
            "No sequence was started.",
            502,
        )
    email_id = extract_email_id_from_webhook_response(body)
    thread_id = extract_thread_id_from_webhook_response(body)
    logger.info(
        "agreement_followup first-touch parsed email_id=%s thread_id=%s json_type=%s",
        email_id or "-",
        thread_id or "-",
        type(body).__name__,
    )
    return {"ok": True, "mode": "n8n", "source": source, "response": body}


def _require_delivered_first_touch(n8n_result: Any) -> tuple[str | None, str | None]:
    if not isinstance(n8n_result, dict) or not n8n_result.get("ok"):
        raise AgreementFollowupError(
            "The agreement email was not sent (no webhook result). No sequence was started.",
            502,
        )
    if n8n_result.get("mode") == "placeholder":
        raise AgreementFollowupError(
            "The agreement email was not sent: the webhook was skipped (placeholder). "
            "No sequence was started.",
            502,
        )
    webhook_body = n8n_result.get("response")
    email_id = extract_email_id_from_webhook_response(webhook_body)
    thread_id = extract_thread_id_from_webhook_response(webhook_body)
    logger.info(
        "agreement_followup first-touch confirm source=%s email_id=%s thread_id=%s",
        n8n_result.get("source") or n8n_result.get("mode") or "-",
        email_id or "-",
        thread_id or "-",
    )
    if not email_id and not thread_id:
        raise AgreementFollowupError(
            "The agreement webhook did not return Gmail email_id or thread_id, so the send "
            "was not confirmed. No sequence was started. Check the n8n Respond node.",
            502,
        )
    return email_id, thread_id


def _insert_completed_first_touch_step(
    db: Session,
    run_id: int,
    *,
    to: str,
    filename: str,
    email_id: str | None,
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    existing = (
        db.query(AutonomousSequenceStep)
        .filter(AutonomousSequenceStep.run_id == run_id)
        .order_by(AutonomousSequenceStep.step_index.desc())
        .all()
    )
    for step in existing:
        step.step_index += 1
    db.flush()
    db.add(
        AutonomousSequenceStep(
            run_id=run_id,
            step_index=0,
            day_number=0,
            channel="email",
            offset_minutes_from_day_start=0,
            step_status="completed",
            scheduled_at=now,
            started_at=now,
            completed_at=now,
            last_outcome_summary=(
                f"First-touch PDF sent to {to} ({filename})"
                + (f"; email_id={email_id}" if email_id else "")
            ),
        )
    )
    db.commit()


def is_agreement_followup_test_offer(offer: Offer | None) -> bool:
    if offer is None:
        return False
    if (offer.identifier or "").strip() == AGREEMENT_FOLLOWUP_TEST_OFFER_IDENTIFIER:
        return True
    campaign = getattr(offer, "campaign", None)
    if campaign is not None and (getattr(campaign, "name", None) or "") == AGREEMENT_FOLLOWUP_TEST_CAMPAIGN_NAME:
        return True
    return False


def ensure_agreement_followup_test_campaign(db: Session) -> Campaign:
    row = (
        db.query(Campaign)
        .filter(Campaign.name == AGREEMENT_FOLLOWUP_TEST_CAMPAIGN_NAME)
        .first()
    )
    if row:
        return row
    row = Campaign(
        name=AGREEMENT_FOLLOWUP_TEST_CAMPAIGN_NAME,
        sequence_type=AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
        status="draft",
        archived=1,
        provenance_note="Hidden stub campaign for Agreement Follow Up test sends. Not a real outbound campaign.",
        created_by="system",
    )
    db.add(row)
    db.flush()
    return row


def _create_test_stub_offer(
    db: Session,
    *,
    business_name: str,
    utility_type: str,
    agreement_label: str,
    created_by: str | None,
) -> Offer:
    campaign = ensure_agreement_followup_test_campaign(db)
    offer = Offer(
        client_id=None,
        business_name=business_name,
        utility_type=utility_type,
        utility_type_identifier=agreement_label,
        identifier=AGREEMENT_FOLLOWUP_TEST_OFFER_IDENTIFIER,
        status=OfferStatus.AUTONOMOUS_AGENT_TRIGGER.value,
        campaign_id=campaign.id,
        created_by=created_by,
    )
    db.add(offer)
    db.flush()
    return offer


def purge_agreement_followup_test_stubs(db: Session) -> dict[str, int]:
    campaign = (
        db.query(Campaign)
        .filter(Campaign.name == AGREEMENT_FOLLOWUP_TEST_CAMPAIGN_NAME)
        .first()
    )
    if not campaign:
        return {"offers": 0, "runs": 0, "campaign_id": 0}
    offer_ids = [row.id for row in db.query(Offer.id).filter(Offer.campaign_id == campaign.id).all()]
    if not offer_ids:
        return {"offers": 0, "runs": 0, "campaign_id": campaign.id}
    run_ids = [
        row.id
        for row in db.query(AutonomousSequenceRun.id)
        .filter(AutonomousSequenceRun.offer_id.in_(offer_ids))
        .all()
    ]
    if run_ids:
        db.query(AutonomousSequenceEvent).filter(
            AutonomousSequenceEvent.run_id.in_(run_ids)
        ).delete(synchronize_session=False)
        db.query(AutonomousSequenceStep).filter(
            AutonomousSequenceStep.run_id.in_(run_ids)
        ).delete(synchronize_session=False)
        db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.id.in_(run_ids)).delete(
            synchronize_session=False
        )
    db.query(StrategyItem).filter(StrategyItem.offer_id.in_(offer_ids)).delete(synchronize_session=False)
    db.query(OfferActivity).filter(OfferActivity.offer_id.in_(offer_ids)).delete(synchronize_session=False)
    db.query(Offer).filter(Offer.id.in_(offer_ids)).delete(synchronize_session=False)
    db.commit()
    return {"offers": len(offer_ids), "runs": len(run_ids), "campaign_id": campaign.id}


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
    test_mode: bool = False,
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
    if test_mode:
        offer = _create_test_stub_offer(
            db,
            business_name=client.business_name,
            utility_type=agreement["utility_type"],
            agreement_label=agreement["label"],
            created_by=created_by,
        )
        created_offer = True
        db.commit()
        db.refresh(offer)
    elif offer_id is not None:
        offer = db.query(Offer).filter(Offer.id == offer_id, Offer.client_id == client_id).first()
        if not offer:
            raise AgreementFollowupError("Offer not found for this member", 404)
        if is_agreement_followup_test_offer(offer):
            raise AgreementFollowupError(
                "That offer is a test stub. Use test mode for another throwaway send, "
                "or pick a real CRM offer."
            )
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
        db.commit()
        db.refresh(offer)

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

    if not test_mode and offer.status not in {OfferStatus.ACCEPTED.value, OfferStatus.LOST.value}:
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
    if test_mode and not subject.upper().startswith("[TEST]"):
        subject = f"[TEST] {subject}"
        html = wrap_first_touch_html(_body, ACES_TEAM_FOLLOWUP_SIGNATURE_HTML)
        text = html_to_plain_text(html)
    upload_name = _safe_pdf_filename(filename, agreement["label"], business_name)

    logger.info(
        "agreement_followup start begin client_id=%s offer_id=%s created_offer=%s test_mode=%s "
        "to=%s agreement=%s filename=%s pdf_bytes=%s",
        client.id,
        offer.id,
        created_offer,
        test_mode,
        to,
        agreement["id"],
        upload_name,
        len(pdf_bytes or b""),
    )

    def _abort_without_run(exc: BaseException) -> NoReturn:
        if created_offer:
            db.delete(offer)
            db.commit()
        if isinstance(exc, AgreementFollowupError):
            raise exc
        raise AgreementFollowupError(
            "Could not send the agreement email via n8n. No sequence was started.",
            502,
        ) from exc

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
            client_id=None if test_mode else client.id,
        )
        email_id, thread_id = _require_delivered_first_touch(n8n_result)
    except AgreementFollowupError as exc:
        logger.exception("agreement follow-up first-touch rejected client_id=%s", client.id)
        _abort_without_run(exc)
    except (httpx.HTTPError, httpx.RequestError) as exc:
        logger.exception("agreement follow-up first-touch n8n failed client_id=%s", client.id)
        _abort_without_run(exc)

    activity = None
    if not test_mode:
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
    if test_mode:
        context["agreement_test"] = True
        context["dashboard_test"] = True
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
        client_id=None if test_mode else client.id,
        crm_activity_id=activity.id if activity else None,
        anchor_at=datetime.now(timezone.utc),
        tz=None,
        context=context,
    )
    _insert_completed_first_touch_step(
        db,
        run.id,
        to=to,
        filename=upload_name,
        email_id=email_id,
    )
    db.refresh(run)

    return {
        "ok": True,
        "run_id": run.id,
        "offer_id": offer.id,
        "client_id": None if test_mode else client.id,
        "created_offer": created_offer,
        "test": test_mode,
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
