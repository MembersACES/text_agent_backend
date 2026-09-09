"""Campaign persistence, test-send, stub offers, and bulk start."""

from __future__ import annotations

import json
import logging
import os
import hmac
import hashlib
import base64
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy.orm import Session
from sqlalchemy import func

from crm_enums import OfferStatus
from models import (
    AutonomousSequenceRun,
    AutonomousSequenceStep,
    Campaign,
    CampaignEvent,
    CampaignRow,
    Offer,
    Suppression,
)
from services.merge_template import (
    html_to_plain_text,
    looks_like_email,
    mapped_keys,
    parse_json_obj,
    recipient_key_from_merge,
    render_template,
    sanitize_html,
    split_row,
    validate_template,
)

logger = logging.getLogger(__name__)

N8N_EMAIL_URL = os.getenv("N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL", "").strip()
UNSUBSCRIBE_SECRET = (
    os.getenv("CAMPAIGN_UNSUBSCRIBE_SECRET")
    or os.getenv("NEXTAUTH_SECRET")
    or os.getenv("BACKEND_API_KEY")
    or "campaign-unsubscribe-dev"
)
PUBLIC_API_BASE = (
    os.getenv("PUBLIC_API_BASE_URL")
    or os.getenv("BACKEND_API_URL")
    or os.getenv("NEXTAUTH_URL")
    or ""
).rstrip("/")
MELBOURNE = ZoneInfo("Australia/Melbourne")
TEST_SEND_LIMIT = 20
TEST_SEND_WINDOW = timedelta(hours=1)
FIGURE_COLUMNS = (
    "annual_savings",
    "current_cost",
    "new_cost",
    "contracted_rate",
    "offer_rate",
    "annual_usage_gj",
    "estimated_value",
    "energy_charge_pct",
    "current_peak_rate",
    "current_shoulder_rate",
    "current_offpeak_rate",
    "new_peak_rate",
    "new_shoulder_rate",
    "new_offpeak_rate",
)


class CampaignError(ValueError):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _map(campaign: Campaign) -> dict[str, str]:
    return {str(k): str(v) for k, v in parse_json_obj(campaign.merge_field_map).items()}


def _merge(row: CampaignRow) -> dict[str, str]:
    data = parse_json_obj(row.merge_json)
    return {str(k): "" if v is None else str(v) for k, v in data.items()}


def campaign_to_dict(campaign: Campaign, db: Session, include_rows: bool = False, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    rows_q = db.query(CampaignRow).filter(CampaignRow.campaign_id == campaign.id)
    total = rows_q.count()
    pending = rows_q.filter(CampaignRow.row_status == "pending", CampaignRow.human_only == 0).count()
    human_only = rows_q.filter(CampaignRow.human_only != 0).count()
    unique = (
        db.query(func.count(func.distinct(CampaignRow.recipient_key)))
        .filter(CampaignRow.campaign_id == campaign.id, CampaignRow.recipient_key != "", CampaignRow.recipient_key.isnot(None))
        .scalar()
        or 0
    )
    test_sends = (
        db.query(func.count(CampaignEvent.id))
        .filter(CampaignEvent.campaign_id == campaign.id, CampaignEvent.event_type == "test_send")
        .scalar()
        or 0
    )
    payload: dict[str, Any] = {
        "id": campaign.id,
        "name": campaign.name,
        "sequence_type": campaign.sequence_type,
        "status": campaign.status,
        "first_touch_subject": campaign.first_touch_subject,
        "first_touch_html": campaign.first_touch_html,
        "first_touch_text": campaign.first_touch_text,
        "merge_field_map": parse_json_obj(campaign.merge_field_map),
        "provenance_note": campaign.provenance_note,
        "daily_cap": campaign.daily_cap,
        "send_window_start": campaign.send_window_start,
        "send_window_end": campaign.send_window_end,
        "created_by": campaign.created_by,
        "created_at": campaign.created_at.isoformat() if campaign.created_at else None,
        "updated_at": campaign.updated_at.isoformat() if campaign.updated_at else None,
        "row_counts": {
            "rows": total,
            "unique_recipients": unique,
            "pending": pending,
            "human_only": human_only,
            "test_sends": test_sends,
        },
    }
    if include_rows:
        page = (
            rows_q.order_by(CampaignRow.id)
            .offset(offset)
            .limit(limit)
            .all()
        )
        payload["rows"] = [row_to_dict(r) for r in page]
        payload["rows_total"] = total
        payload["rows_offset"] = offset
        payload["rows_limit"] = limit
    return payload


def row_to_dict(row: CampaignRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "campaign_id": row.campaign_id,
        "merge_json": parse_json_obj(row.merge_json),
        "intelligence_json": parse_json_obj(row.intelligence_json),
        "recipient_key": row.recipient_key,
        "row_status": row.row_status,
        "suppression_reason": row.suppression_reason,
        "run_id": row.run_id,
        "offer_id": row.offer_id,
        "human_only": bool(row.human_only),
        "human_only_reason": row.human_only_reason,
        "started_at": row.started_at.isoformat() if row.started_at else None,
    }


def create_campaign(db: Session, name: str, sequence_type: str, created_by: str | None) -> Campaign:
    if not (name or "").strip():
        raise CampaignError("name is required")
    if not (sequence_type or "").strip():
        raise CampaignError("sequence_type is required")
    campaign = Campaign(
        name=name.strip(),
        sequence_type=sequence_type.strip(),
        status="draft",
        created_by=created_by,
    )
    db.add(campaign)
    db.commit()
    db.refresh(campaign)
    return campaign


def list_campaigns(db: Session) -> list[Campaign]:
    return db.query(Campaign).order_by(Campaign.created_at.desc()).all()


def get_campaign(db: Session, campaign_id: int) -> Campaign:
    campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
    if not campaign:
        raise CampaignError("Campaign not found", 404)
    return campaign


def _assert_draft(campaign: Campaign) -> None:
    if campaign.status != "draft":
        raise CampaignError("Only a draft campaign can be edited", 409)


def patch_campaign(db: Session, campaign: Campaign, body: dict[str, Any], actor: str | None) -> Campaign:
    next_status = body.get("status")
    if next_status and next_status != campaign.status and next_status != "ready":
        raise CampaignError(f"Cannot set status to {next_status} via PATCH")

    if campaign.status == "draft":
        if "name" in body and body["name"] is not None:
            campaign.name = str(body["name"]).strip() or campaign.name
        if "sequence_type" in body and body["sequence_type"]:
            campaign.sequence_type = str(body["sequence_type"]).strip()
        if "first_touch_subject" in body:
            campaign.first_touch_subject = body["first_touch_subject"]
        if "first_touch_html" in body:
            campaign.first_touch_html = sanitize_html(body["first_touch_html"] or "")
            campaign.first_touch_text = html_to_plain_text(campaign.first_touch_html)
        if "merge_field_map" in body and body["merge_field_map"] is not None:
            campaign.merge_field_map = _json(body["merge_field_map"])
        if "provenance_note" in body:
            campaign.provenance_note = body["provenance_note"]
        if "daily_cap" in body:
            campaign.daily_cap = body["daily_cap"]
        if "send_window_start" in body:
            campaign.send_window_start = body["send_window_start"]
        if "send_window_end" in body:
            campaign.send_window_end = body["send_window_end"]
        db.flush()

    if next_status and next_status != campaign.status:
        if next_status == "ready":
            _ready_gate(db, campaign)
            campaign.status = "ready"
        else:
            raise CampaignError(f"Cannot set status to {next_status} via PATCH")

    if campaign.status != "draft" and next_status != "ready" and any(
        k in body for k in ("name", "first_touch_subject", "first_touch_html", "merge_field_map", "provenance_note")
    ):
        raise CampaignError("Only a draft campaign can be edited", 409)

    db.commit()
    db.refresh(campaign)
    return campaign


def delete_campaign(db: Session, campaign: Campaign) -> None:
    _assert_draft(campaign)
    db.query(CampaignEvent).filter(CampaignEvent.campaign_id == campaign.id).delete()
    db.query(CampaignRow).filter(CampaignRow.campaign_id == campaign.id).delete()
    db.delete(campaign)
    db.commit()


def replace_rows(
    db: Session,
    campaign: Campaign,
    headers: list[str],
    rows: list[list[str]],
    column_map: dict[str, str],
    client_merge_json: dict | None = None,
) -> dict[str, Any]:
    _assert_draft(campaign)
    mapping = {str(k): str(v) for k, v in (column_map or {}).items()}
    campaign.merge_field_map = _json(mapping)
    db.query(CampaignRow).filter(CampaignRow.campaign_id == campaign.id).delete()

    groups: dict[str, list[dict[str, str]]] = {}
    stored = 0
    blank_keys = 0
    for cells in rows:
        merge, intelligence = split_row(headers, cells, mapping)
        # Guard: never persist a client-supplied merge blob. If one was sent, ignore it.
        _ = client_merge_json
        key = recipient_key_from_merge(merge)
        db.add(
            CampaignRow(
                campaign_id=campaign.id,
                merge_json=_json(merge),
                intelligence_json=_json(intelligence),
                recipient_key=key,
                row_status="pending",
                human_only=0,
            )
        )
        stored += 1
        if key:
            groups.setdefault(key, []).append(merge)
        else:
            blank_keys += 1

    db.commit()
    conflicts = []
    for email, members in groups.items():
        if len(members) < 2:
            continue
        keys = set()
        for member in members:
            keys.update(member.keys())
        differing = []
        for field in keys:
            values = {(m.get(field) or "").strip().lower() for m in members}
            if len(values) > 1:
                differing.append(field)
        if differing:
            conflicts.append({"email": email, "count": len(members), "differing_keys": differing})

    return {
        "rows": stored,
        "unique_recipients": len(groups) + blank_keys,
        "groups_with_conflicts": conflicts,
        "pending": stored,
        "human_only": 0,
    }


def set_human_only(db: Session, campaign: Campaign, row_id: int, human_only: bool, reason: str | None) -> CampaignRow:
    _assert_draft(campaign)
    row = (
        db.query(CampaignRow)
        .filter(CampaignRow.id == row_id, CampaignRow.campaign_id == campaign.id)
        .first()
    )
    if not row:
        raise CampaignError("Row not found", 404)
    row.human_only = 1 if human_only else 0
    row.human_only_reason = reason if human_only else None
    db.commit()
    db.refresh(row)
    return row


def _ready_gate(db: Session, campaign: Campaign) -> None:
    subject = (campaign.first_touch_subject or "").strip()
    body = (campaign.first_touch_html or "").strip()
    if not subject:
        raise CampaignError("A subject is required before a campaign can go ready")
    if not body:
        raise CampaignError("A body is required before a campaign can go ready")
    mapping = _map(campaign)
    allowed = mapped_keys(mapping)
    if not allowed:
        raise CampaignError("At least one merge field must be mapped before a campaign can go ready")
    pending = (
        db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign.id, CampaignRow.row_status == "pending", CampaignRow.human_only == 0)
        .count()
    )
    if pending < 1:
        raise CampaignError("At least one pending sendable row is required before a campaign can go ready")
    unknown = validate_template(subject, allowed) + validate_template(body, allowed)
    if unknown:
        raise CampaignError(f"Unknown merge tokens: {', '.join(f'{{{{{t}}}}}' for t in unknown)}")
    tests = (
        db.query(CampaignEvent)
        .filter(CampaignEvent.campaign_id == campaign.id, CampaignEvent.event_type == "test_send")
        .count()
    )
    if tests < 1:
        raise CampaignError("Send at least one test email before a campaign can go ready")


def _assert_single_to(to: str) -> str:
    address = (to or "").strip()
    if not address:
        raise CampaignError("to is required")
    if any(sep in address for sep in (",", ";", " ")):
        raise CampaignError("to must be a single address — lists, commas and BCC are rejected")
    if not looks_like_email(address):
        raise CampaignError("to is not a valid email address")
    return address.lower()


def _test_send_count(db: Session, campaign_id: int) -> int:
    cutoff = _now() - TEST_SEND_WINDOW
    return (
        db.query(CampaignEvent)
        .filter(
            CampaignEvent.campaign_id == campaign_id,
            CampaignEvent.event_type == "test_send",
            CampaignEvent.created_at >= cutoff,
        )
        .count()
    )


def render_first_touch(campaign: Campaign, merge: dict[str, str], test: bool) -> tuple[str, str, str]:
    subject_t, _ = render_template(campaign.first_touch_subject or "", merge)
    html_t, _ = render_template(campaign.first_touch_html or "", merge)
    text_t = html_to_plain_text(html_t)
    if test:
        subject_t = f"[TEST] {subject_t}"
    return subject_t, html_t, text_t


def extract_email_id_from_webhook_response(webhook_response: Any) -> str | None:
    """Same shape as frontend extractEmailIdFromWebhookResponse."""

    def _read_from_object(obj: dict[str, Any]) -> str | None:
        for key in ("email_id", "email_ID", "threadId", "thread_id"):
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in obj.values():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        nested = _read_from_object(item)
                        if nested:
                            return nested
            elif isinstance(value, dict):
                nested = _read_from_object(value)
                if nested:
                    return nested
        return None

    if isinstance(webhook_response, list):
        for item in webhook_response:
            if isinstance(item, dict):
                found = _read_from_object(item)
                if found:
                    return found
        return None
    if isinstance(webhook_response, dict):
        return _read_from_object(webhook_response)
    return None


def send_first_touch_email(
    *,
    db: Session,
    to: str,
    subject: str,
    html: str,
    text: str,
    campaign: Campaign,
    row: CampaignRow | None,
    test: bool,
) -> dict[str, Any]:
    if not test and suppressed(db, to):
        raise CampaignError("That address is on the suppression list", 409)
    unsub = unsubscribe_url(to, campaign.id)
    payload = {
        "channel": "email",
        "to": to,
        "subject": subject,
        "body_html": html,
        "body_text": text,
        "campaign_id": campaign.id,
        "row_id": row.id if row else None,
        "test": test,
        "list_unsubscribe": f"<{unsub}>",
        "list_unsubscribe_post": "List-Unsubscribe=One-Click",
        "unsubscribe_url": unsub,
        "offer_id": None,
        "run_id": None,
        "step_id": None,
        "sequence_type": campaign.sequence_type,
        "reply_in_thread": False,
        "use_html_signature": False,
    }
    if not N8N_EMAIL_URL:
        logger.info("[campaign] email webhook not set; placeholder to=%s test=%s", to, test)
        return {"ok": True, "mode": "placeholder", "payload": payload}
    with httpx.Client(timeout=30.0) as client:
        response = client.post(N8N_EMAIL_URL, json=payload)
        response.raise_for_status()
        try:
            return {"ok": True, "response": response.json()}
        except Exception:
            return {"ok": True, "response_text": response.text[:2000]}


def fire_test_send(db: Session, campaign: Campaign, to: str, row_id: int, actor: str | None) -> dict[str, Any]:
    address = _assert_single_to(to)
    if _test_send_count(db, campaign.id) >= TEST_SEND_LIMIT:
        raise CampaignError("Test-send rate limit: 20 per campaign per hour", 429)
    row = (
        db.query(CampaignRow)
        .filter(CampaignRow.id == row_id, CampaignRow.campaign_id == campaign.id)
        .first()
    )
    if not row:
        raise CampaignError("Row not found", 404)
    merge = _merge(row)
    before_runs = db.query(AutonomousSequenceRun).count()
    before_offers = db.query(Offer).count()
    row_status = row.row_status
    subject, html, text = render_first_touch(campaign, merge, test=True)
    send_first_touch_email(
        db=db, to=address, subject=subject, html=html, text=text, campaign=campaign, row=row, test=True
    )
    event = CampaignEvent(
        campaign_id=campaign.id,
        event_type="test_send",
        actor=actor,
        to_email=address,
        row_id=row.id,
        payload_json=_json({"subject": subject, "company_name": merge.get("company_name")}),
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    db.refresh(row)
    if db.query(AutonomousSequenceRun).count() != before_runs:
        raise CampaignError("Test send must not create a run", 500)
    if db.query(Offer).count() != before_offers:
        raise CampaignError("Test send must not create an Offer", 500)
    if row.row_status != row_status:
        raise CampaignError("Test send must not change the row", 500)
    return {
        "ok": True,
        "event_id": event.id,
        "to": address,
        "subject": subject,
        "row_id": row.id,
    }


def suppressed(db: Session, email: str) -> bool:
    key = (email or "").strip().lower()
    if not key:
        return False
    return db.query(Suppression).filter(Suppression.email == key).first() is not None


def add_suppression(db: Session, email: str, reason: str, source: str) -> Suppression:
    key = (email or "").strip().lower()
    existing = db.query(Suppression).filter(Suppression.email == key).first()
    if existing:
        return existing
    row = Suppression(email=key, reason=reason, source=source)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def unsubscribe_url(email: str, campaign_id: int) -> str:
    token = sign_unsubscribe_token(email, campaign_id)
    base = PUBLIC_API_BASE or ""
    return f"{base}/api/autonomous/campaigns/unsubscribe?token={token}"


def sign_unsubscribe_token(email: str, campaign_id: int) -> str:
    payload = f"{(email or '').strip().lower()}|{int(campaign_id)}"
    sig = hmac.new(UNSUBSCRIBE_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    raw = f"{payload}|{sig}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def verify_unsubscribe_token(token: str) -> tuple[str, int]:
    padded = token + "=" * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode()).decode()
    except Exception as exc:
        raise CampaignError("Invalid unsubscribe token", 400) from exc
    parts = raw.split("|")
    if len(parts) != 3:
        raise CampaignError("Invalid unsubscribe token", 400)
    email, campaign_id_s, sig = parts
    payload = f"{email}|{campaign_id_s}"
    expected = hmac.new(UNSUBSCRIBE_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise CampaignError("Invalid unsubscribe token", 400)
    return email, int(campaign_id_s)


def apply_unsubscribe(db: Session, token: str) -> dict[str, Any]:
    email, campaign_id = verify_unsubscribe_token(token)
    add_suppression(db, email, "unsubscribed", "one_click")
    runs = (
        db.query(AutonomousSequenceRun)
        .filter(
            AutonomousSequenceRun.run_status == "running",
            func.lower(AutonomousSequenceRun.contact_email) == email,
        )
        .all()
    )
    from services.autonomous_sequence import skip_remaining_steps

    stopped = []
    for run in runs:
        run.run_status = "stopped"
        run.stop_reason = "unsubscribed"
        skip_remaining_steps(db, run.id)
        stopped.append(run.id)
    db.commit()
    return {"ok": True, "email": email, "campaign_id": campaign_id, "stopped_runs": stopped}


def _inside_send_window(campaign: Campaign, now: datetime | None = None) -> bool:
    start = (campaign.send_window_start or "").strip()
    end = (campaign.send_window_end or "").strip()
    if not start or not end:
        return True
    current = (now or datetime.now(MELBOURNE)).astimezone(MELBOURNE).strftime("%H:%M")
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def _started_today(db: Session, campaign_id: int) -> int:
    start = datetime.now(MELBOURNE).replace(hour=0, minute=0, second=0, microsecond=0)
    start_naive = start.astimezone(timezone.utc).replace(tzinfo=None)
    return (
        db.query(CampaignRow)
        .filter(
            CampaignRow.campaign_id == campaign_id,
            CampaignRow.row_status == "started",
            CampaignRow.started_at >= start_naive,
        )
        .count()
    )


def _create_stub_offer(db: Session, campaign: Campaign, merge: dict[str, str], actor: str | None) -> Offer:
    offer = Offer(
        client_id=None,
        business_name=(merge.get("company_name") or "").strip() or None,
        utility_type="gas",
        status=OfferStatus.AUTONOMOUS_AGENT_TRIGGER.value,
        campaign_id=campaign.id,
        created_by=actor,
    )
    db.add(offer)
    db.flush()
    return offer


def start_campaign(db: Session, campaign: Campaign, actor: str | None) -> dict[str, Any]:
    if campaign.status == "draft":
        raise CampaignError("A draft campaign can only send a test to one typed-in address", 409)
    if campaign.status == "paused":
        raise CampaignError("This campaign is paused. Resume it before starting more rows.", 409)
    if campaign.status == "done":
        return {
            "ok": True,
            "started": 0,
            "pending": 0,
            "reason": "done",
            "status": "done",
        }
    if campaign.status not in {"ready", "sending"}:
        raise CampaignError(f"Cannot start from status {campaign.status}", 409)

    if not _inside_send_window(campaign):
        return {
            "ok": True,
            "started": 0,
            "pending": _pending_sendable(db, campaign.id),
            "reason": "outside_send_window",
            "status": campaign.status,
        }

    cap = campaign.daily_cap
    already = _started_today(db, campaign.id)
    remaining = None if cap is None else max(0, int(cap) - already)
    if remaining == 0:
        return {
            "ok": True,
            "started": 0,
            "pending": _pending_sendable(db, campaign.id),
            "reason": "daily_cap",
            "status": campaign.status,
        }

    from services.autonomous_sequence import start_gas_base2_sequence

    rows = (
        db.query(CampaignRow)
        .filter(
            CampaignRow.campaign_id == campaign.id,
            CampaignRow.row_status == "pending",
            CampaignRow.human_only == 0,
        )
        .order_by(CampaignRow.id)
        .all()
    )
    started = 0
    skipped_suppressed = 0
    skipped_idempotent = 0
    seen_keys: set[str] = set(
        r.recipient_key
        for r in db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign.id, CampaignRow.run_id.isnot(None))
        .all()
        if r.recipient_key
    )

    for row in rows:
        if remaining is not None and started >= remaining:
            break
        if row.run_id:
            skipped_idempotent += 1
            continue
        key = (row.recipient_key or "").strip().lower()
        if key and suppressed(db, key):
            row.row_status = "suppressed"
            row.suppression_reason = "unsubscribed"
            skipped_suppressed += 1
            continue
        if key and key in seen_keys:
            continue
        merge = _merge(row)
        if key and suppressed(db, merge.get("contact_email") or key):
            row.row_status = "suppressed"
            row.suppression_reason = "unsubscribed"
            skipped_suppressed += 1
            continue
        subject, html, text = render_first_touch(campaign, merge, test=False)
        to = (merge.get("contact_email") or key or "").strip()
        if not looks_like_email(to):
            row.row_status = "failed"
            row.suppression_reason = "manual"
            continue
        n8n_result = send_first_touch_email(
            db=db, to=to, subject=subject, html=html, text=text, campaign=campaign, row=row, test=False
        )
        email_id = extract_email_id_from_webhook_response(n8n_result)
        if not email_id:
            logger.warning(
                "[campaign] first-touch n8n returned no email_id campaign_id=%s row_id=%s "
                "recipient=%s company=%s",
                campaign.id,
                row.id,
                to,
                merge.get("company_name") or "",
            )
        offer = _create_stub_offer(db, campaign, merge, actor)
        context = dict(merge)
        context["campaign_id"] = campaign.id
        context["campaign_first_touch_sent"] = True
        if email_id:
            context["email_ID"] = email_id
            context["email_id"] = email_id
        run = start_gas_base2_sequence(
            db,
            sequence_type=campaign.sequence_type,
            offer_id=offer.id,
            client_id=None,
            crm_activity_id=None,
            anchor_at=datetime.now(timezone.utc),
            tz="Australia/Melbourne",
            context=context,
        )
        _complete_first_email_step(db, run.id)
        row.row_status = "started"
        row.run_id = run.id
        row.offer_id = offer.id
        row.started_at = _now()
        if key:
            seen_keys.add(key)
        started += 1
        campaign.status = "sending"
        db.commit()

    pending = _pending_sendable(db, campaign.id)
    if pending == 0:
        campaign.status = "done"
    elif started > 0:
        campaign.status = "sending"
    db.commit()
    db.refresh(campaign)
    return {
        "ok": True,
        "started": started,
        "pending": pending,
        "skipped_suppressed": skipped_suppressed,
        "skipped_idempotent": skipped_idempotent,
        "status": campaign.status,
    }


def _pending_sendable(db: Session, campaign_id: int) -> int:
    return (
        db.query(CampaignRow)
        .filter(
            CampaignRow.campaign_id == campaign_id,
            CampaignRow.row_status == "pending",
            CampaignRow.human_only == 0,
        )
        .count()
    )


def _complete_first_email_step(db: Session, run_id: int) -> None:
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
        step.completed_at = _now()
        step.last_outcome_summary = "campaign first-touch sent as merge template (not LLM-drafted)"
        db.commit()


def pause_campaign(db: Session, campaign: Campaign) -> Campaign:
    if campaign.status not in {"ready", "sending"}:
        raise CampaignError("Only a ready or sending campaign can be paused", 409)
    campaign.status = "paused"
    db.commit()
    db.refresh(campaign)
    return campaign


def resume_campaign(db: Session, campaign: Campaign) -> Campaign:
    if campaign.status != "paused":
        raise CampaignError("Only a paused campaign can be resumed", 409)
    campaign.status = "sending" if _started_today(db, campaign.id) or (
        db.query(CampaignRow).filter(CampaignRow.campaign_id == campaign.id, CampaignRow.run_id.isnot(None)).count()
    ) else "ready"
    db.commit()
    db.refresh(campaign)
    return campaign
