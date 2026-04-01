"""
Autonomous follow-up sequences after Base 2: shared schedule for C&I gas & C&I electricity
comparison follow-up; differentiate in n8n via context (utility_lane, base2_trigger, etc.).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy.orm import Session, joinedload

from models import AutonomousSequenceEvent, AutonomousSequenceRun, AutonomousSequenceStep, Offer


def delete_autonomous_sequence_run(db: Session, run_id: int) -> bool:
    """Remove a run and all steps/events. Events reference steps, so delete events first."""
    run = db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.id == run_id).first()
    if not run:
        return False
    db.query(AutonomousSequenceEvent).filter(AutonomousSequenceEvent.run_id == run_id).delete(
        synchronize_session=False
    )
    db.query(AutonomousSequenceStep).filter(AutonomousSequenceStep.run_id == run_id).delete(
        synchronize_session=False
    )
    db.delete(run)
    db.commit()
    return True

logger = logging.getLogger(__name__)

N8N_EMAIL_URL = os.getenv("N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL", "").strip()
N8N_SMS_URL = os.getenv("N8N_AUTONOMOUS_SMS_WEBHOOK_URL", "").strip()
RETELL_BASE = os.getenv("RETELL_API_BASE_URL", "https://api.retellai.com").rstrip("/")
RETELL_KEY = os.getenv("RETELL_API_KEY", "").strip()

# All autonomous step times are computed in fixed Australian Eastern Standard Time (UTC+10, no DST).
# IANA zone Australia/Brisbane matches AEST year-round (unlike Sydney/Melbourne).
AUTONOMOUS_SCHEDULE_TZ = "Australia/Brisbane"


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _to_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def next_business_day(d: date) -> date:
    n = d + timedelta(days=1)
    while n.weekday() >= 5:
        n += timedelta(days=1)
    return n


def ensure_weekday(d: date) -> date:
    n = d
    while n.weekday() >= 5:
        n += timedelta(days=1)
    return n


def plan_gas_base2_followup_times(anchor: datetime) -> list[tuple[int, str, datetime]]:
    """Returns (day_number, channel, scheduled_at UTC naive). Always uses AEST (Australia/Brisbane)."""
    tz = ZoneInfo(AUTONOMOUS_SCHEDULE_TZ)
    a = anchor if anchor.tzinfo else anchor.replace(tzinfo=tz)
    local = a.astimezone(tz)
    base_date = local.date()

    d1 = next_business_day(base_date)
    email1_local = datetime.combine(d1, time(9, 0), tzinfo=tz)
    call1_local = email1_local + timedelta(minutes=30)

    d2_raw = d1 + timedelta(days=1)
    d2 = ensure_weekday(d2_raw)
    sms2_local = datetime.combine(d2, time(10, 0), tzinfo=tz)

    d3_raw = d2 + timedelta(days=1)
    d3 = ensure_weekday(d3_raw)
    call3_local = datetime.combine(d3, time(11, 0), tzinfo=tz)
    email3_local = call3_local + timedelta(minutes=30)

    out = []
    for day_num, ch, loc in [
        (1, "email", email1_local),
        (1, "voice_call", call1_local),
        (2, "sms", sms2_local),
        (3, "voice_call", call3_local),
        (3, "email", email3_local),
    ]:
        out.append((day_num, ch, loc.astimezone(timezone.utc).replace(tzinfo=None)))
    return out


def _log_event(
    db: Session,
    run_id: int,
    event_type: str,
    step_id: Optional[int] = None,
    payload: Optional[dict[str, Any]] = None,
) -> None:
    ev = AutonomousSequenceEvent(
        run_id=run_id,
        step_id=step_id,
        event_type=event_type,
        payload_json=json.dumps(payload) if payload is not None else None,
    )
    db.add(ev)


def _parse_context(run: AutonomousSequenceRun) -> dict[str, Any]:
    if not run.context_json:
        return {}
    try:
        return json.loads(run.context_json)
    except (TypeError, json.JSONDecodeError):
        return {}


def _merge_context(db: Session, run: AutonomousSequenceRun, extra: dict[str, Any]) -> None:
    base = _parse_context(run)
    base.update(extra)
    update_run_context(db, run, base)


def _context_contact_fields(context: dict[str, Any]) -> dict[str, Optional[str]]:
    def _norm(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    return {
        "email_ID": _norm(context.get("email_ID")),
        "contact_phone": _norm(context.get("contact_phone")),
        "contact_name": _norm(context.get("contact_name")),
        "contact_email": _norm(context.get("contact_email")),
    }


def skip_remaining_steps(db: Session, run_id: int) -> None:
    steps = db.query(AutonomousSequenceStep).filter(AutonomousSequenceStep.run_id == run_id).all()
    for s in steps:
        if s.step_status in ("to_start", "ready", "in_progress"):
            s.step_status = "skipped"
            s.completed_at = _utc_now_naive()


_RESTARTABLE_SEQUENCE_TYPES = frozenset(
    {
        "gas_base2_followup_v1",
        "ci_electricity_base2_followup_v1",
    }
)


def restart_sequence_from_finished_run(db: Session, run_id: int) -> Optional[dict[str, Any]]:
    """
    Start a new Base-2 follow-up run for the same offer/type as a finished run, reusing stored
    context and client/activity IDs. Anchor is current time in AEST (Australia/Brisbane).
    If an active run already exists for that offer+type, returns that run with reused_existing=True.
    """
    run = db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.id == run_id).first()
    if not run:
        return None
    if run.run_status not in ("stopped", "completed", "cancelled"):
        raise ValueError("Only stopped, completed, or cancelled runs can be restarted")
    if run.sequence_type not in _RESTARTABLE_SEQUENCE_TYPES:
        raise ValueError(
            f"Unsupported sequence_type for restart; allowed: {sorted(_RESTARTABLE_SEQUENCE_TYPES)}",
        )

    offer = db.query(Offer).filter(Offer.id == run.offer_id).first()
    if not offer:
        raise ValueError("Offer not found for this sequence")

    existing = (
        db.query(AutonomousSequenceRun)
        .filter(
            AutonomousSequenceRun.offer_id == run.offer_id,
            AutonomousSequenceRun.sequence_type == run.sequence_type,
            AutonomousSequenceRun.run_status == "running",
        )
        .first()
    )
    reused_existing = existing is not None

    anchor_at = datetime.now(ZoneInfo(AUTONOMOUS_SCHEDULE_TZ))
    ctx = _parse_context(run)
    client_id = run.client_id if run.client_id is not None else offer.client_id

    out = start_gas_base2_sequence(
        db,
        sequence_type=run.sequence_type,
        offer_id=run.offer_id,
        client_id=client_id,
        crm_activity_id=run.crm_activity_id,
        anchor_at=anchor_at,
        tz=AUTONOMOUS_SCHEDULE_TZ,
        context=ctx,
    )

    steps_planned = (
        db.query(AutonomousSequenceStep).filter(AutonomousSequenceStep.run_id == out.id).count()
    )

    if not reused_existing:
        _log_event(db, out.id, "run_restarted_from", payload={"prior_run_id": run_id})
        db.commit()
        db.refresh(out)

    return {
        "run_id": out.id,
        "prior_run_id": run_id,
        "reused_existing": reused_existing,
        "sequence_type": out.sequence_type,
        "offer_id": out.offer_id,
        "run_status": out.run_status,
        "steps_planned": steps_planned,
    }


def manual_stop_run(db: Session, run_id: int) -> Optional[AutonomousSequenceRun]:
    """Staff/dashboard: stop run immediately and skip pending steps."""
    run = db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.id == run_id).first()
    if not run:
        return None
    if run.run_status != "running":
        return run
    _log_event(db, run.id, "manual_stop", payload={"source": "dashboard"})
    run.run_status = "stopped"
    run.stop_reason = "manual_stop"
    skip_remaining_steps(db, run.id)
    db.commit()
    db.refresh(run)
    return run


def update_run_context(db: Session, run: AutonomousSequenceRun, context: dict[str, Any]) -> None:
    run.context_json = json.dumps(context) if context else None
    contact_fields = _context_contact_fields(context or {})
    run.email_ID = contact_fields["email_ID"]
    run.contact_phone = contact_fields["contact_phone"]
    run.contact_name = contact_fields["contact_name"]
    run.contact_email = contact_fields["contact_email"]


_SCHEDULE_EDITABLE_STATUSES = frozenset(("ready", "to_start"))


def update_step_schedules(
    db: Session,
    run: AutonomousSequenceRun,
    updates: list[tuple[int, datetime]],
) -> None:
    """Set scheduled_at for steps on this run. Raises ValueError if a step is missing or not reschedulable."""
    if not updates:
        return
    step_ids = [u[0] for u in updates]
    if len(step_ids) != len(set(step_ids)):
        raise ValueError("Duplicate step_id in updates")
    rows = (
        db.query(AutonomousSequenceStep)
        .filter(
            AutonomousSequenceStep.run_id == run.id,
            AutonomousSequenceStep.id.in_(step_ids),
        )
        .all()
    )
    by_id = {s.id: s for s in rows}
    missing = [sid for sid in step_ids if sid not in by_id]
    if missing:
        raise ValueError(f"Step(s) not on this run: {missing}")
    for sid, at in updates:
        step = by_id[sid]
        if step.step_status not in _SCHEDULE_EDITABLE_STATUSES:
            raise ValueError(
                f"Step {sid} is {step.step_status!r}; only ready or to_start can be rescheduled"
            )
        step.scheduled_at = _to_utc_naive(at)
    run.updated_at = _utc_now_naive()
    _log_event(
        db,
        run.id,
        "steps_rescheduled",
        payload={"updates": [{"step_id": sid, "scheduled_at": at.isoformat()} for sid, at in updates]},
    )


def _should_stop_run(db: Session, run: AutonomousSequenceRun) -> tuple[bool, Optional[str]]:
    ev = (
        db.query(AutonomousSequenceEvent)
        .filter(AutonomousSequenceEvent.run_id == run.id)
        .filter(
            AutonomousSequenceEvent.event_type.in_(
                ("inbound_agreement_signed", "inbound_stop_sentiment", "manual_stop")
            )
        )
        .first()
    )
    if not ev:
        return False, None
    return True, ev.event_type


def start_gas_base2_sequence(
    db: Session,
    *,
    sequence_type: str,
    offer_id: int,
    client_id: Optional[int],
    crm_activity_id: Optional[int],
    anchor_at: datetime,
    tz: str,
    context: dict[str, Any],
) -> AutonomousSequenceRun:
    """tz is accepted for API compatibility but ignored; schedules always use AUTONOMOUS_SCHEDULE_TZ (AEST)."""
    existing = (
        db.query(AutonomousSequenceRun)
        .filter(
            AutonomousSequenceRun.offer_id == offer_id,
            AutonomousSequenceRun.sequence_type == sequence_type,
            AutonomousSequenceRun.run_status == "running",
        )
        .first()
    )
    if existing:
        logger.warning("Active autonomous run already exists offer_id=%s type=%s", offer_id, sequence_type)
        return existing

    anchor_utc = _to_utc_naive(anchor_at)
    _ = tz  # caller may pass client timezone; scheduling is always AEST
    contact_fields = _context_contact_fields(context)
    run = AutonomousSequenceRun(
        sequence_type=sequence_type,
        offer_id=offer_id,
        client_id=client_id,
        crm_activity_id=crm_activity_id,
        run_status="running",
        anchor_at=anchor_utc,
        timezone=AUTONOMOUS_SCHEDULE_TZ,
        context_json=json.dumps(context) if context else None,
        email_ID=contact_fields["email_ID"],
        contact_phone=contact_fields["contact_phone"],
        contact_name=contact_fields["contact_name"],
        contact_email=contact_fields["contact_email"],
    )
    db.add(run)
    db.flush()

    plan = plan_gas_base2_followup_times(anchor_at)
    retell_agent_id = context.get("retell_agent_id")

    for idx, (day_num, channel, scheduled_utc_naive) in enumerate(plan):
        step = AutonomousSequenceStep(
            run_id=run.id,
            step_index=idx,
            day_number=day_num,
            channel=channel,
            offset_minutes_from_day_start=0,
            step_status="ready",
            scheduled_at=scheduled_utc_naive,
            retell_agent_id=str(retell_agent_id) if retell_agent_id and channel == "voice_call" else None,
        )
        db.add(step)

    _log_event(db, run.id, "run_started", payload={"offer_id": offer_id, "steps": len(plan)})
    db.commit()
    db.refresh(run)
    return run


def apply_inbound(db: Session, run: AutonomousSequenceRun, payload: dict[str, Any]) -> AutonomousSequenceRun:
    intent = (payload.get("intent") or "").lower()
    sentiment_negative = bool(payload.get("sentiment_negative"))

    _log_event(db, run.id, "inbound_message", payload=payload)

    if intent == "agreement_signed" or payload.get("agreement_signed"):
        _log_event(db, run.id, "inbound_agreement_signed", payload=payload)
        run.run_status = "stopped"
        run.stop_reason = "agreement_signed"
        skip_remaining_steps(db, run.id)
        db.commit()
        db.refresh(run)
        return run

    if sentiment_negative or intent in ("stop", "stop_sentiment"):
        _log_event(db, run.id, "inbound_stop_sentiment", payload=payload)
        run.run_status = "stopped"
        run.stop_reason = "negative_sentiment_stop"
        skip_remaining_steps(db, run.id)
        db.commit()
        db.refresh(run)
        return run

    if intent == "will_do_later" or "later" in (payload.get("raw_text") or "").lower():
        _merge_context(
            db,
            run,
            {
                "last_client_intent": "will_do_later",
                "last_client_message": (payload.get("raw_text") or "")[:2000],
            },
        )
        _log_event(db, run.id, "inbound_will_do_later", payload=payload)

    db.commit()
    db.refresh(run)
    return run


def _send_email_placeholder(offer_id: int, run_id: int, step_id: int, context: dict[str, Any]) -> dict[str, Any]:
    if not N8N_EMAIL_URL:
        logger.info(
            "[autonomous] email webhook not set; placeholder offer_id=%s run_id=%s step_id=%s",
            offer_id,
            run_id,
            step_id,
        )
        return {"ok": True, "mode": "placeholder", "channel": "email"}
    payload = {"channel": "email", "offer_id": offer_id, "run_id": run_id, "step_id": step_id, "context": context}
    with httpx.Client(timeout=30.0) as client:
        r = client.post(N8N_EMAIL_URL, json=payload)
        r.raise_for_status()
        try:
            return {"ok": True, "channel": "email", "response": r.json()}
        except Exception:
            return {"ok": True, "channel": "email", "response_text": r.text[:2000]}


def _send_sms_placeholder(offer_id: int, run_id: int, step_id: int, context: dict[str, Any]) -> dict[str, Any]:
    if not N8N_SMS_URL:
        logger.info(
            "[autonomous] SMS webhook not set; placeholder offer_id=%s run_id=%s step_id=%s",
            offer_id,
            run_id,
            step_id,
        )
        return {"ok": True, "mode": "placeholder", "channel": "sms"}
    payload = {"channel": "sms", "offer_id": offer_id, "run_id": run_id, "step_id": step_id, "context": context}
    with httpx.Client(timeout=30.0) as client:
        r = client.post(N8N_SMS_URL, json=payload)
        r.raise_for_status()
        try:
            return {"ok": True, "channel": "sms", "response": r.json()}
        except Exception:
            return {"ok": True, "channel": "sms", "response_text": r.text[:2000]}


def _voice_retell_placeholder(
    offer_id: int,
    run_id: int,
    step_id: int,
    retell_agent_id: Optional[str],
    context: dict[str, Any],
) -> dict[str, Any]:
    if not RETELL_KEY or not retell_agent_id:
        logger.info(
            "[autonomous] Retell placeholder offer_id=%s run_id=%s step_id=%s agent=%s",
            offer_id,
            run_id,
            step_id,
            retell_agent_id,
        )
        return {
            "ok": True,
            "mode": "placeholder",
            "channel": "voice_call",
            "external_id": f"placeholder-call-{run_id}-{step_id}",
        }
    url = f"{RETELL_BASE}/v2/create-phone-call"
    headers = {"Authorization": f"Bearer {RETELL_KEY}", "Content-Type": "application/json"}
    body = {
        "agent_id": retell_agent_id,
        "metadata": {"offer_id": offer_id, "run_id": run_id, "step_id": step_id, "context": context},
    }
    with httpx.Client(timeout=60.0) as client:
        r = client.post(url, headers=headers, json=body)
        r.raise_for_status()
        try:
            return {"ok": True, "channel": "voice_call", "response": r.json()}
        except Exception:
            return {"ok": True, "channel": "voice_call", "response_text": r.text[:2000]}


def execute_due_steps_sync(db: Session) -> int:
    now = _utc_now_naive()
    runs = (
        db.query(AutonomousSequenceRun)
        .options(joinedload(AutonomousSequenceRun.steps))
        .filter(AutonomousSequenceRun.run_status == "running")
        .all()
    )
    executed = 0

    for run in runs:
        stop, reason = _should_stop_run(db, run)
        if stop:
            run.run_status = "stopped"
            run.stop_reason = reason or "policy"
            skip_remaining_steps(db, run.id)
            db.commit()
            continue

        for step in sorted(run.steps, key=lambda s: s.step_index):
            if step.step_status not in ("ready", "to_start"):
                continue
            if step.scheduled_at is None or step.scheduled_at > now:
                continue

            step.step_status = "in_progress"
            step.started_at = now
            db.flush()

            ctx = _parse_context(run)
            ctx["offer_id"] = run.offer_id
            ctx["run_id"] = run.id

            try:
                if step.channel == "email":
                    out = _send_email_placeholder(run.offer_id, run.id, step.id, ctx)
                elif step.channel == "sms":
                    out = _send_sms_placeholder(run.offer_id, run.id, step.id, ctx)
                elif step.channel == "voice_call":
                    out = _voice_retell_placeholder(
                        run.offer_id,
                        run.id,
                        step.id,
                        step.retell_agent_id,
                        ctx,
                    )
                else:
                    out = {"ok": False, "error": "unknown_channel", "channel": step.channel}

                step.step_status = "completed"
                step.completed_at = _utc_now_naive()
                step.last_outcome_summary = json.dumps(out)[:4000]
                _log_event(
                    db,
                    run.id,
                    "step_executed",
                    step_id=step.id,
                    payload={"channel": step.channel, "result": out},
                )
                executed += 1
            except Exception as e:
                logger.exception("Autonomous step failed run_id=%s step_id=%s", run.id, step.id)
                step.step_status = "failed"
                step.last_outcome_summary = str(e)[:4000]
                _log_event(
                    db,
                    run.id,
                    "step_failed",
                    step_id=step.id,
                    payload={"error": str(e), "channel": step.channel},
                )

        db.commit()

        pending = (
            db.query(AutonomousSequenceStep)
            .filter(
                AutonomousSequenceStep.run_id == run.id,
                AutonomousSequenceStep.step_status.in_(("ready", "to_start", "in_progress")),
            )
            .count()
        )
        if pending == 0 and run.run_status == "running":
            run.run_status = "completed"
            run.stop_reason = None
            _log_event(db, run.id, "run_completed", payload={})
            db.commit()

    return executed
