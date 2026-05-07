"""
Autonomous follow-up sequences after Base 2: shared schedule for C&I gas & C&I electricity
comparison follow-up; differentiate in n8n via context (utility_lane, base2_trigger, etc.).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional, Union
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session, joinedload

from models import (
    AutonomousSequenceEvent,
    AutonomousSequenceRun,
    AutonomousSequenceStep,
    AutonomousSequenceTemplate,
    AutonomousSequenceTemplateStep,
    Offer,
)

def _is_postgresql(bind) -> bool:
    return bind.dialect.name == "postgresql"


def _reflect_table_names(insp, bind) -> set[str]:
    """Table names for introspection; avoid schema=public on SQLite (breaks sqlite_master)."""
    names = set(insp.get_table_names())
    if _is_postgresql(bind):
        names |= set(insp.get_table_names(schema="public"))
    return names


def _inspector_schema_kw(bind) -> dict:
    return {"schema": "public"} if _is_postgresql(bind) else {}


def _qualified_table(bind, table: str) -> str:
    return f"public.{table}" if _is_postgresql(bind) else table


def _set_run_validity_date_if_supported(db: Session, run_id: int, validity: date) -> None:
    """Persist validity_date when the DB column exists (safe across mixed env schemas)."""
    if not validity:
        return
    insp = inspect(db.bind)
    tables = _reflect_table_names(insp, db.bind)
    if "autonomous_sequence_runs" not in tables:
        return
    skw = _inspector_schema_kw(db.bind)
    cols = [str(c.get("name") or "") for c in insp.get_columns("autonomous_sequence_runs", **skw)]
    if "validity_date" not in cols:
        return
    runs_tbl = _qualified_table(db.bind, "autonomous_sequence_runs")
    db.execute(
        text(f"UPDATE {runs_tbl} SET validity_date = :validity_date WHERE id = :run_id"),
        {"validity_date": validity, "run_id": run_id},
    )


def delete_autonomous_sequence_run(db: Session, run_id: int) -> bool:
    """Remove a run and all dependent rows (events, steps, context extension table)."""
    run = db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.id == run_id).first()
    if not run:
        return False
    db.query(AutonomousSequenceEvent).filter(AutonomousSequenceEvent.run_id == run_id).delete(
        synchronize_session=False
    )
    db.query(AutonomousSequenceStep).filter(AutonomousSequenceStep.run_id == run_id).delete(
        synchronize_session=False
    )
    # Postgres FK autonomous_sequence_context_run_id_fkey — table may exist only in some envs
    insp = inspect(db.bind)
    tables = _reflect_table_names(insp, db.bind)
    if "autonomous_sequence_context" in tables:
        ctx_tbl = _qualified_table(db.bind, "autonomous_sequence_context")
        db.execute(
            text(f"DELETE FROM {ctx_tbl} WHERE run_id = :run_id"),
            {"run_id": run_id},
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


def _parse_local_time_hhmm(value: str) -> tuple[int, int]:
    raw = (value or "").strip()
    parts = raw.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid send_time_local {value!r}; expected HH:MM")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Invalid send_time_local {value!r}; hour/minute out of range")
    return hour, minute


def _plan_template_times(
    anchor: datetime,
    template_steps: list[AutonomousSequenceTemplateStep],
    *,
    timezone_name: str = AUTONOMOUS_SCHEDULE_TZ,
) -> list[tuple[int, str, datetime, Optional[str], Optional[str]]]:
    """
    Returns tuples:
      (day_number, channel, scheduled_at_utc_naive, prompt_text, retell_agent_id)
    """
    tz = ZoneInfo(timezone_name)
    a = anchor if anchor.tzinfo else anchor.replace(tzinfo=tz)
    local = a.astimezone(tz)
    base_date = local.date()
    day1 = next_business_day(base_date)

    plan: list[tuple[int, str, datetime, Optional[str], Optional[str]]] = []
    ordered_steps = sorted(
        [s for s in template_steps if bool(s.is_active)],
        key=lambda s: s.step_index,
    )
    for s in ordered_steps:
        target_date = day1 + timedelta(days=max(0, int(s.day_number) - 1))
        target_date = ensure_weekday(target_date)
        hh, mm = _parse_local_time_hhmm(s.send_time_local)
        local_dt = datetime.combine(target_date, time(hh, mm), tzinfo=tz)
        plan.append(
            (
                int(s.day_number),
                str(s.channel),
                local_dt.astimezone(timezone.utc).replace(tzinfo=None),
                s.prompt_text,
                s.retell_agent_id,
            )
        )
    return plan


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
        # Accept both historical `email_ID` and snake_case `email_id`.
        "email_ID": _norm(context.get("email_ID") or context.get("email_id")),
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


def ensure_autonomous_sequence_type_row(db: Session, sequence_type: str) -> bool:
    """
    Insert a minimal autonomous_sequence_type row when missing so
    autonomous_sequence_runs.sequence_type FK can resolve. Idempotent.
    Returns True if a row was inserted.
    """
    st = (sequence_type or "").strip()
    if not st:
        return False
    bind = db.bind
    insp = inspect(bind)
    tables = _reflect_table_names(insp, bind)
    if "autonomous_sequence_type" not in tables:
        return False
    skw = _inspector_schema_kw(bind)
    cols = [c.get("name") for c in insp.get_columns("autonomous_sequence_type", **skw)]
    colset = {str(c) for c in cols if c}
    if "sequence_type" not in colset:
        return False
    ast_tbl = _qualified_table(bind, "autonomous_sequence_type")
    exists = db.execute(
        text(f"SELECT 1 FROM {ast_tbl} WHERE sequence_type = :sequence_type LIMIT 1"),
        {"sequence_type": st},
    ).first()
    if exists:
        return False

    default_agent = ""
    agent_was_copied = False
    if "retell_agent_id" in colset:
        for ref_type in ("gas_base2_followup_v1", "ci_electricity_base2_followup_v1"):
            row = db.execute(
                text(
                    f"SELECT retell_agent_id FROM {ast_tbl} "
                    "WHERE sequence_type = :sequence_type AND COALESCE(TRIM(retell_agent_id), '') <> '' LIMIT 1"
                ),
                {"sequence_type": ref_type},
            ).first()
            if row and row[0]:
                default_agent = str(row[0]).strip()
                agent_was_copied = True
                break
        if not default_agent:
            any_row = db.execute(
                text(
                    f"SELECT retell_agent_id FROM {ast_tbl} "
                    "WHERE COALESCE(TRIM(retell_agent_id), '') <> '' LIMIT 1"
                ),
            ).first()
            if any_row and any_row[0]:
                default_agent = str(any_row[0]).strip()
                agent_was_copied = True

    if "retell_agent_id" in colset:
        insert_cols = ["sequence_type", "retell_agent_id"]
        insert_params: dict[str, Union[str, int]] = {
            "sequence_type": st,
            "retell_agent_id": default_agent,
        }
        if "retell_agent_copied" in colset:
            insert_cols.append("retell_agent_copied")
            insert_params["retell_agent_copied"] = 1 if agent_was_copied else 0
        cols_sql = ", ".join(insert_cols)
        vals_sql = ", ".join(f":{c}" for c in insert_cols)
        db.execute(
            text(f"INSERT INTO {ast_tbl} ({cols_sql}) VALUES ({vals_sql})"),
            insert_params,
        )
    else:
        db.execute(
            text(f"INSERT INTO {ast_tbl} (sequence_type) VALUES (:sequence_type)"),
            {"sequence_type": st},
        )
    return True


def ensure_default_sequence_templates(db: Session) -> None:
    """Seed default templates if missing (idempotent)."""
    defaults = [
        {
            "sequence_type": "gas_base2_followup_v1",
            "display_name": "Gas Base 2 Follow-up v1",
            "description": "Default Base 2 cadence for gas offers.",
            "is_restartable": 1,
        },
        {
            "sequence_type": "ci_electricity_base2_followup_v1",
            "display_name": "C&I Electricity Base 2 Follow-up v1",
            "description": "Default Base 2 cadence for C&I electricity offers.",
            "is_restartable": 1,
        },
    ]
    step_defaults = [
        # step_index, day_number, channel, send_time_local
        (0, 1, "email", "09:00"),
        (1, 1, "voice_call", "09:30"),
        (2, 2, "sms", "10:00"),
        (3, 3, "voice_call", "11:00"),
        (4, 3, "email", "11:30"),
    ]
    changed = False
    for d in defaults:
        existing = (
            db.query(AutonomousSequenceTemplate)
            .filter(AutonomousSequenceTemplate.sequence_type == d["sequence_type"])
            .first()
        )
        if existing:
            if ensure_autonomous_sequence_type_row(db, d["sequence_type"]):
                changed = True
            continue
        t = AutonomousSequenceTemplate(
            sequence_type=d["sequence_type"],
            display_name=d["display_name"],
            description=d["description"],
            timezone=AUTONOMOUS_SCHEDULE_TZ,
            is_active=1,
            is_restartable=d["is_restartable"],
        )
        db.add(t)
        db.flush()
        for idx, day_num, channel, hhmm in step_defaults:
            db.add(
                AutonomousSequenceTemplateStep(
                    template_id=t.id,
                    step_index=idx,
                    day_number=day_num,
                    channel=channel,
                    send_time_local=hhmm,
                    prompt_text=None,
                    retell_agent_id=None,
                    is_active=1,
                )
            )
        ensure_autonomous_sequence_type_row(db, d["sequence_type"])
        changed = True
    if changed:
        db.commit()

    # Bootstrap templates from existing run data where needed.
    # This migrates pre-existing sequence types into template-driven scheduling.
    run_types = [
        r[0]
        for r in db.query(AutonomousSequenceRun.sequence_type)
        .distinct()
        .all()
        if isinstance(r[0], str) and r[0].strip()
    ]
    for seq_type in run_types:
        existing_template = (
            db.query(AutonomousSequenceTemplate)
            .filter(AutonomousSequenceTemplate.sequence_type == seq_type)
            .first()
        )
        if existing_template:
            continue
        latest_run = (
            db.query(AutonomousSequenceRun)
            .filter(AutonomousSequenceRun.sequence_type == seq_type)
            .order_by(AutonomousSequenceRun.created_at.desc())
            .first()
        )
        if not latest_run:
            continue
        run_steps = (
            db.query(AutonomousSequenceStep)
            .filter(AutonomousSequenceStep.run_id == latest_run.id)
            .order_by(AutonomousSequenceStep.step_index.asc())
            .all()
        )
        # Even if there are no existing steps, still create a template so staff can edit it.
        template = AutonomousSequenceTemplate(
            sequence_type=seq_type,
            display_name=seq_type.replace("_", " ").replace(" v", " V").title(),
            description="Bootstrapped from existing run data.",
            timezone=AUTONOMOUS_SCHEDULE_TZ,
            is_active=1,
            is_restartable=1,
        )
        db.add(template)
        db.flush()
        tz = ZoneInfo(AUTONOMOUS_SCHEDULE_TZ)
        if run_steps:
            for st in run_steps:
                hhmm = "09:00"
                if st.scheduled_at:
                    hhmm = (
                        st.scheduled_at.replace(tzinfo=timezone.utc)
                        .astimezone(tz)
                        .strftime("%H:%M")
                    )
                db.add(
                    AutonomousSequenceTemplateStep(
                        template_id=template.id,
                        step_index=int(st.step_index),
                        day_number=max(1, int(st.day_number)),
                        channel=str(st.channel),
                        send_time_local=hhmm,
                        prompt_text=None,
                        retell_agent_id=st.retell_agent_id,
                        is_active=1,
                    )
                )
        else:
            db.add(
                AutonomousSequenceTemplateStep(
                    template_id=template.id,
                    step_index=0,
                    day_number=1,
                    channel="email",
                    send_time_local="09:00",
                    prompt_text=None,
                    retell_agent_id=None,
                    is_active=1,
                )
            )
        ensure_autonomous_sequence_type_row(db, seq_type)
        db.commit()


def get_sequence_template_by_type(db: Session, sequence_type: str) -> Optional[AutonomousSequenceTemplate]:
    return (
        db.query(AutonomousSequenceTemplate)
        .options(joinedload(AutonomousSequenceTemplate.steps))
        .filter(AutonomousSequenceTemplate.sequence_type == sequence_type)
        .first()
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
    tpl = get_sequence_template_by_type(db, run.sequence_type)
    if tpl and not bool(tpl.is_restartable):
        raise ValueError("This sequence type is not restartable")
    if not tpl and run.sequence_type not in _RESTARTABLE_SEQUENCE_TYPES:
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
    # Restart should refresh offer validity window from the new anchor.
    if anchor_at.tzinfo is None:
        anchor_utc = anchor_at.replace(tzinfo=timezone.utc)
    else:
        anchor_utc = anchor_at.astimezone(timezone.utc)
    valid_until_utc = anchor_utc + timedelta(days=7)
    valid_until_local = valid_until_utc.astimezone(ZoneInfo(AUTONOMOUS_SCHEDULE_TZ))
    ctx["offer_generated_at"] = anchor_utc.isoformat()
    ctx["offer_valid_until"] = valid_until_utc.isoformat()
    ctx["offer_validity_date"] = valid_until_local.date().isoformat()
    ctx["offer_validity_days"] = 7
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
    context_payload = dict(context or {})
    validity_raw = str(context_payload.get("offer_validity_date") or "").strip()
    run_validity_date: Optional[date] = None
    if validity_raw:
        try:
            run_validity_date = date.fromisoformat(validity_raw[:10])
        except ValueError:
            logger.warning("Invalid offer_validity_date in context: %r", validity_raw)
    if run_validity_date is None:
        anchor_aware_utc = anchor_utc.replace(tzinfo=timezone.utc)
        run_validity_date = (
            (anchor_aware_utc + timedelta(days=7))
            .astimezone(ZoneInfo(AUTONOMOUS_SCHEDULE_TZ))
            .date()
        )
        context_payload.setdefault("offer_validity_date", run_validity_date.isoformat())

    _ = tz  # caller may pass client timezone; scheduling is always AEST
    contact_fields = _context_contact_fields(context_payload)
    run = AutonomousSequenceRun(
        sequence_type=sequence_type,
        offer_id=offer_id,
        client_id=client_id,
        crm_activity_id=crm_activity_id,
        run_status="running",
        anchor_at=anchor_utc,
        timezone=AUTONOMOUS_SCHEDULE_TZ,
        context_json=json.dumps(context_payload) if context_payload else None,
        email_ID=contact_fields["email_ID"],
        contact_phone=contact_fields["contact_phone"],
        contact_name=contact_fields["contact_name"],
        contact_email=contact_fields["contact_email"],
    )
    db.add(run)
    db.flush()
    _set_run_validity_date_if_supported(db, run.id, run_validity_date)

    template = get_sequence_template_by_type(db, sequence_type)
    if template and bool(template.is_active):
        plan = _plan_template_times(
            anchor_at,
            template.steps,
            timezone_name=template.timezone or AUTONOMOUS_SCHEDULE_TZ,
        )
    else:
        fallback_plan = plan_gas_base2_followup_times(anchor_at)
        plan = [(d, c, at, None, None) for d, c, at in fallback_plan]

    ctx_retell_agent_id = context_payload.get("retell_agent_id")

    for idx, (day_num, channel, scheduled_utc_naive, prompt_text, step_retell_agent_id) in enumerate(plan):
        resolved_retell_agent_id = step_retell_agent_id or (
            str(ctx_retell_agent_id) if ctx_retell_agent_id else None
        )
        step = AutonomousSequenceStep(
            run_id=run.id,
            step_index=idx,
            day_number=day_num,
            channel=channel,
            offset_minutes_from_day_start=0,
            step_status="ready",
            scheduled_at=scheduled_utc_naive,
            retell_agent_id=resolved_retell_agent_id if channel == "voice_call" else None,
        )
        db.add(step)

    _log_event(
        db,
        run.id,
        "run_started",
        payload={
            "offer_id": offer_id,
            "steps": len(plan),
            "sequence_type": sequence_type,
            "template_found": bool(template),
            "template_id": template.id if template else None,
        },
    )
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
            if step.scheduled_at is None or step.scheduled_at > now:
                continue
            # Runner only picks `ready`; promote due `to_start` so autonomous_agent_backend can execute.
            if step.step_status == "to_start":
                step.step_status = "ready"
                db.flush()
            if step.step_status != "ready":
                continue

            step.started_at = now
            db.flush()

            ctx = _parse_context(run)
            ctx["offer_id"] = run.offer_id
            ctx["run_id"] = run.id
            template = get_sequence_template_by_type(db, run.sequence_type)
            if template:
                by_idx = {int(ts.step_index): ts for ts in template.steps if bool(ts.is_active)}
                t_step = by_idx.get(int(step.step_index))
                if t_step and t_step.prompt_text:
                    ctx["step_prompt"] = t_step.prompt_text

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

                step.step_status = "executed"
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
                step.step_status = "error"
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
