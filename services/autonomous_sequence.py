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
N8N_ENGAGEMENT_FORM_URL = os.getenv("N8N_AUTONOMOUS_ENGAGEMENT_FORM_WEBHOOK_URL", "").strip()

SOLAR_PANEL_CLEANING_ENGAGEMENT_FORM_TYPE = "Solar Panel Cleaning"
SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE = "solar_panel_cleaning_engagement_form_v1"

SOLAR_ENGAGEMENT_INITIAL_SUBJECT = (
    "Solar cleaning — quick win to protect performance and your solar investment"
)

# HTML signature aligned with the Document Generation / send-eoi outbound email.
SOLAR_ENGAGEMENT_SIGNATURE_HTML = """<p style="margin-bottom:0;"><strong>Amelia Williams</strong><br>
<span style="color:#666;">Customer Success Manager (CSM) – Implementation: Connects onboarding directly to future success.</span></p>
<p style="margin-top:16px; margin-bottom:0;"><strong>Carbon Zero Australasia</strong><br>
Australian Circular Economy Solutions Division<br>
Direct: 1300 938 638<br>
Email: <a href="mailto:business@acesolutions.com.au" style="color:#1a73e8;">business@acesolutions.com.au</a><br>
470 St Kilda Road, Melbourne VIC 3004<br>
Ph: 1300 849 908 | Website: <a href="https://acesolutions.com.au" style="color:#1a73e8;">acesolutions.com.au</a></p>"""

SOLAR_ENGAGEMENT_SYSTEM_PROMPT = """You write follow-up emails for ACES Solar Panel Cleaning engagement forms.

The client already received the initial email with the engagement form and testimonial PDFs attached. These follow-ups must REPLY on that Gmail thread (do not start a new email). Do not include Google Drive links — the client cannot access them; attachments are on the original message.

Solar Panel Cleaning engagement forms do NOT have offer validity dates — never mention "valid until", expiry, or deadlines unless explicitly provided in context.

Tone: professional, warm, Australian English. Sign as Amelia Williams with the HTML signature provided in context.

Step 0: light follow-up. Step 1: polite reminder. Step 2: final friendly nudge (offer to close out if not proceeding). Keep body under 120 words before the signature."""

SOLAR_ENGAGEMENT_EMAIL_EXAMPLE = """Hi {{contact_name}},

Just following up on the Solar Panel Cleaning engagement form for {{business_name}}.

Regular cleaning helps protect generation and your solar investment — dust and buildup can reduce output over time. If you're happy to proceed, we only need the signed Engagement Form returned so we can lock in the next steps.

Happy to run through the form or answer any questions.

Best regards,"""

SOLAR_ENGAGEMENT_STEP_PROMPTS: tuple[str, str, str] = (
    "Follow-up 1 (reply on thread): gentle check-in; no validity date; no Drive links; do not re-attach files.",
    "Follow-up 2 (reply on thread): polite reminder to return signed engagement form; no validity; no links.",
    "Follow-up 3 (reply on thread): final friendly nudge; offer to close out if not proceeding; no validity; no links.",
)

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


def add_business_days(start: date, business_days: int) -> date:
    """Advance `start` by `business_days` Mon–Fri days (weekends skipped)."""
    if business_days <= 0:
        return ensure_weekday(start)
    current = start
    added = 0
    while added < business_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


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


def plan_solar_engagement_form_times(anchor: datetime) -> list[tuple[int, str, datetime]]:
    """
    Three follow-up emails after the client has already received the engagement form (n8n send).
    Email 1 at +2 business days, email 2 at +4, email 3 at +6 — all 09:00 AEST. Returns UTC-naive.
    """
    tz = ZoneInfo(AUTONOMOUS_SCHEDULE_TZ)
    a = anchor if anchor.tzinfo else anchor.replace(tzinfo=timezone.utc)
    base_date = a.astimezone(tz).date()
    out: list[tuple[int, str, datetime]] = []
    for step_num, offset in enumerate((2, 4, 6), start=1):
        target_date = add_business_days(base_date, offset)
        local_dt = datetime.combine(target_date, time(9, 0), tzinfo=tz)
        out.append(
            (
                step_num,
                "email",
                local_dt.astimezone(timezone.utc).replace(tzinfo=None),
            )
        )
    return out


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
        {
            "sequence_type": SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE,
            "display_name": "Solar Panel Cleaning — Engagement Form v1",
            "description": (
                "Three follow-up emails (every 2 business days) after the engagement form is "
                "emailed to the client from Document Generation."
            ),
            "is_restartable": 0,
        },
        {
            "sequence_type": "solar_panel_cleaning_followup_v1",
            "display_name": "Solar Panel Cleaning Follow-up v1",
            "description": "Outreach cadence (email, voice, SMS) after solar cleaning quote sent.",
            "is_restartable": 0,
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
    solar_engagement_step_defaults = [
        (0, 1, "email", "09:00"),
        (1, 2, "email", "09:00"),
        (2, 3, "email", "09:00"),
    ]
    solar_followup_step_defaults = step_defaults
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
        if d["sequence_type"] == SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE:
            steps_for_template = solar_engagement_step_defaults
        elif d["sequence_type"] == "solar_panel_cleaning_followup_v1":
            steps_for_template = solar_followup_step_defaults
        else:
            steps_for_template = step_defaults
        for idx, day_num, channel, hhmm in steps_for_template:
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

    if sync_solar_engagement_form_template_steps(db):
        db.commit()
    elif sync_solar_engagement_step_prompts_only(db):
        db.commit()
    if ensure_solar_engagement_type_prompts(db):
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


def sync_solar_engagement_form_template_steps(db: Session) -> bool:
    """
    Keep the engagement-form template aligned with the 3× email / 2-business-day cadence.
    Upgrades legacy single-step templates idempotently.
    """
    tpl = get_sequence_template_by_type(db, SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE)
    if not tpl:
        return False
    steps = sorted(
        [s for s in tpl.steps if bool(s.is_active)],
        key=lambda s: int(s.step_index),
    )
    needs_sync = len(steps) != 3 or any(str(s.channel) != "email" for s in steps)
    if not needs_sync:
        new_desc = (
            "Three follow-up emails (every 2 business days) after the engagement form is "
            "emailed to the client from Document Generation."
        )
        if (tpl.description or "").strip() != new_desc:
            tpl.description = new_desc
            return True
        return False

    db.query(AutonomousSequenceTemplateStep).filter(
        AutonomousSequenceTemplateStep.template_id == tpl.id
    ).delete(synchronize_session=False)
    step_rows = [
        (0, 1, "email", "09:00", SOLAR_ENGAGEMENT_STEP_PROMPTS[0]),
        (1, 2, "email", "09:00", SOLAR_ENGAGEMENT_STEP_PROMPTS[1]),
        (2, 3, "email", "09:00", SOLAR_ENGAGEMENT_STEP_PROMPTS[2]),
    ]
    for idx, day_num, channel, hhmm, prompt in step_rows:
        db.add(
            AutonomousSequenceTemplateStep(
                template_id=tpl.id,
                step_index=idx,
                day_number=day_num,
                channel=channel,
                send_time_local=hhmm,
                prompt_text=prompt,
                retell_agent_id=None,
                is_active=1,
            )
        )
    tpl.description = (
        "Three follow-up emails (every 2 business days) after the engagement form is "
        "emailed to the client from Document Generation. Replies on the original Gmail thread."
    )
    logger.info("Synced solar engagement form template to 3 email steps (template_id=%s)", tpl.id)
    return True


def ensure_solar_engagement_type_prompts(db: Session) -> bool:
    """Seed / refresh autonomous_sequence_type prompts for solar engagement follow-ups."""
    if not ensure_autonomous_sequence_type_row(db, SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE):
        return False
    insp = inspect(db.bind)
    tables = _reflect_table_names(insp, db.bind)
    if "autonomous_sequence_type" not in tables:
        return False
    ast_tbl = _qualified_table(db.bind, "autonomous_sequence_type")
    row = db.execute(
        text(f"SELECT system_prompt, email_example FROM {ast_tbl} WHERE sequence_type = :st LIMIT 1"),
        {"st": SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE},
    ).mappings().first()
    if not row:
        return False
    cur_sys = str(row.get("system_prompt") or "")
    cur_email = str(row.get("email_example") or "")
    needs = (
        not cur_sys.strip()
        or not cur_email.strip()
        or "valid until" in cur_email.lower()
        or "access the document" in cur_email.lower()
        or "drive.google" in cur_email.lower()
    )
    if not needs:
        return False
    db.execute(
        text(
            f"UPDATE {ast_tbl} SET system_prompt = :system_prompt, email_example = :email_example "
            "WHERE sequence_type = :st"
        ),
        {
            "st": SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE,
            "system_prompt": SOLAR_ENGAGEMENT_SYSTEM_PROMPT,
            "email_example": SOLAR_ENGAGEMENT_EMAIL_EXAMPLE,
        },
    )
    logger.info("Updated solar engagement form type prompts in autonomous_sequence_type")
    return True


def sync_solar_engagement_step_prompts_only(db: Session) -> bool:
    """Update step prompt_text on existing 3-step template without resetting schedules."""
    tpl = get_sequence_template_by_type(db, SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE)
    if not tpl or len(tpl.steps) != 3:
        return False
    changed = False
    ordered = sorted([s for s in tpl.steps if bool(s.is_active)], key=lambda s: int(s.step_index))
    for i, st in enumerate(ordered):
        if i >= len(SOLAR_ENGAGEMENT_STEP_PROMPTS):
            break
        want = SOLAR_ENGAGEMENT_STEP_PROMPTS[i]
        if (st.prompt_text or "").strip() != want:
            st.prompt_text = want
            changed = True
    return changed


def _prepare_email_context(
    run: AutonomousSequenceRun,
    step: AutonomousSequenceStep,
    ctx: dict[str, Any],
) -> dict[str, Any]:
    out = dict(ctx)
    out["sequence_type"] = run.sequence_type
    out["step_index"] = int(step.step_index)
    msg_id = str(run.email_ID or out.get("email_ID") or out.get("email_id") or "").strip()
    if msg_id:
        out["email_ID"] = msg_id
        out["email_id"] = msg_id
        out["gmail_message_id"] = msg_id
    thread_id = str(
        out.get("gmail_thread_id") or out.get("thread_id") or out.get("gmail_threadId") or ""
    ).strip()
    if thread_id:
        out["gmail_thread_id"] = thread_id
        out["thread_id"] = thread_id
    if run.sequence_type == SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE:
        out["reply_in_thread"] = True
        out["omit_validity"] = True
        out["omit_document_links"] = True
        out.pop("offer_validity_date", None)
        out.pop("offer_valid_until", None)
        out.pop("offer_validity_days", None)
        out.setdefault("initial_email_subject", SOLAR_ENGAGEMENT_INITIAL_SUBJECT)
        out.setdefault("signature_html", SOLAR_ENGAGEMENT_SIGNATURE_HTML)
        out.setdefault("use_html_signature", True)
    return out


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
    run_validity_date: Optional[date] = None
    if sequence_type == SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE:
        context_payload.pop("offer_validity_date", None)
        context_payload.pop("offer_valid_until", None)
        context_payload.pop("offer_validity_days", None)
        context_payload.setdefault("reply_in_thread", True)
        context_payload.setdefault("omit_validity", True)
        context_payload.setdefault("omit_document_links", True)
        context_payload.setdefault("initial_email_subject", SOLAR_ENGAGEMENT_INITIAL_SUBJECT)
        context_payload.setdefault("signature_html", SOLAR_ENGAGEMENT_SIGNATURE_HTML)
    else:
        validity_raw = str(context_payload.get("offer_validity_date") or "").strip()
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
    if run_validity_date is not None:
        _set_run_validity_date_if_supported(db, run.id, run_validity_date)

    template = get_sequence_template_by_type(db, sequence_type)
    if sequence_type == SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE:
        fallback_plan = plan_solar_engagement_form_times(anchor_at)
        plan = [(d, c, at, None, None) for d, c, at in fallback_plan]
    elif template and bool(template.is_active):
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


def _str_from_ctx(ctx: dict[str, Any], *keys: str) -> str:
    for key in keys:
        raw = ctx.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text and text.lower() not in ("n/a", "none", "null"):
            return text
    return ""


def _engagement_form_fields_from_context(context: dict[str, Any]) -> dict[str, str]:
    """Build engagement form merge fields; prefer context, then optional n8n business lookup."""
    business_name = _str_from_ctx(context, "business_name", "site_name", "client_name")
    fields = {
        "business_name": business_name,
        "abn": _str_from_ctx(context, "abn"),
        "trading_as": _str_from_ctx(context, "trading_as", "trading_name"),
        "postal_address": _str_from_ctx(context, "postal_address"),
        "site_address": _str_from_ctx(context, "site_address", "street_address"),
        "telephone": _str_from_ctx(context, "telephone", "contact_phone", "phone"),
        "email": _str_from_ctx(context, "contact_email", "email"),
        "contact_name": _str_from_ctx(context, "contact_name", "site_contact"),
        "position": _str_from_ctx(context, "position"),
        "client_folder_url": _str_from_ctx(context, "client_folder_url"),
        "engagement_form_type": _str_from_ctx(
            context, "engagement_form_type"
        ) or SOLAR_PANEL_CLEANING_ENGAGEMENT_FORM_TYPE,
    }
    if business_name and not fields["client_folder_url"]:
        try:
            from tools.business_info import get_business_information

            info = get_business_information(business_name)
            if isinstance(info, dict) and info.get("business_details"):
                bd = info.get("business_details") or {}
                ci = info.get("contact_information") or {}
                rd = info.get("representative_details") or {}
                gdrive = info.get("gdrive") or {}
                fields["abn"] = fields["abn"] or str(bd.get("abn") or "").strip()
                fields["trading_as"] = fields["trading_as"] or str(
                    bd.get("trading_name") or bd.get("name") or ""
                ).strip()
                fields["postal_address"] = fields["postal_address"] or str(
                    ci.get("postal_address") or ""
                ).strip()
                fields["site_address"] = fields["site_address"] or str(
                    ci.get("site_address") or ""
                ).strip()
                fields["telephone"] = fields["telephone"] or str(ci.get("telephone") or "").strip()
                fields["email"] = fields["email"] or str(ci.get("email") or "").strip()
                fields["contact_name"] = fields["contact_name"] or str(
                    rd.get("contact_name") or ""
                ).strip()
                fields["position"] = fields["position"] or str(rd.get("position") or "").strip()
                fields["client_folder_url"] = fields["client_folder_url"] or str(
                    gdrive.get("folder_url") or ""
                ).strip()
                fields["business_name"] = fields["business_name"] or str(bd.get("name") or "").strip()
        except Exception:
            logger.exception(
                "[autonomous] business lookup failed for engagement form business_name=%s",
                business_name,
            )
    return fields


def _execute_engagement_form_generation(
    db: Session,
    offer_id: int,
    run_id: int,
    step_id: int,
    context: dict[str, Any],
) -> dict[str, Any]:
    if N8N_ENGAGEMENT_FORM_URL:
        payload = {
            "channel": "engagement_form_generation",
            "offer_id": offer_id,
            "run_id": run_id,
            "step_id": step_id,
            "context": context,
            "engagement_form_type": context.get("engagement_form_type")
            or SOLAR_PANEL_CLEANING_ENGAGEMENT_FORM_TYPE,
        }
        with httpx.Client(timeout=120.0) as client:
            r = client.post(N8N_ENGAGEMENT_FORM_URL, json=payload)
            r.raise_for_status()
            try:
                return {"ok": True, "channel": "engagement_form_generation", "response": r.json()}
            except Exception:
                return {
                    "ok": True,
                    "channel": "engagement_form_generation",
                    "response_text": r.text[:2000],
                }

    from crm_enums import OfferActivityType
    from services.crm import create_offer_activity
    from tools.document_generation import engagement_form_generation

    fields = _engagement_form_fields_from_context(context)
    if not fields["business_name"]:
        return {"ok": False, "error": "business_name missing in sequence context"}

    result = engagement_form_generation(
        business_name=fields["business_name"],
        abn=fields.get("abn") or "",
        trading_as=fields.get("trading_as") or fields["business_name"],
        postal_address=fields.get("postal_address") or "",
        site_address=fields.get("site_address") or "",
        telephone=fields.get("telephone") or "",
        email=fields.get("email") or "",
        contact_name=fields.get("contact_name") or "",
        position=fields.get("position") or "",
        engagement_form_type=fields["engagement_form_type"],
        client_folder_url=fields.get("client_folder_url") or "",
    )

    offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if (
        isinstance(result, dict)
        and result.get("status") == "success"
        and offer is not None
    ):
        doc_link = result.get("document_link")
        try:
            client = None
            if offer.client_id:
                from models import Client

                client = db.query(Client).filter(Client.id == offer.client_id).first()
            create_offer_activity(
                db,
                offer=offer,
                client=client,
                activity_type=OfferActivityType.ENGAGEMENT_FORM,
                document_link=doc_link if isinstance(doc_link, str) else None,
                metadata={
                    "form_type": fields["engagement_form_type"],
                    "source": "autonomous_solar_engagement_form_v1",
                    "run_id": run_id,
                    "step_id": step_id,
                },
                created_by="autonomous_agent",
            )
            db.commit()
        except Exception:
            logger.exception(
                "[autonomous] failed to log engagement_form activity offer_id=%s run_id=%s",
                offer_id,
                run_id,
            )
        run = db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.id == run_id).first()
        if run:
            _merge_context(
                db,
                run,
                {
                    "engagement_form_document_link": doc_link,
                    "engagement_form_type": fields["engagement_form_type"],
                },
            )
            db.commit()

    ok = isinstance(result, dict) and result.get("status") == "success"
    return {
        "ok": ok,
        "channel": "engagement_form_generation",
        "result": result if isinstance(result, dict) else {"raw": str(result)[:500]},
    }


def _send_email_placeholder(offer_id: int, run_id: int, step_id: int, context: dict[str, Any]) -> dict[str, Any]:
    if not N8N_EMAIL_URL:
        logger.info(
            "[autonomous] email webhook not set; placeholder offer_id=%s run_id=%s step_id=%s",
            offer_id,
            run_id,
            step_id,
        )
        return {"ok": True, "mode": "placeholder", "channel": "email"}
    payload = {
        "channel": "email",
        "offer_id": offer_id,
        "run_id": run_id,
        "step_id": step_id,
        "sequence_type": context.get("sequence_type"),
        "step_index": context.get("step_index"),
        "reply_in_thread": bool(context.get("reply_in_thread")),
        "gmail_message_id": context.get("gmail_message_id") or context.get("email_ID") or context.get("email_id"),
        "gmail_thread_id": context.get("gmail_thread_id") or context.get("thread_id"),
        "omit_validity": bool(context.get("omit_validity")),
        "omit_document_links": bool(context.get("omit_document_links")),
        "initial_email_subject": context.get("initial_email_subject"),
        "signature_html": context.get("signature_html"),
        "use_html_signature": context.get("use_html_signature"),
        "context": context,
    }
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
                    email_ctx = _prepare_email_context(run, step, ctx)
                    out = _send_email_placeholder(run.offer_id, run.id, step.id, email_ctx)
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
                elif step.channel == "engagement_form_generation":
                    ctx.setdefault(
                        "engagement_form_type",
                        SOLAR_PANEL_CLEANING_ENGAGEMENT_FORM_TYPE,
                    )
                    out = _execute_engagement_form_generation(
                        db, run.offer_id, run.id, step.id, ctx
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
