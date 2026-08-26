"""
Autonomous follow-up sequences after Base 2: shared schedule for C&I gas & C&I electricity
comparison follow-up; differentiate in n8n via context (utility_lane, base2_trigger, etc.).
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from types import SimpleNamespace
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
    OfferActivity,
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
    # Use the Session's connection so reflection does not checkout a second pooled
    # connection mid-transaction (breaks SQLite :memory: / StaticPool tests).
    conn = db.connection()
    insp = inspect(conn)
    tables = _reflect_table_names(insp, conn)
    if "autonomous_sequence_runs" not in tables:
        return
    skw = _inspector_schema_kw(conn)
    cols = [str(c.get("name") or "") for c in insp.get_columns("autonomous_sequence_runs", **skw)]
    if "validity_date" not in cols:
        return
    runs_tbl = _qualified_table(conn, "autonomous_sequence_runs")
    db.execute(
        text(f"UPDATE {runs_tbl} SET validity_date = :validity_date WHERE id = :run_id"),
        {"validity_date": validity, "run_id": run_id},
    )


# --- Offer validity configuration -------------------------------------------
# Validity used to be a hardcoded "anchor + 7 days" in five places. That meant the
# software invented a deadline the retailer never set, and a restart silently moved
# a deadline the client had already been given. Mode + days now live on the sequence
# template. Columns are read defensively: if the migration has not run, callers get
# ("fixed_days", 7) and behaviour is identical to before.

VALIDITY_MODE_NONE = "none"           # never mention validity
VALIDITY_MODE_FIXED_DAYS = "fixed_days"  # anchor + N days (our review window)
VALIDITY_MODE_RETAILER = "retailer_date"  # only a date a human supplied; never invented
VALIDITY_MODES = (VALIDITY_MODE_NONE, VALIDITY_MODE_FIXED_DAYS, VALIDITY_MODE_RETAILER)

DEFAULT_VALIDITY_MODE = VALIDITY_MODE_FIXED_DAYS
DEFAULT_VALIDITY_DAYS = 7


def _template_validity_columns_present(conn) -> bool:
    insp = inspect(conn)
    tables = _reflect_table_names(insp, conn)
    if "autonomous_sequence_templates" not in tables:
        return False
    skw = _inspector_schema_kw(conn)
    cols = {str(c.get("name") or "") for c in insp.get_columns("autonomous_sequence_templates", **skw)}
    return "validity_mode" in cols and "validity_days" in cols


def get_template_validity_config(db: Session, template: Optional[Any]) -> tuple[str, int]:
    """Return (mode, days) for a template, defaulting to today's behaviour."""
    if template is None or getattr(template, "id", None) is None:
        return DEFAULT_VALIDITY_MODE, DEFAULT_VALIDITY_DAYS
    try:
        conn = db.connection()
        if not _template_validity_columns_present(conn):
            return DEFAULT_VALIDITY_MODE, DEFAULT_VALIDITY_DAYS
        tbl = _qualified_table(conn, "autonomous_sequence_templates")
        row = db.execute(
            text(f"SELECT validity_mode, validity_days FROM {tbl} WHERE id = :tid"),
            {"tid": int(template.id)},
        ).first()
    except Exception:  # noqa: BLE001 - never let config lookup break a send
        logger.warning("Validity config lookup failed; using defaults", exc_info=True)
        return DEFAULT_VALIDITY_MODE, DEFAULT_VALIDITY_DAYS
    if not row:
        return DEFAULT_VALIDITY_MODE, DEFAULT_VALIDITY_DAYS
    mode = str(row[0] or "").strip() or DEFAULT_VALIDITY_MODE
    if mode not in VALIDITY_MODES:
        mode = DEFAULT_VALIDITY_MODE
    try:
        days = int(row[1]) if row[1] is not None else DEFAULT_VALIDITY_DAYS
    except (TypeError, ValueError):
        days = DEFAULT_VALIDITY_DAYS
    if days < 1:
        days = DEFAULT_VALIDITY_DAYS
    return mode, days


def set_template_validity_config(
    db: Session, template_id: int, mode: Optional[str], days: Optional[int]
) -> None:
    """Persist validity config when the columns exist (no-op before the migration)."""
    if mode is None and days is None:
        return
    conn = db.connection()
    if not _template_validity_columns_present(conn):
        logger.warning(
            "autonomous_sequence_templates.validity_mode/validity_days missing - "
            "run migrations/add_template_validity_config.sql"
        )
        return
    tbl = _qualified_table(conn, "autonomous_sequence_templates")
    sets, params = [], {"tid": int(template_id)}
    if mode is not None:
        clean = str(mode).strip()
        if clean not in VALIDITY_MODES:
            raise ValueError(f"validity_mode must be one of {VALIDITY_MODES}")
        sets.append("validity_mode = :mode")
        params["mode"] = clean
    if days is not None:
        n = int(days)
        if n < 1 or n > 365:
            raise ValueError("validity_days must be between 1 and 365")
        sets.append("validity_days = :days")
        params["days"] = n
    db.execute(text(f"UPDATE {tbl} SET {', '.join(sets)} WHERE id = :tid"), params)


def clear_validity_context(context: dict[str, Any]) -> None:
    """Strip every validity key and tell the agent not to mention one."""
    for key in (
        "offer_validity_date",
        "offer_valid_until",
        "offer_validity_days",
        "offer_validity_label",
        "validity_date",
        "offer_validity",
    ):
        context.pop(key, None)
    context["omit_validity"] = True


def apply_validity_to_context(
    context: dict[str, Any],
    anchor_utc: datetime,
    mode: str,
    days: int,
    schedule_zi: ZoneInfo,
    explicit_date: Optional[date] = None,
) -> Optional[date]:
    """Single place that decides an offer validity date. Returns the local date, or None.

    - none          -> no validity at all
    - retailer_date -> only an explicitly supplied date; never invents one
    - fixed_days    -> anchor + N days (previous behaviour, N was hardcoded 7)
    """
    if mode == VALIDITY_MODE_NONE:
        clear_validity_context(context)
        return None

    if explicit_date is None and mode == VALIDITY_MODE_RETAILER:
        # No human-supplied retailer expiry: say nothing rather than manufacture one.
        logger.info("validity_mode=retailer_date but no date supplied - omitting validity")
        clear_validity_context(context)
        return None

    if explicit_date is not None:
        valid_local = datetime(
            explicit_date.year, explicit_date.month, explicit_date.day, 12, 0, 0, tzinfo=schedule_zi
        )
    else:
        valid_local = (anchor_utc + timedelta(days=int(days))).astimezone(schedule_zi)

    valid_date = valid_local.date()
    context.pop("omit_validity", None)
    context["offer_validity_date"] = valid_date.isoformat()
    context["offer_valid_until"] = valid_local.astimezone(timezone.utc).isoformat()
    context["validity_date"] = valid_local.strftime("%d/%m/%Y") + " (12pm)"
    if explicit_date is not None:
        context.pop("offer_validity_days", None)
    else:
        context["offer_validity_days"] = int(days)
    return valid_date


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


def _type_row_retell(db: Session, sequence_type: str) -> tuple[str, bool]:
    """Return (retell_agent_id, was_copied_from_another_sequence)."""
    bind = db.bind
    insp = inspect(bind)
    tables = _reflect_table_names(insp, bind)
    if "autonomous_sequence_type" not in tables:
        return "", False
    ast_tbl = _qualified_table(bind, "autonomous_sequence_type")
    skw = _inspector_schema_kw(bind)
    cols = {str(c.get("name") or "") for c in insp.get_columns("autonomous_sequence_type", **skw)}
    select_cols = ["retell_agent_id"]
    if "retell_agent_copied" in cols:
        select_cols.append("retell_agent_copied")
    row = db.execute(
        text(f"SELECT {', '.join(select_cols)} FROM {ast_tbl} WHERE sequence_type = :st LIMIT 1"),
        {"st": sequence_type},
    ).mappings().first()
    if not row:
        return "", False
    agent_id = str(row.get("retell_agent_id") or "").strip()
    copied = False
    if "retell_agent_copied" in row:
        raw = row.get("retell_agent_copied")
        copied = raw in (1, True, "1", "true", "TRUE")
    return agent_id, copied


def _retell_agent_used_elsewhere(db: Session, agent_id: str, sequence_type: str) -> list[str]:
    """Other sequence_type keys that still point at this Retell agent."""
    agent_id = (agent_id or "").strip()
    if not agent_id:
        return []
    others: list[str] = []
    bind = db.bind
    insp = inspect(bind)
    tables = _reflect_table_names(insp, bind)
    if "autonomous_sequence_type" in tables:
        ast_tbl = _qualified_table(bind, "autonomous_sequence_type")
        rows = db.execute(
            text(
                f"SELECT sequence_type FROM {ast_tbl} "
                "WHERE COALESCE(TRIM(retell_agent_id), '') = :aid AND sequence_type <> :st"
            ),
            {"aid": agent_id, "st": sequence_type},
        ).all()
        others.extend(str(r[0]) for r in rows if r and r[0])
    step_rows = (
        db.query(AutonomousSequenceTemplate.sequence_type)
        .join(
            AutonomousSequenceTemplateStep,
            AutonomousSequenceTemplateStep.template_id == AutonomousSequenceTemplate.id,
        )
        .filter(
            AutonomousSequenceTemplateStep.retell_agent_id == agent_id,
            AutonomousSequenceTemplate.sequence_type != sequence_type,
        )
        .distinct()
        .all()
    )
    others.extend(str(r[0]) for r in step_rows if r and r[0])
    seen: list[str] = []
    for key in others:
        if key not in seen:
            seen.append(key)
    return seen


def preview_sequence_template_delete(db: Session, template_id: int) -> Optional[dict[str, Any]]:
    template = (
        db.query(AutonomousSequenceTemplate)
        .options(joinedload(AutonomousSequenceTemplate.steps))
        .filter(AutonomousSequenceTemplate.id == template_id)
        .first()
    )
    if not template:
        return None
    seq_type = str(template.sequence_type)
    run_count = (
        db.query(AutonomousSequenceRun)
        .filter(AutonomousSequenceRun.sequence_type == seq_type)
        .count()
    )
    type_agent, copied = _type_row_retell(db, seq_type)
    step_agents = {
        str(s.retell_agent_id).strip()
        for s in template.steps
        if (s.retell_agent_id or "").strip()
    }
    agent_id = type_agent or next(iter(step_agents), "")
    others = _retell_agent_used_elsewhere(db, agent_id, seq_type) if agent_id else []
    skip_reason = None
    will_delete = False
    if not agent_id:
        skip_reason = "No Retell agent is linked on this sequence."
    elif copied:
        skip_reason = (
            "This sequence still points at a shared default Retell agent, so that agent will not be deleted."
        )
    elif others:
        skip_reason = (
            "This Retell agent is also used by: "
            + ", ".join(others)
            + ". It will be unlinked here, not deleted."
        )
    else:
        will_delete = True
    return {
        "template_id": template.id,
        "sequence_type": seq_type,
        "display_name": template.display_name,
        "run_count": int(run_count),
        "retell_agent_id": agent_id or None,
        "retell_will_delete": will_delete,
        "retell_skip_reason": skip_reason,
    }


def delete_sequence_template_db(db: Session, template_id: int) -> Optional[dict[str, Any]]:
    """Delete template, type row, and all runs of this sequence_type. Does not commit."""
    plan = preview_sequence_template_delete(db, template_id)
    if not plan:
        return None
    seq_type = plan["sequence_type"]
    run_ids = [
        int(row[0])
        for row in db.query(AutonomousSequenceRun.id)
        .filter(AutonomousSequenceRun.sequence_type == seq_type)
        .all()
    ]
    if run_ids:
        db.query(AutonomousSequenceEvent).filter(
            AutonomousSequenceEvent.run_id.in_(run_ids)
        ).delete(synchronize_session=False)
        db.query(AutonomousSequenceStep).filter(
            AutonomousSequenceStep.run_id.in_(run_ids)
        ).delete(synchronize_session=False)
        insp = inspect(db.bind)
        tables = _reflect_table_names(insp, db.bind)
        if "autonomous_sequence_context" in tables:
            ctx_tbl = _qualified_table(db.bind, "autonomous_sequence_context")
            for rid in run_ids:
                db.execute(
                    text(f"DELETE FROM {ctx_tbl} WHERE run_id = :run_id"),
                    {"run_id": rid},
                )
        db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.id.in_(run_ids)).delete(
            synchronize_session=False
        )
    bind = db.bind
    insp = inspect(bind)
    tables = _reflect_table_names(insp, bind)
    if "autonomous_sequence_type" in tables:
        ast_tbl = _qualified_table(bind, "autonomous_sequence_type")
        db.execute(
            text(f"DELETE FROM {ast_tbl} WHERE sequence_type = :st"),
            {"st": seq_type},
        )
    template = db.query(AutonomousSequenceTemplate).filter(
        AutonomousSequenceTemplate.id == template_id
    ).first()
    if template:
        db.delete(template)
    plan["deleted_runs"] = len(run_ids)
    return plan


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
Direct: 0468 050 399<br>
Email: <a href="mailto:business@acesolutions.com.au" style="color:#1a73e8;">business@acesolutions.com.au</a><br>
470 St Kilda Road, Melbourne VIC 3004<br>
Ph: 1300 849 908 | Website: <a href="https://acesolutions.com.au" style="color:#1a73e8;">acesolutions.com.au</a></p>"""

# Default for gas / electricity follow-ups. LLM already writes "Kind regards," — do not repeat it.
ACES_TEAM_FOLLOWUP_SIGNATURE_HTML = """<p style="margin-bottom:0;"><strong>The Team</strong><br>
Australian Circular Economy Solutions</p>
<p style="margin-top:16px; margin-bottom:0;"><strong>Carbon Zero Australasia</strong><br>
Australian Circular Economy Solutions Division<br>
Direct: 0468 050 399<br>
Email: <a href="mailto:business@acesolutions.com.au" style="color:#1a73e8;">business@acesolutions.com.au</a><br>
470 St Kilda Road, Melbourne VIC 3004<br>
Website: <a href="https://acesolutions.com.au" style="color:#1a73e8;">acesolutions.com.au</a></p>"""


def default_signature_html_for_type(sequence_type: str) -> str:
    if "solar" in (sequence_type or "").lower():
        return SOLAR_ENGAGEMENT_SIGNATURE_HTML
    return ACES_TEAM_FOLLOWUP_SIGNATURE_HTML


def _resolve_signature_html(
    sequence_type: str,
    template: Optional[AutonomousSequenceTemplate],
    context: Optional[dict[str, Any]],
) -> str:
    existing = str((context or {}).get("signature_html") or "").strip()
    if existing:
        return existing
    tpl_sig = str(getattr(template, "signature_html", None) or "").strip() if template else ""
    if tpl_sig:
        return tpl_sig
    return default_signature_html_for_type(sequence_type)


def _resolve_extra_context(
    template: Optional[AutonomousSequenceTemplate],
    context: Optional[dict[str, Any]],
) -> str:
    existing = str((context or {}).get("extra_context") or "").strip()
    if existing:
        return existing
    if template is None:
        return ""
    return str(getattr(template, "extra_context", None) or "").strip()


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

# Default when run/template do not supply an IANA zone. Scheduling is per-run via resolve_schedule_tz.
AUTONOMOUS_SCHEDULE_TZ = "Australia/Melbourne"


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _to_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def resolve_schedule_tz(
    run: Optional[AutonomousSequenceRun] = None,
    template: Optional[AutonomousSequenceTemplate] = None,
) -> ZoneInfo:
    """First present of run.timezone → template.timezone → Australia/Melbourne."""
    for raw in (
        run.timezone if run is not None else None,
        template.timezone if template is not None else None,
        AUTONOMOUS_SCHEDULE_TZ,
    ):
        name = (str(raw).strip() if raw is not None else "")
        if name:
            return ZoneInfo(name)
    return ZoneInfo(AUTONOMOUS_SCHEDULE_TZ)


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


def plan_solar_engagement_form_times(
    anchor: datetime,
    timezone_name: str = AUTONOMOUS_SCHEDULE_TZ,
) -> list[tuple[int, str, datetime]]:
    """
    Three follow-up emails after the client has already received the engagement form (n8n send).
    Email 1 at +2 business days, email 2 at +4, email 3 at +6 — all 09:00 local. Returns UTC-naive.
    """
    tz = ZoneInfo(timezone_name)
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


def plan_gas_base2_followup_times(
    anchor: datetime,
    timezone_name: str = AUTONOMOUS_SCHEDULE_TZ,
) -> list[tuple[int, str, datetime]]:
    """Returns (day_number, channel, scheduled_at UTC naive) in the given IANA zone."""
    tz = ZoneInfo(timezone_name)
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


def _to_e164_au(value: Any) -> Optional[str]:
    """Turn AU local numbers into E.164 so Twilio/Retell can dial them.

    0401941385 → +61401941385
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(c for c in text if c.isdigit())
    if not digits:
        return None
    if text.startswith("+"):
        return "+" + digits
    if digits.startswith("61") and len(digits) >= 11:
        return "+" + digits
    if digits.startswith("0") and len(digits) >= 9:
        return "+61" + digits[1:]
    if len(digits) == 9 and digits.startswith("4"):
        return "+61" + digits
    return "+" + digits


def _context_contact_fields(context: dict[str, Any]) -> dict[str, Optional[str]]:
    def _norm(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    return {
        # Accept both historical `email_ID` and snake_case `email_id`.
        "email_ID": _norm(context.get("email_ID") or context.get("email_id")),
        "contact_phone": _to_e164_au(context.get("contact_phone")),
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
        "ci_electricity_offer",
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


_SEQUENCE_TYPE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")


def _copy_autonomous_sequence_type_row(db: Session, old_type: str, new_type: str) -> None:
    """Insert `new_type` as a copy of `old_type` in autonomous_sequence_type (no-op if table missing)."""
    bind = db.bind
    insp = inspect(bind)
    tables = _reflect_table_names(insp, bind)
    if "autonomous_sequence_type" not in tables:
        return
    ast_tbl = _qualified_table(bind, "autonomous_sequence_type")
    skw = _inspector_schema_kw(bind)
    cols = [str(c.get("name") or "") for c in insp.get_columns("autonomous_sequence_type", **skw)]
    if "sequence_type" not in cols:
        return
    exists_new = db.execute(
        text(f"SELECT 1 FROM {ast_tbl} WHERE sequence_type = :st LIMIT 1"),
        {"st": new_type},
    ).first()
    if exists_new:
        return
    src = db.execute(
        text(f"SELECT * FROM {ast_tbl} WHERE sequence_type = :st LIMIT 1"),
        {"st": old_type},
    ).mappings().first()
    if src is None:
        ensure_autonomous_sequence_type_row(db, new_type)
        return
    copy_cols = [c for c in cols if c != "sequence_type" and c.isidentifier()]
    insert_cols = ["sequence_type"] + copy_cols
    params: dict[str, Any] = {"sequence_type": new_type}
    for col in copy_cols:
        params[col] = src.get(col)
    cols_sql = ", ".join(insert_cols)
    vals_sql = ", ".join(f":{c}" for c in insert_cols)
    db.execute(text(f"INSERT INTO {ast_tbl} ({cols_sql}) VALUES ({vals_sql})"), params)


def rename_sequence_template_type(
    db: Session,
    template: AutonomousSequenceTemplate,
    new_type: str,
) -> None:
    """Rename template.sequence_type and cascade to runs + type-prompt row."""
    old = str(template.sequence_type or "").strip()
    new = (new_type or "").strip()
    if not old or new == old:
        return
    if not _SEQUENCE_TYPE_RE.match(new):
        raise ValueError(
            "Call key must start with a letter or number and use only letters, numbers, dots, underscores, or hyphens (max 80)."
        )
    clash = (
        db.query(AutonomousSequenceTemplate)
        .filter(
            AutonomousSequenceTemplate.sequence_type == new,
            AutonomousSequenceTemplate.id != template.id,
        )
        .first()
    )
    if clash:
        raise ValueError(f"Call key already in use: {new}")
    _copy_autonomous_sequence_type_row(db, old, new)
    db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.sequence_type == old).update(
        {AutonomousSequenceRun.sequence_type: new},
        synchronize_session=False,
    )
    template.sequence_type = new
    bind = db.bind
    insp = inspect(bind)
    tables = _reflect_table_names(insp, bind)
    if "autonomous_sequence_type" in tables:
        ast_tbl = _qualified_table(bind, "autonomous_sequence_type")
        db.execute(
            text(f"DELETE FROM {ast_tbl} WHERE sequence_type = :st"),
            {"st": old},
        )


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
            "sequence_type": "ci_electricity_offer",
            "display_name": "C&I Electricity Offer",
            "description": "Utility Invoice Info C&I electricity offer comparison follow-up.",
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
    template: Optional[AutonomousSequenceTemplate] = None,
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
    out["signature_html"] = _resolve_signature_html(run.sequence_type, template, out)
    out["use_html_signature"] = True
    extra = _resolve_extra_context(template, out)
    if extra:
        out["extra_context"] = extra
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
    context and client/activity IDs. Anchor is current time in the resolved schedule timezone.
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

    schedule_zi = resolve_schedule_tz(run, tpl)
    anchor_at = datetime.now(schedule_zi)
    ctx = _parse_context(run)
    # Restart refreshes the validity window from the new anchor, using the template's
    # configured mode/days rather than a hardcoded 7. A restart previously moved a
    # deadline the client had already been told, with no record that it changed.
    if anchor_at.tzinfo is None:
        anchor_utc = anchor_at.replace(tzinfo=timezone.utc)
    else:
        anchor_utc = anchor_at.astimezone(timezone.utc)
    ctx["offer_generated_at"] = anchor_utc.isoformat()
    _restart_template = get_sequence_template_by_type(db, run.sequence_type)
    _restart_mode, _restart_days = get_template_validity_config(db, _restart_template)
    _previous_validity = str(ctx.get("offer_validity_date") or "").strip()
    _restart_explicit: Optional[date] = None
    if _restart_mode == VALIDITY_MODE_RETAILER and _previous_validity:
        try:
            _restart_explicit = date.fromisoformat(_previous_validity[:10])
        except ValueError:
            _restart_explicit = None
    apply_validity_to_context(
        ctx, anchor_utc, _restart_mode, _restart_days, schedule_zi, _restart_explicit
    )
    if _previous_validity and ctx.get("offer_validity_date") != _previous_validity:
        logger.info(
            "Restart moved offer validity for run %s: %s -> %s",
            getattr(run, "id", "?"), _previous_validity, ctx.get("offer_validity_date"),
        )
    client_id = run.client_id if run.client_id is not None else offer.client_id

    out = start_gas_base2_sequence(
        db,
        sequence_type=run.sequence_type,
        offer_id=run.offer_id,
        client_id=client_id,
        crm_activity_id=run.crm_activity_id,
        anchor_at=anchor_at,
        tz=schedule_zi.key,
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
    payload = dict(context or {})
    contact_fields = _context_contact_fields(payload)
    if contact_fields["contact_phone"]:
        payload["contact_phone"] = contact_fields["contact_phone"]
    run.context_json = json.dumps(payload) if payload else None
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
    tz: Optional[str],
    context: dict[str, Any],
) -> AutonomousSequenceRun:
    """Store request timezone (or NULL); schedule via resolve_schedule_tz(run, template)."""
    existing = (
        db.query(AutonomousSequenceRun)
        .filter(
            AutonomousSequenceRun.offer_id == offer_id,
            AutonomousSequenceRun.sequence_type == sequence_type,
            AutonomousSequenceRun.run_status == "running",
        )
        .first()
    )
    dashboard_test = bool((context or {}).get("dashboard_test"))
    if existing:
        if dashboard_test:
            raise ValueError(
                f"This offer already has a running sequence (run #{existing.id}). "
                "Open that run, or pick a different offer. A test uses the offer for comparison "
                "data only — it does not copy the offer."
            )
        logger.warning("Active autonomous run already exists offer_id=%s type=%s", offer_id, sequence_type)
        return existing

    template = get_sequence_template_by_type(db, sequence_type)
    # Explicit request only — do not inject Melbourne (or template) before resolve_schedule_tz.
    request_tz = (tz or "").strip() or None
    schedule_zi = resolve_schedule_tz(
        SimpleNamespace(timezone=request_tz),
        template,
    )
    schedule_tz_name = schedule_zi.key

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
    else:
        validity_raw = str(context_payload.get("offer_validity_date") or "").strip()
        if validity_raw:
            try:
                run_validity_date = date.fromisoformat(validity_raw[:10])
            except ValueError:
                logger.warning("Invalid offer_validity_date in context: %r", validity_raw)
        _mode, _days = get_template_validity_config(db, template)
        anchor_aware_utc = anchor_utc.replace(tzinfo=timezone.utc)
        run_validity_date = apply_validity_to_context(
            context_payload, anchor_aware_utc, _mode, _days, schedule_zi, run_validity_date
        )

    context_payload["signature_html"] = _resolve_signature_html(
        sequence_type, template, context_payload
    )
    context_payload["use_html_signature"] = True
    extra = _resolve_extra_context(template, context_payload)
    if extra:
        context_payload["extra_context"] = extra

    contact_fields = _context_contact_fields(context_payload)
    if contact_fields["contact_phone"]:
        context_payload["contact_phone"] = contact_fields["contact_phone"]
    run = AutonomousSequenceRun(
        sequence_type=sequence_type,
        offer_id=offer_id,
        client_id=client_id,
        crm_activity_id=crm_activity_id,
        run_status="running",
        anchor_at=anchor_utc,
        timezone=schedule_tz_name,
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

    if sequence_type == SOLAR_ENGAGEMENT_FORM_SEQUENCE_TYPE:
        fallback_plan = plan_solar_engagement_form_times(anchor_at, timezone_name=schedule_tz_name)
        plan = [(d, c, at, None, None) for d, c, at in fallback_plan]
    elif template and bool(template.is_active):
        plan = _plan_template_times(
            anchor_at,
            template.steps,
            timezone_name=schedule_tz_name,
        )
    else:
        fallback_plan = plan_gas_base2_followup_times(anchor_at, timezone_name=schedule_tz_name)
        plan = [(d, c, at, None, None) for d, c, at in fallback_plan]

    if dashboard_test and plan:
        # Production Day 1 is the next business day, so a test started today is not due yet.
        # Shift the whole cadence so the first step is due immediately.
        first_at = plan[0][2]
        delta = _utc_now_naive() - first_at
        plan = [(d, c, at + delta, p, r) for d, c, at, p, r in plan]

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

            try:
                out = _send_one_step(db, run, step)
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


def _send_one_step(db: Session, run: AutonomousSequenceRun, step: AutonomousSequenceStep) -> dict[str, Any]:
    ctx = _parse_context(run)
    ctx["offer_id"] = run.offer_id
    ctx["run_id"] = run.id
    e164 = _to_e164_au(ctx.get("contact_phone") or run.contact_phone)
    if e164:
        ctx["contact_phone"] = e164
    template = get_sequence_template_by_type(db, run.sequence_type)
    if template:
        by_idx = {int(ts.step_index): ts for ts in template.steps if bool(ts.is_active)}
        t_step = by_idx.get(int(step.step_index))
        if t_step and t_step.prompt_text:
            ctx["step_prompt"] = t_step.prompt_text
    if step.channel == "email":
        email_ctx = _prepare_email_context(run, step, ctx, template)
        return _send_email_placeholder(run.offer_id, run.id, step.id, email_ctx)
    if step.channel == "sms":
        return _send_sms_placeholder(run.offer_id, run.id, step.id, ctx)
    if step.channel == "voice_call":
        return _voice_retell_placeholder(
            run.offer_id,
            run.id,
            step.id,
            step.retell_agent_id,
            ctx,
        )
    if step.channel == "engagement_form_generation":
        ctx.setdefault("engagement_form_type", SOLAR_PANEL_CLEANING_ENGAGEMENT_FORM_TYPE)
        return _execute_engagement_form_generation(db, run.offer_id, run.id, step.id, ctx)
    return {"ok": False, "error": "unknown_channel", "channel": step.channel}


def execute_step_now(db: Session, run_id: int, step_id: int) -> dict[str, Any]:
    """Send one step immediately from this API's database, ignoring scheduled_at."""
    run = (
        db.query(AutonomousSequenceRun)
        .options(joinedload(AutonomousSequenceRun.steps))
        .filter(AutonomousSequenceRun.id == run_id)
        .first()
    )
    if not run:
        raise ValueError("Run not found")
    if run.run_status != "running":
        raise ValueError(f"Run is {run.run_status}, not running")
    step = next((s for s in run.steps if s.id == step_id), None)
    if step is None:
        raise ValueError("Step not found on this run")
    if step.step_status == "to_start":
        step.step_status = "ready"
        db.flush()
    if step.step_status != "ready":
        raise ValueError(f"Step is {step.step_status}, not ready")

    now = _utc_now_naive()
    step.started_at = now
    db.flush()
    try:
        out = _send_one_step(db, run, step)
    except Exception as e:
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
        raise
    if out.get("mode") == "placeholder":
        step.step_status = "ready"
        step.started_at = None
        db.commit()
        raise ValueError(
            "This API cannot actually send this step (email/SMS webhook or Retell key missing), "
            "and the send worker could not see the step in its database."
        )
    if out.get("ok") is False:
        step.step_status = "error"
        step.last_outcome_summary = json.dumps(out)[:4000]
        _log_event(
            db,
            run.id,
            "step_failed",
            step_id=step.id,
            payload={"error": out, "channel": step.channel},
        )
        db.commit()
        raise ValueError(str(out.get("error") or "Send failed"))

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
    pending = (
        db.query(AutonomousSequenceStep)
        .filter(
            AutonomousSequenceStep.run_id == run.id,
            AutonomousSequenceStep.id != step.id,
            AutonomousSequenceStep.step_status.in_(("ready", "to_start", "in_progress")),
        )
        .count()
    )
    if pending == 0 and run.run_status == "running":
        run.run_status = "completed"
        run.stop_reason = None
        _log_event(db, run.id, "run_completed", payload={})
    db.commit()
    db.refresh(run)
    return {"ok": True, "run_id": run.id, "step_id": step.id, "result": out}


def _load_type_prompts(db: Session, sequence_type: str) -> dict[str, str]:
    out = {
        "retell_agent_id": "",
        "email_system_prompt": "",
        "email_example": "",
        "sms_system_prompt": "",
        "sms_example": "",
    }
    bind = db.bind
    insp = inspect(bind)
    tables = _reflect_table_names(insp, bind)
    if "autonomous_sequence_type" not in tables:
        return out
    ast_tbl = _qualified_table(bind, "autonomous_sequence_type")
    cols = {
        str(c.get("name") or "")
        for c in insp.get_columns("autonomous_sequence_type", **_inspector_schema_kw(bind))
    }
    wanted = [k for k in out if k in cols]
    if not wanted:
        return out
    row = (
        db.execute(
            text(f"SELECT {', '.join(wanted)} FROM {ast_tbl} WHERE sequence_type = :st LIMIT 1"),
            {"st": sequence_type},
        )
        .mappings()
        .first()
    )
    if not row:
        return out
    for key in out:
        raw = row.get(key)
        if raw:
            out[key] = str(raw)
    return out


def _activity_meta(db: Session, run: AutonomousSequenceRun) -> dict[str, Any]:
    if not run.crm_activity_id:
        return {}
    activity = db.get(OfferActivity, run.crm_activity_id)
    if not activity or activity.metadata_ is None:
        return {}
    raw = activity.metadata_
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _flatten_worker_variables(ctx: dict[str, Any], activity_meta: dict[str, Any]) -> dict[str, str]:
    flat: dict[str, Any] = {}
    snap = ctx.get("comparison_snapshot")
    if isinstance(snap, dict):
        for key, value in snap.items():
            if value is None or isinstance(value, (dict, list)):
                continue
            flat[key] = value
    for key, value in ctx.items():
        if key in ("comparison_snapshot", "signature_html"):
            continue
        if value is None or isinstance(value, (dict, list)):
            continue
        flat[key] = value
    for key, value in activity_meta.items():
        if value is None or isinstance(value, (dict, list)):
            continue
        flat[key] = value
    label = str(flat.get("offer_validity_label") or "").strip()
    if label:
        flat["validity_date"] = label
    out = {str(k): str(v) for k, v in flat.items()}
    converted = _to_e164_au(out.get("contact_phone"))
    if converted:
        out["contact_phone"] = converted
    return out


def _jsonable_context(ctx: dict[str, Any], activity_meta: dict[str, Any]) -> dict[str, Any]:
    merged = {**ctx, **activity_meta}
    try:
        return json.loads(json.dumps(merged, default=str))
    except (TypeError, ValueError):
        return {str(k): str(v) for k, v in merged.items() if v is not None}


def export_step_action(db: Session, run_id: int, step_id: int) -> dict[str, Any]:
    """Build a worker Action payload from this API's database (the one the dashboard reads)."""
    run = (
        db.query(AutonomousSequenceRun)
        .options(joinedload(AutonomousSequenceRun.steps))
        .filter(AutonomousSequenceRun.id == run_id)
        .first()
    )
    if not run:
        raise ValueError("Run not found")
    if run.run_status != "running":
        raise ValueError(f"Run is {run.run_status}, not running")
    step = next((s for s in run.steps if s.id == step_id), None)
    if step is None:
        raise ValueError("Step not found on this run")
    if step.step_status == "to_start":
        step.step_status = "ready"
        db.flush()
    if step.step_status != "ready":
        raise ValueError(f"Step is {step.step_status}, not ready")

    ctx = _parse_context(run)
    ctx["offer_id"] = run.offer_id
    ctx["run_id"] = run.id
    template = get_sequence_template_by_type(db, run.sequence_type)
    extra = _resolve_extra_context(template, ctx)
    if extra:
        ctx["extra_context"] = extra
    if template:
        by_idx = {int(ts.step_index): ts for ts in template.steps if bool(ts.is_active)}
        t_step = by_idx.get(int(step.step_index))
        if t_step and t_step.prompt_text:
            ctx["step_prompt"] = t_step.prompt_text
    prompts = _load_type_prompts(db, run.sequence_type)
    activity_meta = _activity_meta(db, run)
    channel = (step.channel or "").strip()
    email = str(ctx.get("contact_email") or run.contact_email or "").strip()
    phone = _to_e164_au(ctx.get("contact_phone") or run.contact_phone) or ""

    if channel == "email":
        if not email:
            raise ValueError("No contact email on this run")
        email_ctx = _prepare_email_context(run, step, ctx, template)
        payload = {
            "to": email,
            "email_id": str(run.email_ID or email_ctx.get("email_ID") or email_ctx.get("email_id") or ""),
            "context": _jsonable_context(email_ctx, activity_meta),
            "system_prompt": prompts["email_system_prompt"],
            "example": prompts["email_example"],
        }
        action_type = "email"
    elif channel == "sms":
        if not phone:
            raise ValueError("No contact phone on this run")
        payload = {
            "to": phone,
            "context": _jsonable_context(ctx, activity_meta),
            "system_prompt": prompts["sms_system_prompt"],
            "example": prompts["sms_example"],
        }
        action_type = "sms"
    elif channel == "voice_call":
        if not phone:
            raise ValueError("No contact phone on this run")
        payload = {
            "to": phone,
            "agent_id": step.retell_agent_id or prompts["retell_agent_id"],
            "dynamic_variables": _flatten_worker_variables(ctx, activity_meta),
        }
        action_type = "phone_call"
    else:
        raise ValueError(f"Channel {channel!r} cannot be sent via the worker")

    return {
        "action_type": action_type,
        "step_id": step.id,
        "run_id": run.id,
        "payload": payload,
        "channel": channel,
    }


def mark_step_dispatched(
    db: Session,
    run_id: int,
    step_id: int,
    success: bool,
    summary: Optional[str] = None,
) -> AutonomousSequenceRun:
    run = (
        db.query(AutonomousSequenceRun)
        .options(joinedload(AutonomousSequenceRun.steps))
        .filter(AutonomousSequenceRun.id == run_id)
        .first()
    )
    if not run:
        raise ValueError("Run not found")
    step = next((s for s in run.steps if s.id == step_id), None)
    if step is None:
        raise ValueError("Step not found on this run")
    now = _utc_now_naive()
    if success:
        step.step_status = "executed"
        step.started_at = step.started_at or now
        step.completed_at = now
        if summary:
            step.last_outcome_summary = summary[:4000]
        _log_event(
            db,
            run.id,
            "step_executed",
            step_id=step.id,
            payload={"channel": step.channel, "result": summary},
        )
    else:
        step.step_status = "error"
        step.completed_at = now
        if summary:
            step.last_outcome_summary = summary[:4000]
        _log_event(
            db,
            run.id,
            "step_failed",
            step_id=step.id,
            payload={"error": summary, "channel": step.channel},
        )
    db.flush()
    pending = (
        db.query(AutonomousSequenceStep)
        .filter(
            AutonomousSequenceStep.run_id == run.id,
            AutonomousSequenceStep.step_status.in_(("ready", "to_start", "in_progress")),
        )
        .count()
    )
    if success and pending == 0 and run.run_status == "running":
        run.run_status = "completed"
        run.stop_reason = None
        _log_event(db, run.id, "run_completed", payload={})
    db.commit()
    db.refresh(run)
    return run
