"""Campaign persistence, test-send, stub offers, and bulk start."""

from __future__ import annotations

import json
import logging
import os
import hmac
import hashlib
import base64
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Optional
from urllib.parse import parse_qs, quote, urlparse

import httpx
from sqlalchemy.orm import Session
from sqlalchemy import func, inspect, text

from crm_enums import OfferStatus
from models import (
    AutonomousSequenceEvent,
    AutonomousSequenceRun,
    AutonomousSequenceStep,
    Campaign,
    CampaignEvent,
    CampaignRow,
    ClientStatusNote,
    Offer,
    OfferActivity,
    StrategyItem,
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
    shape_summary,
    split_row,
    validate_template,
)
from services.schedule_tz import schedule_tz, schedule_tz_name

logger = logging.getLogger(__name__)

N8N_EMAIL_URL = os.getenv("N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL", "").strip()
DEV_UNSUBSCRIBE_SECRET = "campaign-unsubscribe-dev"
LOCAL_DEV_ENVIRONMENTS = frozenset({"development", "dev", "local"})
TERMINAL_RUN_STATUSES = frozenset({"stopped", "completed", "cancelled", "errored"})
UNSUBSCRIBE_FOOTER_INTRO = "You can unsubscribe from these emails at any time."
UNSUBSCRIBE_LINK_TEXT = "Unsubscribe"
ONE_CLICK_BODY = "List-Unsubscribe=One-Click"
CONFIRM_FIELD = "confirm"
CONFIRM_VALUE = "page"
WORDMARK = "Carbon Zero Australasia"
TEST_SEND_LIMIT = 20
TEST_SEND_WINDOW = timedelta(hours=1)
CONTENT_PATCH_KEYS = (
    "name",
    "sequence_type",
    "first_touch_subject",
    "first_touch_html",
    "merge_field_map",
    "provenance_note",
)
THROTTLE_PATCH_KEYS = ("daily_cap", "send_window_start", "send_window_end")
TERMINAL_CAMPAIGN_STATUSES = frozenset({"done"})
SEND_NEXT_MAX = 100
CAMPAIGN_DETAIL_ROW_LIMIT = 2000
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
    def __init__(self, message: str, status_code: int = 400, payload: dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


def is_local_dev(environment: str | None = None) -> bool:
    if os.getenv("K_SERVICE"):
        return False
    env = (
        environment
        if environment is not None
        else (os.getenv("ENVIRONMENT") or "development")
    ).strip().lower()
    return env in LOCAL_DEV_ENVIRONMENTS


def unsubscribe_secret() -> str:
    return (
        os.getenv("CAMPAIGN_UNSUBSCRIBE_SECRET")
        or os.getenv("NEXTAUTH_SECRET")
        or os.getenv("BACKEND_API_KEY")
        or DEV_UNSUBSCRIBE_SECRET
    )


def public_api_base() -> str:
    return (
        os.getenv("PUBLIC_API_BASE_URL")
        or os.getenv("BACKEND_API_URL")
        or os.getenv("NEXTAUTH_URL")
        or ""
    ).rstrip("/")


def _is_absolute_http_url(value: str) -> bool:
    parsed = urlparse((value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def assert_campaign_unsubscribe_config(environment: str | None = None) -> None:
    """Raise outside local dev when unsubscribe signing/base URL is not production-safe."""
    if is_local_dev(environment):
        return
    secret = (os.getenv("CAMPAIGN_UNSUBSCRIBE_SECRET") or "").strip()
    if not secret or secret == DEV_UNSUBSCRIBE_SECRET:
        raise RuntimeError(
            "CAMPAIGN_UNSUBSCRIBE_SECRET must be set to a non-default value outside local dev"
        )
    base = (os.getenv("PUBLIC_API_BASE_URL") or "").strip()
    if not _is_absolute_http_url(base):
        raise RuntimeError(
            "PUBLIC_API_BASE_URL must be an absolute http(s) URL outside local dev"
        )


def assert_campaign_can_send() -> None:
    """Block ready/send unless unsubscribe links can be signed as absolute http(s) URLs."""
    if not is_local_dev():
        try:
            assert_campaign_unsubscribe_config()
        except RuntimeError as exc:
            raise CampaignError(str(exc), 500) from exc
        return
    if not _is_absolute_http_url(public_api_base()):
        raise CampaignError(
            "PUBLIC_API_BASE_URL must be an absolute http(s) URL before a campaign can send",
            500,
        )


def append_unsubscribe_footer(html: str, text: str, url: str) -> tuple[str, str]:
    safe_url = escape(url, quote=True)
    html_footer = (
        f'<p style="font-size:12px;color:#666;margin-top:24px;">'
        f"{escape(UNSUBSCRIBE_FOOTER_INTRO)} "
        f'<a href="{safe_url}">{escape(UNSUBSCRIBE_LINK_TEXT)}</a></p>'
    )
    text_footer = f"\n\n{UNSUBSCRIBE_FOOTER_INTRO} {url}"
    return (html or "") + html_footer, (text or "").rstrip() + text_footer


def unsubscribe_action_url(token: str) -> str:
    path = "/api/autonomous/campaigns/unsubscribe?token=" + quote(token, safe="")
    base = public_api_base()
    if _is_absolute_http_url(base):
        return f"{base}{path}"
    return path


def _unsubscribe_page(title: str, inner: str) -> str:
    return (
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        f"<title>{escape(title)}</title></head>"
        "<body style=\"margin:0;background:#f4f4f5;color:#18181b;"
        "font-family:Arial,Helvetica,sans-serif;\">"
        "<div style=\"min-height:100vh;display:flex;align-items:center;justify-content:center;"
        "padding:24px;box-sizing:border-box;\">"
        "<div style=\"width:100%;max-width:28rem;background:#fff;border:1px solid #e4e4e7;"
        "border-radius:16px;padding:32px 28px;text-align:center;box-sizing:border-box;\">"
        f"<p style=\"margin:0 0 16px;font-size:13px;letter-spacing:.08em;text-transform:uppercase;"
        f"color:#5750F1;font-weight:700;\">{escape(WORDMARK)}</p>"
        f"{inner}"
        "</div></div></body></html>"
    )


def unsubscribe_confirm_html(email: str, token: str) -> str:
    action = unsubscribe_action_url(token)
    inner = (
        f"<p style=\"margin:0 0 24px;font-size:16px;line-height:1.5;\">"
        f"Unsubscribe <strong>{escape(email)}</strong> from these emails?</p>"
        f"<form method=\"post\" action=\"{escape(action, quote=True)}\">"
        f"<input type=\"hidden\" name=\"{CONFIRM_FIELD}\" value=\"{CONFIRM_VALUE}\">"
        "<button type=\"submit\" style=\"appearance:none;border:0;border-radius:999px;"
        "background:#5750F1;color:#fff;font-size:14px;font-weight:700;padding:10px 22px;"
        "cursor:pointer;\">Unsubscribe</button>"
        "</form>"
    )
    return _unsubscribe_page("Unsubscribe", inner)


def unsubscribe_post_kind(body: str) -> str | None:
    """one_click or confirm_page. Anything else is refused."""
    text = (body or "").strip()
    if text == ONE_CLICK_BODY:
        return "one_click"
    fields = parse_qs(text, keep_blank_values=False)
    if set(fields) == {CONFIRM_FIELD} and fields.get(CONFIRM_FIELD) == [CONFIRM_VALUE]:
        return "confirm_page"
    return None


def unsubscribe_done_html(email: str) -> str:
    inner = (
        f"<p style=\"margin:0;font-size:16px;line-height:1.5;\">"
        f"{escape(email)} has been unsubscribed. You will not receive further emails "
        "from this campaign.</p>"
    )
    return _unsubscribe_page("Unsubscribed", inner)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _map(campaign: Campaign) -> dict[str, str]:
    return {str(k): str(v) for k, v in parse_json_obj(campaign.merge_field_map).items()}


def _merge(row: CampaignRow) -> dict[str, str]:
    data = parse_json_obj(row.merge_json)
    return {str(k): "" if v is None else str(v) for k, v in data.items()}


def _row_key(row: CampaignRow) -> str:
    return (row.recipient_key or "").strip()


def _human_only_keys(rows: list[CampaignRow]) -> set[str]:
    return {_row_key(row) for row in rows if row.human_only and _row_key(row)}


def _counts_from_rows(rows: list[CampaignRow]) -> dict[str, Any]:
    merges = [_merge(row) for row in rows]
    warning_rows, shape_warnings, per_row = shape_summary(merges)
    blank_keys = 0
    keys: set[str] = set()
    human_only_keys = _human_only_keys(rows)
    human_only_blanks = 0
    started_keys = {
        _row_key(row)
        for row in rows
        if row.run_id and _row_key(row)
    }
    sendable_keys: set[str] = set()
    sendable_blanks = 0
    warning_ids: set[int] = set()
    for row, fails in zip(rows, per_row):
        key = _row_key(row)
        if key:
            keys.add(key)
        else:
            blank_keys += 1
            if row.human_only:
                human_only_blanks += 1
        if fails:
            warning_ids.add(row.id)
            continue
        if row.row_status != "pending":
            continue
        if row.human_only or (key and key in human_only_keys):
            continue
        if key and key in started_keys:
            continue
        if key:
            sendable_keys.add(key)
        else:
            sendable_blanks += 1
    sendable = len(sendable_keys) + sendable_blanks
    return {
        "rows": len(rows),
        "unique_recipients": len(keys) + blank_keys,
        "pending": sendable,
        "sendable": sendable,
        "human_only": len(human_only_keys) + human_only_blanks,
        "warnings": warning_rows,
        "shape_warnings": shape_warnings,
        "per_row_shape_warnings": per_row,
        "warning_ids": warning_ids,
    }


def _campaign_row_counts(db: Session, campaign_id: int) -> dict[str, Any]:
    rows = (
        db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign_id)
        .order_by(CampaignRow.id)
        .all()
    )
    return _counts_from_rows(rows)


def campaign_to_dict(campaign: Campaign, db: Session, include_rows: bool = False, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    all_rows = (
        db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign.id)
        .order_by(CampaignRow.id)
        .all()
    )
    counted = _counts_from_rows(all_rows)
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
        "archived": bool(campaign.archived),
        "schedule_timezone": schedule_tz_name(),
        "created_by": campaign.created_by,
        "created_at": campaign.created_at.isoformat() if campaign.created_at else None,
        "updated_at": campaign.updated_at.isoformat() if campaign.updated_at else None,
        "row_counts": {
            "rows": counted["rows"],
            "unique_recipients": counted["unique_recipients"],
            "pending": counted["pending"],
            "sendable": counted["sendable"],
            "human_only": counted["human_only"],
            "warnings": counted["warnings"],
            "test_sends": test_sends,
        },
        "shape_warnings": counted["shape_warnings"],
        "mid_send_edits": [
            {
                "id": event.id,
                "actor": event.actor,
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "payload": parse_json_obj(event.payload_json),
            }
            for event in (
                db.query(CampaignEvent)
                .filter(
                    CampaignEvent.campaign_id == campaign.id,
                    CampaignEvent.event_type == "mid_send_template_edit",
                )
                .order_by(CampaignEvent.created_at.desc())
                .all()
            )
        ],
    }
    if include_rows:
        page = all_rows[offset : offset + limit]
        flags = counted["per_row_shape_warnings"][offset : offset + limit]
        payload["rows"] = [row_to_dict(row, shape_warnings) for row, shape_warnings in zip(page, flags)]
        payload["rows_total"] = counted["rows"]
        payload["rows_offset"] = offset
        payload["rows_limit"] = limit
    return payload


def campaign_detail(campaign: Campaign, db: Session) -> dict[str, Any]:
    return campaign_to_dict(campaign, db, include_rows=True, limit=CAMPAIGN_DETAIL_ROW_LIMIT)


def row_to_dict(row: CampaignRow, shape_warnings: list[str] | None = None) -> dict[str, Any]:
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
        "shape_warnings": list(shape_warnings or []),
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


def list_campaigns(db: Session, include_archived: bool = False) -> list[Campaign]:
    query = db.query(Campaign)
    if not include_archived:
        query = query.filter((Campaign.archived == 0) | (Campaign.archived.is_(None)))
    return query.order_by(Campaign.created_at.desc()).all()


def get_campaign(db: Session, campaign_id: int) -> Campaign:
    campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
    if not campaign:
        raise CampaignError("Campaign not found", 404)
    return campaign


def _assert_draft(campaign: Campaign) -> None:
    if campaign.status != "draft":
        raise CampaignError("Only a draft campaign can be edited", 409)


def _campaign_is_terminal(campaign: Campaign) -> bool:
    return campaign.status in TERMINAL_CAMPAIGN_STATUSES or bool(campaign.archived)


def _started_row_count(db: Session, campaign_id: int) -> int:
    return (
        db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign_id, CampaignRow.run_id.isnot(None))
        .count()
    )


def patch_campaign(db: Session, campaign: Campaign, body: dict[str, Any], actor: str | None) -> Campaign:
    next_status = body.get("status")
    if next_status and next_status != campaign.status and next_status != "ready":
        raise CampaignError(f"Cannot set status to {next_status} via PATCH")

    wants_content = any(k in body for k in CONTENT_PATCH_KEYS)
    wants_email = "first_touch_subject" in body or "first_touch_html" in body
    wants_throttle = any(k in body for k in THROTTLE_PATCH_KEYS)
    terminal = _campaign_is_terminal(campaign)
    already_sent = _started_row_count(db, campaign.id)

    if wants_content and campaign.status != "draft" and not wants_email:
        raise CampaignError("Only a draft campaign can be edited", 409)
    if wants_email and terminal:
        raise CampaignError("A finished campaign's email cannot be edited", 409)
    if wants_email and campaign.status != "draft" and already_sent > 0 and not body.get("confirm_mid_send_edit"):
        remaining = _pending_sendable(db, campaign.id)
        raise CampaignError(
            f"{already_sent} already sent. {remaining} will get the new version.",
            409,
            payload={
                "code": "mid_send_edit_confirmation",
                "message": f"{already_sent} already sent. {remaining} will get the new version.",
                "already_sent": already_sent,
                "will_get_new": remaining,
            },
        )
    if wants_throttle and terminal:
        raise CampaignError(
            "Daily cap and send window can only be changed before a campaign is finished",
            409,
        )

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
    elif wants_email and not terminal:
        if "first_touch_subject" in body:
            campaign.first_touch_subject = body["first_touch_subject"]
        if "first_touch_html" in body:
            campaign.first_touch_html = sanitize_html(body["first_touch_html"] or "")
            campaign.first_touch_text = html_to_plain_text(campaign.first_touch_html)
        if already_sent > 0:
            remaining = _pending_sendable(db, campaign.id)
            db.add(
                CampaignEvent(
                    campaign_id=campaign.id,
                    event_type="mid_send_template_edit",
                    actor=actor,
                    payload_json=_json(
                        {
                            "already_sent": already_sent,
                            "will_get_new": remaining,
                            "subject": campaign.first_touch_subject,
                        }
                    ),
                )
            )

    if wants_throttle and not terminal:
        if "daily_cap" in body:
            campaign.daily_cap = body["daily_cap"]
        if "send_window_start" in body:
            campaign.send_window_start = body["send_window_start"]
        if "send_window_end" in body:
            campaign.send_window_end = body["send_window_end"]

    db.flush()

    if next_status and next_status != campaign.status:
        if next_status == "ready":
            _ready_gate(db, campaign, body.get("acknowledge_warnings"))
            campaign.status = "ready"
        else:
            raise CampaignError(f"Cannot set status to {next_status} via PATCH")

    db.commit()
    db.refresh(campaign)
    return campaign


def set_archived(db: Session, campaign: Campaign, archived: bool) -> Campaign:
    campaign.archived = 1 if archived else 0
    db.commit()
    db.refresh(campaign)
    return campaign


def archive_campaigns(db: Session, campaign_ids: list[int]) -> list[Campaign]:
    seen: set[int] = set()
    updated: list[Campaign] = []
    for campaign_id in campaign_ids:
        if campaign_id in seen:
            continue
        seen.add(campaign_id)
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign:
            continue
        campaign.archived = 1
        updated.append(campaign)
    db.commit()
    for campaign in updated:
        db.refresh(campaign)
    return updated


def _campaign_side_effects(db: Session, campaign: Campaign) -> tuple[list[int], list[int]]:
    offer_ids = [offer.id for offer in db.query(Offer).filter(Offer.campaign_id == campaign.id).all()]
    run_ids = {
        row.run_id
        for row in db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign.id, CampaignRow.run_id.isnot(None))
        .all()
        if row.run_id
    }
    if offer_ids:
        run_ids.update(
            run.id
            for run in db.query(AutonomousSequenceRun)
            .filter(AutonomousSequenceRun.offer_id.in_(offer_ids))
            .all()
        )
    return sorted(run_ids), offer_ids


def _delete_sequence_context(db: Session, run_ids: list[int]) -> None:
    if not run_ids:
        return
    conn = db.connection()
    insp = inspect(conn)
    names = set(insp.get_table_names())
    if conn.dialect.name == "postgresql":
        names |= set(insp.get_table_names(schema="public"))
    if "autonomous_sequence_context" not in names:
        return
    ctx_tbl = (
        "public.autonomous_sequence_context"
        if conn.dialect.name == "postgresql"
        else "autonomous_sequence_context"
    )
    for run_id in run_ids:
        db.execute(text(f"DELETE FROM {ctx_tbl} WHERE run_id = :run_id"), {"run_id": run_id})


def _delete_runs_and_offers(db: Session, run_ids: list[int], offer_ids: list[int]) -> None:
    if run_ids:
        db.query(AutonomousSequenceEvent).filter(
            AutonomousSequenceEvent.run_id.in_(run_ids)
        ).delete(synchronize_session=False)
        db.query(AutonomousSequenceStep).filter(
            AutonomousSequenceStep.run_id.in_(run_ids)
        ).delete(synchronize_session=False)
        _delete_sequence_context(db, run_ids)
        db.query(AutonomousSequenceRun).filter(
            AutonomousSequenceRun.id.in_(run_ids)
        ).delete(synchronize_session=False)
    if offer_ids:
        activity_ids = [
            row.id
            for row in db.query(OfferActivity.id).filter(OfferActivity.offer_id.in_(offer_ids)).all()
        ]
        if activity_ids:
            db.query(StrategyItem).filter(
                StrategyItem.offer_activity_id.in_(activity_ids)
            ).delete(synchronize_session=False)
        db.query(StrategyItem).filter(StrategyItem.offer_id.in_(offer_ids)).delete(
            synchronize_session=False
        )
        db.query(OfferActivity).filter(OfferActivity.offer_id.in_(offer_ids)).delete(
            synchronize_session=False
        )
        db.query(ClientStatusNote).filter(
            ClientStatusNote.related_offer_id.in_(offer_ids)
        ).delete(synchronize_session=False)
        db.query(Offer).filter(Offer.id.in_(offer_ids)).delete(synchronize_session=False)


def delete_campaign(db: Session, campaign: Campaign, confirm: bool = False) -> None:
    run_ids, offer_ids = _campaign_side_effects(db, campaign)
    if (run_ids or offer_ids) and not confirm:
        runs = len(run_ids)
        offers = len(offer_ids)
        message = (
            f"This campaign created {runs} run{'s' if runs != 1 else ''} and "
            f"{offers} offer{'s' if offers != 1 else ''}. Deleting it will also delete them."
        )
        raise CampaignError(
            message,
            409,
            payload={
                "message": message,
                "runs": runs,
                "offers": offers,
                "confirm_required": True,
            },
        )
    _delete_runs_and_offers(db, run_ids, offer_ids)
    db.query(CampaignEvent).filter(CampaignEvent.campaign_id == campaign.id).delete()
    db.query(CampaignRow).filter(CampaignRow.campaign_id == campaign.id).delete()
    db.delete(campaign)
    db.commit()


def _summary_from_counted(
    counted: dict[str, Any],
    stored: int,
    suppressed_emails: list[str],
    conflicts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "rows": stored,
        "unique_recipients": counted["unique_recipients"],
        "groups_with_conflicts": conflicts or [],
        "pending": counted["pending"],
        "sendable": counted["sendable"],
        "human_only": counted["human_only"],
        "warnings": counted["warnings"],
        "shape_warnings": counted["shape_warnings"],
        "suppressed_addresses": list(dict.fromkeys(suppressed_emails)),
    }


def _ephemeral_rows(
    db: Session,
    headers: list[str],
    rows: list[list[str]],
    column_map: dict[str, str],
    campaign_id: int | None = None,
) -> tuple[list[CampaignRow], list[str], dict[str, list[dict[str, str]]]]:
    mapping = {str(k): str(v) for k, v in (column_map or {}).items()}
    built: list[CampaignRow] = []
    suppressed_emails: list[str] = []
    groups: dict[str, list[dict[str, str]]] = {}
    for cells in rows:
        merge, intelligence = split_row(headers, cells, mapping)
        key = recipient_key_from_merge(merge)
        blocked = bool(key and suppressed(db, key))
        built.append(
            CampaignRow(
                campaign_id=campaign_id or 0,
                merge_json=_json(merge),
                intelligence_json=_json(intelligence),
                recipient_key=key,
                row_status="suppressed" if blocked else "pending",
                suppression_reason="unsubscribed" if blocked else None,
                human_only=0,
            )
        )
        if blocked and key:
            suppressed_emails.append(key)
        if key:
            groups.setdefault(key, []).append(merge)
    return built, suppressed_emails, groups


def _row_conflicts(groups: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    conflicts = []
    for email, members in groups.items():
        if len(members) < 2:
            continue
        keys: set[str] = set()
        for member in members:
            keys.update(member.keys())
        differing = []
        for field in keys:
            values = {(m.get(field) or "").strip().lower() for m in members}
            if len(values) > 1:
                differing.append(field)
        if differing:
            conflicts.append({"email": email, "count": len(members), "differing_keys": differing})
    return conflicts


def preview_rows(
    db: Session,
    headers: list[str],
    rows: list[list[str]],
    column_map: dict[str, str],
) -> dict[str, Any]:
    mapping = {str(k): str(v) for k, v in (column_map or {}).items()}
    built, suppressed_emails, groups = _ephemeral_rows(db, headers, rows, mapping)
    counted = _counts_from_rows(built)
    flags = counted["per_row_shape_warnings"]
    summary = _summary_from_counted(counted, len(built), suppressed_emails, _row_conflicts(groups))
    summary["preview"] = True
    summary["preview_rows"] = [
        {
            "id": -(index + 1),
            "campaign_id": None,
            "merge_json": _merge(row),
            "intelligence_json": parse_json_obj(row.intelligence_json),
            "recipient_key": row.recipient_key,
            "row_status": row.row_status,
            "suppression_reason": row.suppression_reason,
            "run_id": None,
            "offer_id": None,
            "human_only": False,
            "human_only_reason": None,
            "shape_warnings": list(flag),
            "started_at": None,
        }
        for index, (row, flag) in enumerate(zip(built, flags))
    ]
    return summary


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
    existing = (
        db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign.id)
        .all()
    )
    preserved: dict[str, str | None] = {}
    for row in existing:
        key = _row_key(row)
        if key and row.human_only:
            preserved[key] = row.human_only_reason
    db.query(CampaignRow).filter(CampaignRow.campaign_id == campaign.id).delete()
    _ = client_merge_json
    built, suppressed_emails, groups = _ephemeral_rows(db, headers, rows, mapping, campaign.id)
    for row in built:
        row.campaign_id = campaign.id
        key = _row_key(row)
        if key in preserved:
            row.human_only = 1
            row.human_only_reason = preserved[key]
        db.add(row)
    db.commit()
    stored_rows = (
        db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign.id)
        .order_by(CampaignRow.id)
        .all()
    )
    counted = _counts_from_rows(stored_rows)
    return _summary_from_counted(counted, len(stored_rows), suppressed_emails, _row_conflicts(groups))


def _assert_can_flag_human_only(campaign: Campaign, row: CampaignRow) -> None:
    if bool(campaign.archived) or campaign.status == "done":
        raise CampaignError("Human-only can only be changed before a campaign is finished", 409)
    if row.row_status == "started" or row.run_id:
        raise CampaignError("A row that has already been sent cannot be flagged human-only", 409)


def set_human_only(db: Session, campaign: Campaign, row_id: int, human_only: bool, reason: str | None) -> CampaignRow:
    row = (
        db.query(CampaignRow)
        .filter(CampaignRow.id == row_id, CampaignRow.campaign_id == campaign.id)
        .first()
    )
    if not row:
        raise CampaignError("Row not found", 404)
    _assert_can_flag_human_only(campaign, row)
    trimmed = (reason or "").strip()[:255] or None
    key = _row_key(row).strip().lower()
    targets = [row]
    if key:
        targets = (
            db.query(CampaignRow)
            .filter(
                CampaignRow.campaign_id == campaign.id,
                func.lower(func.trim(CampaignRow.recipient_key)) == key,
            )
            .all()
        )
    for target in targets:
        if target.row_status == "started" or target.run_id:
            continue
        target.human_only = 1 if human_only else 0
        target.human_only_reason = trimmed if human_only else None
    db.commit()
    db.refresh(row)
    return row


def _ready_gate(db: Session, campaign: Campaign, acknowledge_warnings: Any = None) -> None:
    assert_campaign_can_send()
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
    counted = _campaign_row_counts(db, campaign.id)
    warnings = counted["warnings"]
    if warnings:
        try:
            acknowledged = int(acknowledge_warnings)
        except (TypeError, ValueError):
            acknowledged = None
        if acknowledged != warnings:
            raise CampaignError(
                f"{warnings} rows have shape warnings and will not be sent. "
                f"Set acknowledge_warnings to {warnings} to mark ready."
            )
    if counted["sendable"] < 1:
        blocked = (
            db.query(CampaignRow)
            .filter(
                CampaignRow.campaign_id == campaign.id,
                CampaignRow.row_status == "suppressed",
            )
            .all()
        )
        emails = [r.recipient_key for r in blocked if r.recipient_key]
        if emails:
            raise CampaignError(_unsubscribed_message(emails), 409)
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
        raise CampaignError(f"{to} has unsubscribed", 409)
    assert_campaign_can_send()
    unsub = unsubscribe_url(to, campaign.id)
    html, text = append_unsubscribe_footer(html, text, unsub)
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
    return suppression_reason_for(db, email) is not None


def suppression_reason_for(db: Session, email: str) -> str | None:
    key = (email or "").strip().lower()
    if not key:
        return None
    row = db.query(Suppression).filter(Suppression.email == key).first()
    return row.reason if row else None


def _unsubscribed_message(emails: list[str]) -> str:
    unique = list(dict.fromkeys(e.strip().lower() for e in emails if e and e.strip()))
    if not unique:
        return "An address on this list has unsubscribed"
    if len(unique) == 1:
        return f"{unique[0]} has unsubscribed"
    return "These addresses have unsubscribed: " + ", ".join(unique)


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


def list_suppressions(db: Session) -> list[dict[str, Any]]:
    rows = db.query(Suppression).order_by(Suppression.created_at.desc(), Suppression.id.desc()).all()
    return [
        {
            "id": row.id,
            "email": row.email,
            "reason": row.reason,
            "source": row.source,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def delete_suppression(db: Session, suppression_id: int) -> None:
    row = db.query(Suppression).filter(Suppression.id == suppression_id).first()
    if not row:
        raise CampaignError("Suppression not found", 404)
    db.delete(row)
    db.commit()


def unsubscribe_url(email: str, campaign_id: int) -> str:
    token = sign_unsubscribe_token(email, campaign_id)
    base = public_api_base()
    return f"{base}/api/autonomous/campaigns/unsubscribe?token={token}"


def sign_unsubscribe_token(email: str, campaign_id: int) -> str:
    payload = f"{(email or '').strip().lower()}|{int(campaign_id)}"
    sig = hmac.new(unsubscribe_secret().encode(), payload.encode(), hashlib.sha256).hexdigest()
    raw = f"{payload}|{sig}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def verify_unsubscribe_token(token: str) -> tuple[str, int]:
    padded = (token or "") + "=" * (-len(token or "") % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode()).decode()
    except Exception as exc:
        raise CampaignError("Invalid unsubscribe token", 400) from exc
    parts = raw.split("|")
    if len(parts) != 3:
        raise CampaignError("Invalid unsubscribe token", 400)
    email, campaign_id_s, sig = parts
    payload = f"{email}|{campaign_id_s}"
    expected = hmac.new(unsubscribe_secret().encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise CampaignError("Invalid unsubscribe token", 400)
    return email, int(campaign_id_s)


def apply_unsubscribe(db: Session, token: str) -> dict[str, Any]:
    email, campaign_id = verify_unsubscribe_token(token)
    add_suppression(db, email, "unsubscribed", "one_click")
    runs = (
        db.query(AutonomousSequenceRun)
        .filter(
            ~AutonomousSequenceRun.run_status.in_(TERMINAL_RUN_STATUSES),
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
    zone = schedule_tz()
    current = (now or datetime.now(zone)).astimezone(zone).strftime("%H:%M")
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def _started_today(db: Session, campaign_id: int) -> int:
    zone = schedule_tz()
    start = datetime.now(zone).replace(hour=0, minute=0, second=0, microsecond=0)
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


def _refuse_inactive_start(campaign: Campaign) -> dict[str, Any] | None:
    if bool(campaign.archived):
        raise CampaignError("An archived campaign cannot send", 409)
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
    return None


def _outside_window_result(db: Session, campaign: Campaign) -> dict[str, Any]:
    return {
        "ok": True,
        "started": 0,
        "pending": _pending_sendable(db, campaign.id),
        "reason": "outside_send_window",
        "status": campaign.status,
    }


def _start_pending_rows(
    db: Session,
    campaign: Campaign,
    actor: str | None,
    remaining: int | None,
) -> dict[str, Any]:
    from services.autonomous_sequence import explicit_failure_detail, start_gas_base2_sequence

    all_rows = (
        db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign.id)
        .order_by(CampaignRow.id)
        .all()
    )
    counted = _counts_from_rows(all_rows)
    warning_ids = counted["warning_ids"]
    blocked_keys = _human_only_keys(all_rows)
    rows = [
        row
        for row in all_rows
        if row.row_status == "pending"
        and not row.human_only
        and _row_key(row) not in blocked_keys
        and row.id not in warning_ids
    ]
    started = 0
    skipped_suppressed = 0
    skipped_idempotent = 0
    skipped_addresses: list[str] = []
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
        merge = _merge(row)
        to = (merge.get("contact_email") or key or "").strip()
        blocked_as = ""
        if key and suppressed(db, key):
            blocked_as = key
        elif to and suppressed(db, to):
            blocked_as = to.lower()
        if blocked_as:
            row.row_status = "suppressed"
            row.suppression_reason = suppression_reason_for(db, blocked_as) or "unsubscribed"
            skipped_suppressed += 1
            skipped_addresses.append(blocked_as)
            continue
        if key and key in seen_keys:
            continue
        subject, html, text = render_first_touch(campaign, merge, test=False)
        if not looks_like_email(to):
            row.row_status = "failed"
            row.suppression_reason = "manual"
            continue
        n8n_result = send_first_touch_email(
            db=db, to=to, subject=subject, html=html, text=text, campaign=campaign, row=row, test=False
        )
        failure = explicit_failure_detail(n8n_result)
        if failure:
            row.row_status = "failed"
            logger.warning(
                "[campaign] first-touch success false campaign_id=%s row_id=%s recipient=%s detail=%s",
                campaign.id,
                row.id,
                to,
                failure,
            )
            db.commit()
            continue
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
            tz=schedule_tz_name(),
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
        "skipped_suppressed_addresses": list(dict.fromkeys(skipped_addresses)),
        "skipped_idempotent": skipped_idempotent,
        "status": campaign.status,
    }


def start_campaign(db: Session, campaign: Campaign, actor: str | None) -> dict[str, Any]:
    done = _refuse_inactive_start(campaign)
    if done is not None:
        return done
    db.expire(campaign)
    campaign = get_campaign(db, campaign.id)
    if not _inside_send_window(campaign):
        return _outside_window_result(db, campaign)

    cap = campaign.daily_cap
    already = _started_today(db, campaign.id)
    remaining = None if cap is None else max(0, int(cap) - already)
    if remaining == 0:
        return {
            "ok": True,
            "started": 0,
            "pending": _pending_sendable(db, campaign.id),
            "reason": "daily_cap",
            "daily_cap": cap,
            "started_today": already,
            "status": campaign.status,
        }
    result = _start_pending_rows(db, campaign, actor, remaining)
    result["daily_cap"] = cap
    result["started_today"] = already + result["started"]
    return result


def send_next_n(db: Session, campaign: Campaign, n: int, actor: str | None) -> dict[str, Any]:
    done = _refuse_inactive_start(campaign)
    if done is not None:
        raise CampaignError("A finished campaign has nothing left to send", 409)
    if not _inside_send_window(campaign):
        return _outside_window_result(db, campaign)
    try:
        count = int(n)
    except (TypeError, ValueError):
        raise CampaignError("n must be a positive integer") from None
    if count < 1:
        raise CampaignError("n must be at least 1")
    if count > SEND_NEXT_MAX:
        raise CampaignError(f"n cannot exceed {SEND_NEXT_MAX}")

    result = _start_pending_rows(db, campaign, actor, count)
    event = CampaignEvent(
        campaign_id=campaign.id,
        event_type="send_next_n",
        actor=actor,
        payload_json=_json(
            {
                "n": count,
                "started": result["started"],
                "pending": result["pending"],
                "bypassed_daily_cap": True,
            }
        ),
    )
    db.add(event)
    db.commit()
    result["bypassed_daily_cap"] = True
    result["requested"] = count
    return result


def _pending_sendable(db: Session, campaign_id: int) -> int:
    return _campaign_row_counts(db, campaign_id)["sendable"]


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
