"""Staff email on partner lead collision. Uses the existing n8n Gmail send webhook."""

from __future__ import annotations

import html
import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx

from tools.share_folder_email import share_folder_email_webhook_url

logger = logging.getLogger(__name__)

DEFAULT_NOTIFY_EMAILS = "members@acesolutions.com.au"
NOTIFY_EMAILS_ENV = "PARTNER_COLLISION_NOTIFY_EMAILS"
MATCH_ACES = "ACES client"
MATCH_OTHER_DISTRIBUTOR = "another distributor's lead"


def collision_notify_recipients() -> list[str]:
    raw = os.getenv(NOTIFY_EMAILS_ENV)
    if raw is None:
        raw = DEFAULT_NOTIFY_EMAILS
    return [part.strip() for part in raw.split(",") if part.strip()]


def _skip_notify() -> bool:
    return os.getenv("PARTNER_COLLISION_SKIP_NOTIFY") == "1" or os.getenv(
        "PARTNER_BASE1_SKIP_N8N"
    ) == "1"


def build_collision_staff_email(payload: dict[str, Any]) -> dict[str, str]:
    distributor = (payload.get("distributor_name") or "").strip() or "Unknown distributor"
    slug = (payload.get("distributor_slug") or "").strip() or "—"
    submitted_by = (payload.get("submitted_by_email") or "").strip() or "—"
    submitted_name = (payload.get("submitted_business_name") or "").strip() or "—"
    existing_id = payload.get("existing_client_id")
    existing_name = (payload.get("existing_business_name") or "").strip() or "—"
    match_kind = (payload.get("match_kind") or MATCH_ACES).strip()
    matched_partner = (payload.get("matched_partner_name") or "").strip()
    folder_url = (payload.get("folder_url") or "").strip()
    timestamp = (payload.get("timestamp") or "").strip() or datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    partner_id = payload.get("partner_id")
    collision_id = payload.get("collision_id")

    match_line = f"{existing_name} (client id {existing_id}) — {match_kind}"
    if match_kind == MATCH_OTHER_DISTRIBUTOR and matched_partner:
        match_line += f" ({matched_partner})"

    subject = f"[ACES] Distributor lead collision — {submitted_name}"
    text_body = (
        "A distributor Base 1 submission could not be processed automatically "
        "and needs manual handling.\n\n"
        f"Timestamp: {timestamp}\n"
        f"Distributor: {distributor} (slug={slug}, partner_id={partner_id})\n"
        f"Submitted by: {submitted_by}\n"
        f"Submitted business name: {submitted_name}\n"
        f"Matched existing client: {match_line}\n"
        f"Collision id: {collision_id}\n"
        f"Files: {folder_url or '(no Drive folder link)'}\n"
    )
    safe_folder = html.escape(folder_url, quote=True)
    folder_html = (
        f'<p><strong>Files:</strong> <a href="{safe_folder}">{html.escape(folder_url)}</a></p>'
        if folder_url
        else "<p><strong>Files:</strong> (no Drive folder link)</p>"
    )
    html_body = f"""<!DOCTYPE html>
<html>
<body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.5;color:#111827;">
  <p>A distributor Base 1 submission could not be processed automatically and needs manual handling.</p>
  <p><strong>Timestamp:</strong> {html.escape(timestamp)}</p>
  <p><strong>Distributor:</strong> {html.escape(distributor)} (slug={html.escape(slug)}, partner_id={html.escape(str(partner_id))})</p>
  <p><strong>Submitted by:</strong> {html.escape(submitted_by)}</p>
  <p><strong>Submitted business name:</strong> {html.escape(submitted_name)}</p>
  <p><strong>Matched existing client:</strong> {html.escape(match_line)}</p>
  <p><strong>Collision id:</strong> {html.escape(str(collision_id))}</p>
  {folder_html}
</body>
</html>"""
    return {"subject": subject, "html_body": html_body, "body_text": text_body}


def notify_staff_of_lead_collision(payload: dict[str, Any]) -> None:
    if _skip_notify():
        return
    recipients = collision_notify_recipients()
    if not recipients:
        logger.warning("ACES_PARTNER_COLLISION_NOTIFY_SKIP no recipients")
        return
    webhook = share_folder_email_webhook_url()
    if not webhook:
        logger.error("ACES_PARTNER_COLLISION_NOTIFY_FAIL no email webhook")
        return
    content = build_collision_staff_email(payload)
    for recipient in recipients:
        n8n_payload = {
            "to": recipient,
            "subject": content["subject"],
            "body_html": content["html_body"],
            "body_text": content["body_text"],
            "html_body": content["html_body"],
            "email_subject": content["subject"],
            "recipient_email": recipient,
            "event": "partner_lead_collision",
            "recipient": {"email": recipient},
            "message": {"subject": content["subject"], "html_body": content["html_body"]},
        }
        try:
            response = httpx.post(webhook, json=n8n_payload, timeout=30.0)
            if response.status_code >= 400:
                logger.error(
                    "ACES_PARTNER_COLLISION_NOTIFY_FAIL to=%s status=%s body=%s",
                    recipient,
                    response.status_code,
                    (response.text or "")[:300],
                )
        except Exception:
            logger.exception("ACES_PARTNER_COLLISION_NOTIFY_FAIL to=%s", recipient)
