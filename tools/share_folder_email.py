"""Branded Carbon Zero emails when a member Shared Folder is shared."""

from __future__ import annotations

import html
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BRAND_NAME = "Carbon Zero Australasia"
DEFAULT_WEBHOOK_URL = "https://membersaces.app.n8n.cloud/webhook/aces-autonomous-agent/send-email"
PRIMARY_COLOR = "#5750F1"


def share_folder_email_webhook_url() -> str:
    return (
        os.getenv("SHARE_FOLDER_EMAIL_WEBHOOK_URL")
        or os.getenv("N8N_SHARE_FOLDER_EMAIL_WEBHOOK_URL")
        or os.getenv("N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL")
        or DEFAULT_WEBHOOK_URL
    ).strip()


def build_share_folder_subject(business_name: str) -> str:
    name = (business_name or "").strip() or "your organisation"
    return f"{BRAND_NAME} has shared documents with you — {name}"


def build_share_folder_email(
    *,
    business_name: str,
    folder_url: str,
    file_names: list[str],
    sender_name: str = "",
    sender_email: str = "",
) -> dict[str, str]:
    biz = html.escape((business_name or "").strip() or "your organisation")
    folder = html.escape((folder_url or "").strip())
    sender = html.escape((sender_name or "").strip() or BRAND_NAME)
    sender_em = html.escape((sender_email or "").strip())
    files = [html.escape(name) for name in file_names if str(name).strip()]
    file_items = "".join(f"<li style=\"margin:0 0 6px;\">{name}</li>" for name in files)
    files_block = (
        f"""<p style="margin:16px 0 8px;font-size:14px;color:#374151;">Documents included:</p>
        <ul style="margin:0 0 16px;padding-left:18px;font-size:14px;color:#111827;">{file_items}</ul>"""
        if file_items
        else ""
    )
    signoff = sender
    if sender_em:
        signoff = f"{sender}<br>{sender_em}"
    html_body = f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,Helvetica,sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6;padding:24px 12px;">
    <tr>
      <td align="center">
        <table role="presentation" width="600" cellspacing="0" cellpadding="0" style="max-width:600px;background:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb;">
          <tr>
            <td style="background:{PRIMARY_COLOR};padding:20px 28px;">
              <p style="margin:0;font-size:18px;font-weight:700;color:#ffffff;">{html.escape(BRAND_NAME)}</p>
            </td>
          </tr>
          <tr>
            <td style="padding:28px;">
              <p style="margin:0 0 12px;font-size:16px;color:#111827;">Hello,</p>
              <p style="margin:0 0 16px;font-size:15px;line-height:1.55;color:#374151;">
                {html.escape(BRAND_NAME)} has shared documents with you for <strong>{biz}</strong>.
                You can open the folder below (Google account required).
              </p>
              <p style="margin:0 0 20px;">
                <a href="{folder}" style="display:inline-block;background:{PRIMARY_COLOR};color:#ffffff;text-decoration:none;font-size:14px;font-weight:700;padding:12px 18px;border-radius:8px;">
                  Open shared documents
                </a>
              </p>
              {files_block}
              <p style="margin:0 0 16px;font-size:13px;line-height:1.5;color:#6b7280;">
                If the button does not work, copy this link:<br>
                <a href="{folder}" style="color:{PRIMARY_COLOR};word-break:break-all;">{folder}</a>
              </p>
              <p style="margin:0;font-size:14px;line-height:1.55;color:#374151;">
                Kind regards,<br>
                {signoff}<br>
                {html.escape(BRAND_NAME)}
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
    text_names = "\n".join(f"- {name}" for name in (file_names or []) if str(name).strip())
    text_body = (
        f"Hello,\n\n{BRAND_NAME} has shared documents with you for {(business_name or '').strip() or 'your organisation'}.\n\n"
        f"Open the folder: {(folder_url or '').strip()}\n"
    )
    if text_names:
        text_body += f"\nDocuments included:\n{text_names}\n"
    text_body += f"\nKind regards,\n{(sender_name or '').strip() or BRAND_NAME}\n"
    return {
        "subject": build_share_folder_subject(business_name),
        "html_body": html_body,
        "body_text": text_body,
    }


def send_share_folder_emails(
    *,
    recipients: list[str],
    business_name: str,
    folder_url: str,
    file_names: list[str],
    sender_name: str = "",
    sender_email: str = "",
) -> list[dict[str, Any]]:
    content = build_share_folder_email(
        business_name=business_name,
        folder_url=folder_url,
        file_names=file_names,
        sender_name=sender_name,
        sender_email=sender_email,
    )
    webhook = share_folder_email_webhook_url()
    if not webhook:
        return [
            {
                "email": recipient,
                "action": "failed",
                "error": "Share-folder email webhook is not configured.",
            }
            for recipient in recipients
        ]
    results: list[dict[str, Any]] = []
    for recipient in recipients:
        payload = {
            "to": recipient,
            "subject": content["subject"],
            "body_html": content["html_body"],
            "body_text": content["body_text"],
            "html_body": content["html_body"],
            "email_subject": content["subject"],
            "recipient_email": recipient,
            "event": "share_folder_email",
            "business_name": business_name,
            "folder_url": folder_url,
            "requested_by": sender_email or None,
            "recipient": {"email": recipient},
            "message": {"subject": content["subject"], "html_body": content["html_body"]},
        }
        try:
            response = httpx.post(webhook, json=payload, timeout=30.0)
            if response.status_code >= 400:
                results.append(
                    {
                        "email": recipient,
                        "action": "failed",
                        "error": f"Email webhook {response.status_code}: {response.text[:300]}",
                    }
                )
                continue
            results.append({"email": recipient, "action": "sent", "subject": content["subject"]})
        except Exception as exc:
            logger.warning("[share_folder] email send failed for %s: %s", recipient, exc)
            results.append({"email": recipient, "action": "failed", "error": str(exc)})
    return results
