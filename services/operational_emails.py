"""CRUD, seed, render, and lookup for operational email templates/recipients."""

from __future__ import annotations

import json
import logging
import re
from html import escape
from typing import Any, Optional

from sqlalchemy.orm import Session

from models import OperationalEmailRecipient, OperationalEmailTemplate
from services.operational_email_defaults import all_recipients, all_templates

logger = logging.getLogger(__name__)

TOKEN_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")
ALLOWED_EDITOR_DOMAINS = frozenset({"acesolutions.com.au", "czeroanz.com"})


class OperationalEmailError(ValueError):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def editor_domain_allowed(email: str | None) -> bool:
    if not email or "@" not in email:
        return False
    domain = email.rsplit("@", 1)[-1].strip().lower()
    return domain in ALLOWED_EDITOR_DOMAINS


def assert_editor_allowed(email: str | None) -> str:
    actor = (email or "").strip()
    if not editor_domain_allowed(actor):
        raise OperationalEmailError(
            "Email templates can only be edited by @acesolutions.com.au or @czeroanz.com accounts.",
            status_code=403,
        )
    return actor


def _loads_list(raw: Any) -> list[str]:
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return [part.strip() for part in str(raw).split(",") if part.strip()]
    if isinstance(parsed, list):
        return [str(item).strip() for item in parsed if str(item).strip()]
    if isinstance(parsed, str):
        return [part.strip() for part in parsed.split(",") if part.strip()]
    return []


def _dumps_list(values: list[str] | None) -> str:
    return json.dumps([str(item).strip() for item in (values or []) if str(item).strip()])


def _loads_obj(raw: Any) -> dict[str, Any]:
    if raw is None or raw == "":
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def render_tokens(text: str, values: dict[str, Any], html_escape: bool = False) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = values.get(key)
        if value is None:
            return ""
        rendered = str(value)
        if html_escape and not key.endswith("_html"):
            return escape(rendered)
        return rendered

    return TOKEN_RE.sub(replace, text or "")


def template_to_dict(row: OperationalEmailTemplate) -> dict[str, Any]:
    return {
        "id": row.id,
        "key": row.key,
        "category": row.category,
        "name": row.name,
        "description": row.description or "",
        "subject": row.subject,
        "html_body": row.html_body,
        "merge_fields": _loads_list(row.merge_fields),
        "sample_values": _loads_obj(row.sample_values),
        "updated_by": row.updated_by,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def recipient_to_dict(row: OperationalEmailRecipient) -> dict[str, Any]:
    emails = _loads_list(row.emails_json)
    return {
        "id": row.id,
        "flow": row.flow,
        "key": row.key,
        "display_name": row.display_name,
        "emails": emails,
        "email": ", ".join(emails),
        "aliases": _loads_list(row.aliases_json),
        "group_name": row.group_name or "",
        "extra_groups": _loads_list(row.extra_groups_json),
        "is_placeholder": bool(row.is_placeholder),
        "is_active": bool(row.is_active),
        "sort_order": row.sort_order,
        "updated_by": row.updated_by,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def seed_operational_emails(db: Session) -> dict[str, int]:
    inserted_templates = 0
    inserted_recipients = 0
    existing_template_keys = {
        key for (key,) in db.query(OperationalEmailTemplate.key).all()
    }
    for spec in all_templates():
        if spec["key"] in existing_template_keys:
            continue
        db.add(
            OperationalEmailTemplate(
                key=spec["key"],
                category=spec["category"],
                name=spec["name"],
                description=spec.get("description") or None,
                subject=spec["subject"],
                html_body=spec["html_body"],
                merge_fields=_dumps_list(spec.get("merge_fields") or []),
                sample_values=json.dumps(spec.get("sample_values") or {}),
            )
        )
        inserted_templates += 1
    existing_recipient_keys = {
        (flow, key)
        for flow, key in db.query(OperationalEmailRecipient.flow, OperationalEmailRecipient.key).all()
    }
    for spec in all_recipients():
        pair = (spec["flow"], spec["key"])
        if pair in existing_recipient_keys:
            continue
        db.add(
            OperationalEmailRecipient(
                flow=spec["flow"],
                key=spec["key"],
                display_name=spec["display_name"],
                emails_json=_dumps_list(spec["emails"]),
                aliases_json=_dumps_list(spec.get("aliases") or []),
                group_name=spec.get("group_name") or None,
                extra_groups_json=_dumps_list(spec.get("extra_groups") or []),
                is_placeholder=1 if spec.get("is_placeholder") else 0,
                is_active=1,
                sort_order=int(spec.get("sort_order") or 0),
            )
        )
        inserted_recipients += 1
    if inserted_templates or inserted_recipients:
        db.commit()
    return {"templates": inserted_templates, "recipients": inserted_recipients}


def list_templates(db: Session, category: str | None = None) -> list[OperationalEmailTemplate]:
    query = db.query(OperationalEmailTemplate)
    if category:
        query = query.filter(OperationalEmailTemplate.category == category)
    return query.order_by(OperationalEmailTemplate.category, OperationalEmailTemplate.name).all()


def get_template(db: Session, key: str) -> OperationalEmailTemplate:
    row = db.query(OperationalEmailTemplate).filter(OperationalEmailTemplate.key == key).first()
    if not row:
        raise OperationalEmailError(f"Unknown email template: {key}", status_code=404)
    return row


def get_template_or_none(db: Session, key: str) -> Optional[OperationalEmailTemplate]:
    return db.query(OperationalEmailTemplate).filter(OperationalEmailTemplate.key == key).first()


def update_template(
    db: Session,
    key: str,
    *,
    subject: str | None = None,
    html_body: str | None = None,
    name: str | None = None,
    description: str | None = None,
    updated_by: str | None = None,
) -> OperationalEmailTemplate:
    row = get_template(db, key)
    if subject is not None:
        row.subject = subject
    if html_body is not None:
        row.html_body = html_body
    if name is not None:
        row.name = name.strip() or row.name
    if description is not None:
        row.description = description
    if updated_by:
        row.updated_by = updated_by
    db.commit()
    db.refresh(row)
    return row


def list_recipients(db: Session, flow: str | None = None, include_inactive: bool = False) -> list[OperationalEmailRecipient]:
    query = db.query(OperationalEmailRecipient)
    if flow:
        query = query.filter(OperationalEmailRecipient.flow == flow)
    if not include_inactive:
        query = query.filter(OperationalEmailRecipient.is_active == 1)
    return query.order_by(
        OperationalEmailRecipient.flow,
        OperationalEmailRecipient.sort_order,
        OperationalEmailRecipient.key,
    ).all()


def get_recipient(db: Session, recipient_id: int) -> OperationalEmailRecipient:
    row = db.query(OperationalEmailRecipient).filter(OperationalEmailRecipient.id == recipient_id).first()
    if not row:
        raise OperationalEmailError("Recipient list not found.", status_code=404)
    return row


def get_recipient_by_key(
    db: Session,
    flow: str,
    key: str,
    include_inactive: bool = False,
) -> Optional[OperationalEmailRecipient]:
    query = db.query(OperationalEmailRecipient).filter(
        OperationalEmailRecipient.flow == flow,
        OperationalEmailRecipient.key == key,
    )
    if not include_inactive:
        query = query.filter(OperationalEmailRecipient.is_active == 1)
    return query.first()


def create_recipient(
    db: Session,
    *,
    flow: str,
    key: str,
    display_name: str,
    emails: list[str],
    group_name: str | None = None,
    aliases: list[str] | None = None,
    extra_groups: list[str] | None = None,
    is_placeholder: bool = False,
    updated_by: str | None = None,
) -> OperationalEmailRecipient:
    flow_value = (flow or "").strip()
    key_value = (key or "").strip()
    if not flow_value or not key_value:
        raise OperationalEmailError("flow and key are required.")
    if get_recipient_by_key(db, flow_value, key_value, include_inactive=True):
        raise OperationalEmailError(f"A recipient list already exists for {flow_value} / {key_value}.")
    if not emails:
        raise OperationalEmailError("At least one email address is required.")
    row = OperationalEmailRecipient(
        flow=flow_value,
        key=key_value,
        display_name=(display_name or key_value).strip(),
        emails_json=_dumps_list(emails),
        aliases_json=_dumps_list(aliases or []),
        group_name=(group_name or "").strip() or None,
        extra_groups_json=_dumps_list(extra_groups or []),
        is_placeholder=1 if is_placeholder else 0,
        is_active=1,
        sort_order=0,
        updated_by=updated_by,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def update_recipient(
    db: Session,
    recipient_id: int,
    *,
    display_name: str | None = None,
    emails: list[str] | None = None,
    aliases: list[str] | None = None,
    group_name: str | None = None,
    extra_groups: list[str] | None = None,
    is_placeholder: bool | None = None,
    is_active: bool | None = None,
    key: str | None = None,
    updated_by: str | None = None,
) -> OperationalEmailRecipient:
    row = get_recipient(db, recipient_id)
    if key is not None:
        next_key = key.strip()
        if not next_key:
            raise OperationalEmailError("key cannot be empty.")
        clash = get_recipient_by_key(db, row.flow, next_key, include_inactive=True)
        if clash and clash.id != row.id:
            raise OperationalEmailError(f"A recipient list already exists for {row.flow} / {next_key}.")
        row.key = next_key
    if display_name is not None:
        row.display_name = display_name.strip() or row.display_name
    if emails is not None:
        if not emails:
            raise OperationalEmailError("At least one email address is required.")
        row.emails_json = _dumps_list(emails)
    if aliases is not None:
        row.aliases_json = _dumps_list(aliases)
    if group_name is not None:
        row.group_name = group_name.strip() or None
    if extra_groups is not None:
        row.extra_groups_json = _dumps_list(extra_groups)
    if is_placeholder is not None:
        row.is_placeholder = 1 if is_placeholder else 0
    if is_active is not None:
        row.is_active = 1 if is_active else 0
    if updated_by:
        row.updated_by = updated_by
    db.commit()
    db.refresh(row)
    return row


def deactivate_recipient(db: Session, recipient_id: int, updated_by: str | None = None) -> OperationalEmailRecipient:
    return update_recipient(db, recipient_id, is_active=False, updated_by=updated_by)


def emails_csv(row: OperationalEmailRecipient) -> str:
    return ", ".join(_loads_list(row.emails_json))


def find_recipient_match(
    db: Session,
    flow: str,
    key: str,
) -> Optional[OperationalEmailRecipient]:
    exact = get_recipient_by_key(db, flow, key)
    if exact:
        return exact
    lowered = key.strip().lower()
    for row in list_recipients(db, flow):
        if row.key.lower() == lowered or row.display_name.lower() == lowered:
            return row
    return None


def find_data_request_recipient(
    db: Session,
    supplier_name: str,
    service_type: str,
) -> tuple[str, str, bool] | None:
    rows = list_recipients(db, "data_request")
    if not rows:
        return None
    supplier = (supplier_name or "").strip()
    normalized = supplier.lower().replace("pty ltd", "").replace("pty. limited", "").replace("(vic)", "").strip()

    def by_key(key: str) -> Optional[OperationalEmailRecipient]:
        return next((row for row in rows if row.key == key), None)

    for row in rows:
        if supplier.lower() == row.display_name.lower() or supplier.lower() == row.key.lower():
            return emails_csv(row), row.display_name, False
        for alias in _loads_list(row.aliases_json):
            if supplier.lower() == alias.lower():
                return emails_csv(row), row.display_name, False

    if "origin" in normalized:
        if service_type in ["electricity_ci", "gas_ci"]:
            match = by_key("Origin C&I Electricity") or by_key("Origin C&I Gas")
        else:
            match = by_key("Origin SME")
        if match:
            return emails_csv(match), match.display_name, False
    if "momentum" in normalized:
        if service_type in ["electricity_ci", "gas_ci"]:
            match = by_key("Momentum C&I Electricity")
        else:
            match = by_key("Momentum SME")
        if match:
            return emails_csv(match), match.display_name, False
    if "agl" in normalized:
        match = by_key("AGL C&I E & G")
        if match:
            return emails_csv(match), match.display_name, False
    if "shell" in normalized:
        match = by_key("Shell Energy")
        if match:
            return emails_csv(match), match.display_name, False
    if "alinta" in normalized:
        match = by_key("Alinta C&I Electricity & Gas") or by_key("Alinta C&I Electricity")
        if match:
            return emails_csv(match), match.display_name, False
    if "energyaustralia" in normalized or "energy australia" in normalized:
        match = by_key("Energy Australia C&I E & G")
        if match:
            return emails_csv(match), match.display_name, False

    for row in rows:
        haystacks = [row.key, row.display_name, *_loads_list(row.aliases_json)]
        if any(normalized and normalized in value.lower() for value in haystacks):
            return emails_csv(row), row.display_name, False

    other = by_key("Other")
    if other:
        return emails_csv(other), f"{supplier} (Unknown Retailer)", True
    return None


def quote_retailer_lookup(db: Session, selected: list[str]) -> tuple[list[str], dict[str, OperationalEmailRecipient]]:
    rows = {row.key: row for row in list_recipients(db, "quote_request")}
    missing = [name for name in selected if name not in rows]
    return missing, rows


def load_template_content(db: Session, key: str) -> tuple[str, str] | None:
    row = get_template_or_none(db, key)
    if not row:
        return None
    return row.subject, row.html_body


def with_db(callback):
    """Run a callback with a short-lived session. Returns None if DB is unavailable."""
    try:
        from database import SessionLocal

        db = SessionLocal()
    except Exception as exc:
        logger.warning("operational email db unavailable: %s", exc)
        return None
    try:
        return callback(db)
    except Exception as exc:
        logger.warning("operational email db lookup failed: %s", exc)
        return None
    finally:
        try:
            db.close()
        except Exception:
            pass
