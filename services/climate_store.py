"""Persistence for climate_* tables (drift events, activity records, ingest runs)."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from models import ClimateActivityRecord, ClimateDriftEvent, ClimateIngestRun


def _parse_emitted_at(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def save_drift_event(db: Session, payload: dict) -> ClimateDriftEvent:
    event_id = str(payload.get("event_id") or "").strip()
    if not event_id:
        raise ValueError("event_id required")

    existing = db.query(ClimateDriftEvent).filter(ClimateDriftEvent.event_id == event_id).first()
    if existing:
        return existing

    affected = payload.get("affected") if isinstance(payload.get("affected"), dict) else {}
    entity_ids = affected.get("entity_ids") if isinstance(affected.get("entity_ids"), list) else []

    row = ClimateDriftEvent(
        event_id=event_id,
        event_type=str(payload.get("event_type") or "") or None,
        severity=str(payload.get("severity") or "") or None,
        emitted_at=_parse_emitted_at(payload.get("emitted_at")),
        affected_scope=str(affected.get("scope") or "") or None,
        affected_entity_ids_json=json.dumps(entity_ids),
        payload_json=json.dumps(payload),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _entity_matches(reporting_entity: str, entity_ids: list[str]) -> bool:
    if not reporting_entity:
        return False
    re = reporting_entity.strip().lower()
    for raw in entity_ids:
        eid = str(raw or "").strip().lower()
        if not eid:
            continue
        if eid == re:
            return True
        # rc3 ent_* vs A1 kebab-case loose match
        eid_slug = eid.removeprefix("ent_").replace("_", "-")
        if eid_slug == re or re in eid or eid.endswith(re):
            return True
    return False


def drift_event_affects_entity(row: ClimateDriftEvent, reporting_entity: str) -> bool:
    if row.affected_scope == "global":
        return True
    if not reporting_entity:
        return row.affected_scope == "global"

    try:
        entity_ids = json.loads(row.affected_entity_ids_json or "[]")
    except json.JSONDecodeError:
        entity_ids = []

    if not entity_ids and row.affected_scope in (None, "", "global"):
        return True
    return _entity_matches(reporting_entity, entity_ids if isinstance(entity_ids, list) else [])


def list_drift_events(
    db: Session,
    *,
    reporting_entity: Optional[str] = None,
    unacknowledged_only: bool = False,
    limit: int = 100,
) -> list[ClimateDriftEvent]:
    q = db.query(ClimateDriftEvent).order_by(ClimateDriftEvent.received_at.desc())
    if unacknowledged_only:
        q = q.filter(ClimateDriftEvent.acknowledged_at.is_(None))
    rows = q.limit(max(1, min(limit, 500))).all()
    if reporting_entity:
        rows = [r for r in rows if drift_event_affects_entity(r, reporting_entity)]
    return rows


def drift_event_to_dict(row: ClimateDriftEvent) -> dict:
    try:
        payload = json.loads(row.payload_json)
    except json.JSONDecodeError:
        payload = {}
    return {
        "id": row.id,
        "event_id": row.event_id,
        "event_type": row.event_type,
        "severity": row.severity,
        "emitted_at": row.emitted_at.isoformat() if row.emitted_at else None,
        "affected_scope": row.affected_scope,
        "received_at": row.received_at.isoformat() if row.received_at else None,
        "acknowledged": row.acknowledged_at is not None,
        "payload": payload,
    }


def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def upsert_activity_record(
    db: Session,
    *,
    record_id: str,
    client_id: Optional[int],
    body: dict,
    source_system: str = "airtable_invoice",
    source_utility_type: Optional[str] = None,
    source_row_id: Optional[str] = None,
    status: str = "draft",
) -> tuple[ClimateActivityRecord, bool]:
    existing = db.query(ClimateActivityRecord).filter(ClimateActivityRecord.record_id == record_id).first()
    period = body.get("reporting_period") if isinstance(body.get("reporting_period"), dict) else {}
    created = existing is None

    fields = dict(
        client_id=client_id,
        entity_id=str(body.get("entity_id") or ""),
        site_id=body.get("site_id"),
        loa_client_id=body.get("client_id"),
        activity_type=str(body.get("activity_type") or ""),
        scope=int(body.get("scope") or 0),
        reporting_period_start=_coerce_date(period.get("start")),
        reporting_period_end=_coerce_date(period.get("end")),
        quantity=body.get("quantity"),
        unit=body.get("unit"),
        status=status,
        source_system=source_system,
        source_utility_type=source_utility_type,
        source_row_id=source_row_id,
        body_json=json.dumps(body),
    )

    if existing:
        for key, val in fields.items():
            setattr(existing, key, val)
        db.commit()
        db.refresh(existing)
        return existing, False

    row = ClimateActivityRecord(record_id=record_id, **fields)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row, created


def list_activity_records(
    db: Session,
    *,
    client_id: int,
    limit: int = 50,
) -> list[ClimateActivityRecord]:
    return (
        db.query(ClimateActivityRecord)
        .filter(ClimateActivityRecord.client_id == client_id)
        .order_by(ClimateActivityRecord.reporting_period_start.desc(), ClimateActivityRecord.id.desc())
        .limit(max(1, min(limit, 200)))
        .all()
    )


def activity_record_to_summary(row: ClimateActivityRecord) -> dict:
    return {
        "record_id": row.record_id,
        "entity_id": row.entity_id,
        "site_id": row.site_id,
        "activity_type": row.activity_type,
        "scope": row.scope,
        "quantity": row.quantity,
        "unit": row.unit,
        "status": row.status,
        "reporting_period": {
            "start": row.reporting_period_start.isoformat() if row.reporting_period_start else None,
            "end": row.reporting_period_end.isoformat() if row.reporting_period_end else None,
        },
        "source_utility_type": row.source_utility_type,
        "source_row_id": row.source_row_id,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def create_ingest_run(
    db: Session,
    *,
    client_id: int,
    utility_type: str,
    identifier: str,
    period_start,
    period_end,
    records_created: int,
    records_updated: int,
    records_skipped: int,
    status: str,
    diagnostics: dict,
) -> ClimateIngestRun:
    row = ClimateIngestRun(
        client_id=client_id,
        utility_type=utility_type,
        identifier=identifier,
        reporting_period_start=period_start,
        reporting_period_end=period_end,
        records_created=records_created,
        records_updated=records_updated,
        records_skipped=records_skipped,
        status=status,
        diagnostics_json=json.dumps(diagnostics),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
