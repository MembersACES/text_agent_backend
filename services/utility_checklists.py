"""Electricity and gas checklists stored on the client. No Drive upload in version 1."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models import Client, UtilityChecklist
from utils.timezone import to_melbourne_iso

FUEL_BY_UTILITY_TYPE = {
    "electricity": "electricity",
    "gas": "gas",
    "electricity_ci": "electricity",
    "electricity_sme": "electricity",
    "gas_ci": "gas",
    "gas_sme": "gas",
}

# One checklist per fuel. Account identifiers stay on the Airtable records, not on this row.
FUEL_SCOPE_IDENTIFIER = "*"

CONFLICT_DETAIL = "Someone else saved this checklist. Reload to see the latest version."


class ChecklistError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


@dataclass
class ChecklistWrite:
    utility_type: str
    identifier: str
    status: str
    answers: dict[str, Any]
    template_version: int
    row_version: Optional[int]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def normalise_identifier(value: str) -> str:
    return (value or "").strip().upper()


def fuel_for_utility_type(utility_type: str) -> str:
    fuel = FUEL_BY_UTILITY_TYPE.get((utility_type or "").strip())
    if fuel is None:
        raise ChecklistError(400, "utility_type must be electricity_ci, electricity_sme, gas_ci, or gas_sme")
    return fuel


def _answers_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def checklist_to_dict(row: UtilityChecklist) -> dict[str, Any]:
    return {
        "id": row.id,
        "client_id": row.client_id,
        "fuel": row.fuel,
        "utility_type": row.utility_type,
        "identifier": row.identifier,
        "status": row.status,
        "answers": _answers_dict(row.answers),
        "template_version": row.template_version,
        "row_version": row.row_version,
        "completed_at": to_melbourne_iso(row.completed_at) if row.completed_at else None,
        "updated_by": row.updated_by,
        "drive_file_id": row.drive_file_id,
        "created_at": to_melbourne_iso(row.created_at) if row.created_at else None,
        "updated_at": to_melbourne_iso(row.updated_at) if row.updated_at else None,
    }


def _require_client(db: Session, client_id: int) -> None:
    exists = db.query(Client.id).filter(Client.id == client_id).first()
    if exists is None:
        raise ChecklistError(404, "Client not found")


def _prepare(write: ChecklistWrite) -> tuple[str, str, str, datetime | None]:
    if write.status not in ("draft", "complete"):
        raise ChecklistError(400, "status must be draft or complete")
    if not isinstance(write.answers, dict):
        raise ChecklistError(400, "answers must be an object")
    if write.template_version < 1:
        raise ChecklistError(400, "template_version must be a positive integer")
    utility_type = (write.utility_type or "").strip()
    fuel = fuel_for_utility_type(utility_type)
    if utility_type in ("electricity", "gas"):
        identifier = FUEL_SCOPE_IDENTIFIER
    else:
        identifier = normalise_identifier(write.identifier)
    if not identifier:
        raise ChecklistError(400, "identifier is required")
    if len(identifier) > 255:
        raise ChecklistError(400, "identifier is too long")
    completed_at = _utcnow() if write.status == "complete" else None
    return fuel, utility_type, identifier, completed_at


def list_checklists(db: Session, client_id: int) -> list[dict[str, Any]]:
    _require_client(db, client_id)
    rows = (
        db.query(UtilityChecklist)
        .filter(UtilityChecklist.client_id == client_id)
        .order_by(UtilityChecklist.fuel.asc(), UtilityChecklist.identifier.asc())
        .all()
    )
    return [checklist_to_dict(row) for row in rows]


def _insert_checklist(
    db: Session,
    *,
    client_id: int,
    fuel: str,
    utility_type: str,
    identifier: str,
    status: str,
    answers: dict[str, Any],
    template_version: int,
    completed_at: datetime | None,
    updated_by: Optional[str],
) -> dict[str, Any]:
    row = UtilityChecklist(
        client_id=client_id,
        fuel=fuel,
        utility_type=utility_type,
        identifier=identifier,
        status=status,
        answers=json.dumps(answers),
        template_version=template_version,
        row_version=1,
        completed_at=completed_at,
        updated_by=updated_by,
        drive_file_id=None,
    )
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise ChecklistError(409, CONFLICT_DETAIL) from None
    db.refresh(row)
    return checklist_to_dict(row)


def _update_checklist(
    db: Session,
    existing: UtilityChecklist,
    *,
    utility_type: str,
    status: str,
    answers: dict[str, Any],
    template_version: int,
    loaded_version: int,
    completed_at: datetime | None,
    updated_by: Optional[str],
) -> dict[str, Any]:
    if status == "complete":
        stored_completed = existing.completed_at or completed_at
    else:
        stored_completed = None
    row_id = existing.id
    stmt = (
        update(UtilityChecklist)
        .where(
            UtilityChecklist.id == row_id,
            UtilityChecklist.row_version == loaded_version,
        )
        .values(
            utility_type=utility_type,
            status=status,
            answers=json.dumps(answers),
            template_version=template_version,
            completed_at=stored_completed,
            updated_by=updated_by,
            updated_at=_utcnow(),
            row_version=UtilityChecklist.row_version + 1,
        )
        .execution_options(synchronize_session=False)
    )
    result = db.execute(stmt)
    if result.rowcount != 1:
        db.rollback()
        raise ChecklistError(409, CONFLICT_DETAIL)
    db.commit()
    db.expire_all()
    fresh = db.query(UtilityChecklist).filter(UtilityChecklist.id == row_id).one()
    return checklist_to_dict(fresh)


def save_checklist(
    db: Session,
    client_id: int,
    write: ChecklistWrite,
    updated_by: Optional[str],
) -> dict[str, Any]:
    _require_client(db, client_id)
    fuel, utility_type, identifier, completed_at = _prepare(write)
    existing = (
        db.query(UtilityChecklist)
        .filter(
            UtilityChecklist.client_id == client_id,
            UtilityChecklist.fuel == fuel,
            UtilityChecklist.identifier == identifier,
        )
        .one_or_none()
    )
    if existing is None:
        return _insert_checklist(
            db,
            client_id=client_id,
            fuel=fuel,
            utility_type=utility_type,
            identifier=identifier,
            status=write.status,
            answers=write.answers,
            template_version=write.template_version,
            completed_at=completed_at,
            updated_by=updated_by,
        )
    if write.row_version is None:
        raise ChecklistError(409, CONFLICT_DETAIL)
    return _update_checklist(
        db,
        existing,
        utility_type=utility_type,
        status=write.status,
        answers=write.answers,
        template_version=write.template_version,
        loaded_version=write.row_version,
        completed_at=completed_at,
        updated_by=updated_by,
    )
