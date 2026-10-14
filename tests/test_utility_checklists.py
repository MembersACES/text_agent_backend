"""Utility checklist persistence: version conflicts, unique races, and answer retention."""

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import models  # noqa: F401
from database import Base, get_db
from models import Client, UtilityChecklist
from services.utility_checklists import (
    CONFLICT_DETAIL,
    ChecklistError,
    ChecklistWrite,
    _insert_checklist,
    save_checklist,
)
from utility_checklist_routes import register_utility_checklist_routes


def _session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, Session


def _client(db) -> Client:
    client = Client(business_name="Acme Hotel")
    db.add(client)
    db.commit()
    db.refresh(client)
    return client


def _write(**overrides) -> ChecklistWrite:
    payload = {
        "utility_type": "electricity_sme",
        "identifier": " nmi12ab ",
        "status": "draft",
        "answers": {"retailer": "Alinta", "retired_question": "keep me"},
        "template_version": 1,
        "row_version": None,
    }
    payload.update(overrides)
    return ChecklistWrite(**payload)


def test_fuel_level_checklist_is_not_stored_per_account():
    _engine, Session = _session()
    db = Session()
    client = _client(db)
    saved = save_checklist(
        db,
        client.id,
        _write(utility_type="electricity", identifier="VEEEOUY2S"),
        "pat@acesolutions.com.au",
    )
    assert saved["fuel"] == "electricity"
    assert saved["identifier"] == "*"
    assert saved["utility_type"] == "electricity"
    again = save_checklist(
        db,
        client.id,
        _write(utility_type="electricity", identifier="64076417567", row_version=saved["row_version"]),
        "pat@acesolutions.com.au",
    )
    assert again["id"] == saved["id"]
    assert db.query(UtilityChecklist).filter(UtilityChecklist.client_id == client.id).count() == 1


def test_insert_normalises_identifier_and_keeps_unknown_answers():
    _engine, Session = _session()
    db = Session()
    client = _client(db)
    saved = save_checklist(db, client.id, _write(), "pat@acesolutions.com.au")
    assert saved["identifier"] == "NMI12AB"
    assert saved["fuel"] == "electricity"
    assert saved["utility_type"] == "electricity_sme"
    assert saved["row_version"] == 1
    assert saved["drive_file_id"] is None
    assert saved["answers"]["retired_question"] == "keep me"
    assert saved["status"] == "draft"
    assert saved["completed_at"] is None


def test_same_fuel_and_identifier_updates_utility_type():
    _engine, Session = _session()
    db = Session()
    client = _client(db)
    first = save_checklist(db, client.id, _write(), "pat@acesolutions.com.au")
    second = save_checklist(
        db,
        client.id,
        _write(utility_type="electricity_ci", identifier="nmi12ab", row_version=first["row_version"]),
        "pat@acesolutions.com.au",
    )
    assert second["id"] == first["id"]
    assert second["utility_type"] == "electricity_ci"
    assert second["fuel"] == "electricity"
    assert second["row_version"] == 2
    assert second["answers"]["retired_question"] == "keep me"
    assert db.query(UtilityChecklist).filter(UtilityChecklist.client_id == client.id).count() == 1


def test_stale_row_version_returns_409_and_leaves_the_row():
    _engine, Session = _session()
    db = Session()
    client = _client(db)
    first = save_checklist(db, client.id, _write(), "pat@acesolutions.com.au")
    save_checklist(
        db,
        client.id,
        _write(row_version=first["row_version"], answers={"retailer": "Origin", "retired_question": "keep me"}),
        "pat@acesolutions.com.au",
    )
    try:
        save_checklist(
            db,
            client.id,
            _write(row_version=first["row_version"], answers={"retailer": "stale"}),
            "other@acesolutions.com.au",
        )
        raise AssertionError("expected conflict")
    except ChecklistError as exc:
        assert exc.status_code == 409
        assert exc.detail == CONFLICT_DETAIL
    row = db.query(UtilityChecklist).filter(UtilityChecklist.id == first["id"]).one()
    assert row.row_version == 2
    assert "Origin" in row.answers


def test_update_is_a_single_conditional_statement():
    engine, Session = _session()
    statements: list[str] = []

    def record(conn, cursor, statement, parameters, context, executemany):
        del conn, cursor, parameters, context, executemany
        if statement.lstrip().upper().startswith("UPDATE"):
            statements.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    db = Session()
    client = _client(db)
    first = save_checklist(db, client.id, _write(), "pat@acesolutions.com.au")
    statements.clear()
    save_checklist(
        db,
        client.id,
        _write(row_version=first["row_version"], status="complete"),
        "pat@acesolutions.com.au",
    )
    assert len(statements) == 1
    sql = statements[0].upper()
    assert "ROW_VERSION" in sql
    assert "WHERE" in sql


def test_complete_then_reopen_clears_completed_at():
    _engine, Session = _session()
    db = Session()
    client = _client(db)
    draft = save_checklist(db, client.id, _write(), "pat@acesolutions.com.au")
    done = save_checklist(
        db,
        client.id,
        _write(row_version=draft["row_version"], status="complete"),
        "pat@acesolutions.com.au",
    )
    assert done["completed_at"]
    again = save_checklist(
        db,
        client.id,
        _write(row_version=done["row_version"], status="complete", answers={"retailer": "Alinta", "retired_question": "keep me"}),
        "pat@acesolutions.com.au",
    )
    assert again["completed_at"] == done["completed_at"]
    reopened = save_checklist(
        db,
        client.id,
        _write(row_version=again["row_version"], status="draft"),
        "pat@acesolutions.com.au",
    )
    assert reopened["completed_at"] is None
    assert reopened["status"] == "draft"


def test_duplicate_insert_is_409_not_500():
    _engine, Session = _session()
    db = Session()
    client = _client(db)
    save_checklist(db, client.id, _write(), "pat@acesolutions.com.au")
    try:
        _insert_checklist(
            db,
            client_id=client.id,
            fuel="electricity",
            utility_type="electricity_sme",
            identifier="NMI12AB",
            status="draft",
            answers={"retailer": "race"},
            template_version=1,
            completed_at=None,
            updated_by="pat@acesolutions.com.au",
        )
        raise AssertionError("expected conflict")
    except ChecklistError as exc:
        assert exc.status_code == 409
    again = save_checklist(
        db,
        client.id,
        _write(row_version=1, answers={"retailer": "after race", "retired_question": "keep me"}),
        "pat@acesolutions.com.au",
    )
    assert again["answers"]["retailer"] == "after race"


def test_routes_map_conflicts_to_409():
    _engine, Session = _session()
    app = FastAPI()

    def fake_user():
        return {"idinfo": {"email": "pat@acesolutions.com.au"}}

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    register_utility_checklist_routes(app, fake_user)
    app.dependency_overrides[get_db] = override_get_db
    db = Session()
    client = _client(db)
    db.close()

    client_http = TestClient(app)
    created = client_http.put(
        f"/api/clients/{client.id}/utility-checklists",
        json={
            "utility_type": "gas_ci",
            "identifier": "mr9",
            "status": "draft",
            "answers": {"notes": "first", "old_key": "stay"},
            "template_version": 1,
        },
    )
    assert created.status_code == 200
    body = created.json()
    assert body["fuel"] == "gas"
    assert body["identifier"] == "MR9"
    assert body["row_version"] == 1
    assert body["answers"]["old_key"] == "stay"
    assert body["drive_file_id"] is None

    missing_version = client_http.put(
        f"/api/clients/{client.id}/utility-checklists",
        json={
            "utility_type": "gas_sme",
            "identifier": "mr9",
            "status": "draft",
            "answers": {"notes": "second"},
            "template_version": 1,
        },
    )
    assert missing_version.status_code == 409

    stale = client_http.put(
        f"/api/clients/{client.id}/utility-checklists",
        json={
            "utility_type": "gas_sme",
            "identifier": "mr9",
            "status": "complete",
            "answers": {"notes": "first", "old_key": "stay"},
            "template_version": 1,
            "row_version": 99,
        },
    )
    assert stale.status_code == 409
    listed = client_http.get(f"/api/clients/{client.id}/utility-checklists")
    assert listed.status_code == 200
    rows = listed.json()
    assert len(rows) == 1
    assert rows[0]["utility_type"] == "gas_ci"
    assert rows[0]["row_version"] == 1
    assert rows[0]["answers"]["old_key"] == "stay"

    missing = client_http.get("/api/clients/999999/utility-checklists")
    assert missing.status_code == 404
