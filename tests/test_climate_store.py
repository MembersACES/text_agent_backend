"""Tests for climate_* persistence helpers."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import ClimateDriftEvent
from services.climate_store import list_drift_events, save_drift_event, upsert_activity_record


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()


def test_save_drift_event_dedupes():
    db = _session()
    payload = {
        "event_id": "drift_test1234567890",
        "event_type": "factor_pack_revision",
        "severity": "medium",
        "emitted_at": "2026-06-01T00:00:00Z",
        "affected": {"scope": "entity", "entity_ids": ["parramatta-leagues-club"]},
    }
    a = save_drift_event(db, payload)
    b = save_drift_event(db, payload)
    assert a.id == b.id
    assert db.query(ClimateDriftEvent).count() == 1


def test_drift_event_entity_filter():
    db = _session()
    save_drift_event(
        db,
        {
            "event_id": "drift_entitymatch0001",
            "event_type": "standards_update",
            "severity": "high",
            "affected": {"scope": "entity", "entity_ids": ["parramatta-leagues-club"]},
        },
    )
    save_drift_event(
        db,
        {
            "event_id": "drift_global0000000001",
            "event_type": "standards_update",
            "severity": "info",
            "affected": {"scope": "global"},
        },
    )
    rows = list_drift_events(db, reporting_entity="parramatta-leagues-club")
    assert len(rows) == 2


def test_upsert_activity_record():
    db = _session()
    body = {
        "entity_id": "parramatta-leagues-club",
        "client_id": "rec1",
        "activity_type": "electricity_grid",
        "scope": 2,
        "quantity": 100,
        "unit": "kWh",
        "reporting_period": {"start": "2025-07-01", "end": "2026-06-30"},
    }
    row, created = upsert_activity_record(db, record_id="act_test1", client_id=5, body=body)
    assert created
    assert row.client_id == 5
    row2, created2 = upsert_activity_record(
        db,
        record_id="act_test1",
        client_id=5,
        body={**body, "quantity": 200},
    )
    assert not created2
    assert row2.quantity == 200
