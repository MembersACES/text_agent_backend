"""Association records: name, status, and Drive folder linkage without live Drive."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from services.associations import create_association, update_association
from tools.association_folders import clean_association_name, normalize_association_status


def test_clean_association_name():
    assert clean_association_name("  RSL / Victoria  ") == "RSL - Victoria"
    assert clean_association_name("   ") == ""


def test_normalize_status():
    assert normalize_association_status("Targeting") == "targeting"
    assert normalize_association_status("working with") == "working_with"
    assert normalize_association_status("endorsed") is None


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()


def test_create_association_stores_folder(monkeypatch):
    monkeypatch.setattr(
        "services.associations.create_named_folder",
        lambda parent_id, name, user_access_token=None: ("folder-1", True),
    )
    monkeypatch.setattr(
        "services.associations.ensure_testimonials_folder",
        lambda folder_id, user_access_token=None: "testimonials-1",
    )
    monkeypatch.setattr("services.associations.get_associations_parent_id", lambda: "parent-1")

    db = _db()
    payload, err, status = create_association(
        db,
        name="RSL Victoria",
        status="working_with",
        contact_name="Alex",
        contact_email="",
        notes=None,
        results_note="Group electricity review",
    )
    assert err is None
    assert status == 200
    assert payload is not None
    assert payload["name"] == "RSL Victoria"
    assert payload["status"] == "working_with"
    assert payload["drive_folder_id"] == "folder-1"
    assert payload["testimonials_folder_id"] == "testimonials-1"
    assert payload["contact_email"] is None
    assert payload["results_note"] == "Group electricity review"
    assert payload["endorsed"] is False

    again, err, status = create_association(
        db,
        name="rsl victoria",
        status="targeting",
        contact_name=None,
        contact_email=None,
        notes=None,
        results_note=None,
    )
    assert again is None
    assert status == 409
    assert err


def test_update_rejects_unknown_status(monkeypatch):
    monkeypatch.setattr(
        "services.associations.create_named_folder",
        lambda parent_id, name, user_access_token=None: ("folder-2", True),
    )
    monkeypatch.setattr(
        "services.associations.ensure_testimonials_folder",
        lambda folder_id, user_access_token=None: "testimonials-2",
    )
    monkeypatch.setattr("services.associations.get_associations_parent_id", lambda: "parent-1")

    db = _db()
    created, err, _status = create_association(
        db,
        name="Clubs WA",
        status="targeting",
        contact_name=None,
        contact_email=None,
        notes=None,
        results_note=None,
    )
    assert err is None and created is not None
    updated, err, status = update_association(db, created["id"], {"status": "paused"})
    assert updated is None
    assert status == 400
    assert err
