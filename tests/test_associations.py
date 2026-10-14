"""Association records: name, status, and Drive folder linkage without live Drive."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from services.associations import (
    add_association_contact,
    create_association,
    delete_association_contact,
    ensure_association_contacts,
    register_existing_testimonial,
    update_association,
    update_association_contact,
)
from tools.association_contacts import (
    CONTACT_HEADERS,
    contacts_from_sheet_values,
    contacts_to_rows,
)
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


def test_register_existing_file_links_testimonial(monkeypatch):
    monkeypatch.setattr(
        "services.associations.create_named_folder",
        lambda parent_id, name, user_access_token=None: ("folder-3", True),
    )
    monkeypatch.setattr(
        "services.associations.ensure_testimonials_folder",
        lambda folder_id, user_access_token=None: "testimonials-3",
    )
    monkeypatch.setattr("services.associations.get_associations_parent_id", lambda: "parent-1")
    monkeypatch.setattr(
        "services.associations.file_is_under_association",
        lambda association_folder_id, file_id, user_access_token=None: (True, None, 200),
    )

    db = _db()
    created, err, _status = create_association(
        db,
        name="RSL Victoria",
        status="targeting",
        contact_name=None,
        contact_email=None,
        notes=None,
        results_note=None,
    )
    assert err is None and created is not None
    first, err, status = register_existing_testimonial(
        db,
        created["id"],
        file_id="drive-file-1",
        file_name="RSL Victoria Testimonial.png",
        testimonial_savings="Savings across the network",
    )
    assert err is None
    assert status == 200
    assert first is not None
    assert first.association_id == created["id"]
    assert first.status == "Draft"
    assert first.testimonial_solution_type_id == "association_endorsement"

    again, err, status = register_existing_testimonial(
        db,
        created["id"],
        file_id="drive-file-1",
        file_name="RSL Victoria Testimonial.png",
        testimonial_savings=None,
    )
    assert err is None and again is not None
    assert again.id == first.id
    assert status == 200


def test_contacts_sheet_round_trip_and_primary():
    source = [
        {
            "id": "abc",
            "name": "Alex",
            "role": "GM",
            "email": "alex@example.com",
            "phone": "1",
            "mobile": "2",
            "primary": True,
            "notes": "Main",
        },
        {
            "id": "def",
            "name": "Sam",
            "role": "",
            "email": "",
            "phone": "",
            "mobile": "",
            "primary": False,
            "notes": "",
        },
    ]
    parsed, dirty = contacts_from_sheet_values([list(CONTACT_HEADERS), *contacts_to_rows(source)])
    assert dirty is False
    assert parsed == source

    duplicated, dirty = contacts_from_sheet_values(
        [
            list(CONTACT_HEADERS),
            ["abc", "Alex", "GM", "alex@example.com", "1", "2", "Yes", ""],
            ["def", "Sam", "", "", "", "", "Yes", ""],
        ]
    )
    assert dirty is True
    assert [row["primary"] for row in duplicated] == [True, False]

    missing_id, dirty = contacts_from_sheet_values(
        [
            ["Contact Name", "Email", "Phone"],
            ["Alex", "alex@example.com", "1"],
        ]
    )
    assert dirty is False
    assert missing_id[0]["name"] == "Alex"
    assert missing_id[0]["email"] == "alex@example.com"
    assert missing_id[0]["phone"] == "1"
    assert missing_id[0]["id"]


def _patch_contact_sheet(monkeypatch):
    stored = {"rows": []}
    created = {"count": 0}

    def locate(folder_id, existing_id, user_access_token=None):
        if existing_id:
            return existing_id, "https://docs.example/sheet", False
        created["count"] += 1
        return "sheet-1", "https://docs.example/sheet", True

    def load(sheet_id, user_access_token=None):
        return [dict(row) for row in stored["rows"]], False

    def save(sheet_id, contacts, user_access_token=None):
        stored["rows"] = [dict(row) for row in contacts]

    monkeypatch.setattr("services.associations.locate_or_create_contacts_sheet", locate)
    monkeypatch.setattr("services.associations.load_sheet_contacts", load)
    monkeypatch.setattr("services.associations.save_sheet_contacts", save)
    return stored, created


def _association(monkeypatch, name="RSL Victoria", contact_name="Alex", contact_email="alex@example.com"):
    monkeypatch.setattr(
        "services.associations.create_named_folder",
        lambda parent_id, name, user_access_token=None: ("folder-contacts", True),
    )
    monkeypatch.setattr(
        "services.associations.ensure_testimonials_folder",
        lambda folder_id, user_access_token=None: "testimonials-contacts",
    )
    monkeypatch.setattr("services.associations.get_associations_parent_id", lambda: "parent-1")
    db = _db()
    created, err, status = create_association(
        db,
        name=name,
        status="working_with",
        contact_name=contact_name,
        contact_email=contact_email,
        notes=None,
        results_note=None,
    )
    assert err is None and status == 200 and created is not None
    return db, created


def test_ensure_contacts_seeds_primary_once(monkeypatch):
    stored, created_sheet = _patch_contact_sheet(monkeypatch)
    db, association = _association(monkeypatch)

    opened, err, status = ensure_association_contacts(db, association["id"])
    assert err is None and status == 200 and opened is not None
    assert opened["contacts_sheet_id"] == "sheet-1"
    assert opened["contact_name"] == "Alex"
    assert opened["contact_email"] == "alex@example.com"
    assert opened["contacts"][0]["primary"] is True
    assert opened["contacts"][0]["name"] == "Alex"
    assert created_sheet["count"] == 1
    assert len(stored["rows"]) == 1

    again, err, status = ensure_association_contacts(db, association["id"])
    assert err is None and status == 200 and again is not None
    assert created_sheet["count"] == 1
    assert len(again["contacts"]) == 1

    blocked, err, status = update_association(
        db,
        association["id"],
        {"contact_name": "Bob", "notes": "Keep this"},
    )
    assert err is None and status == 200 and blocked is not None
    assert blocked["contact_name"] == "Alex"
    assert blocked["notes"] == "Keep this"


def test_contact_primary_switches_and_delete_promotes(monkeypatch):
    _patch_contact_sheet(monkeypatch)
    db, association = _association(monkeypatch)
    opened, err, _status = ensure_association_contacts(db, association["id"])
    assert err is None and opened is not None
    alex_id = opened["contacts"][0]["id"]

    added, err, status = add_association_contact(
        db,
        association["id"],
        {
            "name": "Sam",
            "role": "President",
            "email": "sam@example.com",
            "phone": "08",
            "mobile": "0400",
            "primary": True,
            "notes": "Calls back",
        },
    )
    assert err is None and status == 200 and added is not None
    assert added["contact_name"] == "Sam"
    assert added["contact_email"] == "sam@example.com"
    assert sum(1 for row in added["contacts"] if row["primary"]) == 1

    renamed, err, status = update_association_contact(
        db,
        association["id"],
        alex_id,
        {"phone": "9321"},
    )
    assert err is None and status == 200 and renamed is not None
    alex = next(row for row in renamed["contacts"] if row["id"] == alex_id)
    assert alex["phone"] == "9321"
    assert renamed["contact_name"] == "Sam"

    sam_id = next(row["id"] for row in renamed["contacts"] if row["name"] == "Sam")
    removed, err, status = delete_association_contact(db, association["id"], sam_id)
    assert err is None and status == 200 and removed is not None
    assert len(removed["contacts"]) == 1
    assert removed["contacts"][0]["primary"] is True
    assert removed["contact_name"] == "Alex"

    empty_name, err, status = add_association_contact(db, association["id"], {"name": "  "})
    assert empty_name is None
    assert status == 400
    assert err

    alex_only = removed["contacts"][0]["id"]
    cleared, err, status = delete_association_contact(db, association["id"], alex_only)
    assert err is None and status == 200 and cleared is not None
    assert cleared["contacts"] == []
    assert cleared["contact_name"] is None
    assert cleared["contact_email"] is None

    reopened, err, status = ensure_association_contacts(db, association["id"])
    assert err is None and status == 200 and reopened is not None
    assert reopened["contacts"] == []
    assert reopened["contact_name"] is None
