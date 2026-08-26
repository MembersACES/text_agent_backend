"""Unit tests for staff-created testimonial solution types (in-memory SQLite)."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from tools.testimonial_solution_content import (
    ALL_SOLUTION_TYPE_IDS,
    create_custom_type,
    delete_custom_type,
    get_merged_content,
    save_override,
    slugify_solution_type,
    solution_type_id_from_label,
)


def _session():
    from models import TestimonialSolutionType  # noqa: F401 — register table on Base.metadata

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def test_slugify_solution_type():
    assert slugify_solution_type("LED Upgrade") == "led_upgrade"
    assert slugify_solution_type("  GHG Roadmap  ") == "ghg_roadmap"
    try:
        slugify_solution_type("   ")
    except ValueError as exc:
        assert "letter or number" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_create_custom_type_is_listed_and_generatable():
    db = _session()
    created = create_custom_type(
        db,
        "LED Upgrade",
        {"key_outcome_metrics": "Lower lighting cost", "conclusion": "LEDs paid back."},
    )
    assert created["solution_type"] == "led_upgrade"
    assert created["solution_type_label"] == "LED Upgrade"
    assert created["key_outcome_metrics"] == "Lower lighting cost"

    listed = get_merged_content(None, db)
    ids = [item["solution_type"] for item in listed]
    assert "led_upgrade" in ids
    for built_in in ALL_SOLUTION_TYPE_IDS:
        assert built_in in ids

    fetched = get_merged_content("led_upgrade", db)
    assert fetched["conclusion"] == "LEDs paid back."


def test_create_rejects_built_in_name():
    db = _session()
    try:
        create_custom_type(db, "Client Endorsement")
    except ValueError as exc:
        assert "already exists" in str(exc).lower() or "existing type" in str(exc).lower()
    else:
        raise AssertionError("expected ValueError")


def test_delete_custom_type_removes_it_from_list():
    from models import TestimonialSolutionType

    db = _session()
    create_custom_type(db, "LED Upgrade")
    delete_custom_type(db, "led_upgrade")
    assert db.query(TestimonialSolutionType).filter_by(solution_type="led_upgrade").first() is None
    ids = [item["solution_type"] for item in get_merged_content(None, db)]
    assert "led_upgrade" not in ids


def test_delete_rejects_built_in_type():
    db = _session()
    try:
        delete_custom_type(db, "ci_electricity")
    except ValueError as exc:
        assert "built-in" in str(exc).lower()
    else:
        raise AssertionError("expected ValueError")


def test_delete_unknown_custom_type():
    db = _session()
    try:
        delete_custom_type(db, "not_a_real_type")
    except LookupError:
        pass
    else:
        raise AssertionError("expected LookupError")


def test_save_override_updates_custom_type():
    from models import TestimonialSolutionType

    db = _session()
    create_custom_type(db, "Battery Storage")
    updated = save_override(
        "battery_storage",
        {"key_approach_of_solution": "Sized against the evening peak."},
        db,
    )
    assert updated["key_approach_of_solution"] == "Sized against the evening peak."
    row = db.query(TestimonialSolutionType).filter_by(solution_type="battery_storage").one()
    assert row.key_approach_of_solution == "Sized against the evening peak."


def test_sheet_labels_map_to_solution_type_ids():
    assert solution_type_id_from_label("Client Endorsement") == "client_endorsement"
    assert solution_type_id_from_label("C&I Gas Reviews") == "ci_gas"
    assert solution_type_id_from_label("Oil / Resource Recovery") == "resource_recovery"
    assert solution_type_id_from_label("CDS (Container Deposit Scheme)") == "cds"
    assert solution_type_id_from_label("client_endorsement") == "client_endorsement"
    assert solution_type_id_from_label("not a real type") is None
    assert solution_type_id_from_label(
        "Gas Account contract for MRIN 53313283865 From AGL To Alinta Energy"
    ) is None


def test_resolve_testimonial_type_maps_gas_review_and_clears_junk():
    from tools.testimonial_solution_content import resolve_testimonial_type

    db = _session()
    type_id, label = resolve_testimonial_type(db, "ci_gas", None)
    assert type_id == "ci_gas"
    assert label == "C&I Gas Reviews"

    type_id, label = resolve_testimonial_type(
        db,
        None,
        "Gas Account contract for MRIN 53313283865 From AGL To Alinta Energy",
    )
    assert type_id is None
    assert "MRIN" in (label or "")

    type_id, label = resolve_testimonial_type(db, "", "")
    assert type_id is None
    assert label is None


def test_sheet_row_gets_solution_type_id():
    from tools.testimonial_sheet import _sheet_row_to_testimonial

    mapped = _sheet_row_to_testimonial(
        [
            "Longbeach RSL Sub Branch Inc",
            "Client Endorsement",
            "",
            "Testimonial for Longbeach RSL Sub Branch Inc - Client Endorsement",
            "https://drive.google.com/file/d/abc123xyz/view",
            "Approved",
            "",
        ],
        27,
    )
    assert mapped is not None
    assert mapped["testimonial_solution_type_id"] == "client_endorsement"
    assert mapped["source"] == "sheet"
