"""Unit tests for staff-created testimonial solution types (in-memory SQLite)."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from tools.testimonial_solution_content import (
    ALL_SOLUTION_TYPE_IDS,
    DEFAULT_CONTENT,
    create_custom_type,
    delete_custom_type,
    get_merged_content,
    save_override,
    slugify_solution_type,
    solution_family_fields,
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
    assert solution_type_id_from_label("Association Endorsement") == "association_endorsement"
    assert solution_type_id_from_label("association_endorsement") == "association_endorsement"
    assert solution_type_id_from_label("C&I Gas Reviews") == "ci_gas"
    assert solution_type_id_from_label("Oil / Resource Recovery") == "resource_recovery"
    assert solution_type_id_from_label("CDS (Container Deposit Scheme)") == "cds"
    assert solution_type_id_from_label("Value Added Service") == "vas"
    assert solution_type_id_from_label("vas") == "vas"
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


_UNMAPPED_FAMILIES = {"client_endorsement", "association_endorsement", "ghg_roadmap"}


def test_default_copy_stays_within_the_one_page_budget():
    limits = {
        "key_outcome_metrics": 42,
        "key_challenge_of_solution": 170,
        "key_approach_of_solution": 120,
        "key_outcome_of_solution": 250,
        "key_outcome_dotpoints_1": 42,
        "key_outcome_dotpoints_2": 42,
        "key_outcome_dotpoints_3": 42,
        "key_outcome_dotpoints_4": 42,
        "key_outcome_dotpoints_5": 42,
        "conclusion": 95,
    }
    for type_id, item in DEFAULT_CONTENT.items():
        for key, limit in limits.items():
            text = item.get(key) or ""
            assert len(text) <= limit, f"{type_id} {key} is {len(text)} characters"


def test_every_builtin_type_has_a_family_or_is_explicitly_blank():
    for type_id in ALL_SOLUTION_TYPE_IDS:
        fields = solution_family_fields(type_id)
        if type_id in _UNMAPPED_FAMILIES:
            assert fields == {"solution_family": "", "solution_family_lead_in": ""}
            continue
        assert fields["solution_family"] in {
            "Profile Reset",
            "Asset Optimisation",
            "Resource Recovery",
        }
        assert fields["solution_family_lead_in"].startswith("Starting with")
    assert solution_family_fields("led_upgrade")["solution_family"] == ""
    assert solution_family_fields("ci_gas")["solution_family"] == "Profile Reset"
    assert solution_family_fields("cds")["solution_family"] == "Resource Recovery"
    assert solution_family_fields("solar_review")["solution_family"] == "Asset Optimisation"


def test_generate_payload_sends_family_to_n8n(monkeypatch):
    captured = {}

    class _Response:
        status_code = 200

        def json(self):
            return {"document_link": "https://drive.google.com/file/d/abc/view"}

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        return _Response()

    monkeypatch.setattr("tools.document_generation.requests.post", fake_post)
    from tools.document_generation import generate_testimonial_document

    result = generate_testimonial_document(
        business_name="Club Rivers",
        trading_as="",
        testimonial_business_name="Club Rivers",
        testimonial_business_name_source="business_name",
        contact_name="",
        position="",
        email="",
        telephone="",
        client_folder_url="https://drive.google.com/drive/folders/folder",
        solution_type_id="ci_gas",
        savings_amount=4533.18,
    )
    assert result["status"] == "success"
    data = captured["json"]["data"]
    assert data["solution_type"] == "C&I Gas Reviews"
    assert data["solution_family"] == "Profile Reset"
    assert "Profile Resets" in data["solution_family_lead_in"]

    generate_testimonial_document(
        business_name="Club Rivers",
        trading_as="",
        testimonial_business_name="",
        testimonial_business_name_source="business_name",
        contact_name="",
        position="",
        email="",
        telephone="",
        client_folder_url="",
        solution_type_id="ghg_roadmap",
        savings_amount=0,
    )
    blank = captured["json"]["data"]
    assert blank["solution_family"] == ""
    assert blank["solution_family_lead_in"] == ""
    assert blank["solution_type"] == "GHG Roadmap"

    generate_testimonial_document(
        business_name="Club Rivers",
        trading_as="",
        testimonial_business_name="",
        testimonial_business_name_source="business_name",
        contact_name="",
        position="",
        email="",
        telephone="",
        client_folder_url="",
        solution_type_id="solar_panel_cleaning",
        savings_amount=100,
        pv_system_size="99",
        solar_pre_daily_kwh=100,
        solar_post_daily_kwh=110,
    )
    solar = captured["json"]["data"]
    assert solar["solution_family"] == "Asset Optimisation"
    assert solar["estimated_annual_production_increase_percent"] == "10.0"
