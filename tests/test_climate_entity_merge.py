"""Tests for multi-client climate activity source rollup."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import Client, EntityGroup
from services.climate_entity_sources import build_entity_activity_sources, _merge_loa_utility_maps


def _make_test_session():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()


def test_merge_loa_utility_maps_dedupes_identifiers():
    linked, retailers, extra = {}, {}, {}
    _merge_loa_utility_maps(
        linked,
        retailers,
        extra,
        {"C&I Electricity": ["111", "222"]},
        {"C&I Electricity": ["A", "B"]},
        {"C&I Electricity": [{"identifier": "111"}, {"identifier": "222"}]},
    )
    _merge_loa_utility_maps(
        linked,
        retailers,
        extra,
        {"C&I Electricity": ["222", "333"]},
        {"C&I Electricity": ["B", "C"]},
        {"C&I Electricity": [{"identifier": "222"}, {"identifier": "333"}]},
    )
    assert linked["C&I Electricity"] == ["111", "222", "333"]


def test_build_entity_activity_sources_merges_multiple_clients():
    db = _make_test_session()
    c1 = Client(business_name="Site A", reporting_entity="test-entity", external_business_id="recA")
    c2 = Client(business_name="Site B", reporting_entity="test-entity", external_business_id="recB")
    db.add_all([c1, c2])
    db.commit()

    from unittest.mock import patch
    from services import airtable_client

    def fake_resolve(client, *, airtable_configured):
        if client.external_business_id == "recA":
            return (
                {"id": "recA", "fields": {}},
                "recA",
            )
        return ({"id": "recB", "fields": {}}, "recB")

    def fake_linked(loa_record):
        rid = loa_record.get("id")
        if rid == "recA":
            return ({"C&I Electricity": ["111"]}, {"C&I Electricity": ["RetA"]}, {"C&I Electricity": [{}]})
        return ({"C&I Electricity": ["222"]}, {"C&I Electricity": ["RetB"]}, {"C&I Electricity": [{}]})

    with patch.object(airtable_client, "AIRTABLE_API_KEY", "key"):
        with patch.object(airtable_client, "USE_AIRTABLE_DIRECT", True):
            with patch(
                "services.climate_entity_sources._resolve_loa_for_client",
                side_effect=fake_resolve,
            ):
                with patch.object(
                    airtable_client,
                    "get_linked_utility_records",
                    side_effect=fake_linked,
                ):
                    with patch(
                        "services.climate_entity_sources._invoice_payload",
                        return_value={"configured": False, "total_count": 0, "sample_rows": []},
                    ):
                        payload = build_entity_activity_sources(
                            db, "test-entity", include_etl_preview=False
                        )

    assert payload["found"] is True
    assert payload["aces_client_ids"] == [c1.id, c2.id]
    assert set(payload["loa_record_ids"]) == {"recA", "recB"}
    assert payload["site_count"] == 2
    ids = {s["identifier"] for s in payload["sites"]}
    assert ids == {"111", "222"}


def test_build_entity_activity_sources_group_inherit_member():
    """Box Hill pattern: primary member has slug; sibling inherits via group.reporting_entity."""
    db = _make_test_session()
    group = EntityGroup(
        slug="frankston-rsl",
        display_name="Frankston RSL",
        reporting_entity="frankston-rsl",
    )
    db.add(group)
    db.flush()
    c1 = Client(
        business_name="Frankston RSL",
        reporting_entity="frankston-rsl",
        external_business_id="recA",
        entity_group_id=group.id,
    )
    c2 = Client(
        business_name="Box Hill RSL",
        reporting_entity=None,
        external_business_id="recB",
        entity_group_id=group.id,
    )
    db.add_all([c1, c2])
    db.commit()

    from unittest.mock import patch
    from services import airtable_client

    def fake_resolve(client, *, airtable_configured):
        return ({"id": client.external_business_id, "fields": {}}, client.external_business_id)

    def fake_linked(loa_record):
        rid = loa_record.get("id")
        if rid == "recA":
            return ({"C&I Electricity": ["111"]}, {"C&I Electricity": ["RetA"]}, {"C&I Electricity": [{}]})
        return ({"C&I Gas": ["MRIN-99"]}, {"C&I Gas": ["RetB"]}, {"C&I Gas": [{}]})

    with patch.object(airtable_client, "AIRTABLE_API_KEY", "key"):
        with patch.object(airtable_client, "USE_AIRTABLE_DIRECT", True):
            with patch(
                "services.climate_entity_sources._resolve_loa_for_client",
                side_effect=fake_resolve,
            ):
                with patch.object(
                    airtable_client,
                    "get_linked_utility_records",
                    side_effect=fake_linked,
                ):
                    with patch(
                        "services.climate_entity_sources._invoice_payload",
                        return_value={"configured": False, "total_count": 0, "sample_rows": []},
                    ):
                        payload = build_entity_activity_sources(
                            db, "frankston-rsl", include_etl_preview=False
                        )

    assert payload["found"] is True
    assert payload["entity_group_slug"] == "frankston-rsl"
    assert payload["aces_client_ids"] == [c1.id, c2.id]
    assert len(payload["members"]) == 2
    sources = {m["disclosure_source"] for m in payload["members"]}
    assert sources == {"member", "group_inherit"}
    assert payload["site_count"] == 2
    member_ids = {s["member_aces_client_id"] for s in payload["sites"]}
    assert member_ids == {c1.id, c2.id}
