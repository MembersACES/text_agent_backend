"""Tests for entity group model, summary, suggestions, and CRM endpoints."""
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import Client, EntityGroup, Offer
from schemas import EntityGroupCreate
from services.crm import enrich_client_response
from services.entity_groups import (
    build_entity_group_summary,
    compute_entity_group_suggestions,
    delete_entity_group,
    normalize_business_name,
)


def _make_test_session():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()


def _add_client(
    db,
    *,
    business_name: str,
    entity_group_id=None,
    stage: str = "lead",
    has_signed_contract: int = 0,
    reporting_entity=None,
    primary_contact_email=None,
):
    client = Client(
        business_name=business_name,
        stage=stage,
        entity_group_id=entity_group_id,
        has_signed_contract=has_signed_contract,
        reporting_entity=reporting_entity,
        primary_contact_email=primary_contact_email,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(client)
    db.commit()
    db.refresh(client)
    return client


def _add_offer(db, *, client_id: int):
    offer = Offer(
        client_id=client_id,
        business_name="Offer",
        status="requested",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(offer)
    db.commit()
    db.refresh(offer)
    return offer


def test_entity_group_create_schema_normalizes_slug():
    body = EntityGroupCreate(slug="  Bentleigh-RSL-Group  ", display_name="Bentleigh RSL Group")
    assert body.slug == "bentleigh-rsl-group"


def test_enrich_client_response_includes_entity_group_fields():
    db = _make_test_session()
    group = EntityGroup(slug="test-group", display_name="Test Group")
    db.add(group)
    db.commit()
    db.refresh(group)

    client = Client(business_name="Site A", entity_group_id=group.id)
    db.add(client)
    db.commit()
    db.refresh(client)

    resp = enrich_client_response(db, client)
    assert resp.entity_group_id == group.id
    assert resp.entity_group_slug == "test-group"
    assert resp.entity_group_display_name == "Test Group"


def test_summary_empty_group_skips_offers_query():
    db = _make_test_session()
    group = EntityGroup(slug="empty-group", display_name="Empty Group")
    db.add(group)
    db.commit()
    db.refresh(group)

    summary = build_entity_group_summary(db, group)
    assert summary["member_count"] == 0
    assert summary["total_offers"] == 0
    assert summary["any_signed"] is False
    assert summary["stage_breakdown"] == {}
    assert summary["reporting_entity"]["aligned"] is True
    assert summary["reporting_entity"]["distinct_values"] == []


def test_summary_single_member():
    db = _make_test_session()
    group = EntityGroup(slug="solo", display_name="Solo Group")
    db.add(group)
    db.commit()
    db.refresh(group)

    _add_client(db, business_name="Site A", entity_group_id=group.id, stage="qualified")

    summary = build_entity_group_summary(db, group)
    assert summary["member_count"] == 1
    assert summary["total_offers"] == 0
    assert summary["any_signed"] is False
    assert summary["stage_breakdown"] == {"qualified": 1}


def test_summary_multi_member_stage_breakdown():
    db = _make_test_session()
    group = EntityGroup(slug="multi", display_name="Multi Group")
    db.add(group)
    db.commit()
    db.refresh(group)

    _add_client(db, business_name="Site A", entity_group_id=group.id, stage="lead")
    _add_client(db, business_name="Site B", entity_group_id=group.id, stage="won")
    _add_client(db, business_name="Site C", entity_group_id=group.id, stage="lead")

    summary = build_entity_group_summary(db, group)
    assert summary["member_count"] == 3
    assert summary["stage_breakdown"] == {"lead": 2, "won": 1}


def test_summary_mixed_reporting_entity_not_aligned():
    db = _make_test_session()
    group = EntityGroup(slug="mixed-re", display_name="Mixed RE")
    db.add(group)
    db.commit()
    db.refresh(group)

    _add_client(
        db,
        business_name="Site A",
        entity_group_id=group.id,
        reporting_entity="entity-a",
    )
    _add_client(
        db,
        business_name="Site B",
        entity_group_id=group.id,
        reporting_entity="entity-b",
    )

    summary = build_entity_group_summary(db, group)
    assert summary["reporting_entity"]["aligned"] is False
    assert summary["reporting_entity"]["distinct_values"] == ["entity-a", "entity-b"]


def test_summary_any_signed_true_and_false():
    db = _make_test_session()
    group = EntityGroup(slug="signed", display_name="Signed Group")
    db.add(group)
    db.commit()
    db.refresh(group)

    _add_client(
        db,
        business_name="Unsigned",
        entity_group_id=group.id,
        has_signed_contract=0,
    )
    _add_client(
        db,
        business_name="Signed",
        entity_group_id=group.id,
        has_signed_contract=1,
    )

    summary = build_entity_group_summary(db, group)
    assert summary["any_signed"] is True

    db2 = _make_test_session()
    group2 = EntityGroup(slug="unsigned", display_name="Unsigned Group")
    db2.add(group2)
    db2.commit()
    db2.refresh(group2)
    _add_client(db2, business_name="A", entity_group_id=group2.id, has_signed_contract=0)
    summary2 = build_entity_group_summary(db2, group2)
    assert summary2["any_signed"] is False


def test_summary_total_offers_subset_of_members():
    db = _make_test_session()
    group = EntityGroup(slug="offers", display_name="Offers Group")
    db.add(group)
    db.commit()
    db.refresh(group)

    a = _add_client(db, business_name="With Offer", entity_group_id=group.id)
    b = _add_client(db, business_name="No Offer", entity_group_id=group.id)
    _add_offer(db, client_id=a.id)
    _add_offer(db, client_id=a.id)

    summary = build_entity_group_summary(db, group)
    assert summary["member_count"] == 2
    assert summary["total_offers"] == 2

    orphan = Offer(
        client_id=None,
        business_name="Orphan",
        status="requested",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(orphan)
    db.commit()
    summary2 = build_entity_group_summary(db, group)
    assert summary2["total_offers"] == 2


def test_suggestions_clusters_similar_names():
    db = _make_test_session()
    _add_client(db, business_name="Centurion SA Investments")
    _add_client(db, business_name="Centurion SA Investments Pty Ltd")

    clusters = compute_entity_group_suggestions(db)
    assert len(clusters) == 1
    cluster = clusters[0]
    assert len(cluster["member_ids"]) == 2
    assert cluster["suggested_slug"] == "centurion-sa-investments-pty-ltd"
    assert cluster["confidence"] in ("high", "medium", "low")
    assert "centurion" in cluster["reason"].lower()


def test_suggestions_ungrouped_only():
    db = _make_test_session()
    group = EntityGroup(slug="existing", display_name="Existing")
    db.add(group)
    db.commit()
    db.refresh(group)

    _add_client(db, business_name="Grouped Site", entity_group_id=group.id)
    _add_client(db, business_name="Alpha Corp")
    _add_client(db, business_name="Alpha Corp Pty Ltd")

    clusters = compute_entity_group_suggestions(db)
    assert len(clusters) == 1
    assert all(mid != 1 for mid in clusters[0]["member_ids"])


def test_suggestions_no_writes():
    db = _make_test_session()
    _add_client(db, business_name="Beta Holdings")
    _add_client(db, business_name="Beta Holdings Limited")

    before_groups = db.query(EntityGroup).count()
    before_assigned = db.query(Client).filter(Client.entity_group_id.isnot(None)).count()

    compute_entity_group_suggestions(db)

    assert db.query(EntityGroup).count() == before_groups
    assert (
        db.query(Client).filter(Client.entity_group_id.isnot(None)).count() == before_assigned
    )


def test_normalize_business_name_strips_legal_suffixes():
    assert (
        normalize_business_name("Centurion SA Investments Pty Ltd")
        == "centurion sa investments"
    )
    assert normalize_business_name("Foo Trust as trustee for Bar") == "foo bar"


def test_delete_entity_group_unlinks_members_and_removes_group():
    db = _make_test_session()
    group = EntityGroup(slug="frankston-rsl", display_name="Frankston RSL")
    db.add(group)
    db.commit()
    db.refresh(group)

    a = _add_client(db, business_name="Site A", entity_group_id=group.id)
    b = _add_client(db, business_name="Site B", entity_group_id=group.id)
    other_group = EntityGroup(slug="other", display_name="Other")
    db.add(other_group)
    db.commit()
    db.refresh(other_group)
    c = _add_client(db, business_name="Other Site", entity_group_id=other_group.id)

    unlinked = delete_entity_group(db, group)
    assert unlinked == 2
    assert db.query(EntityGroup).filter(EntityGroup.slug == "frankston-rsl").first() is None
    assert db.query(EntityGroup).filter(EntityGroup.slug == "other").count() == 1

    db.refresh(a)
    db.refresh(b)
    db.refresh(c)
    assert a.entity_group_id is None
    assert b.entity_group_id is None
    assert c.entity_group_id == other_group.id


def test_delete_entity_group_with_no_members():
    db = _make_test_session()
    group = EntityGroup(slug="empty-group", display_name="Empty Group")
    db.add(group)
    db.commit()
    db.refresh(group)

    unlinked = delete_entity_group(db, group)
    assert unlinked == 0
    assert db.query(EntityGroup).filter(EntityGroup.slug == "empty-group").first() is None
