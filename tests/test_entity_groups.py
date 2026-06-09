"""Tests for entity group model and CRM endpoints."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import Client, EntityGroup
from schemas import EntityGroupCreate
from services.crm import enrich_client_response


def _make_test_session():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()


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
