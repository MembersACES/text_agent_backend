from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from crm_enums import ClientStage, OfferStatus
from database import Base
from models import Client, Offer
from schemas import ClientCreate, OfferCreate
from main import ClientBulkUpdateRequest, bulk_update_clients
from services.crm import (
    update_offer_status_and_propagate_client_stage,
    upsert_client_from_business_info,
)


def _make_test_session():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def _make_client(
    db,
    *,
    business_name: str = "Client",
    stage: ClientStage = ClientStage.LEAD,
    owner_email: Optional[str] = None,
):
    client = Client(
        business_name=business_name,
        external_business_id=None,
        primary_contact_email=None,
        stage=stage.value,
        owner_email=owner_email,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(client)
    db.commit()
    db.refresh(client)
    return client


def test_offer_status_propagates_client_stage():
    db = _make_test_session()

    client = Client(
        business_name="Acme Pty Ltd",
        external_business_id="ext-1",
        primary_contact_email="acme@example.com",
        stage=ClientStage.LEAD.value,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(client)
    db.commit()
    db.refresh(client)

    offer = Offer(
        client_id=client.id,
        business_name=client.business_name,
        status=OfferStatus.REQUESTED.value,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(offer)
    db.commit()
    db.refresh(offer)

    updated = update_offer_status_and_propagate_client_stage(
        db=db,
        offer_id=offer.id,
        new_status=OfferStatus.ACCEPTED,
    )

    db.refresh(client)

    assert updated.status == OfferStatus.ACCEPTED.value
    assert client.stage == ClientStage.WON.value


def test_offer_status_accepted_does_not_downgrade_existing_client():
    """When an offer is accepted for a client already in Won/ExistingClient, stage stays unchanged."""
    db = _make_test_session()

    client = Client(
        business_name="Existing Co",
        external_business_id="ext-2",
        primary_contact_email="existing@example.com",
        stage=ClientStage.WON.value,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(client)
    db.commit()
    db.refresh(client)

    offer = Offer(
        client_id=client.id,
        business_name=client.business_name,
        status=OfferStatus.REQUESTED.value,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(offer)
    db.commit()
    db.refresh(offer)

    updated = update_offer_status_and_propagate_client_stage(
        db=db,
        offer_id=offer.id,
        new_status=OfferStatus.ACCEPTED,
    )
    db.refresh(client)

    assert updated.status == OfferStatus.ACCEPTED.value
    assert client.stage == ClientStage.WON.value  # unchanged; no downgrade


def test_client_stage_enum_validation():
    data = {
        "business_name": "Valid Co",
        "stage": ClientStage.QUALIFIED.value,
    }
    client = ClientCreate(**data)
    assert client.stage == ClientStage.QUALIFIED

    bad_data = {
        "business_name": "Invalid Co",
        "stage": "not_a_real_stage",
    }
    from pydantic import ValidationError

    try:
        ClientCreate(**bad_data)
        assert False, "Expected ValidationError for invalid stage"
    except ValidationError:
        pass


def test_get_business_info_upsert_respects_external_business_id():
    db = _make_test_session()

    first = upsert_client_from_business_info(
        db=db,
        business_name="Acme",
        external_business_id="airtable-123",
        primary_contact_email="first@example.com",
        gdrive_folder_url=None,
    )

    second = upsert_client_from_business_info(
        db=db,
        business_name="Acme",
        external_business_id="airtable-123",
        primary_contact_email="second@example.com",
        gdrive_folder_url="https://drive.example/folder",
    )

    assert first.id == second.id
    assert second.external_business_id == "airtable-123"
    assert second.primary_contact_email == "second@example.com"
    assert second.gdrive_folder_url == "https://drive.example/folder"


def test_bulk_update_clients_assign_owner_only():
    db = _make_test_session()

    c1 = _make_client(db, business_name="A")
    c2 = _make_client(db, business_name="B")

    body = ClientBulkUpdateRequest(
        client_ids=[c1.id, c2.id],
        owner_email="owner@example.com",
    )

    result = bulk_update_clients(
        body=body,
        db=db,
        user_data={"idinfo": {"email": "admin@example.com"}},
    )

    assert len(result) == 2

    db.refresh(c1)
    db.refresh(c2)

    assert c1.owner_email == "owner@example.com"
    assert c2.owner_email == "owner@example.com"


def test_bulk_update_clients_change_stage_only():
    db = _make_test_session()

    c1 = _make_client(db, stage=ClientStage.LEAD)

    body = ClientBulkUpdateRequest(
        client_ids=[c1.id],
        stage=ClientStage.WON,
    )

    result = bulk_update_clients(
        body=body,
        db=db,
        user_data={"idinfo": {"email": "admin@example.com"}},
    )

    assert len(result) == 1
    assert result[0]["id"] == c1.id
    assert result[0]["stage"] == ClientStage.WON.value

    db.refresh(c1)
    assert c1.stage == ClientStage.WON.value


def test_bulk_update_clients_owner_and_stage():
    db = _make_test_session()

    c1 = _make_client(db, stage=ClientStage.LEAD)
    c2 = _make_client(db, stage=ClientStage.LEAD)

    body = ClientBulkUpdateRequest(
        client_ids=[c1.id, c2.id],
        owner_email="owner2@example.com",
        stage=ClientStage.QUALIFIED,
    )

    result = bulk_update_clients(
        body=body,
        db=db,
        user_data={"idinfo": {"email": "admin@example.com"}},
    )

    assert len(result) == 2
    returned_ids = {item["id"] for item in result}
    assert returned_ids == {c1.id, c2.id}

    db.refresh(c1)
    db.refresh(c2)

    assert c1.owner_email == "owner2@example.com"
    assert c2.owner_email == "owner2@example.com"
    assert c1.stage == ClientStage.QUALIFIED.value
    assert c2.stage == ClientStage.QUALIFIED.value


def test_bulk_update_clients_ignores_missing_ids():
    db = _make_test_session()

    c1 = _make_client(db, stage=ClientStage.LEAD)

    missing_id = c1.id + 999

    body = ClientBulkUpdateRequest(
        client_ids=[c1.id, missing_id],
        owner_email="owner3@example.com",
    )

    result = bulk_update_clients(
        body=body,
        db=db,
        user_data={"idinfo": {"email": "admin@example.com"}},
    )

    # Only the existing client should be returned
    assert len(result) == 1
    assert result[0]["id"] == c1.id

    db.refresh(c1)
    assert c1.owner_email == "owner3@example.com"

