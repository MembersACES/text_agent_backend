from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from crm_enums import ClientStage, OfferStatus
from database import Base
from models import Client, Offer
from schemas import ClientCreate, OfferCreate
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

