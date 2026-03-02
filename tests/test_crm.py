from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from crm_enums import ClientStage, OfferStatus, OfferActivityType, OfferPipelineStage
from database import Base
from models import Client, Offer
from schemas import ClientCreate, OfferCreate
from main import ClientBulkUpdateRequest, bulk_update_clients, DataRequest, data_request
from services.crm import (
    update_offer_status_and_propagate_client_stage,
    upsert_client_from_business_info,
    create_offer_activity,
)
import json


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


def _fake_successful_data_request_message(
    supplier_name: str,
    business_name: str,
    service_type: str,
    account_identifier: str,
    identifier_type: str = "NMI",
) -> str:
    return (
        f"✅ Data request successfully sent to {supplier_name} for "
        f"{business_name} ({service_type}) {identifier_type} - {account_identifier}"
    )


def test_data_request_creates_crm_records_electricity_ci():
    db = _make_test_session()

    # Patch supplier_data_request in main module to avoid external HTTP calls
    import main as backend_main

    original = backend_main.supplier_data_request
    backend_main.supplier_data_request = _fake_successful_data_request_message

    try:
        req = DataRequest(
            business_name="Electricity CI Biz",
            supplier_name="Origin",
            request_type="electricity_ci",
            details="NMI123456",
        )

        user_info = {"email": "user@example.com"}

        response = data_request(req, user_info=user_info, db=db)

        assert response["status"] == "success"
        assert "Data request successfully sent" in response["message"]

        client = db.query(Client).filter(Client.business_name == "Electricity CI Biz").first()
        assert client is not None

        offer = (
            db.query(Offer)
            .filter(
                Offer.business_name == "Electricity CI Biz",
                Offer.utility_type == "electricity",
            )
            .first()
        )
        assert offer is not None
        assert offer.identifier == "NMI123456"

        # Directly query OfferActivity for assertions
        from models import OfferActivity as OfferActivityModel

        act = (
            db.query(OfferActivityModel)
            .filter(OfferActivityModel.offer_id == offer.id)
            .first()
        )
        assert act is not None
        assert act.activity_type == OfferActivityType.DATA_REQUEST.value
        metadata = json.loads(act.metadata_ or "{}")
        assert metadata.get("service_type") == "electricity_ci"
        assert metadata.get("utility_type") == "electricity"
        assert metadata.get("utility_type_identifier") == "C&I Electricity"
        assert metadata.get("identifier_type") == "NMI"
        assert metadata.get("identifier") == "NMI123456"
        assert metadata.get("supplier_name") == "Origin"
        assert metadata.get("source") == "data_request_page"

    finally:
        backend_main.supplier_data_request = original


def test_create_offer_activity_sets_status_to_awaiting_response_for_proposal_events():
    db = _make_test_session()

    client = _make_client(db, business_name="Proposal Co")
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

    create_offer_activity(
        db=db,
        offer=offer,
        client=client,
        activity_type=OfferActivityType.COMPARISON,
        document_link=None,
        external_id=None,
        metadata={},
        created_by="user@example.com",
    )

    db.refresh(offer)

    assert offer.status == OfferStatus.AWAITING_RESPONSE.value
    assert offer.pipeline_stage == OfferPipelineStage.COMPARISON_SENT.value


def test_create_offer_activity_signed_engagement_accepts_offer_and_updates_client_stage():
    db = _make_test_session()

    client = _make_client(db, business_name="Signed Co", stage=ClientStage.LEAD)
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

    create_offer_activity(
        db=db,
        offer=offer,
        client=client,
        activity_type=OfferActivityType.ENGAGEMENT_FORM_SIGNED,
        document_link=None,
        external_id=None,
        metadata={},
        created_by="user@example.com",
    )

    db.refresh(offer)
    db.refresh(client)

    assert offer.status == OfferStatus.ACCEPTED.value
    assert client.stage == ClientStage.WON.value
