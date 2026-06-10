from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import Client
from services.crm_loa_link import (
    link_or_create_client_from_loa,
    resolve_crm_client_for_loa,
)


def _make_test_session():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def _make_client(db, *, business_name: str, external_business_id: str | None = None):
    client = Client(
        business_name=business_name,
        external_business_id=external_business_id,
        stage="lead",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(client)
    db.commit()
    db.refresh(client)
    return client


def test_resolve_matched_by_record_id():
    db = _make_test_session()
    melbourne = _make_client(
        db,
        business_name="Centurion Investments Pty Ltd (Melbourne)",
        external_business_id="recMelbourne",
    )
    _make_client(
        db,
        business_name="Centurion Investments Pty Ltd (Adelaide)",
        external_business_id="recAdelaide",
    )

    link = resolve_crm_client_for_loa(db, "recMelbourne")
    assert link["status"] == "matched"
    assert link["client_id"] == melbourne.id
    assert link["record_id"] == "recMelbourne"


def test_resolve_no_match_does_not_use_business_name():
    db = _make_test_session()
    _make_client(db, business_name="Centurion Investments Pty Ltd", external_business_id=None)

    link = resolve_crm_client_for_loa(db, "recNewSite")
    assert link["status"] == "no_match"
    assert link["client_id"] is None


def test_resolve_ambiguous_without_record_id():
    db = _make_test_session()
    link = resolve_crm_client_for_loa(db, None)
    assert link["status"] == "ambiguous"
    assert link["client_id"] is None


def test_resolve_conflict_multiple_clients_same_record():
    db = _make_test_session()
    _make_client(db, business_name="Dup A", external_business_id="recDup")
    _make_client(db, business_name="Dup B", external_business_id="recDup")

    link = resolve_crm_client_for_loa(db, "recDup")
    assert link["status"] == "conflict"
    assert len(link["candidates"]) == 2


def test_link_existing_client_pins_record_id():
    db = _make_test_session()
    adelaide = _make_client(db, business_name="Centurion Adelaide", external_business_id=None)

    result = link_or_create_client_from_loa(
        db,
        record_id="recMelbourne",
        business_name="Centurion Melbourne",
        client_id=adelaide.id,
    )
    assert result.id == adelaide.id
    assert result.external_business_id == "recMelbourne"


def test_create_from_loa_new_member():
    db = _make_test_session()
    result = link_or_create_client_from_loa(
        db,
        record_id="recMelbourne",
        business_name="Centurion Melbourne",
        primary_contact_email="ops@example.com",
    )
    assert result.external_business_id == "recMelbourne"
    assert result.primary_contact_email == "ops@example.com"


def test_get_business_info_does_not_silent_upsert():
    from main import get_business_info, BusinessInfoRequest

    db = _make_test_session()
    before = db.query(Client).count()

    mock_payload = {
        "business_details": {"name": "Centurion Melbourne"},
        "record_ID": "recMelbourne",
        "contact_information": {"email": "ops@example.com"},
        "gdrive": {"folder_url": "https://drive.example/folder"},
    }

    with patch("main.get_business_information", return_value=mock_payload):
        result = get_business_info(
            BusinessInfoRequest(business_name="Centurion"),
            user_info={"email": "staff@acesolutions.com.au"},
            db=db,
        )

    after = db.query(Client).count()
    assert after == before
    assert result["crm_link"]["status"] == "no_match"
    assert "client_id" not in result or result.get("client_id") is None
