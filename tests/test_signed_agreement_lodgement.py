"""Signed-agreement lodgement: filing-type map, skip flag, and CRM activity."""

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from crm_enums import ClientStage, OfferActivityType, OfferStatus
from database import Base
from models import Client, Offer, OfferActivity
from services.crm import record_signed_agreement_lodgement
from tools.send_supplier_signed_agreement import (
    filing_type_for_supplier,
    resolve_lodgement_identifier,
    send_supplier_signed_agreement,
    unmapped_drive_note,
)


class _EmailResponse:
    status_code = 200
    text = "ok"

    def json(self):
        return {"message": "sent"}


def _db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def test_supplier_label_maps_to_filing_type():
    assert filing_type_for_supplier("Alinta C&I Electricity") == "signed_CI_E"
    assert filing_type_for_supplier("Origin C&I Electricity") == "signed_CI_E"
    assert filing_type_for_supplier("Momentum C&I Electricity") == "signed_CI_E"
    assert filing_type_for_supplier("Origin SME Electricity") == "signed_SME_E"
    assert filing_type_for_supplier("Alinta C&I Gas") == "signed_CI_G"
    assert filing_type_for_supplier("PowerMetric DMA") == "signed_DMA"
    assert filing_type_for_supplier("Veolia Waste") == "signed_WASTE"
    assert (
        filing_type_for_supplier("Other", utility_type="C&I Electricity")
        == "signed_CI_E"
    )
    assert filing_type_for_supplier("Not a supplier") is None


def test_skip_drive_filing_omits_unmapped_note_and_does_not_upload(tmp_path, monkeypatch):
    import tools.send_supplier_signed_agreement as mod

    called = {"drive": 0, "email_nmi": None}

    def fake_drive(**kwargs):
        called["drive"] += 1
        raise AssertionError(f"drive filing should be skipped, got {kwargs}")

    def fake_post(url, data=None, files=None):
        called["email_nmi"] = (data or {}).get("nmi")
        called["business_name"] = (data or {}).get("business_name")
        return _EmailResponse()

    monkeypatch.setattr(mod, "drive_filing", fake_drive)
    monkeypatch.setattr(mod.requests, "post", fake_post)

    pdf = tmp_path / "signed.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    message = send_supplier_signed_agreement(
        file_path=str(pdf),
        business_name="Oakleigh-Carnegie RSL",
        contract_type="Alinta C&I Electricity",
        agreement_type="contract",
        nmi="6408123456",
        utility_type="C&I Electricity",
        skip_drive_filing=True,
    )
    assert called["drive"] == 0
    assert "not mapped" not in message
    assert "Drive Filing" not in message
    assert called["email_nmi"] == "6408123456"
    assert called["business_name"] == "Oakleigh-Carnegie RSL"
    assert unmapped_drive_note("Alinta C&I Electricity", skip_drive_filing=True) == ""


def test_recipient_override_replaces_shared_list(tmp_path, monkeypatch):
    import tools.send_supplier_signed_agreement as mod

    seen = {}

    def fake_post(url, data=None, files=None):
        seen["supplier_email"] = (data or {}).get("supplier_email")
        return _EmailResponse()

    monkeypatch.setattr(mod.requests, "post", fake_post)
    pdf = tmp_path / "signed.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    message = send_supplier_signed_agreement(
        file_path=str(pdf),
        business_name="Oakleigh-Carnegie RSL",
        contract_type="Alinta C&I Electricity",
        agreement_type="contract",
        utility_type="C&I Electricity",
        skip_drive_filing=True,
        recipient_emails="only.this.send@example.com, only.this.send@example.com",
    )
    assert seen["supplier_email"] == "only.this.send@example.com"
    assert "only.this.send@example.com" in message
    assert "Andrew.Barnes" not in seen["supplier_email"]


def test_mapped_supplier_files_without_status_cell(tmp_path, monkeypatch):
    import tools.send_supplier_signed_agreement as mod

    seen = {}

    def fake_drive(**kwargs):
        seen.update(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(mod, "drive_filing", fake_drive)
    monkeypatch.setattr(mod, "get_business_information", lambda name: {
        "gdrive": {"folder_url": "https://drive.google.com/drive/folders/abc"}
    })
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: _EmailResponse())

    pdf = tmp_path / "signed.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    message = send_supplier_signed_agreement(
        file_path=str(pdf),
        business_name="Oakleigh-Carnegie RSL",
        contract_type="Alinta C&I Electricity",
        utility_type="C&I Electricity",
        skip_drive_filing=False,
    )
    assert seen["filing_type"] == "signed_CI_E"
    assert seen.get("contract_status") is None
    assert "not mapped" not in message
    assert "Drive Filing Result" in message


def test_explicit_nmi_overrides_business_name_suffix():
    name, identifier, kind = resolve_lodgement_identifier(
        "Oakleigh-Carnegie RSL NMI: 1111111111",
        nmi="6408123456",
    )
    assert name == "Oakleigh-Carnegie RSL"
    assert identifier == "6408123456"
    assert kind == "nmi"


def test_lodgement_activity_created_without_status_change():
    db = _db()
    client = Client(
        business_name="Oakleigh-Carnegie RSL",
        stage=ClientStage.EXISTING_CLIENT.value,
        gdrive_folder_url="https://drive.google.com/drive/folders/member",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(client)
    db.commit()
    db.refresh(client)

    activity = record_signed_agreement_lodgement(
        db,
        business_name="Oakleigh-Carnegie RSL",
        utility_label="C&I Electricity",
        supplier="Alinta C&I Electricity",
        retailer_name="Alinta",
        recipients="Andrew.Barnes@alintaenergy.com.au, data.quote@fornrg.com",
        created_by="morgan.h@acesolutions.com.au",
        client_id=client.id,
        nmi="6408123456",
    )
    assert activity is not None
    assert activity.activity_type == OfferActivityType.SIGNED_AGREEMENT_LODGED.value
    assert activity.document_link == "https://drive.google.com/drive/folders/member"
    meta = activity.metadata_
    assert isinstance(meta, str)
    assert "Signed C&I Electricity agreement lodged with Alinta C&I Electricity" in meta
    assert "sent to Andrew.Barnes@alintaenergy.com.au" in meta
    assert '"retailer_name": "Alinta"' in meta

    offer = db.query(Offer).filter(Offer.client_id == client.id).one()
    assert offer.status == OfferStatus.REQUESTED.value
    assert offer.pipeline_stage in (None, "")
    stored = db.query(OfferActivity).one()
    assert stored.activity_type == OfferActivityType.SIGNED_AGREEMENT_LODGED.value
