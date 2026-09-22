"""Agreement Follow Up first-touch + sequence start."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base
from models import (
    AutonomousSequenceRun,
    AutonomousSequenceStep,
    AutonomousSequenceTemplate,
    AutonomousSequenceTemplateStep,
    Client,
    Offer,
    OfferActivity,
)
from services.agreement_followup import (
    AgreementFollowupError,
    create_agreement_type,
    list_agreement_types,
    render_first_touch,
    resolve_agreement_type,
    start_agreement_followup,
)
from services.autonomous_sequence import AGREEMENT_FOLLOWUP_SEQUENCE_TYPE


@pytest.fixture(autouse=True)
def _skip_loa_lookup(monkeypatch):
    monkeypatch.setattr(
        "services.agreement_followup.loa_contact_for_business",
        lambda _name: {"contact_name": "", "contact_email": "", "contact_phone": ""},
    )


def _db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def _template(db):
    template = AutonomousSequenceTemplate(
        sequence_type=AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
        display_name="Agreement Follow-up v1",
        timezone="Australia/Melbourne",
        is_active=1,
        is_restartable=0,
        figures_mode="none",
        stop_on='["agreement_signed","negative_sentiment_stop"]',
    )
    db.add(template)
    db.flush()
    for idx, day in enumerate((1, 3, 5, 7)):
        db.add(
            AutonomousSequenceTemplateStep(
                template_id=template.id,
                step_index=idx,
                day_number=day,
                channel="email",
                send_time_local="09:00",
                is_active=1,
            )
        )
    db.commit()
    return template


def _client(db) -> Client:
    client = Client(
        business_name="Acme Bakery",
        primary_contact_email="ada@acme.test",
        stage="qualified",
    )
    db.add(client)
    db.commit()
    db.refresh(client)
    return client


def test_resolve_agreement_type_accepts_label_or_id():
    db = _db()
    assert resolve_agreement_type(db, "Alinta C&I Gas")["id"] == "alinta_ci_gas"
    assert resolve_agreement_type(db, "alinta_sme_electricity")["label"] == "Alinta SME Electricity"


def test_resolve_agreement_type_rejects_unknown():
    db = _db()
    try:
        resolve_agreement_type(db, "Origin C&I Gas")
        raise AssertionError("expected error")
    except AgreementFollowupError as exc:
        assert "Unknown" in str(exc)


def test_staff_can_add_a_new_agreement_type():
    db = _db()
    created = create_agreement_type(
        db,
        label="Origin C&I Gas",
        utility_type="C&I Gas",
        retailer="Origin",
    )
    assert created["id"] == "origin_c_i_gas"
    ids = [row["id"] for row in list_agreement_types(db)]
    assert "origin_c_i_gas" in ids
    assert resolve_agreement_type(db, "Origin C&I Gas")["utility_type"] == "C&I Gas"


def test_first_touch_names_the_agreement():
    subject, body_text, html, text = render_first_touch(
        agreement_label="Alinta C&I Gas",
        business_name="Acme Bakery",
        contact_name="Ada Lovelace",
    )
    assert "Alinta C&I Gas" in subject
    assert "Acme Bakery" in subject
    assert "Hi Ada," in body_text
    assert "Ada Lovelace" not in body_text
    assert "Hi Ada," in html
    assert "signed PDF" in text.lower() or "signed agreement" in text.lower()


def test_custom_subject_and_body_are_used():
    subject, body_text, html, text = render_first_touch(
        agreement_label="Alinta C&I Gas",
        business_name="Acme Bakery",
        contact_name="Ada Lovelace",
        subject="Please sign this",
        body_text="Hi Ada,\n\nHere is the agreement.\n\nKind regards,",
    )
    assert subject == "Please sign this"
    assert "Here is the agreement." in body_text
    assert "Please find attached" not in html
    assert "Here is the agreement." in text


def test_start_sends_then_completes_step_zero(monkeypatch):
    db = _db()
    _template(db)
    client = _client(db)
    sent = {}

    def fake_send(**kwargs):
        sent.update(kwargs)
        return {"ok": True, "response": {"email_id": "msg-1", "gmail_thread_id": "thr-1"}}

    monkeypatch.setattr(
        "services.agreement_followup.send_agreement_first_touch_email",
        fake_send,
    )

    result = start_agreement_followup(
        db,
        client_id=client.id,
        agreement_type_raw="Alinta C&I Electricity",
        contact_email="ada@acme.test",
        pdf_bytes=b"%PDF-1.4 fake",
        filename="Alinta agreement.pdf",
        contact_name="Ada Lovelace",
        subject="Please sign — Acme",
        body_text="Hi Ada,\n\nCustom body from the form.\n\nKind regards,",
        created_by="staff@acesolutions.com.au",
    )
    assert result["ok"] is True
    assert result["email_id"] == "msg-1"
    assert result["thread_id"] == "thr-1"
    assert result["created_offer"] is True
    assert result["subject"] == "Please sign — Acme"
    assert sent["to"] == "ada@acme.test"
    assert sent["subject"] == "Please sign — Acme"
    assert "Custom body from the form." in sent["html"]
    assert sent["filename"].endswith(".pdf")

    run = db.query(AutonomousSequenceRun).one()
    assert run.sequence_type == AGREEMENT_FOLLOWUP_SEQUENCE_TYPE
    assert run.client_id == client.id
    assert run.contact_email == "ada@acme.test"
    steps = (
        db.query(AutonomousSequenceStep)
        .filter(AutonomousSequenceStep.run_id == run.id)
        .order_by(AutonomousSequenceStep.step_index)
        .all()
    )
    assert [s.step_index for s in steps] == [0, 1, 2, 3]
    assert steps[0].channel == "email"
    assert steps[0].step_status == "completed"
    assert all(s.channel == "email" for s in steps)
    assert steps[1].step_status in {"ready", "to_start"}

    types = {a.activity_type for a in db.query(OfferActivity).all()}
    assert "contract_received" in types
    assert "contract_sent_for_signing" in types
    offer = db.query(Offer).one()
    assert offer.pipeline_stage == "contract_sent_for_signing"


def test_start_rejects_duplicate_running_run(monkeypatch):
    db = _db()
    _template(db)
    client = _client(db)
    offer = Offer(
        client_id=client.id,
        business_name=client.business_name,
        status="autonomous_agent_trigger",
    )
    db.add(offer)
    db.flush()
    db.add(
        AutonomousSequenceRun(
            sequence_type=AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
            offer_id=offer.id,
            client_id=client.id,
            run_status="running",
            anchor_at=datetime.now(timezone.utc),
        )
    )
    db.commit()

    def boom(**kwargs):
        raise AssertionError("must not send when a run is already going")

    monkeypatch.setattr(
        "services.agreement_followup.send_agreement_first_touch_email",
        boom,
    )
    try:
        start_agreement_followup(
            db,
            client_id=client.id,
            offer_id=offer.id,
            agreement_type_raw="alinta_ci_gas",
            contact_email="ada@acme.test",
            pdf_bytes=b"%PDF-1.4 fake",
            filename="x.pdf",
        )
        raise AssertionError("expected duplicate error")
    except AgreementFollowupError as exc:
        assert exc.status_code == 409


def test_start_rejects_non_pdf(monkeypatch):
    db = _db()
    _template(db)
    client = _client(db)
    monkeypatch.setattr(
        "services.agreement_followup.send_agreement_first_touch_email",
        lambda **kwargs: {"ok": True, "response": {}},
    )
    try:
        start_agreement_followup(
            db,
            client_id=client.id,
            agreement_type_raw="alinta_ci_gas",
            contact_email="ada@acme.test",
            pdf_bytes=b"not-a-pdf",
            filename="notes.txt",
            content_type="text/plain",
        )
        raise AssertionError("expected pdf error")
    except AgreementFollowupError as exc:
        assert "PDF" in str(exc)
