"""Agreement Follow Up first-touch + sequence start."""

import json
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
    Campaign,
    Client,
    Offer,
    OfferActivity,
)
from services.agreement_followup import (
    AGREEMENT_FOLLOWUP_TEST_CAMPAIGN_NAME,
    AGREEMENT_FOLLOWUP_TEST_OFFER_IDENTIFIER,
    N8N_AGREEMENT_FOLLOWUP_HARDCODED_URL,
    AgreementFollowupError,
    create_agreement_type,
    list_agreement_types,
    purge_agreement_followup_test_stubs,
    render_first_touch,
    resolve_agreement_followup_webhook,
    resolve_agreement_type,
    send_agreement_first_touch_email,
    start_agreement_followup,
    update_agreement_type,
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


def test_first_touch_title_cases_the_greeting():
    _subject, body_text, html, _text = render_first_touch(
        agreement_label="Alinta C&I Gas",
        business_name="Acme Bakery",
        contact_name="MATTHEW HAAS",
    )
    assert "Hi Matthew," in body_text
    assert "Hi MATTHEW," not in body_text
    assert "Matthew Haas" not in body_text
    assert "Hi Matthew," in html
    _subject, empty_body, _html, _plain = render_first_touch(
        agreement_label="Alinta C&I Gas",
        business_name="Acme Bakery",
        contact_name="",
    )
    assert "Hi there," in empty_body
    _subject, mcdonald, _html, _plain = render_first_touch(
        agreement_label="Alinta C&I Gas",
        business_name="Acme Bakery",
        contact_name="McDonald",
    )
    assert "Hi McDonald," in mcdonald
    _subject, obrien, _html, _plain = render_first_touch(
        agreement_label="Alinta C&I Gas",
        business_name="Acme Bakery",
        contact_name="O'Brien",
    )
    assert "Hi O'Brien," in obrien


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
    assert [s.step_index for s in steps] == [0, 1, 2, 3, 4]
    assert [s.day_number for s in steps] == [0, 1, 3, 5, 7]
    assert steps[0].channel == "email"
    assert steps[0].step_status == "completed"
    assert steps[0].day_number == 0
    assert all(s.channel == "email" for s in steps)
    assert all(s.step_status in {"ready", "to_start"} for s in steps[1:])

    types = {a.activity_type for a in db.query(OfferActivity).all()}
    assert "contract_received" in types
    assert "contract_sent_for_signing" in types
    offer = db.query(Offer).one()
    assert offer.pipeline_stage == "contract_sent_for_signing"
    assert offer.campaign_id is None
    assert offer.client_id == client.id


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


def test_start_refuses_same_recipient_and_type_before_send(monkeypatch):
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
            contact_email="Ada@acme.test",
            context_json=json.dumps({"agreement_type": "alinta_ci_gas"}),
            anchor_at=datetime.now(timezone.utc),
        )
    )
    db.commit()

    def boom(**kwargs):
        raise AssertionError("must not send when this person already has this agreement running")

    monkeypatch.setattr(
        "services.agreement_followup.send_agreement_first_touch_email",
        boom,
    )
    try:
        start_agreement_followup(
            db,
            client_id=client.id,
            agreement_type_raw="alinta_ci_gas",
            contact_email="ada@acme.test",
            pdf_bytes=b"%PDF-1.4 fake",
            filename="x.pdf",
        )
        raise AssertionError("expected recipient duplicate")
    except AgreementFollowupError as exc:
        assert exc.status_code == 409
        assert "Nothing was sent" in str(exc)
        assert "run #" in str(exc)
    assert db.query(AutonomousSequenceRun).count() == 1
    assert db.query(Offer).count() == 1


def test_shared_thread_still_starts_the_chase(monkeypatch):
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
    first = AutonomousSequenceRun(
        sequence_type=AGREEMENT_FOLLOWUP_SEQUENCE_TYPE,
        offer_id=offer.id,
        client_id=client.id,
        run_status="running",
        contact_email="ada@acme.test",
        email_ID="thr-shared",
        context_json=json.dumps(
            {"agreement_type": "alinta_ci_gas", "gmail_thread_id": "thr-shared"}
        ),
        anchor_at=datetime.now(timezone.utc),
    )
    db.add(first)
    db.commit()
    monkeypatch.setattr(
        "services.agreement_followup.send_agreement_first_touch_email",
        lambda **kwargs: {
            "ok": True,
            "response": {"email_id": "msg-2", "gmail_thread_id": "thr-shared"},
        },
    )
    result = start_agreement_followup(
        db,
        client_id=client.id,
        agreement_type_raw="Alinta C&I Electricity",
        contact_email="ada@acme.test",
        pdf_bytes=b"%PDF-1.4 fake",
        filename="electricity.pdf",
        contact_name="Ada Lovelace",
    )
    assert result["ok"] is True
    assert result["run_id"] != first.id
    assert result["warning"].startswith("The email was sent.")
    assert f"run #{first.id}" in result["warning"]
    assert db.query(AutonomousSequenceRun).count() == 2
    created = db.query(AutonomousSequenceRun).filter(AutonomousSequenceRun.id == result["run_id"]).one()
    assert json.loads(created.context_json)["shared_gmail_thread_run_id"] == first.id
    assert created.run_status == "running"
    db.refresh(first)
    assert json.loads(first.context_json)["shared_gmail_thread_run_id"] == result["run_id"]


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


def test_duplicate_type_name_is_rejected():
    db = _db()
    try:
        create_agreement_type(db, label="Alinta C&I Gas", utility_type="C&I Gas")
        raise AssertionError("expected duplicate error")
    except AgreementFollowupError as exc:
        assert exc.status_code == 400
        assert "already exists" in str(exc)


def test_hidden_type_tells_staff_to_show_it():
    db = _db()
    created = create_agreement_type(db, label="Origin C&I Gas", utility_type="C&I Gas", retailer="Origin")
    update_agreement_type(db, created["id"], is_active=False)
    try:
        create_agreement_type(db, label="Origin C&I Gas", utility_type="C&I Gas")
        raise AssertionError("expected hidden duplicate error")
    except AgreementFollowupError as exc:
        assert "hidden" in str(exc).lower()
        assert "Show" in str(exc)


def test_start_test_mode_uses_hidden_stub_offer(monkeypatch):
    db = _db()
    _template(db)
    client = _client(db)
    monkeypatch.setattr(
        "services.agreement_followup.send_agreement_first_touch_email",
        lambda **kwargs: {"ok": True, "response": {"email_id": "msg-test", "gmail_thread_id": "thr-test"}},
    )
    result = start_agreement_followup(
        db,
        client_id=client.id,
        agreement_type_raw="alinta_ci_gas",
        contact_email="ada@acme.test",
        pdf_bytes=b"%PDF-1.4 fake",
        filename="test.pdf",
        contact_name="Ada Lovelace",
        test_mode=True,
    )
    assert result["test"] is True
    assert result["subject"].startswith("[TEST]")
    assert result["client_id"] is None
    offer = db.query(Offer).one()
    assert offer.campaign_id is not None
    assert offer.client_id is None
    assert offer.identifier == AGREEMENT_FOLLOWUP_TEST_OFFER_IDENTIFIER
    campaign = db.query(Campaign).filter(Campaign.id == offer.campaign_id).one()
    assert campaign.name == AGREEMENT_FOLLOWUP_TEST_CAMPAIGN_NAME
    assert bool(campaign.archived)
    assert db.query(Offer).filter(Offer.campaign_id.is_(None)).count() == 0
    assert db.query(OfferActivity).count() == 0
    run = db.query(AutonomousSequenceRun).one()
    assert run.client_id is None
    assert '"agreement_test": true' in (run.context_json or "")
    purged = purge_agreement_followup_test_stubs(db)
    assert purged["offers"] == 1
    assert purged["runs"] == 1
    assert db.query(Offer).count() == 0
    assert db.query(AutonomousSequenceRun).count() == 0


def _start_kwargs(db, client, **overrides):
    payload = dict(
        db=db,
        client_id=client.id,
        agreement_type_raw="alinta_ci_gas",
        contact_email="ada@acme.test",
        pdf_bytes=b"%PDF-1.4 fake",
        filename="x.pdf",
    )
    payload.update(overrides)
    return payload


def test_start_fails_on_placeholder_and_creates_no_run(monkeypatch):
    db = _db()
    _template(db)
    client = _client(db)
    monkeypatch.setattr(
        "services.agreement_followup.send_agreement_first_touch_email",
        lambda **kwargs: {"ok": True, "mode": "placeholder", "payload": {}},
    )
    try:
        start_agreement_followup(**_start_kwargs(db, client))
        raise AssertionError("expected send failure")
    except AgreementFollowupError as exc:
        assert exc.status_code == 502
        assert "not sent" in str(exc).lower() or "placeholder" in str(exc).lower()
    assert db.query(AutonomousSequenceRun).count() == 0
    assert db.query(Offer).count() == 0


def test_start_fails_when_webhook_returns_no_gmail_ids(monkeypatch):
    db = _db()
    _template(db)
    client = _client(db)
    monkeypatch.setattr(
        "services.agreement_followup.send_agreement_first_touch_email",
        lambda **kwargs: {"ok": True, "mode": "n8n", "response": {"ok": True}},
    )
    try:
        start_agreement_followup(**_start_kwargs(db, client))
        raise AssertionError("expected send failure")
    except AgreementFollowupError as exc:
        assert exc.status_code == 502
        assert "email_id" in str(exc) or "thread_id" in str(exc)
    assert db.query(AutonomousSequenceRun).count() == 0
    assert db.query(Offer).count() == 0


def test_webhook_url_falls_back_to_hardcoded_when_env_empty(monkeypatch):
    for key in (
        "N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL",
        "N8N_AGREEMENT_FOLLOWUP_WEBHOOK_URL",
        "AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr("services.agreement_followup.N8N_AGREEMENT_FOLLOWUP_URL", "")
    url, source = resolve_agreement_followup_webhook()
    assert source == "hardcoded"
    assert url == N8N_AGREEMENT_FOLLOWUP_HARDCODED_URL
    assert "agreement-followup-email" in url


def test_webhook_url_prefers_env_over_hardcoded(monkeypatch):
    monkeypatch.setenv(
        "N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL",
        "https://example.test/agreement-hook",
    )
    url, source = resolve_agreement_followup_webhook()
    assert source == "env:N8N_AGREEMENT_FOLLOWUP_EMAIL_WEBHOOK_URL"
    assert url == "https://example.test/agreement-hook"


def test_timeout_does_not_say_the_email_was_not_sent(monkeypatch):
    import httpx

    class _Client:
        def __init__(self, timeout=None):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def post(self, *args, **kwargs):
            raise httpx.TimeoutException("timed out")

    monkeypatch.setattr("services.agreement_followup.httpx.Client", _Client)
    try:
        send_agreement_first_touch_email(
            to="ada@acme.test",
            subject="Please sign",
            html="<p>Hi</p>",
            text="Hi",
            pdf_bytes=b"%PDF-1.4",
            filename="agreement.pdf",
            agreement_type="alinta_ci_gas",
            agreement_label="Alinta C&I Gas",
            business_name="Acme Bakery",
            offer_id=1,
            client_id=1,
        )
        raise AssertionError("expected timeout")
    except AgreementFollowupError as exc:
        assert str(exc) == (
            "We could not confirm whether this was sent. Check the inbox before sending again."
        )
        assert "not sent" not in str(exc).lower()
