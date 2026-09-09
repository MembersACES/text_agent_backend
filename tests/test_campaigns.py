"""Campaign persistence, test-send, stub offers, and bulk start."""

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base
from models import (
    AutonomousSequenceRun,
    AutonomousSequenceStep,
    AutonomousSequenceTemplate,
    AutonomousSequenceTemplateStep,
    CampaignEvent,
    CampaignRow,
    Offer,
    Suppression,
)
from services.campaigns import (
    CampaignError,
    FIGURE_COLUMNS,
    add_suppression,
    create_campaign,
    extract_email_id_from_webhook_response,
    patch_campaign,
    replace_rows,
    set_human_only,
    start_campaign,
    fire_test_send,
)
from services.merge_template import sanitize_html, split_row


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
        sequence_type="gci_outbound_v1",
        display_name="GCI outbound",
        timezone="Australia/Melbourne",
        is_active=1,
        is_restartable=1,
    )
    db.add(template)
    db.flush()
    db.add(
        AutonomousSequenceTemplateStep(
            template_id=template.id,
            step_index=0,
            day_number=3,
            channel="email",
            send_time_local="09:00",
            is_active=1,
        )
    )
    db.commit()
    return template


def _draft_with_rows(db, n=2, human_only_last=False):
    campaign = create_campaign(db, "GCI", "gci_outbound_v1", "morgan@acesolutions.com.au")
    headers = ["company_name", "contact_email", "XX_current_rate"]
    rows = [
        [f"Co {i}", f"person{i}@example.com", "16.76"]
        for i in range(n)
    ]
    replace_rows(
        db,
        campaign,
        headers,
        rows,
        {
            "company_name": "company_name",
            "contact_email": "contact_email",
            "XX_current_rate": "__intelligence__",
        },
        client_merge_json={"current_rate": "16.76"},
    )
    db.refresh(campaign)
    patch_campaign(
        db,
        campaign,
        {
            "first_touch_subject": "Hello {{company_name}}",
            "first_touch_html": "<p>Hi {{company_name}}<script>alert(1)</script></p>",
            "provenance_note": "Public records research.",
        },
        "morgan@acesolutions.com.au",
    )
    db.refresh(campaign)
    if human_only_last:
        last = (
            db.query(CampaignRow)
            .filter(CampaignRow.campaign_id == campaign.id)
            .order_by(CampaignRow.id.desc())
            .first()
        )
        set_human_only(db, campaign, last.id, True, "sensitive")
    return campaign


def test_split_ignores_client_supplied_intelligence():
    merge, intel = split_row(
        ["company_name", "$ per GJ"],
        ["Acme", "16.76"],
        {"company_name": "company_name", "$ per GJ": "__intelligence__"},
    )
    assert merge == {"company_name": "Acme"}
    assert intel["$ per GJ"] == "16.76"
    assert "16.76" not in merge.values()


def test_rows_endpoint_split_guard():
    db = _db()
    campaign = create_campaign(db, "GCI", "gci_outbound_v1", "a@b.com")
    summary = replace_rows(
        db,
        campaign,
        ["company_name", "contact_email", "$ per GJ"],
        [["Acme", "ada@example.com", "16.76"]] * 10,
        {
            "company_name": "company_name",
            "contact_email": "contact_email",
            "$ per GJ": "__intelligence__",
        },
        client_merge_json={"current_rate": "16.76", "company_name": "Hacked"},
    )
    assert summary["rows"] == 10
    assert summary["unique_recipients"] == 1
    row = db.query(CampaignRow).filter(CampaignRow.campaign_id == campaign.id).first()
    merge = row.merge_json
    assert "16.76" not in merge
    assert "current_rate" not in merge
    assert "16.76" in row.intelligence_json


def test_recipient_key_normalised():
    db = _db()
    campaign = create_campaign(db, "GCI", "gci_outbound_v1", "a@b.com")
    replace_rows(
        db,
        campaign,
        ["contact_email"],
        [["  Ada@Example.com "]],
        {"contact_email": "contact_email"},
    )
    row = db.query(CampaignRow).first()
    assert row.recipient_key == "ada@example.com"


def test_sanitiser_strips_script_and_javascript_href():
    cleaned = sanitize_html(
        '<p>Hi</p><script>alert(1)</script><a href="javascript:alert(1)">x</a>'
    )
    assert "<script>" not in cleaned
    assert "javascript:" not in cleaned
    assert "Hi" in cleaned


def test_ready_requires_test_send_and_provenance():
    db = _db()
    campaign = _draft_with_rows(db, n=1)
    try:
        patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
        raise AssertionError("expected ready to fail without a test send")
    except CampaignError as exc:
        assert "test" in str(exc).lower()
    row = db.query(CampaignRow).first()
    fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "morgan@acesolutions.com.au")
    db.refresh(campaign)
    campaign = patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
    assert campaign.status == "ready"


def test_ready_rejected_without_provenance():
    db = _db()
    campaign = create_campaign(db, "GCI", "gci_outbound_v1", "a@b.com")
    replace_rows(
        db,
        campaign,
        ["company_name", "contact_email"],
        [["Acme", "ada@example.com"]],
        {"company_name": "company_name", "contact_email": "contact_email"},
    )
    patch_campaign(
        db,
        campaign,
        {"first_touch_subject": "Hi", "first_touch_html": "<p>Hi {{company_name}}</p>"},
        "a@b.com",
    )
    db.refresh(campaign)
    try:
        patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
        raise AssertionError("expected provenance failure")
    except CampaignError as exc:
        assert "provenance" in str(exc).lower()


def test_token_validation_rejects_held_back_field():
    db = _db()
    campaign = create_campaign(db, "GCI", "gci_outbound_v1", "a@b.com")
    replace_rows(
        db,
        campaign,
        ["company_name", "industry"],
        [["Acme", "Food"]],
        {"company_name": "company_name", "industry": "__intelligence__"},
    )
    patch_campaign(
        db,
        campaign,
        {
            "first_touch_subject": "Hi",
            "first_touch_html": "<p>{{industry}}</p>",
            "provenance_note": "Public records.",
        },
        "a@b.com",
    )
    db.refresh(campaign)
    row = db.query(CampaignRow).first()
    fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "a@b.com")
    db.refresh(campaign)
    try:
        patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
        raise AssertionError("expected token failure")
    except CampaignError as exc:
        assert "industry" in str(exc)


def test_test_send_creates_no_run_or_offer():
    db = _db()
    campaign = _draft_with_rows(db, n=1)
    row = db.query(CampaignRow).first()
    result = fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "morgan@acesolutions.com.au")
    assert result["subject"].startswith("[TEST]")
    assert db.query(AutonomousSequenceRun).count() == 0
    assert db.query(Offer).count() == 0
    db.refresh(row)
    assert row.row_status == "pending"
    assert db.query(CampaignEvent).filter(CampaignEvent.event_type == "test_send").count() == 1


def test_test_send_rejects_comma_list():
    db = _db()
    campaign = _draft_with_rows(db, n=1)
    row = db.query(CampaignRow).first()
    try:
        fire_test_send(db, campaign, "a@x.com,b@x.com", row.id, "a@b.com")
        raise AssertionError("expected rejection")
    except CampaignError as exc:
        assert "single" in str(exc).lower()


def test_stub_offers_have_null_figures():
    db = _db()
    _template(db)
    campaign = _draft_with_rows(db, n=1)
    row = db.query(CampaignRow).first()
    fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "a@b.com")
    db.refresh(campaign)
    patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
    db.refresh(campaign)
    start_campaign(db, campaign, "a@b.com")
    offer = db.query(Offer).one()
    assert offer.campaign_id == campaign.id
    assert offer.client_id is None
    assert offer.utility_type == "gas"
    assert offer.status == "autonomous_agent_trigger"
    for col in FIGURE_COLUMNS:
        assert getattr(offer, col) is None, col
    run = db.query(AutonomousSequenceRun).one()
    assert "16.76" not in (run.context_json or "")


def test_start_is_idempotent():
    db = _db()
    _template(db)
    campaign = _draft_with_rows(db, n=2)
    row = db.query(CampaignRow).first()
    fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "a@b.com")
    db.refresh(campaign)
    patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
    db.refresh(campaign)
    first = start_campaign(db, campaign, "a@b.com")
    second = start_campaign(db, campaign, "a@b.com")
    assert db.query(AutonomousSequenceRun).count() == first["started"]
    assert second["started"] == 0 or db.query(AutonomousSequenceRun).count() == first["started"]
    assert db.query(AutonomousSequenceRun).count() == 2


def test_human_only_rows_are_never_started():
    db = _db()
    _template(db)
    campaign = _draft_with_rows(db, n=2, human_only_last=True)
    row = db.query(CampaignRow).filter(CampaignRow.human_only == 0).first()
    fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "a@b.com")
    db.refresh(campaign)
    patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
    db.refresh(campaign)
    start_campaign(db, campaign, "a@b.com")
    human = db.query(CampaignRow).filter(CampaignRow.human_only != 0).one()
    assert human.run_id is None
    assert human.row_status == "pending"
    assert db.query(AutonomousSequenceRun).count() == 1


def test_suppressed_addresses_are_never_started():
    db = _db()
    _template(db)
    campaign = _draft_with_rows(db, n=2)
    add_suppression(db, "person0@example.com", "unsubscribed", "test")
    row = db.query(CampaignRow).filter(CampaignRow.recipient_key == "person1@example.com").first()
    fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "a@b.com")
    db.refresh(campaign)
    patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
    db.refresh(campaign)
    start_campaign(db, campaign, "a@b.com")
    suppressed_row = (
        db.query(CampaignRow).filter(CampaignRow.recipient_key == "person0@example.com").one()
    )
    assert suppressed_row.row_status == "suppressed"
    assert suppressed_row.run_id is None
    assert db.query(AutonomousSequenceRun).count() == 1


def test_daily_cap_leaves_rest_pending():
    db = _db()
    _template(db)
    campaign = _draft_with_rows(db, n=5)
    row = db.query(CampaignRow).first()
    fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "a@b.com")
    db.refresh(campaign)
    patch_campaign(db, campaign, {"daily_cap": 2, "status": "ready"}, "a@b.com")
    db.refresh(campaign)
    result = start_campaign(db, campaign, "a@b.com")
    assert result["started"] == 2
    pending = (
        db.query(CampaignRow)
        .filter(CampaignRow.campaign_id == campaign.id, CampaignRow.row_status == "pending")
        .count()
    )
    assert pending == 3


def test_extract_email_id_matches_frontend_shape():
    assert extract_email_id_from_webhook_response({"email_id": "abc"}) == "abc"
    assert extract_email_id_from_webhook_response({"ok": True, "response": {"email_ID": "xyz"}}) == "xyz"
    assert extract_email_id_from_webhook_response([{"threadId": "tid-1"}]) == "tid-1"
    assert extract_email_id_from_webhook_response({"ok": True, "mode": "placeholder"}) is None


class _N8nClient:
    def __init__(self, payload):
        self._payload = payload

    def __call__(self, *a, **k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def post(self, *a, **k):
        class _Resp:
            def __init__(self, payload):
                self._payload = payload
                self.text = ""

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        return _Resp(self._payload)


def _ready_campaign(db, n=1, extra_email_step=False):
    template = _template(db)
    if extra_email_step:
        db.add(
            AutonomousSequenceTemplateStep(
                template_id=template.id,
                step_index=1,
                day_number=5,
                channel="email",
                send_time_local="09:00",
                is_active=1,
            )
        )
        db.commit()
    campaign = _draft_with_rows(db, n=n)
    row = db.query(CampaignRow).filter(CampaignRow.human_only == 0).first()
    fire_test_send(db, campaign, "morgan@acesolutions.com.au", row.id, "a@b.com")
    db.refresh(campaign)
    patch_campaign(db, campaign, {"status": "ready"}, "a@b.com")
    db.refresh(campaign)
    return campaign


def test_start_captures_n8n_email_id(monkeypatch):
    import services.campaigns as campaigns_mod

    monkeypatch.setattr(campaigns_mod, "N8N_EMAIL_URL", "https://n8n.example/webhook")
    monkeypatch.setattr(campaigns_mod.httpx, "Client", _N8nClient({"email_id": "abc"}))

    db = _db()
    campaign = _ready_campaign(db, n=1)
    start_campaign(db, campaign, "a@b.com")
    run = db.query(AutonomousSequenceRun).one()
    assert run.email_ID == "abc"
    ctx = json.loads(run.context_json or "{}")
    assert ctx["email_ID"] == "abc"
    assert ctx["email_id"] == "abc"


def test_start_without_n8n_email_id_does_not_fail(monkeypatch, caplog):
    import logging

    import services.campaigns as campaigns_mod

    monkeypatch.setattr(campaigns_mod, "N8N_EMAIL_URL", "https://n8n.example/webhook")
    monkeypatch.setattr(campaigns_mod.httpx, "Client", _N8nClient({"ok": True}))

    db = _db()
    campaign = _ready_campaign(db, n=1)
    row = db.query(CampaignRow).one()
    with caplog.at_level(logging.WARNING, logger="services.campaigns"):
        result = start_campaign(db, campaign, "a@b.com")
    assert result["started"] == 1
    run = db.query(AutonomousSequenceRun).one()
    assert run.email_ID is None
    assert "email_ID" not in json.loads(run.context_json or "{}")
    assert str(row.id) in caplog.text
    assert "no email_id" in caplog.text


def test_complete_first_email_step_is_step_index_zero():
    db = _db()
    campaign = _ready_campaign(db, n=1, extra_email_step=True)
    start_campaign(db, campaign, "a@b.com")
    run = db.query(AutonomousSequenceRun).one()
    steps = (
        db.query(AutonomousSequenceStep)
        .filter(AutonomousSequenceStep.run_id == run.id)
        .order_by(AutonomousSequenceStep.step_index)
        .all()
    )
    assert [s.step_index for s in steps] == [0, 1]
    assert steps[0].channel == "email"
    assert steps[0].step_status == "completed"
    assert steps[1].channel == "email"
    assert steps[1].step_status in {"ready", "to_start"}
