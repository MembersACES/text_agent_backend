"""Ack/thank-you template save must persist onto the row get_lane_config reads."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base
from models import AutonomousSequenceRun, AutonomousSequenceTemplate, Offer
from schemas import AutonomousSequenceTemplateUpdate
from services.autonomous_sequence import parse_ack_template

MARKER = "ACK_MARKER_ZXQ_991"

UI_SAVE_PAYLOAD = {
    "display_name": "GCI",
    "description": "",
    "timezone": "Australia/Brisbane",
    "is_active": True,
    "is_restartable": True,
    "signature_html": "",
    "extra_context": "",
    "sequence_type": "gci_outbound_v1",
    "linked_flow_keys": [],
    "validity_mode": "fixed_days",
    "validity_days": 7,
    "stop_on": ["agreement_signed", "negative_sentiment_stop"],
    "ack_template_signed": {
        "subject": f"Thanks {MARKER}",
        "html": f"<p>Signed — {MARKER}</p>",
    },
    "ack_template_invoice": None,
    "figures_mode": "comparison",
}


def _db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


class _CaptureClient:
    def __init__(self, timeout=None):
        self.last_json = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def post(self, *args, **kwargs):
        self.last_json = kwargs.get("json")

        class _Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {}

        return _Resp()


def test_ui_ack_save_reaches_lane_config_shape_and_signed_draft(monkeypatch):
    db = _db()
    template = AutonomousSequenceTemplate(
        sequence_type="gci_outbound_v1",
        display_name="GCI",
        timezone="Australia/Melbourne",
        is_active=1,
        is_restartable=1,
        stop_on='["agreement_signed","negative_sentiment_stop"]',
        ack_template_signed=json.dumps({"subject": "Old thanks", "html": "<p>OLD WORDING</p>"}),
    )
    db.add(template)
    offer = Offer(business_name="Ada Co", status="autonomous_agent_trigger")
    db.add(offer)
    db.flush()
    run = AutonomousSequenceRun(
        sequence_type="gci_outbound_v1",
        offer_id=offer.id,
        run_status="running",
        contact_email="ada@x.com",
        email_ID="thread-1",
        anchor_at=datetime.now(timezone.utc),
    )
    db.add(run)
    db.commit()
    db.refresh(template)

    from main import autonomous_sequence_update_template

    body = AutonomousSequenceTemplateUpdate.model_validate(UI_SAVE_PAYLOAD)
    autonomous_sequence_update_template(
        template_id=template.id,
        body=body,
        db=db,
        user_data={"idinfo": {"email": "a@b.com"}},
    )
    db.expire_all()

    row = (
        db.query(AutonomousSequenceTemplate)
        .filter(AutonomousSequenceTemplate.sequence_type == "gci_outbound_v1")
        .first()
    )
    lane = {
        "ack_template_signed": parse_ack_template(row.ack_template_signed),
        "ack_template_invoice": parse_ack_template(row.ack_template_invoice),
    }
    assert lane["ack_template_signed"] is not None
    assert MARKER in lane["ack_template_signed"]["subject"]
    assert MARKER in lane["ack_template_signed"]["html"]
    assert "OLD WORDING" not in lane["ack_template_signed"]["html"]

    monkeypatch.setenv("N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL", "https://n8n.example/hook")
    client = _CaptureClient()
    monkeypatch.setattr("services.autonomous_sequence.httpx.Client", lambda timeout=None: client)

    from services.autonomous_sequence import apply_inbound

    db.refresh(run)
    apply_inbound(db, run, {"intent": "agreement_signed"})
    assert client.last_json is not None
    assert MARKER in client.last_json["subject"]
    assert MARKER in client.last_json["body_html"]
    assert "OLD WORDING" not in client.last_json["body_html"]


def test_ack_payload_accepts_body_alias_like_email_system_prompt():
    body = AutonomousSequenceTemplateUpdate.model_validate(
        {
            "ack_template_signed": {
                "subject": "Thanks",
                "body": f"<p>{MARKER}</p>",
            }
        }
    )
    dumped = parse_ack_template(body.ack_template_signed)
    assert dumped is not None
    assert MARKER in dumped["html"]


def _existing_ack_row(db, signed_html="<p>KEEP THIS ACK</p>"):
    template = AutonomousSequenceTemplate(
        sequence_type="gci_outbound_v1",
        display_name="GCI",
        timezone="Australia/Melbourne",
        is_active=1,
        is_restartable=1,
        ack_template_signed=json.dumps({"subject": "Keep me", "html": signed_html}),
        ack_template_invoice=json.dumps({"subject": "Invoice keep", "html": "<p>invoice keep</p>"}),
    )
    db.add(template)
    db.commit()
    db.refresh(template)
    return template


def test_patch_display_name_only_leaves_ack_template_signed():
    db = _db()
    template = _existing_ack_row(db)
    kept = template.ack_template_signed
    invoice_kept = template.ack_template_invoice

    from main import autonomous_sequence_update_template

    body = AutonomousSequenceTemplateUpdate.model_validate({"display_name": "GCI renamed"})
    assert "ack_template_signed" not in body.model_fields_set
    assert "ack_template_invoice" not in body.model_fields_set
    autonomous_sequence_update_template(
        template_id=template.id,
        body=body,
        db=db,
        user_data={"idinfo": {"email": "a@b.com"}},
    )
    db.expire_all()
    row = db.query(AutonomousSequenceTemplate).filter_by(id=template.id).one()
    assert row.display_name == "GCI renamed"
    assert row.ack_template_signed == kept
    assert row.ack_template_invoice == invoice_kept
    parsed = parse_ack_template(row.ack_template_signed)
    assert parsed is not None
    assert parsed["html"] == "<p>KEEP THIS ACK</p>"


def test_explicit_null_or_empty_ack_clears_present_key_only():
    db = _db()
    template = _existing_ack_row(db)

    from main import autonomous_sequence_update_template

    body = AutonomousSequenceTemplateUpdate.model_validate(
        {"ack_template_signed": None, "ack_template_invoice": ""}
    )
    assert "ack_template_signed" in body.model_fields_set
    assert "ack_template_invoice" in body.model_fields_set
    autonomous_sequence_update_template(
        template_id=template.id,
        body=body,
        db=db,
        user_data={"idinfo": {"email": "a@b.com"}},
    )
    db.expire_all()
    row = db.query(AutonomousSequenceTemplate).filter_by(id=template.id).one()
    assert row.ack_template_signed is None
    assert row.ack_template_invoice is None
