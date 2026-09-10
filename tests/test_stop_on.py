"""stop_on defaults and inbound invoice_received."""

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base
from models import AutonomousSequenceRun, AutonomousSequenceTemplate, Offer
from services.autonomous_sequence import (
    DEFAULT_STOP_ON,
    GCI_STOP_ON,
    apply_inbound,
    get_template_figures_mode,
    get_template_stop_on,
    parse_figures_mode,
    parse_stop_on,
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


def _gci_run(db, stop_on=None):
    db.add(
        AutonomousSequenceTemplate(
            sequence_type="gci_outbound_v1",
            display_name="GCI",
            timezone="Australia/Melbourne",
            is_active=1,
            is_restartable=1,
            stop_on=stop_on,
        )
    )
    offer = Offer(business_name="Ada Co", status="autonomous_agent_trigger")
    db.add(offer)
    db.flush()
    run = AutonomousSequenceRun(
        sequence_type="gci_outbound_v1",
        offer_id=offer.id,
        run_status="running",
        contact_email="ada@x.com",
        anchor_at=datetime.now(timezone.utc),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def test_parse_stop_on_omitted_is_existing_default():
    assert parse_stop_on(None) == DEFAULT_STOP_ON
    assert parse_stop_on("") == DEFAULT_STOP_ON


def test_gci_template_without_stop_on_uses_invoice_lane():
    db = _db()
    template = AutonomousSequenceTemplate(
        sequence_type="gci_outbound_v1",
        display_name="GCI",
        timezone="Australia/Melbourne",
        is_active=1,
        is_restartable=1,
    )
    db.add(template)
    db.commit()
    db.refresh(template)
    assert get_template_stop_on(template) == GCI_STOP_ON


def test_gci_inbound_does_not_stop_on_agreement_signed():
    db = _db()
    run = _gci_run(db, stop_on='["invoice_received","negative_sentiment_stop"]')
    out = apply_inbound(db, run, {"intent": "agreement_signed"})
    assert out.run_status == "running"
    assert out.stop_reason is None


def test_gci_inbound_stops_on_invoice_received_without_draft_when_template_null(caplog):
    db = _db()
    run = _gci_run(db, stop_on='["invoice_received","negative_sentiment_stop"]')
    with caplog.at_level("WARNING"):
        out = apply_inbound(db, run, {"intent": "invoice_received"})
    assert out.run_status == "stopped"
    assert out.stop_reason == "invoice_received"
    assert "No ack_template" in caplog.text


def test_figures_mode_defaults_to_comparison() -> None:
    assert parse_figures_mode(None) == "comparison"
    assert parse_figures_mode("") == "comparison"
    assert parse_figures_mode("none") == "none"
    db = _db()
    template = AutonomousSequenceTemplate(
        sequence_type="gas_base2_followup_v1",
        display_name="Gas",
        timezone="Australia/Melbourne",
        is_active=1,
        is_restartable=1,
    )
    db.add(template)
    db.commit()
    assert get_template_figures_mode(template) == "comparison"


def test_gci_figures_mode_none_is_read_from_the_column() -> None:
    db = _db()
    template = AutonomousSequenceTemplate(
        sequence_type="gci_outbound_v1",
        display_name="GCI",
        timezone="Australia/Melbourne",
        is_active=1,
        is_restartable=1,
        figures_mode="none",
    )
    db.add(template)
    db.commit()
    assert get_template_figures_mode(template) == "none"
