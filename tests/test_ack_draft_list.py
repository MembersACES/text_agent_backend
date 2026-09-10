"""Ack draft list flags and n8n draft-id capture."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base
from models import AutonomousSequenceEvent, AutonomousSequenceRun, AutonomousSequenceTemplate, Offer
from schemas import AutonomousSequenceRunListItem
from services.autonomous_sequence import (
    _log_event,
    apply_inbound,
    latest_ack_drafts_for_runs,
    parse_n8n_draft_response,
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


def _template(db, ack_invoice=None):
    row = AutonomousSequenceTemplate(
        sequence_type="gci_outbound_v1",
        display_name="GCI",
        timezone="Australia/Melbourne",
        is_active=1,
        is_restartable=1,
        stop_on='["invoice_received","negative_sentiment_stop"]',
        ack_template_invoice=json.dumps(ack_invoice) if ack_invoice else None,
    )
    db.add(row)
    db.flush()
    return row


def _run(db, business_name="Ada Co", context_json=None):
    offer = Offer(business_name=business_name, status="autonomous_agent_trigger")
    db.add(offer)
    db.flush()
    run = AutonomousSequenceRun(
        sequence_type="gci_outbound_v1",
        offer_id=offer.id,
        run_status="stopped",
        contact_email="ada@x.com",
        email_ID="thread-1",
        anchor_at=datetime.now(timezone.utc),
        context_json=context_json,
    )
    db.add(run)
    db.flush()
    return run


def _list_item(db, run, ack):
    from main import _autonomous_list_item

    return _autonomous_list_item(db, run, ack).model_dump()


def test_list_payload_pending_true_when_ack_event_exists():
    db = _db()
    _template(db)
    run = _run(db)
    _log_event(
        db,
        run.id,
        "ack_drafted",
        payload={
            "stop_reason": "invoice_received",
            "thread_id": "t-gmail",
            "draft_id": "r-draft",
            "subject": "Thanks",
            "body_html": "<p>Got it</p>",
        },
    )
    db.commit()
    ack = latest_ack_drafts_for_runs(db, [run.id]).get(run.id)
    item = _list_item(db, run, ack)
    assert item["ack_draft_pending"] is True
    assert item["ack_draft_thread_id"] == "t-gmail"


def test_list_payload_pending_false_without_ack_event():
    db = _db()
    _template(db)
    run = _run(db)
    db.commit()
    ack = latest_ack_drafts_for_runs(db, [run.id]).get(run.id)
    item = _list_item(db, run, ack)
    assert item["ack_draft_pending"] is False
    assert item["ack_draft_thread_id"] is None
    defaults = AutonomousSequenceRunListItem(
        id=run.id,
        offer_id=run.offer_id,
        sequence_type=run.sequence_type,
        run_status=run.run_status,
        anchor_at=run.anchor_at,
    )
    assert defaults.ack_draft_pending is False
    assert defaults.ack_draft_thread_id is None


def test_listing_ack_lookup_is_constant_queries_not_per_run():
    db = _db()
    _template(db)
    runs = [_run(db, business_name=f"Co {i}") for i in range(50)]
    for i, run in enumerate(runs):
        if i % 2 == 0:
            _log_event(
                db,
                run.id,
                "ack_drafted",
                payload={"thread_id": f"t-{i}", "stop_reason": "invoice_received"},
            )
    db.commit()
    statements: list[str] = []

    def _capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    bind = db.get_bind()
    event.listen(bind, "before_cursor_execute", _capture)
    try:
        flags = latest_ack_drafts_for_runs(db, [r.id for r in runs])
    finally:
        event.remove(bind, "before_cursor_execute", _capture)

    event_sql = [s for s in statements if "autonomous_sequence_events" in s.lower()]
    assert len(event_sql) <= 2
    assert flags[runs[0].id]["thread_id"] == "t-0"
    assert runs[1].id not in flags
    assert len(flags) == 25


class _Resp:
    def __init__(self, body, error=None):
        self._body = body
        self._error = error

    def raise_for_status(self):
        return None

    def json(self):
        if self._error is not None:
            raise self._error
        return self._body


def test_parse_n8n_draft_response_missing_id_logs_warning(caplog):
    with caplog.at_level("WARNING"):
        draft_id, thread_id = parse_n8n_draft_response(_Resp({"thread_id": "t1"}), run_id=9)
    assert draft_id is None
    assert thread_id == "t1"
    assert "missing draft_id" in caplog.text


def test_missing_n8n_draft_id_still_drafts_and_logs_warning(monkeypatch, caplog):
    db = _db()
    _template(db, ack_invoice={"subject": "Thanks Ada", "html": "<p>Got the invoice</p>"})
    run = _run(db)
    run.run_status = "running"
    db.commit()
    monkeypatch.setenv("N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL", "https://n8n.example/hook")

    class _Client:
        def __init__(self, timeout=None):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def post(self, *args, **kwargs):
            return _Resp({})

    monkeypatch.setattr("services.autonomous_sequence.httpx.Client", _Client)
    with caplog.at_level("WARNING"):
        out = apply_inbound(db, run, {"intent": "invoice_received"})
    assert out.run_status == "stopped"
    assert "missing draft_id" in caplog.text
    ev = (
        db.query(AutonomousSequenceEvent)
        .filter(AutonomousSequenceEvent.event_type == "ack_drafted")
        .one()
    )
    payload = json.loads(ev.payload_json)
    assert payload["draft_id"] is None
    assert payload["subject"] == "Thanks Ada"
    assert payload["body_html"] == "<p>Got the invoice</p>"
    assert payload["thread_id"] == "thread-1"


class _CaptureClient:
    def __init__(self, timeout=None):
        self.last_json = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def post(self, *args, **kwargs):
        self.last_json = kwargs.get("json")
        return _Resp({})


def test_ack_subject_uses_offer_business_name_when_context_has_no_company(monkeypatch, caplog):
    db = _db()
    _template(
        db,
        ack_invoice={
            "subject": "Solar cleaning booking confirmed for {{company_name}}",
            "html": "<p>Thanks</p>",
        },
    )
    run = _run(db, business_name="Polystar Pty Ltd", context_json='{"first_name":"Ada"}')
    run.run_status = "running"
    db.commit()
    monkeypatch.setenv("N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL", "https://n8n.example/hook")
    client = _CaptureClient()

    def _client_factory(timeout=None):
        return client

    monkeypatch.setattr("services.autonomous_sequence.httpx.Client", _client_factory)
    with caplog.at_level("WARNING"):
        apply_inbound(db, run, {"intent": "invoice_received"})
    assert client.last_json["subject"] == "Solar cleaning booking confirmed for Polystar Pty Ltd"
    assert "unresolved merge tokens" not in caplog.text


def test_ack_subject_keeps_company_token_and_warns_when_name_missing(monkeypatch, caplog):
    db = _db()
    _template(
        db,
        ack_invoice={
            "subject": "Solar cleaning booking confirmed for {{company_name}}",
            "html": "<p>Thanks</p>",
        },
    )
    run = _run(db, business_name=None, context_json='{"first_name":"Ada"}')
    run.run_status = "running"
    db.commit()
    monkeypatch.setenv("N8N_AUTONOMOUS_EMAIL_WEBHOOK_URL", "https://n8n.example/hook")
    client = _CaptureClient()

    def _client_factory(timeout=None):
        return client

    monkeypatch.setattr("services.autonomous_sequence.httpx.Client", _client_factory)
    with caplog.at_level("WARNING"):
        apply_inbound(db, run, {"intent": "invoice_received"})
    assert client.last_json["subject"] == "Solar cleaning booking confirmed for {{company_name}}"
    unresolved = [r for r in caplog.records if "unresolved merge tokens" in r.getMessage()]
    assert len(unresolved) == 1
    assert "company_name" in unresolved[0].getMessage()
    assert str(run.id) in unresolved[0].getMessage()
