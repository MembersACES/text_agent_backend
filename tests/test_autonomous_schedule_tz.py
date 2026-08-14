"""Timezone resolution for autonomous sequence scheduling."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base
from models import (
    AutonomousSequenceStep,
    AutonomousSequenceTemplate,
    AutonomousSequenceTemplateStep,
    Offer,
)
from schemas import AutonomousSequenceStartRequest
from services.autonomous_sequence import (
    resolve_schedule_tz,
    start_gas_base2_sequence,
)


def _make_test_session():
    # StaticPool: shared in-memory DB across connections (inspect() opens a second conn).
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def test_start_request_omitted_timezone_is_none():
    body = AutonomousSequenceStartRequest(
        sequence_type="gas_base2_followup_v1",
        offer_id=1,
        anchor_at=datetime(2026, 1, 9, 12, 0, tzinfo=timezone.utc),
    )
    assert body.timezone is None


def test_resolve_schedule_tz_template_wins_when_run_timezone_null():
    class _Run:
        timezone = None

    class _Tpl:
        timezone = "Australia/Brisbane"

    assert resolve_schedule_tz(_Run(), _Tpl()).key == "Australia/Brisbane"


def test_omitted_request_tz_uses_template_brisbane_not_melbourne():
    """
    Regression: schema/ORM Melbourne defaults used to sit above the chain and
    force Melbourne even when the template was Brisbane.
    """
    db = _make_test_session()
    offer = Offer(business_name="TZ Test Co", status="requested")
    db.add(offer)
    db.flush()

    template = AutonomousSequenceTemplate(
        sequence_type="gas_base2_followup_v1",
        display_name="Gas Base2",
        timezone="Australia/Brisbane",
        is_active=1,
        is_restartable=1,
    )
    db.add(template)
    db.flush()
    db.add(
        AutonomousSequenceTemplateStep(
            template_id=template.id,
            step_index=0,
            day_number=1,
            channel="email",
            send_time_local="09:00",
            is_active=1,
        )
    )
    db.commit()

    # Summer: Melbourne is AEDT (UTC+11), Brisbane stays UTC+10 — offsets diverge.
    # Friday 2026-01-09 UTC → D1 = Monday 2026-01-12 09:00 local.
    anchor = datetime(2026, 1, 9, 12, 0, tzinfo=timezone.utc)
    run = start_gas_base2_sequence(
        db,
        sequence_type="gas_base2_followup_v1",
        offer_id=offer.id,
        client_id=None,
        crm_activity_id=None,
        anchor_at=anchor,
        tz=None,
        context={},
    )
    db.commit()

    assert run.timezone == "Australia/Brisbane"

    steps = (
        db.query(AutonomousSequenceStep)
        .filter(AutonomousSequenceStep.run_id == run.id)
        .order_by(AutonomousSequenceStep.step_index)
        .all()
    )
    assert steps, "expected at least one planned step"
    email = steps[0]
    assert email.channel == "email"

    brisbane = ZoneInfo("Australia/Brisbane")
    melbourne = ZoneInfo("Australia/Melbourne")
    expected_brisbane_utc = datetime(2026, 1, 12, 9, 0, tzinfo=brisbane).astimezone(
        timezone.utc
    ).replace(tzinfo=None)
    expected_melbourne_utc = datetime(2026, 1, 12, 9, 0, tzinfo=melbourne).astimezone(
        timezone.utc
    ).replace(tzinfo=None)

    assert expected_brisbane_utc != expected_melbourne_utc
    assert email.scheduled_at == expected_brisbane_utc
    assert email.scheduled_at != expected_melbourne_utc
