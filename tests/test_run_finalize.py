"""Close exhausted runs and count finished steps regardless of last-step errors."""

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base
from models import AutonomousSequenceRun, AutonomousSequenceStep, Offer
from services.autonomous_sequence import (
    count_steps_done,
    finalize_run_if_exhausted,
    mark_step_dispatched,
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


def _run_with_steps(db, statuses: list[str]) -> AutonomousSequenceRun:
    offer = Offer(business_name="Acme", status="autonomous_agent_trigger")
    db.add(offer)
    db.flush()
    run = AutonomousSequenceRun(
        sequence_type="gci_outbound_v1",
        offer_id=offer.id,
        run_status="running",
        anchor_at=datetime.now(timezone.utc),
    )
    db.add(run)
    db.flush()
    for index, status in enumerate(statuses):
        db.add(
            AutonomousSequenceStep(
                run_id=run.id,
                step_index=index,
                day_number=index,
                channel="sms" if index == 1 else "email",
                offset_minutes_from_day_start=0,
                step_status=status,
            )
        )
    db.commit()
    db.refresh(run)
    return run


def test_steps_done_counts_any_non_open_status():
    db = _db()
    run = _run_with_steps(db, ["completed", "error", "executed", "skipped", "dispatch_failed"])
    assert count_steps_done(list(run.steps)) == 5


def test_open_step_is_not_counted_done():
    db = _db()
    run = _run_with_steps(db, ["executed", "error", "ready", "to_start", "in_progress"])
    assert count_steps_done(list(run.steps)) == 2


def test_last_step_error_closes_run_as_errored():
    db = _db()
    run = _run_with_steps(db, ["executed", "completed", "executed", "error", "skipped"])
    assert count_steps_done(list(run.steps)) == 5
    assert finalize_run_if_exhausted(db, run) is True
    db.commit()
    db.refresh(run)
    assert run.run_status == "errored"
    assert run.stop_reason == "step_error"


def test_successful_exhaustion_closes_as_completed():
    db = _db()
    run = _run_with_steps(db, ["executed", "completed", "skipped"])
    assert finalize_run_if_exhausted(db, run) is True
    db.commit()
    db.refresh(run)
    assert run.run_status == "completed"
    assert run.stop_reason is None


def test_pending_step_leaves_run_running():
    db = _db()
    run = _run_with_steps(db, ["executed", "ready"])
    assert finalize_run_if_exhausted(db, run) is False
    db.refresh(run)
    assert run.run_status == "running"


def test_mark_step_dispatched_failure_on_last_step_errors_the_run():
    db = _db()
    run = _run_with_steps(db, ["executed", "ready"])
    last = next(step for step in run.steps if step.step_status == "ready")
    updated = mark_step_dispatched(db, run.id, last.id, success=False, summary="sms failed")
    assert updated.run_status == "errored"
    assert last.step_status == "error" or (
        db.query(AutonomousSequenceStep).filter(AutonomousSequenceStep.id == last.id).one().step_status
        == "error"
    )
