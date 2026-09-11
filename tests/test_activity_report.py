from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import (
    AutonomousSequenceRun,
    Client,
    ClientManualActivity,
    Offer,
    OfferActivity,
    StrategyItem,
)
from services.activity_report import ActivityReportService


def _make_test_session():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def _make_client(db, name: str = "Acme Pty Ltd") -> Client:
    client = Client(
        business_name=name,
        stage="lead",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(client)
    db.commit()
    db.refresh(client)
    return client


def _make_offer(db, client: Client) -> Offer:
    offer = Offer(
        client_id=client.id,
        business_name=client.business_name,
        utility_type="gas",
        identifier="5321568754",
        status="requested",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(offer)
    db.commit()
    db.refresh(offer)
    return offer


def test_purge_test_data_deletes_only_test_user_rows():
    db = _make_test_session()
    service = ActivityReportService()
    client = _make_client(db)
    offer = _make_offer(db, client)

    test_activity = OfferActivity(
        offer_id=offer.id,
        client_id=client.id,
        activity_type="comparison",
        created_by="test@acesolutions.com.au",
        created_at=datetime.utcnow(),
    )
    keep_activity = OfferActivity(
        offer_id=offer.id,
        client_id=client.id,
        activity_type="comparison",
        created_by="morgan.h@acesolutions.com.au",
        created_at=datetime.utcnow(),
    )
    db.add_all([test_activity, keep_activity])
    db.commit()
    db.refresh(test_activity)
    db.refresh(keep_activity)

    strategy = StrategyItem(
        client_id=client.id,
        year=2026,
        section="in_progress",
        row_index=0,
        offer_id=offer.id,
        offer_activity_id=test_activity.id,
        activity_type="comparison",
    )
    run = AutonomousSequenceRun(
        sequence_type="ci_electricity_offer",
        offer_id=offer.id,
        client_id=client.id,
        crm_activity_id=test_activity.id,
        run_status="running",
        anchor_at=datetime.utcnow(),
    )
    manual = ClientManualActivity(
        client_id=client.id,
        note="Test note",
        created_by="TEST@acesolutions.com.au",
        created_at=datetime.utcnow(),
    )
    db.add_all([strategy, run, manual])
    db.commit()
    db.refresh(run)

    preview = service.preview_test_data(db)
    assert preview.offer_activity_count == 1
    assert preview.client_manual_count == 1
    assert preview.strategy_item_count == 1
    assert preview.autonomous_runs_unlinked == 1
    assert preview.by_type["comparison"] == 1
    assert preview.by_type["client_manual_activity"] == 1

    result = service.purge_test_data(db)
    assert result.offer_activities_deleted == 1
    assert result.client_manual_deleted == 1
    assert result.strategy_items_deleted == 1
    assert result.autonomous_runs_unlinked == 1

    remaining = db.query(OfferActivity).all()
    assert len(remaining) == 1
    assert remaining[0].created_by == "morgan.h@acesolutions.com.au"
    assert db.query(ClientManualActivity).count() == 0
    assert db.query(StrategyItem).count() == 0
    assert db.query(Offer).count() == 1
    db.refresh(run)
    assert run.crm_activity_id is None


def test_export_csv_includes_filtered_comparison_rows():
    db = _make_test_session()
    service = ActivityReportService()
    client = _make_client(db, "Export Co")
    offer = _make_offer(db, client)
    today = datetime.utcnow().date().isoformat()
    yesterday = (datetime.utcnow() - timedelta(days=1)).date().isoformat()

    db.add(
        OfferActivity(
            offer_id=offer.id,
            client_id=client.id,
            activity_type="comparison",
            created_by="morgan.h@acesolutions.com.au",
            created_at=datetime.utcnow(),
        )
    )
    db.add(
        OfferActivity(
            offer_id=offer.id,
            client_id=client.id,
            activity_type="base2_review",
            created_by="morgan.h@acesolutions.com.au",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()

    content, filename = service.export_csv(
        db,
        client.id,
        "comparison",
        yesterday,
        today,
    )
    text = content.decode("utf-8-sig")
    assert "activity_type" in text
    assert "Comparison" in text
    assert "Base 2 review run" not in text
    assert "Export Co" in text
    assert filename.startswith("activity-report-")
    assert filename.endswith(".csv")
