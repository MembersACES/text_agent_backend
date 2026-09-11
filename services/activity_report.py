"""Activity report export and test-user cleanup."""

from __future__ import annotations

import csv
import io
import json
import logging
from collections import Counter
from datetime import datetime, timedelta
from typing import List, Optional, Sequence

from sqlalchemy import func
from sqlalchemy.orm import Session

from crm_enums import OfferActivityType
from models import (
    AutonomousSequenceRun,
    Client,
    ClientManualActivity,
    ClientStatusNote,
    Offer,
    OfferActivity,
    StrategyItem,
    Task,
    TaskHistory,
    Testimonial,
)
from schemas import (
    ActivityReportItem,
    ActivityTestDataPreview,
    ActivityTestDataPurgeResult,
)
from utils.timezone import to_melbourne_iso

logger = logging.getLogger(__name__)

CUSTOM_ACTIVITY_TYPES = frozenset(
    {
        "note_added",
        "task_created",
        "task_edited",
        "task_completed",
        "testimonial_activity",
        "client_manual_activity",
    }
)
TASK_ACTIVITY_TYPES = frozenset({"task_created", "task_edited", "task_completed"})

CSV_TYPE_LABELS = {
    "quote_request": "Quote request sent",
    "data_request": "Data request sent",
    "base2_review": "Base 2 review run",
    "comparison": "Comparison",
    "ghg_offer": "GHG offer",
    "engagement_form": "Engagement form generated",
    "engagement_form_signed": "Engagement form signed",
    "contract_requested": "Contract requested",
    "alinta_agreement_requested": "Alinta agreement requested",
    "contract_received": "Contract received",
    "contract_sent_for_signing": "Contract sent for signing",
    "contract_signed_lodged": "Contract signed & lodged",
    "discrepancy_email_sent": "Discrepancy email sent",
    "dma_review_generated": "DMA review generated",
    "dma_email_sent": "DMA email sent",
    "eoi": "EOI generated",
    "loa": "LOA generated",
    "service_agreement": "Service agreement generated",
    "solution_presentation": "Solution presentation generated",
    "manual_document": "Document / link added",
    "manual_activity": "Activity note",
    "one_month_savings_invoice": "1st Month Savings Invoice generated",
    "new_revenue_invoice": "Discrepancy / New Revenue invoice generated",
    "solar_cleaning_quote_generated": "Solar panel cleaning quote generated",
    "solar_cleaning_quote_sent": "Solar panel cleaning quote sent to client",
    "solar_cleaning_signed_offer": "Solar panel cleaning signed offer uploaded",
    "member_document_upload": "Member document uploaded",
    "note_added": "Note added",
    "task_created": "Task created",
    "task_edited": "Task edited",
    "task_completed": "Task completed",
    "testimonial_activity": "Testimonial activity",
    "client_manual_activity": "Manual activity (client)",
}

CSV_COLUMNS = [
    "created_at",
    "activity_type",
    "activity_type_key",
    "client",
    "details",
    "created_by",
    "document_link",
    "offer_id",
    "task_id",
    "client_id",
]


class ActivityReportService:
    TEST_EMAIL = "test@acesolutions.com.au"
    EXPORT_MAX_ROWS = 10000

    def export_csv(
        self,
        db: Session,
        client_id: Optional[int],
        activity_type: Optional[str],
        created_after: Optional[str],
        created_before: Optional[str],
    ) -> tuple[bytes, str]:
        rows = self.collect_rows(
            db,
            client_id,
            activity_type,
            created_after,
            created_before,
        )
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(CSV_COLUMNS)
        for item in rows:
            type_key = item.activity_type or ""
            offer_id = item.offer_id if item.offer_id and item.offer_id > 0 else ""
            writer.writerow(
                [
                    to_melbourne_iso(item.created_at) if item.created_at else "",
                    CSV_TYPE_LABELS.get(type_key, type_key.replace("_", " ")),
                    type_key,
                    item.business_name or "",
                    item.offer_display or "",
                    item.created_by or "",
                    item.document_link or "",
                    offer_id,
                    item.task_id or "",
                    item.client_id or "",
                ]
            )
        after = (created_after or "start").strip() or "start"
        before = (created_before or "end").strip() or "end"
        filename = f"activity-report-{after}-to-{before}.csv"
        return buf.getvalue().encode("utf-8-sig"), filename

    def preview_test_data(self, db: Session) -> ActivityTestDataPreview:
        snapshot = self._test_data_snapshot(db)
        return ActivityTestDataPreview(
            email=self.TEST_EMAIL,
            offer_activity_count=len(snapshot.offer_activities),
            client_manual_count=len(snapshot.manual_rows),
            strategy_item_count=snapshot.strategy_item_count,
            autonomous_runs_unlinked=snapshot.autonomous_run_count,
            by_type=snapshot.by_type,
            sample_clients=snapshot.sample_clients,
        )

    def purge_test_data(self, db: Session) -> ActivityTestDataPurgeResult:
        snapshot = self._test_data_snapshot(db)
        activity_ids = [row.id for row in snapshot.offer_activities]
        strategy_deleted = 0
        unlinked = 0
        if activity_ids:
            strategy_deleted = (
                db.query(StrategyItem)
                .filter(StrategyItem.offer_activity_id.in_(activity_ids))
                .delete(synchronize_session=False)
            )
            unlinked = (
                db.query(AutonomousSequenceRun)
                .filter(AutonomousSequenceRun.crm_activity_id.in_(activity_ids))
                .update(
                    {AutonomousSequenceRun.crm_activity_id: None},
                    synchronize_session=False,
                )
            )
            db.query(OfferActivity).filter(OfferActivity.id.in_(activity_ids)).delete(
                synchronize_session=False
            )
        manual_ids = [row.id for row in snapshot.manual_rows]
        manual_deleted = 0
        if manual_ids:
            manual_deleted = (
                db.query(ClientManualActivity)
                .filter(ClientManualActivity.id.in_(manual_ids))
                .delete(synchronize_session=False)
            )
        db.commit()
        logger.info(
            "Purged test activity data email=%s offer_activities=%s manual=%s strategy=%s unlinked_runs=%s",
            self.TEST_EMAIL,
            len(activity_ids),
            manual_deleted,
            strategy_deleted,
            unlinked,
        )
        return ActivityTestDataPurgeResult(
            email=self.TEST_EMAIL,
            offer_activities_deleted=len(activity_ids),
            client_manual_deleted=manual_deleted,
            strategy_items_deleted=strategy_deleted,
            autonomous_runs_unlinked=unlinked,
        )

    def collect_rows(
        self,
        db: Session,
        client_id: Optional[int],
        activity_type: Optional[str],
        created_after: Optional[str],
        created_before: Optional[str],
    ) -> List[ActivityReportItem]:
        type_filter = (activity_type or "").strip() or None
        items: List[ActivityReportItem] = []
        if self._include_offer_activities(type_filter):
            items.extend(
                self._offer_activity_rows(
                    db, client_id, type_filter, created_after, created_before
                )
            )
        if self._include_testimonials(type_filter):
            items.extend(
                self._testimonial_rows(db, client_id, created_after, created_before)
            )
        if self._include_tasks(type_filter):
            items.extend(
                self._task_rows(
                    db, client_id, type_filter, created_after, created_before
                )
            )
        if self._include_manual(type_filter):
            items.extend(
                self._manual_rows(db, client_id, created_after, created_before)
            )
        if self._include_notes(type_filter) and client_id:
            items.extend(
                self._note_rows(db, client_id, created_after, created_before)
            )
        items.sort(key=lambda row: row.created_at or datetime.min, reverse=True)
        return items[: self.EXPORT_MAX_ROWS]

    def _include_offer_activities(self, activity_type: Optional[str]) -> bool:
        if not activity_type:
            return True
        return activity_type not in CUSTOM_ACTIVITY_TYPES

    def _include_testimonials(self, activity_type: Optional[str]) -> bool:
        if not activity_type:
            return True
        return activity_type == "testimonial_activity"

    def _include_tasks(self, activity_type: Optional[str]) -> bool:
        if not activity_type:
            return True
        return activity_type in TASK_ACTIVITY_TYPES

    def _include_manual(self, activity_type: Optional[str]) -> bool:
        if not activity_type:
            return True
        return activity_type == "client_manual_activity"

    def _include_notes(self, activity_type: Optional[str]) -> bool:
        if not activity_type:
            return True
        return activity_type == "note_added"

    def _offer_activity_rows(
        self,
        db: Session,
        client_id: Optional[int],
        activity_type: Optional[str],
        created_after: Optional[str],
        created_before: Optional[str],
    ) -> List[ActivityReportItem]:
        query = db.query(OfferActivity, Offer).join(Offer, OfferActivity.offer_id == Offer.id)
        if client_id is not None:
            query = query.filter(Offer.client_id == client_id)
        if activity_type:
            query = query.filter(OfferActivity.activity_type == activity_type)
        query = self._apply_date_filters(
            query, OfferActivity.created_at, created_after, created_before
        )
        out: List[ActivityReportItem] = []
        for act, offer in query.order_by(OfferActivity.created_at.desc()).all():
            out.append(
                ActivityReportItem(
                    id=act.id,
                    offer_id=act.offer_id,
                    task_id=None,
                    client_id=act.client_id or offer.client_id,
                    business_name=offer.business_name,
                    activity_type=act.activity_type,
                    document_link=act.document_link,
                    created_at=act.created_at,
                    created_by=act.created_by,
                    offer_display=self._offer_display(offer, act),
                )
            )
        return out

    def _testimonial_rows(
        self,
        db: Session,
        client_id: Optional[int],
        created_after: Optional[str],
        created_before: Optional[str],
    ) -> List[ActivityReportItem]:
        query = db.query(Testimonial)
        client: Optional[Client] = None
        if client_id is not None:
            client = db.query(Client).filter(Client.id == client_id).first()
            if not client or not (client.business_name or "").strip():
                return []
            query = query.filter(
                func.lower(Testimonial.business_name) == client.business_name.strip().lower()
            )
        query = self._apply_date_filters(
            query, Testimonial.created_at, created_after, created_before
        )
        out: List[ActivityReportItem] = []
        for row in query.order_by(Testimonial.created_at.desc()).all():
            drive_link = (
                f"https://drive.google.com/file/d/{row.file_id}/view" if row.file_id else None
            )
            out.append(
                ActivityReportItem(
                    id=-(row.id + 1000000),
                    offer_id=0,
                    task_id=None,
                    client_id=client.id if client else None,
                    business_name=row.business_name,
                    activity_type="testimonial_activity",
                    document_link=drive_link,
                    created_at=row.created_at,
                    created_by=None,
                    offer_display=f"{row.file_name} ({row.status})" if row.status else row.file_name,
                )
            )
        return out

    def _task_rows(
        self,
        db: Session,
        client_id: Optional[int],
        activity_type: Optional[str],
        created_after: Optional[str],
        created_before: Optional[str],
    ) -> List[ActivityReportItem]:
        query = (
            db.query(TaskHistory, Task, Client)
            .join(Task, TaskHistory.task_id == Task.id)
            .outerjoin(Client, Task.client_id == Client.id)
        )
        if client_id is not None:
            query = query.filter(Task.client_id == client_id)
        query = self._apply_date_filters(
            query, TaskHistory.created_at, created_after, created_before
        )
        out: List[ActivityReportItem] = []
        for hist, task, client in query.order_by(TaskHistory.created_at.desc()).all():
            mapped_type = self._task_activity_type(hist.action, hist.new_value)
            if activity_type and mapped_type != activity_type:
                continue
            out.append(
                ActivityReportItem(
                    id=-(hist.id + 2000000),
                    offer_id=0,
                    task_id=task.id,
                    client_id=task.client_id,
                    business_name=client.business_name if client else None,
                    activity_type=mapped_type,
                    document_link=None,
                    created_at=hist.created_at,
                    created_by=hist.user_email,
                    offer_display=self._task_display(hist, task),
                )
            )
        return out

    def _manual_rows(
        self,
        db: Session,
        client_id: Optional[int],
        created_after: Optional[str],
        created_before: Optional[str],
    ) -> List[ActivityReportItem]:
        query = db.query(ClientManualActivity, Client).join(
            Client, ClientManualActivity.client_id == Client.id
        )
        if client_id is not None:
            query = query.filter(ClientManualActivity.client_id == client_id)
        query = self._apply_date_filters(
            query, ClientManualActivity.created_at, created_after, created_before
        )
        out: List[ActivityReportItem] = []
        for row, client in query.order_by(ClientManualActivity.created_at.desc()).all():
            out.append(
                ActivityReportItem(
                    id=row.id,
                    offer_id=0,
                    task_id=None,
                    client_id=row.client_id,
                    business_name=client.business_name,
                    activity_type="client_manual_activity",
                    document_link=row.document_link,
                    created_at=row.created_at,
                    created_by=row.created_by,
                    offer_display=self._manual_display(row),
                    manual_activity_id=row.id,
                )
            )
        return out

    def _note_rows(
        self,
        db: Session,
        client_id: int,
        created_after: Optional[str],
        created_before: Optional[str],
    ) -> List[ActivityReportItem]:
        client = db.query(Client).filter(Client.id == client_id).first()
        if not client:
            return []
        query = db.query(ClientStatusNote).filter(ClientStatusNote.client_id == client_id)
        query = self._apply_date_filters(
            query, ClientStatusNote.created_at, created_after, created_before
        )
        out: List[ActivityReportItem] = []
        for note in query.order_by(ClientStatusNote.created_at.desc()).all():
            preview = (note.note or "").split("\n")[0][:50] or "Note"
            out.append(
                ActivityReportItem(
                    id=-note.id,
                    offer_id=0,
                    task_id=None,
                    client_id=client_id,
                    business_name=client.business_name,
                    activity_type="note_added",
                    document_link=None,
                    created_at=note.created_at,
                    created_by=note.user_email,
                    offer_display=preview,
                )
            )
        return out

    def _test_data_snapshot(self, db: Session) -> "_TestDataSnapshot":
        email = self.TEST_EMAIL
        offer_activities = (
            db.query(OfferActivity)
            .filter(func.lower(OfferActivity.created_by) == email)
            .all()
        )
        manual_rows = (
            db.query(ClientManualActivity)
            .filter(func.lower(ClientManualActivity.created_by) == email)
            .all()
        )
        activity_ids = [row.id for row in offer_activities]
        strategy_item_count = 0
        autonomous_run_count = 0
        if activity_ids:
            strategy_item_count = (
                db.query(func.count(StrategyItem.id))
                .filter(StrategyItem.offer_activity_id.in_(activity_ids))
                .scalar()
                or 0
            )
            autonomous_run_count = (
                db.query(func.count(AutonomousSequenceRun.id))
                .filter(AutonomousSequenceRun.crm_activity_id.in_(activity_ids))
                .scalar()
                or 0
            )
        type_counter: Counter[str] = Counter()
        for row in offer_activities:
            type_counter[row.activity_type or "unknown"] += 1
        if manual_rows:
            type_counter["client_manual_activity"] += len(manual_rows)
        sample_clients = self._sample_client_names(db, offer_activities, manual_rows)
        return _TestDataSnapshot(
            offer_activities=offer_activities,
            manual_rows=manual_rows,
            strategy_item_count=int(strategy_item_count),
            autonomous_run_count=int(autonomous_run_count),
            by_type=dict(type_counter),
            sample_clients=sample_clients,
        )

    def _sample_client_names(
        self,
        db: Session,
        offer_activities: Sequence[OfferActivity],
        manual_rows: Sequence[ClientManualActivity],
    ) -> List[str]:
        names: List[str] = []
        seen: set[str] = set()
        offer_ids = [row.offer_id for row in offer_activities if row.offer_id]
        if offer_ids:
            offers = db.query(Offer).filter(Offer.id.in_(offer_ids)).all()
            for offer in offers:
                name = (offer.business_name or "").strip()
                if name and name.lower() not in seen:
                    seen.add(name.lower())
                    names.append(name)
                if len(names) >= 8:
                    return names
        client_ids = [row.client_id for row in manual_rows if row.client_id]
        if client_ids:
            clients = db.query(Client).filter(Client.id.in_(client_ids)).all()
            for client in clients:
                name = (client.business_name or "").strip()
                if name and name.lower() not in seen:
                    seen.add(name.lower())
                    names.append(name)
                if len(names) >= 8:
                    break
        return names

    def _apply_date_filters(
        self,
        query,
        column,
        created_after: Optional[str],
        created_before: Optional[str],
    ):
        start = self._parse_day(created_after)
        if start is not None:
            query = query.filter(column >= start)
        end = self._parse_day(created_before)
        if end is not None:
            query = query.filter(column < end + timedelta(days=1))
        return query

    def _parse_day(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d")
        except ValueError:
            return None

    def _offer_display(self, offer: Offer, activity: OfferActivity) -> str:
        utility = (
            (offer.utility_type_identifier or offer.utility_type or "Offer") or "Offer"
        ).strip() or "Offer"
        identifier = (offer.identifier or "").strip()
        display = f"{utility} {identifier}".strip() if identifier else utility
        if activity.activity_type == OfferActivityType.MANUAL_ACTIVITY.value:
            meta = self._parse_metadata(getattr(activity, "metadata_", None))
            note = str(meta.get("note") or "").strip()
            if note:
                display = f"{note} · {display}"
        return display

    def _parse_metadata(self, raw: object) -> dict:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _task_activity_type(self, action: Optional[str], new_value: Optional[str]) -> str:
        normalized = (action or "").strip().lower()
        match normalized:
            case "task_created":
                return "task_created"
            case "status_changed" if (new_value or "").strip().lower() == "completed":
                return "task_completed"
            case _:
                return "task_edited"

    def _task_display(self, hist: TaskHistory, task: Task) -> str:
        action = (hist.action or "").strip().lower()
        match action:
            case "status_changed":
                return f"status: {(hist.old_value or '—')} -> {(hist.new_value or '—')}"
            case "field_updated":
                field_name = (hist.field or "field").strip()
                return f"{field_name}: {(hist.old_value or '—')} -> {(hist.new_value or '—')}"
            case _:
                return task.title or f"Task #{task.id}"

    def _manual_display(self, row: ClientManualActivity) -> str:
        parts: List[str] = []
        note = (row.note or "").strip()
        if note:
            parts.append(note)
        type_bits: List[str] = []
        preset = (row.offer_type_preset or "").strip().lower()
        if preset and preset not in ("", "unspecified"):
            type_bits.append(preset.replace("_", " ").title())
        custom = (row.offer_type_custom or "").strip()
        if custom:
            type_bits.append(custom)
        if type_bits:
            parts.append(" · ".join(type_bits))
        return " · ".join(parts) if parts else (note or "—")


class _TestDataSnapshot:
    def __init__(
        self,
        offer_activities: List[OfferActivity],
        manual_rows: List[ClientManualActivity],
        strategy_item_count: int,
        autonomous_run_count: int,
        by_type: dict[str, int],
        sample_clients: List[str],
    ) -> None:
        self.offer_activities = offer_activities
        self.manual_rows = manual_rows
        self.strategy_item_count = strategy_item_count
        self.autonomous_run_count = autonomous_run_count
        self.by_type = by_type
        self.sample_clients = sample_clients


activity_report_service = ActivityReportService()
