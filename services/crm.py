from datetime import datetime
from typing import Optional
import json

from fastapi import HTTPException
from sqlalchemy.orm import Session

from crm_enums import (
    ClientStage,
    OfferStatus,
    OfferActivityType,
    OfferPipelineStage,
    POST_WIN_STAGES,
)
from models import Client, Offer, ClientStatusNote, OfferActivity, StrategyItem


def upsert_client_from_business_info(
    db: Session,
    business_name: str,
    external_business_id: Optional[str],
    primary_contact_email: Optional[str],
    gdrive_folder_url: Optional[str],
) -> Optional[Client]:
    """
    Upsert a Client using data from LOA/Airtable.

    Prefer matching by external_business_id when available, and fall back to a
    constrained business_name + external_business_id combination to reduce collisions.
    """
    if not business_name:
        return None

    client: Optional[Client] = None

    if external_business_id:
        client = (
            db.query(Client)
            .filter(Client.external_business_id == external_business_id)
            .first()
        )

    if client is None:
        query = db.query(Client).filter(Client.business_name == business_name)
        if external_business_id:
            query = query.filter(Client.external_business_id == external_business_id)
        client = query.first()

    if client:
        client.external_business_id = external_business_id or client.external_business_id
        client.primary_contact_email = (
            primary_contact_email or client.primary_contact_email
        )
        client.gdrive_folder_url = gdrive_folder_url or client.gdrive_folder_url
    else:
        client = Client(
            business_name=business_name,
            external_business_id=external_business_id,
            primary_contact_email=primary_contact_email,
            gdrive_folder_url=gdrive_folder_url,
        )
        db.add(client)

    db.commit()
    db.refresh(client)
    return client


def update_client_stage_with_history(
    db: Session,
    client_id: int,
    new_stage: ClientStage,
    user_email: str,
    *,
    commit: bool = True,
) -> Client:
    """
    Update a client's stage and append a structured ClientStatusNote entry.

    This keeps stage history in one place. In the future we may need to consider
    multiple offers per client or a 'primary' offer when deciding which changes
    should drive stage transitions.
    """
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    old_stage = client.stage or ClientStage.LEAD.value
    new_stage_value = new_stage.value if isinstance(new_stage, ClientStage) else str(
        new_stage
    )

    if new_stage_value != old_stage:
        client.stage = new_stage_value
        client.stage_changed_at = datetime.utcnow()

        note_text = f"Stage changed from '{old_stage}' to '{new_stage_value}'"
        stage_note = ClientStatusNote(
            business_name=client.business_name,
            client_id=client.id,
            note=note_text,
            user_email=user_email,
            note_type="status_update",
        )
        db.add(stage_note)

    if commit:
        db.commit()
        db.refresh(client)
    return client


def update_offer_status_and_propagate_client_stage(
    db: Session,
    offer_id: int,
    new_status: OfferStatus,
) -> Offer:
    """
    Update an offer's status and, when appropriate, propagate to the linked client's stage.

    - accepted → client.stage = WON (unless client is already in a post-win stage)
    - lost → client.stage = LOST

    Clients can receive multiple offers after they're already onboarded; we don't
    downgrade or reshuffle their lifecycle stage when an existing client gets
    another accepted offer.
    """
    offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")

    status_value = new_status.value if isinstance(new_status, OfferStatus) else str(
        new_status
    )
    offer.status = status_value

    # Keep pipeline stage aligned with coarse status for terminal outcomes.
    if new_status == OfferStatus.ACCEPTED:
        offer.pipeline_stage = OfferPipelineStage.CONTRACT_ACCEPTED.value
    elif new_status == OfferStatus.LOST:
        offer.pipeline_stage = OfferPipelineStage.LOST.value

    if offer.client_id:
        client = db.query(Client).filter(Client.id == offer.client_id).first()
        if client:
            if new_status == OfferStatus.ACCEPTED:
                current_stage_value = client.stage or ClientStage.LEAD.value
                # Only move to Won if not already in a post-win stage (Won / ExistingClient).
                if current_stage_value not in (s.value for s in POST_WIN_STAGES):
                    client.stage = ClientStage.WON.value
                    client.stage_changed_at = datetime.utcnow()
            elif new_status == OfferStatus.LOST:
                client.stage = ClientStage.LOST.value
                client.stage_changed_at = datetime.utcnow()

    # Keep Strategy & WIP status in sync with offer status.
    sync_strategy_status_from_offer(db, offer)

    db.commit()
    db.refresh(offer)
    return offer


def create_offer_activity(
    db: Session,
    *,
    offer: Offer,
    client: Optional[Client] = None,
    activity_type: OfferActivityType,
    document_link: Optional[str] = None,
    external_id: Optional[str] = None,
    metadata: Optional[dict] = None,
    created_by: Optional[str] = None,
) -> OfferActivity:
    """
    Create a structured activity record for an offer. Used by quote requests,
    Base 2 reviews, comparisons, GHG offers, engagement forms, discrepancy emails, etc.
    When Base 2 is exposed via API: after a successful run call with activity_type=BASE2_REVIEW,
    external_id=run_id, document_link if a doc is generated, metadata with utility_type.
    """
    meta_str = None
    if metadata is not None:
        try:
            meta_str = json.dumps(metadata)
        except (TypeError, ValueError):
            meta_str = None

    activity = OfferActivity(
        offer_id=offer.id,
        client_id=client.id if client else offer.client_id,
        activity_type=activity_type.value if isinstance(activity_type, OfferActivityType) else str(activity_type),
        document_link=document_link,
        external_id=external_id,
        created_by=created_by,
    )
    activity.metadata_ = meta_str
    db.add(activity)

    # Optional: Base 2 / DMA / comparison metadata may include annual_savings, current_cost, new_cost.
    # Update the offer so CRM and Strategy WIP show them; other webhooks don't need to send these.
    if metadata:
        try:
            if "annual_savings" in metadata:
                v = metadata["annual_savings"]
                if v is not None and v != "":
                    offer.annual_savings = float(v) if not isinstance(v, (int, float)) else float(v)
            if "current_cost" in metadata:
                v = metadata["current_cost"]
                if v is not None and v != "":
                    offer.current_cost = float(v) if not isinstance(v, (int, float)) else float(v)
            if "new_cost" in metadata:
                v = metadata["new_cost"]
                if v is not None and v != "":
                    offer.new_cost = float(v) if not isinstance(v, (int, float)) else float(v)
        except (TypeError, ValueError):
            pass

    # Advance offer.pipeline_stage based on key structured activity events.
    # We only move forwards along the defined order; we never move backwards.
    ACTIVITY_TO_STAGE = {
        OfferActivityType.COMPARISON: OfferPipelineStage.COMPARISON_SENT,
        OfferActivityType.ENGAGEMENT_FORM: OfferPipelineStage.ENGAGEMENT_FORM_SENT,
        OfferActivityType.ENGAGEMENT_FORM_SIGNED: OfferPipelineStage.ENGAGEMENT_FORM_SIGNED,
        OfferActivityType.CONTRACT_REQUESTED: OfferPipelineStage.CONTRACT_REQUESTED,
        OfferActivityType.CONTRACT_RECEIVED: OfferPipelineStage.CONTRACT_RECEIVED,
        OfferActivityType.CONTRACT_SENT_FOR_SIGNING: OfferPipelineStage.CONTRACT_SENT_FOR_SIGNING,
        OfferActivityType.CONTRACT_SIGNED_LODGED: OfferPipelineStage.CONTRACT_SIGNED_LODGED,
    }

    PIPELINE_ORDER = {
        OfferPipelineStage.COMPARISON_SENT: 10,
        OfferPipelineStage.ENGAGEMENT_FORM_SENT: 20,
        OfferPipelineStage.ENGAGEMENT_FORM_SIGNED: 30,
        OfferPipelineStage.CONTRACT_REQUESTED: 40,
        OfferPipelineStage.CONTRACT_RECEIVED: 50,
        OfferPipelineStage.CONTRACT_SENT_FOR_SIGNING: 60,
        OfferPipelineStage.CONTRACT_SIGNED_LODGED: 70,
        OfferPipelineStage.CONTRACT_ACCEPTED: 80,
        OfferPipelineStage.LOST: 90,
    }

    target_stage = ACTIVITY_TO_STAGE.get(activity_type)
    if target_stage is not None:
        current_raw = offer.pipeline_stage or ""
        try:
            current_stage = (
                OfferPipelineStage(current_raw)
                if current_raw
                else None
            )
        except ValueError:
            current_stage = None

        current_order = PIPELINE_ORDER.get(current_stage, 0)
        target_order = PIPELINE_ORDER.get(target_stage, 0)
        if target_order > current_order:
            offer.pipeline_stage = target_stage.value

    # Auto-update offer.status based on key activity events, keeping client
    # lifecycle and coarse status aligned with the offer micro-pipeline.
    #
    # - When we send a proposal/comparison/engagement form, move from
    #   requested → awaiting_response.
    # - When the engagement form is signed or a contract is lodged, treat the
    #   offer as accepted (unless it's already accepted/lost) and reuse the
    #   existing propagation logic so the linked client moves to WON.
    current_status_raw = offer.status or OfferStatus.REQUESTED.value
    try:
        current_status = OfferStatus(current_status_raw)
    except ValueError:
        current_status = None

    # 1) Proposal sent → awaiting_response (only from requested).
    PROPOSAL_SENT_TYPES = {
        OfferActivityType.COMPARISON,
        OfferActivityType.GHG_OFFER,
        OfferActivityType.ENGAGEMENT_FORM,
    }
    if (
        activity_type in PROPOSAL_SENT_TYPES
        and current_status == OfferStatus.REQUESTED
    ):
        offer.status = OfferStatus.AWAITING_RESPONSE.value
        current_status = OfferStatus.AWAITING_RESPONSE

    # 2) Signed artefacts → accepted (unless already accepted/lost).
    ACCEPTING_ACTIVITY_TYPES = {
        OfferActivityType.ENGAGEMENT_FORM_SIGNED,
        OfferActivityType.CONTRACT_SIGNED_LODGED,
    }
    if current_status not in (OfferStatus.ACCEPTED, OfferStatus.LOST):
        # If pipeline has already been manually advanced to CONTRACT_ACCEPTED
        # we also treat the offer as accepted.
        pipeline_raw = offer.pipeline_stage or ""
        try:
            pipeline_stage = (
                OfferPipelineStage(pipeline_raw) if pipeline_raw else None
            )
        except ValueError:
            pipeline_stage = None

        should_accept = activity_type in ACCEPTING_ACTIVITY_TYPES or (
            pipeline_stage == OfferPipelineStage.CONTRACT_ACCEPTED
        )

        if should_accept:
            # Reuse centralised propagation so client.stage and terminal
            # pipeline state stay consistent across all entry points.
            update_offer_status_and_propagate_client_stage(
                db=db,
                offer_id=offer.id,
                new_status=OfferStatus.ACCEPTED,
            )
            # Refresh the in-memory offer so callers see the latest values.
            db.refresh(offer)

    # Auto-add a Strategy & WIP row for this activity (same commit as activity).
    upsert_strategy_item_for_activity(
        db,
        offer=offer,
        activity=activity,
        client=client,
        metadata=metadata,
    )

    db.commit()
    db.refresh(activity)
    return activity


def get_or_create_offer_for_activity(
    db: Session,
    client_id: int,
    business_name: str,
    utility_type: str,
    created_by: Optional[str] = None,
    utility_type_identifier: Optional[str] = None,
    identifier: Optional[str] = None,
) -> Offer:
    """
    Find an existing offer for this client + utility type, or create a minimal one.
    Used when recording activities (e.g. GHG offer, engagement form) that may not
    have been created from a quote request.
    """
    offer = (
        db.query(Offer)
        .filter(
            Offer.client_id == client_id,
            Offer.utility_type == utility_type,
            Offer.business_name == business_name,
        )
        .order_by(Offer.created_at.desc())
        .first()
    )
    if offer:
        return offer
    offer = Offer(
        client_id=client_id,
        business_name=business_name,
        utility_type=utility_type,
        utility_type_identifier=utility_type_identifier or None,
        identifier=identifier or None,
        status=OfferStatus.REQUESTED.value,
        created_by=created_by,
    )
    db.add(offer)
    db.commit()
    db.refresh(offer)
    return offer


# --- Strategy & WIP auto-sync from CRM events ---

# Activity types that get a row in Strategy & WIP when they are created.
ACTIVITY_TYPES_FOR_STRATEGY = {
    OfferActivityType.ENGAGEMENT_FORM,
    OfferActivityType.COMPARISON,
    OfferActivityType.DMA_REVIEW_GENERATED,
    OfferActivityType.DMA_EMAIL_SENT,
    OfferActivityType.BASE2_REVIEW,
    OfferActivityType.GHG_OFFER,
    OfferActivityType.ONE_MONTH_SAVINGS_INVOICE,
    OfferActivityType.SOLUTION_PRESENTATION,
    OfferActivityType.ENGAGEMENT_FORM_SIGNED,
    OfferActivityType.CONTRACT_SIGNED_LODGED,
    OfferActivityType.CONTRACT_REQUESTED,
    OfferActivityType.CONTRACT_RECEIVED,
    OfferActivityType.CONTRACT_SENT_FOR_SIGNING,
    OfferActivityType.DISCREPANCY_EMAIL_SENT,
    OfferActivityType.EOI,
    OfferActivityType.LOA,
    OfferActivityType.SERVICE_AGREEMENT,
}

# Human-readable label for each activity type (for key_results / member display).
ACTIVITY_TYPE_LABELS = {
    OfferActivityType.ENGAGEMENT_FORM: "Engagement form generated",
    OfferActivityType.COMPARISON: "Comparison sent",
    OfferActivityType.DMA_REVIEW_GENERATED: "DMA review generated",
    OfferActivityType.DMA_EMAIL_SENT: "DMA email sent",
    OfferActivityType.BASE2_REVIEW: "Base 2 review",
    OfferActivityType.GHG_OFFER: "GHG offer",
    OfferActivityType.ONE_MONTH_SAVINGS_INVOICE: "1st Month Savings invoice",
    OfferActivityType.SOLUTION_PRESENTATION: "Solution presentation",
    OfferActivityType.ENGAGEMENT_FORM_SIGNED: "Engagement form signed",
    OfferActivityType.CONTRACT_SIGNED_LODGED: "Contract signed & lodged",
    OfferActivityType.CONTRACT_REQUESTED: "Contract requested",
    OfferActivityType.CONTRACT_RECEIVED: "Contract received",
    OfferActivityType.CONTRACT_SENT_FOR_SIGNING: "Contract sent for signing",
    OfferActivityType.DISCREPANCY_EMAIL_SENT: "Discrepancy email sent",
    OfferActivityType.EOI: "EOI generated",
    OfferActivityType.LOA: "LOA generated",
    OfferActivityType.SERVICE_AGREEMENT: "Service agreement",
}

# Map offer status to Strategy & WIP status text.
OFFER_STATUS_TO_STRATEGY_STATUS = {
    OfferStatus.REQUESTED: "Requested",
    OfferStatus.AWAITING_RESPONSE: "Awaiting response",
    OfferStatus.RESPONSE_RECEIVED: "Response received",
    OfferStatus.AUTONOMOUS_AGENT_TRIGGER: "Autonomous agent trigger",
    OfferStatus.AUTONOMOUS_AGENT_STOPPED: "Autonomous agent stopped",
    OfferStatus.ACCEPTED: "Accepted",
    OfferStatus.LOST: "Lost",
}


def _strategy_offer_label(offer: Offer, activity_type: OfferActivityType, metadata: Optional[dict]) -> str:
    """Build a short 'Member level / solution' label from offer + activity + metadata."""
    parts = []
    if offer.utility_type_identifier or offer.utility_type:
        parts.append((offer.utility_type_identifier or offer.utility_type or "").strip())
    if offer.identifier:
        parts.append(str(offer.identifier).strip())
    if metadata:
        comp = (metadata.get("comparison_type") or metadata.get("utility_type") or "").strip()
        if comp and (not parts or comp not in " ".join(parts)):
            parts.append(comp)
    label = " ".join(p for p in parts if p).strip()
    if not label:
        label = ACTIVITY_TYPE_LABELS.get(activity_type, activity_type.value)
    return label[:255] if label else "Offer activity"


def upsert_strategy_item_for_activity(
    db: Session,
    *,
    offer: Offer,
    activity: OfferActivity,
    client: Optional[Client] = None,
    metadata: Optional[dict] = None,
) -> Optional[StrategyItem]:
    """
    When a key activity is created for an offer, add a row in Strategy & WIP
    so the member's Strategy tab reflects it. Does not commit; caller must commit.
    """
    activity_type_val = getattr(activity, "activity_type", None) or ""
    try:
        at_enum = OfferActivityType(activity_type_val)
    except ValueError:
        at_enum = None
    if at_enum not in ACTIVITY_TYPES_FOR_STRATEGY:
        return None

    client_id = offer.client_id or (client.id if client else None)
    if not client_id:
        return None

    year = datetime.utcnow().year
    section = "in_progress"
    key_results = ACTIVITY_TYPE_LABELS.get(at_enum, activity_type_val.replace("_", " ").title())
    member_label = _strategy_offer_label(offer, at_enum, metadata or {})

    status_label = OFFER_STATUS_TO_STRATEGY_STATUS.get(
        OfferStatus(offer.status) if offer.status else OfferStatus.REQUESTED
    ) or (offer.status or "Requested")

    row_index = (
        db.query(StrategyItem)
        .filter(
            StrategyItem.client_id == client_id,
            StrategyItem.year == year,
            StrategyItem.section == section,
        )
        .count()
    )

    # Prefer annual_savings from metadata (e.g. DMA) or offer; fall back to estimated_value
    est_saving = None
    if metadata and "annual_savings" in metadata:
        try:
            v = metadata["annual_savings"]
            if v is not None and v != "":
                est_saving = float(v) if not isinstance(v, (int, float)) else float(v)
        except (TypeError, ValueError):
            pass
    if est_saving is None and getattr(offer, "annual_savings", None) is not None:
        try:
            est_saving = float(offer.annual_savings)
        except (TypeError, ValueError):
            pass
    if est_saving is None and offer.estimated_value is not None:
        try:
            est_saving = float(offer.estimated_value)
        except (TypeError, ValueError):
            pass

    item = StrategyItem(
        client_id=client_id,
        year=year,
        section=section,
        row_index=row_index,
        member_level_solutions=member_label,
        details="",
        key_results=key_results,
        offer_id=offer.id,
        offer_activity_id=activity.id,
        activity_type=activity_type_val,
        status=status_label,
        priority="High",
        est_saving_pa=est_saving,
        est_revenue_pa=None,
    )
    db.add(item)
    return item


def sync_strategy_status_from_offer(db: Session, offer: Offer) -> None:
    """
    When an offer's status (or savings) changes, update the status and est_saving_pa
    of all Strategy & WIP rows linked to that offer. Does not commit; caller must commit.
    """
    if not offer.id:
        return
    status_label = OFFER_STATUS_TO_STRATEGY_STATUS.get(
        OfferStatus(offer.status) if offer.status else OfferStatus.REQUESTED
    ) or (offer.status or "Requested")
    items = db.query(StrategyItem).filter(StrategyItem.offer_id == offer.id).all()
    est_saving = None
    if getattr(offer, "annual_savings", None) is not None:
        try:
            est_saving = float(offer.annual_savings)
        except (TypeError, ValueError):
            pass
    if est_saving is None and offer.estimated_value is not None:
        try:
            est_saving = float(offer.estimated_value)
        except (TypeError, ValueError):
            pass
    for item in items:
        item.status = status_label
        if est_saving is not None:
            item.est_saving_pa = est_saving


def _parse_activity_metadata(metadata_raw) -> Optional[dict]:
    """Parse activity metadata from DB (JSON string or dict)."""
    if metadata_raw is None:
        return None
    if isinstance(metadata_raw, dict):
        return metadata_raw
    if isinstance(metadata_raw, str):
        try:
            return json.loads(metadata_raw)
        except (TypeError, json.JSONDecodeError):
            return None
    return None


def sync_strategy_items_from_crm(
    db: Session,
    client_id: int,
    year: Optional[int] = None,
) -> int:
    """
    Backfill Strategy & WIP from existing offers and activities for this client.
    Creates a strategy row for each relevant activity that does not already have one.
    Returns the number of new rows created. Caller should commit.
    """
    if year is None:
        year = datetime.utcnow().year

    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        return 0

    offers = db.query(Offer).filter(Offer.client_id == client_id).all()
    activity_type_values = {t.value for t in ACTIVITY_TYPES_FOR_STRATEGY}
    created = 0

    for offer in offers:
        activities = (
            db.query(OfferActivity)
            .filter(
                OfferActivity.offer_id == offer.id,
                OfferActivity.activity_type.in_(activity_type_values),
            )
            .order_by(OfferActivity.created_at.asc())
            .all()
        )
        for activity in activities:
            existing = (
                db.query(StrategyItem)
                .filter(StrategyItem.offer_activity_id == activity.id)
                .first()
            )
            if existing:
                continue
            meta = _parse_activity_metadata(getattr(activity, "metadata_", None))
            try:
                at_enum = OfferActivityType(activity.activity_type)
            except ValueError:
                at_enum = None
            if at_enum not in ACTIVITY_TYPES_FOR_STRATEGY:
                continue
            key_results = ACTIVITY_TYPE_LABELS.get(
                at_enum, (activity.activity_type or "").replace("_", " ").title()
            )
            member_label = _strategy_offer_label(offer, at_enum, meta)
            status_label = OFFER_STATUS_TO_STRATEGY_STATUS.get(
                OfferStatus(offer.status) if offer.status else OfferStatus.REQUESTED
            ) or (offer.status or "Requested")
            row_index = (
                db.query(StrategyItem)
                .filter(
                    StrategyItem.client_id == client_id,
                    StrategyItem.year == year,
                    StrategyItem.section == "in_progress",
                )
                .count()
            )
            est_saving = None
            if getattr(offer, "annual_savings", None) is not None:
                try:
                    est_saving = float(offer.annual_savings)
                except (TypeError, ValueError):
                    pass
            if est_saving is None and offer.estimated_value is not None:
                try:
                    est_saving = float(offer.estimated_value)
                except (TypeError, ValueError):
                    pass
            item = StrategyItem(
                client_id=client_id,
                year=year,
                section="in_progress",
                row_index=row_index,
                member_level_solutions=member_label,
                details="",
                key_results=key_results,
                offer_id=offer.id,
                offer_activity_id=activity.id,
                activity_type=activity.activity_type,
                status=status_label,
                priority="High",
                est_saving_pa=est_saving,
                est_revenue_pa=None,
            )
            db.add(item)
            created += 1

    return created

