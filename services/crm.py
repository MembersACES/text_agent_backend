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
from models import Client, Offer, ClientStatusNote, OfferActivity


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

