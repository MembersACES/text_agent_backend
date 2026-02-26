from datetime import datetime
from typing import Optional
import json

from fastapi import HTTPException
from sqlalchemy.orm import Session

from crm_enums import ClientStage, OfferStatus, OfferActivityType, POST_WIN_STAGES
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

