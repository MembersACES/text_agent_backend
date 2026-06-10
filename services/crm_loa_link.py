"""Read-only CRM ↔ LOA resolution and explicit link/create (no silent upsert on lookup)."""
from __future__ import annotations

from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from models import Client
from services.crm import enrich_client_response


def _candidate(client: Client) -> dict[str, Any]:
    return {
        "client_id": client.id,
        "business_name": client.business_name,
        "external_business_id": client.external_business_id,
        "stage": client.stage,
    }


def resolve_crm_client_for_loa(
    db: Session,
    record_id: Optional[str],
) -> dict[str, Any]:
    """
    ID-first read-only CRM link resolution. Never creates or updates clients.
    """
    rid = (record_id or "").strip()
    if not rid:
        return {
            "status": "ambiguous",
            "client_id": None,
            "record_id": None,
            "reason": "No LOA record ID in business info — cannot link automatically",
            "candidates": [],
        }

    rows = (
        db.query(Client)
        .filter(Client.external_business_id == rid)
        .order_by(Client.id.asc())
        .all()
    )

    if len(rows) == 1:
        return {
            "status": "matched",
            "client_id": rows[0].id,
            "record_id": rid,
            "reason": "CRM member linked by LOA record ID",
            "candidates": [],
        }

    if len(rows) > 1:
        return {
            "status": "conflict",
            "client_id": None,
            "record_id": rid,
            "reason": "Multiple CRM members share this LOA record ID",
            "candidates": [_candidate(c) for c in rows],
        }

    return {
        "status": "no_match",
        "client_id": None,
        "record_id": rid,
        "reason": "No CRM member linked to this LOA record ID yet",
        "candidates": [],
    }


def link_or_create_client_from_loa(
    db: Session,
    *,
    record_id: str,
    business_name: str,
    primary_contact_email: Optional[str] = None,
    gdrive_folder_url: Optional[str] = None,
    client_id: Optional[int] = None,
    owner_email: Optional[str] = None,
):
    """Explicit user-confirmed link to existing member or create new — ID-first."""
    rid = (record_id or "").strip()
    name = (business_name or "").strip()
    if not rid:
        raise HTTPException(status_code=400, detail="LOA record ID is required")
    if not name:
        raise HTTPException(status_code=400, detail="Business name is required")

    holder = (
        db.query(Client)
        .filter(Client.external_business_id == rid)
        .order_by(Client.id.asc())
        .all()
    )

    if client_id is not None:
        client = db.query(Client).filter(Client.id == client_id).first()
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        other_holders = [c for c in holder if c.id != client_id]
        if other_holders:
            raise HTTPException(
                status_code=400,
                detail=f"LOA record already linked to member #{other_holders[0].id}",
            )
        client.external_business_id = rid
        if primary_contact_email:
            client.primary_contact_email = primary_contact_email
        if gdrive_folder_url:
            client.gdrive_folder_url = gdrive_folder_url
        db.commit()
        db.refresh(client)
        return enrich_client_response(db, client)

    if holder:
        raise HTTPException(
            status_code=400,
            detail=f"LOA record already linked to member #{holder[0].id}",
        )

    existing_name = db.query(Client).filter(Client.business_name == name).first()
    if existing_name:
        raise HTTPException(
            status_code=400,
            detail="A CRM member with this business name already exists — link to that member instead",
        )

    client = Client(
        business_name=name,
        external_business_id=rid,
        primary_contact_email=primary_contact_email,
        gdrive_folder_url=gdrive_folder_url,
        owner_email=owner_email,
    )
    db.add(client)
    db.commit()
    db.refresh(client)
    return enrich_client_response(db, client)
