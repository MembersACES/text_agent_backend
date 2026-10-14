"""Utility checklist HTTP routes. Registered from main.py."""

from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database import get_db
from services.utility_checklists import (
    ChecklistError,
    ChecklistWrite,
    list_checklists,
    save_checklist,
)


class UtilityChecklistUpsert(BaseModel):
    utility_type: str
    identifier: str
    status: Literal["draft", "complete"]
    answers: dict[str, Any]
    template_version: int = Field(ge=1)
    row_version: Optional[int] = Field(default=None, ge=1)


def register_utility_checklist_routes(app, get_current_user_with_db):
    @app.get("/api/clients/{client_id}/utility-checklists")
    def get_utility_checklists(
        client_id: int,
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user_with_db),
    ):
        del user_data
        try:
            return list_checklists(db, client_id)
        except ChecklistError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from None

    @app.put("/api/clients/{client_id}/utility-checklists")
    def put_utility_checklist(
        client_id: int,
        body: UtilityChecklistUpsert,
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user_with_db),
    ):
        email = (user_data.get("idinfo") or {}).get("email")
        try:
            return save_checklist(
                db,
                client_id,
                ChecklistWrite(
                    utility_type=body.utility_type,
                    identifier=body.identifier,
                    status=body.status,
                    answers=body.answers,
                    template_version=body.template_version,
                    row_version=body.row_version,
                ),
                email,
            )
        except ChecklistError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from None
