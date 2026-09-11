"""Campaign HTTP routes. Registered from main.py to avoid circular imports."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Any, Optional

from database import get_db
from services.campaigns import (
    CampaignError,
    apply_unsubscribe,
    campaign_to_dict,
    create_campaign,
    delete_campaign,
    delete_suppression,
    get_campaign,
    list_campaigns,
    list_suppressions,
    pause_campaign,
    patch_campaign,
    replace_rows,
    resume_campaign,
    row_to_dict,
    set_human_only,
    start_campaign,
    fire_test_send,
    unsubscribe_confirm_html,
    unsubscribe_done_html,
    verify_unsubscribe_token,
)


class CampaignCreateBody(BaseModel):
    name: str
    sequence_type: str


class CampaignPatchBody(BaseModel):
    name: Optional[str] = None
    sequence_type: Optional[str] = None
    status: Optional[str] = None
    first_touch_subject: Optional[str] = None
    first_touch_html: Optional[str] = None
    merge_field_map: Optional[dict[str, str]] = None
    provenance_note: Optional[str] = None
    daily_cap: Optional[int] = None
    send_window_start: Optional[str] = None
    send_window_end: Optional[str] = None


class CampaignRowsBody(BaseModel):
    headers: list[str]
    rows: list[list[str]]
    column_map: dict[str, str]
    merge_json: Optional[dict[str, Any]] = None


class TestSendBody(BaseModel):
    to: str
    row_id: int


class HumanOnlyBody(BaseModel):
    human_only: bool
    reason: Optional[str] = None


def _actor(user_data: dict) -> Optional[str]:
    return (user_data.get("idinfo") or {}).get("email")


def _raise(exc: CampaignError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


def register_campaign_routes(app, get_current_user_with_db):
    @app.post("/api/autonomous/campaigns")
    def create(body: CampaignCreateBody, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = create_campaign(db, body.name, body.sequence_type, _actor(user_data))
        except CampaignError as exc:
            _raise(exc)
        return campaign_to_dict(campaign, db)

    @app.get("/api/autonomous/campaigns")
    def list_all(db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        return [campaign_to_dict(c, db) for c in list_campaigns(db)]

    @app.get("/api/autonomous/campaigns/unsubscribe")
    def unsubscribe_get(token: str):
        try:
            email, _campaign_id = verify_unsubscribe_token(token)
        except CampaignError as exc:
            _raise(exc)
        return HTMLResponse(unsubscribe_confirm_html(email, token), status_code=200)

    @app.post("/api/autonomous/campaigns/unsubscribe")
    def unsubscribe_post(request: Request, token: Optional[str] = None, db: Session = Depends(get_db)):
        value = token or request.query_params.get("token") or ""
        try:
            result = apply_unsubscribe(db, value)
        except CampaignError as exc:
            _raise(exc)
        return HTMLResponse(unsubscribe_done_html(result["email"]), status_code=200)

    @app.get("/api/autonomous/campaigns/suppressions")
    def suppressions_list(db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        return list_suppressions(db)

    @app.delete("/api/autonomous/campaigns/suppressions/{suppression_id}")
    def suppressions_delete(
        suppression_id: int,
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user_with_db),
    ):
        try:
            delete_suppression(db, suppression_id)
        except CampaignError as exc:
            _raise(exc)
        return {"ok": True}

    @app.get("/api/autonomous/campaigns/{campaign_id}")
    def get_one(
        campaign_id: int,
        limit: int = Query(100),
        offset: int = Query(0),
        db: Session = Depends(get_db),
        user_data: dict = Depends(get_current_user_with_db),
    ):
        try:
            campaign = get_campaign(db, campaign_id)
        except CampaignError as exc:
            _raise(exc)
        return campaign_to_dict(campaign, db, include_rows=True, limit=limit, offset=offset)

    @app.patch("/api/autonomous/campaigns/{campaign_id}")
    def patch(campaign_id: int, body: CampaignPatchBody, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = get_campaign(db, campaign_id)
            campaign = patch_campaign(db, campaign, body.model_dump(exclude_unset=True), _actor(user_data))
        except CampaignError as exc:
            _raise(exc)
        return campaign_to_dict(campaign, db)

    @app.delete("/api/autonomous/campaigns/{campaign_id}")
    def delete(campaign_id: int, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = get_campaign(db, campaign_id)
            delete_campaign(db, campaign)
        except CampaignError as exc:
            _raise(exc)
        return {"ok": True}

    @app.post("/api/autonomous/campaigns/{campaign_id}/rows")
    def post_rows(campaign_id: int, body: CampaignRowsBody, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = get_campaign(db, campaign_id)
            summary = replace_rows(db, campaign, body.headers, body.rows, body.column_map, body.merge_json)
        except CampaignError as exc:
            _raise(exc)
        return summary

    @app.post("/api/autonomous/campaigns/{campaign_id}/rows/{row_id}/human-only")
    def human_only(campaign_id: int, row_id: int, body: HumanOnlyBody, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = get_campaign(db, campaign_id)
            row = set_human_only(db, campaign, row_id, body.human_only, body.reason)
        except CampaignError as exc:
            _raise(exc)
        return row_to_dict(row)

    @app.post("/api/autonomous/campaigns/{campaign_id}/test-send")
    def post_test_send(campaign_id: int, body: TestSendBody, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = get_campaign(db, campaign_id)
            return fire_test_send(db, campaign, body.to, body.row_id, _actor(user_data))
        except CampaignError as exc:
            _raise(exc)

    @app.post("/api/autonomous/campaigns/{campaign_id}/start")
    def post_start(campaign_id: int, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = get_campaign(db, campaign_id)
            return start_campaign(db, campaign, _actor(user_data))
        except CampaignError as exc:
            _raise(exc)

    @app.post("/api/autonomous/campaigns/{campaign_id}/pause")
    def post_pause(campaign_id: int, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = get_campaign(db, campaign_id)
            campaign = pause_campaign(db, campaign)
        except CampaignError as exc:
            _raise(exc)
        return campaign_to_dict(campaign, db)

    @app.post("/api/autonomous/campaigns/{campaign_id}/resume")
    def post_resume(campaign_id: int, db: Session = Depends(get_db), user_data: dict = Depends(get_current_user_with_db)):
        try:
            campaign = get_campaign(db, campaign_id)
            campaign = resume_campaign(db, campaign)
        except CampaignError as exc:
            _raise(exc)
        return campaign_to_dict(campaign, db)
