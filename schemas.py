"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, field_serializer, field_validator
from typing import Optional, List, Any
from datetime import datetime
import json
from utils.timezone import to_melbourne_iso
from crm_enums import ClientStage, OfferStatus, OfferActivityType, OfferPipelineStage


class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None
    due_date: Optional[datetime] = None
    status: str = "not_started"
    assigned_to: Optional[str] = None
    assigned_by: Optional[str] = None
    business_id: Optional[int] = None
    client_id: Optional[int] = None
    category: Optional[str] = "task"


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    due_date: Optional[datetime] = None
    status: Optional[str] = None
    assigned_to: Optional[str] = None
    assigned_by: Optional[str] = None
    business_id: Optional[int] = None
    client_id: Optional[int] = None
    category: Optional[str] = None


class TaskStatusUpdate(BaseModel):
    status: str


class TaskResponse(BaseModel):
    id: int
    title: str
    description: Optional[str] = None
    due_date: Optional[datetime] = None
    status: str
    assigned_to: Optional[str] = None
    assigned_by: Optional[str] = None
    business_id: Optional[int] = None
    client_id: Optional[int] = None
    category: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    last_notification_sent_at: Optional[datetime] = None

    @field_serializer('created_at', 'updated_at', 'due_date', 'last_notification_sent_at')
    def serialize_datetime(self, dt: Optional[datetime], _info):
        """Convert UTC datetime to Melbourne timezone before serialization"""
        if dt is None:
            return None
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True


class UserResponse(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    picture: Optional[str] = None
    created_at: datetime

    @field_serializer('created_at')
    def serialize_datetime(self, dt: datetime, _info):
        """Convert UTC datetime to Melbourne timezone before serialization"""
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True

class ClientStatusNoteCreate(BaseModel):
    business_name: str
    note: str
    client_id: Optional[int] = None
    note_type: Optional[str] = "general"
    related_task_id: Optional[int] = None
    related_offer_id: Optional[int] = None


class ClientStatusNoteUpdate(BaseModel):
    note: str
    note_type: Optional[str] = None


class ClientStatusNoteResponse(BaseModel):
    id: int
    business_name: str
    client_id: Optional[int] = None
    note: str
    user_email: str
    note_type: str
    related_task_id: Optional[int] = None
    related_offer_id: Optional[int] = None
    created_at: datetime
    updated_at: Optional[datetime] = None  # May be None for legacy/migrated rows

    @field_serializer('created_at', 'updated_at')
    def serialize_datetime(self, dt: Optional[datetime], _info):
        """Convert UTC datetime to Melbourne timezone before serialization"""
        if dt is None:
            return None
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True


class ClientCreate(BaseModel):
    business_name: str
    external_business_id: Optional[str] = None
    primary_contact_email: Optional[str] = None
    gdrive_folder_url: Optional[str] = None
    stage: Optional[ClientStage] = ClientStage.LEAD
    owner_email: Optional[str] = None

    @field_validator("stage", mode="before")
    @classmethod
    def _normalize_stage(cls, v: Optional[str]) -> Optional[ClientStage]:
        """
        Accept legacy granular stages and normalise them into the coarse lifecycle.

        This keeps API compatibility for existing data while moving towards a simpler
        relationship lifecycle model.
        """
        if v is None:
            return v
        if isinstance(v, ClientStage):
            return v
        raw = str(v).strip().lower()
        if not raw:
            return None
        # Legacy granular stages all map to QUALIFIED.
        if raw in {
            "loa_signed",
            "data_collected",
            "analysis_in_progress",
            "offer_sent",
        }:
            return ClientStage.QUALIFIED
        # Pass through known coarse stages; Pydantic will validate.
        return ClientStage(raw)


class ClientUpdate(BaseModel):
    business_name: Optional[str] = None
    external_business_id: Optional[str] = None
    primary_contact_email: Optional[str] = None
    gdrive_folder_url: Optional[str] = None
    stage: Optional[ClientStage] = None
    owner_email: Optional[str] = None

    @field_validator("stage", mode="before")
    @classmethod
    def _normalize_stage(cls, v: Optional[str]) -> Optional[ClientStage]:
        if v is None:
            return v
        if isinstance(v, ClientStage):
            return v
        raw = str(v).strip().lower()
        if not raw:
            return None
        if raw in {
            "loa_signed",
            "data_collected",
            "analysis_in_progress",
            "offer_sent",
        }:
            return ClientStage.QUALIFIED
        return ClientStage(raw)


class ClientResponse(BaseModel):
    id: int
    business_name: str
    external_business_id: Optional[str] = None
    primary_contact_email: Optional[str] = None
    gdrive_folder_url: Optional[str] = None
    stage: ClientStage
    stage_changed_at: Optional[datetime] = None
    owner_email: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at", "updated_at", "stage_changed_at")
    def serialize_datetime(self, dt: Optional[datetime], _info):
        if dt is None:
            return None
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True

    @field_validator("stage", mode="before")
    @classmethod
    def _normalize_stage(cls, v: Optional[str]) -> ClientStage:
        """
        Normalise legacy granular stages when reading from ORM.
        """
        if isinstance(v, ClientStage):
            return v
        raw = str(v or "").strip().lower()
        if not raw:
            return ClientStage.LEAD
        if raw in {
            "loa_signed",
            "data_collected",
            "analysis_in_progress",
            "offer_sent",
        }:
            return ClientStage.QUALIFIED
        try:
            return ClientStage(raw)
        except ValueError:
            # Fallback for truly unexpected historic values.
            return ClientStage.LEAD


class OfferCreate(BaseModel):
    client_id: Optional[int] = None
    business_name: Optional[str] = None
    utility_type: Optional[str] = None
    utility_type_identifier: Optional[str] = None
    identifier: Optional[str] = None
    status: Optional[OfferStatus] = OfferStatus.REQUESTED
    # Optional explicit pipeline stage when creating offers manually.
    pipeline_stage: Optional[OfferPipelineStage] = None
    estimated_value: Optional[int] = None
    external_record_id: Optional[str] = None
    document_link: Optional[str] = None


class OfferUpdate(BaseModel):
    client_id: Optional[int] = None
    business_name: Optional[str] = None
    utility_type: Optional[str] = None
    utility_type_identifier: Optional[str] = None
    identifier: Optional[str] = None
    status: Optional[OfferStatus] = None
    pipeline_stage: Optional[OfferPipelineStage] = None
    estimated_value: Optional[int] = None
    external_record_id: Optional[str] = None
    document_link: Optional[str] = None


class OfferResponse(BaseModel):
    id: int
    client_id: Optional[int] = None
    business_name: Optional[str] = None
    utility_type: Optional[str] = None
    utility_type_identifier: Optional[str] = None
    identifier: Optional[str] = None
    # Display: "Base 2 Gas", "DMA Electricity", "Comparison Gas" (source + utility) for Utility column
    utility_display: Optional[str] = None
    status: OfferStatus
    pipeline_stage: Optional[OfferPipelineStage] = None
    estimated_value: Optional[int] = None
    created_by: Optional[str] = None
    external_record_id: Optional[str] = None
    document_link: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    # Read-only: true when the linked client is already in Won or ExistingClient.
    is_existing_client: bool = False

    @field_serializer("created_at", "updated_at")
    def serialize_datetime(self, dt: datetime, _info):
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True


# --- OfferActivity (structured artefacts on offers) ---

class OfferActivityCreate(BaseModel):
    """Body for creating an offer activity. offer_id comes from path."""
    activity_type: OfferActivityType
    document_link: Optional[str] = None
    external_id: Optional[str] = None
    metadata: Optional[dict] = None  # JSON; stored as text in DB
    created_by: Optional[str] = None


def _parse_metadata(v: Any) -> Optional[dict]:
    """Parse metadata from DB: may be stored as JSON string."""
    if v is None:
        return None
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (TypeError, json.JSONDecodeError):
            return None
    return None


class OfferActivityResponse(BaseModel):
    id: int
    offer_id: int
    client_id: Optional[int] = None
    activity_type: str
    document_link: Optional[str] = None
    external_id: Optional[str] = None
    metadata: Optional[dict] = None
    created_at: datetime
    created_by: Optional[str] = None

    @field_serializer("created_at")
    def serialize_datetime(self, dt: datetime, _info):
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True

    # When loading from ORM, model has metadata_ (column "metadata") - map and parse it
    @classmethod
    def model_validate(cls, obj: Any, **kwargs):
        if hasattr(obj, "metadata_"):
            meta = _parse_metadata(getattr(obj, "metadata_", None))
        else:
            meta = getattr(obj, "metadata", None)
        data = {
            "id": obj.id,
            "offer_id": obj.offer_id,
            "client_id": obj.client_id,
            "activity_type": obj.activity_type,
            "document_link": obj.document_link,
            "external_id": obj.external_id,
            "metadata": meta,
            "created_at": obj.created_at,
            "created_by": obj.created_by,
        }
        return super().model_validate(data, **kwargs)


class ActivityReportItem(BaseModel):
    """Single row for activity report list (with business_name from offer)."""
    id: int
    offer_id: int
    client_id: Optional[int] = None
    business_name: Optional[str] = None
    activity_type: str
    document_link: Optional[str] = None
    created_at: datetime
    created_by: Optional[str] = None
    # Full label "Base 2 Gas 5321568754" (source + utility + identifier) for Offer column
    offer_display: Optional[str] = None

    @field_serializer("created_at")
    def serialize_created_at(self, dt: datetime, _info):
        return to_melbourne_iso(dt)