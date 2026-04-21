"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, field_serializer, field_validator
from typing import Optional, List, Any, Dict
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
    referred_by_client_id: Optional[int] = None
    referred_by_business_name: Optional[str] = None
    referred_by_active: Optional[bool] = None
    advocacy_meeting_date: Optional[str] = None
    advocacy_meeting_time: Optional[str] = None
    advocacy_meeting_completed: Optional[bool] = None

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
    # Advocate / referral: who referred this member (link to another member or free-text if lead not yet in CRM)
    referred_by_client_id: Optional[int] = None
    referred_by_business_name: Optional[str] = None
    referred_by_active: Optional[bool] = True
    referred_by_advocate_name: Optional[str] = None  # display name when referred_by_client_id is set
    # Advocacy meeting details (stored on dashboard)
    advocacy_meeting_date: Optional[str] = None  # ISO date YYYY-MM-DD
    advocacy_meeting_time: Optional[str] = None  # e.g. "11:03 AM"
    advocacy_meeting_completed: Optional[bool] = False

    @field_serializer("created_at", "updated_at", "stage_changed_at")
    def serialize_datetime(self, dt: Optional[datetime], _info):
        if dt is None:
            return None
        return to_melbourne_iso(dt)

    @field_validator("referred_by_active", mode="before")
    @classmethod
    def _referred_by_active_bool(cls, v: Optional[object]) -> Optional[bool]:
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return bool(v)
        return True

    @field_validator("advocacy_meeting_date", mode="before")
    @classmethod
    def _advocacy_meeting_date_str(cls, v: Optional[object]) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, str):
            return v
        if hasattr(v, "isoformat"):
            return v.isoformat()[:10]
        return str(v)[:10]

    @field_validator("advocacy_meeting_completed", mode="before")
    @classmethod
    def _advocacy_meeting_completed_bool(cls, v: Optional[object]) -> bool:
        if v is None:
            return False
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return bool(v)
        return False

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


class ClientReferralCreate(BaseModel):
    advocate_client_id: Optional[int] = None
    advocate_business_name: Optional[str] = None
    active: Optional[bool] = True


class ClientReferralUpdate(BaseModel):
    advocate_client_id: Optional[int] = None
    advocate_business_name: Optional[str] = None
    active: Optional[bool] = None


class ClientReferralResponse(BaseModel):
    id: int
    client_id: int
    advocate_client_id: Optional[int] = None
    advocate_business_name: Optional[str] = None
    active: bool = True
    advocate_display_name: Optional[str] = None  # advocate member's business_name when advocate_client_id set
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at", "updated_at")
    def serialize_datetime(self, dt: Optional[datetime], _info):
        if dt is None:
            return None
        return to_melbourne_iso(dt)

    @field_validator("active", mode="before")
    @classmethod
    def _active_bool(cls, v: Optional[object]) -> bool:
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return bool(v)
        return True

    class Config:
        from_attributes = True


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
    annual_savings: Optional[float] = None
    current_cost: Optional[float] = None
    new_cost: Optional[float] = None
    annual_usage_gj: Optional[float] = None
    energy_charge_pct: Optional[float] = None
    contracted_rate: Optional[float] = None
    offer_rate: Optional[float] = None
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
    annual_savings: Optional[float] = None
    current_cost: Optional[float] = None
    new_cost: Optional[float] = None
    annual_usage_gj: Optional[float] = None
    energy_charge_pct: Optional[float] = None
    contracted_rate: Optional[float] = None
    offer_rate: Optional[float] = None
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


class MemberDocumentUploadActivityCreate(BaseModel):
    """Log a member-area file upload on the activity report (offer resolved server-side)."""
    upload_kind: str
    filename: Optional[str] = None
    document_link: Optional[str] = None
    filing_type: Optional[str] = None
    utility_key: Optional[str] = None
    offer_id: Optional[int] = None
    metadata: Optional[dict] = None


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


class SolarCleaningSignedUploadResponse(BaseModel):
    """Result of POST /api/offers/{id}/solar-cleaning-signed-upload."""

    document_link: Optional[str] = None
    activity_id: int
    sheet_appended: bool
    sheet_error: Optional[str] = None


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


# --- Strategy & WIP (per-client strategy items) ---


class StrategyItemBase(BaseModel):
    year: int
    section: str  # e.g. "past_achievements_annual", "in_progress", "objective", "advocate", "summary"
    row_index: int = 0

    member_level_solutions: Optional[str] = None
    details: Optional[str] = None
    solution_type: Optional[str] = None
    sdg: Optional[str] = None
    key_results: Optional[str] = None

    solution_details_1: Optional[str] = None
    solution_details_2: Optional[str] = None
    solution_details_3: Optional[str] = None

    engagement_form: Optional[str] = None
    contract_signed: Optional[str] = None

    saving_achieved: Optional[float] = None
    new_revenue_achieved: Optional[float] = None
    est_saving_pa: Optional[float] = None
    est_revenue_pa: Optional[float] = None
    est_sav_rev_over_duration: Optional[float] = None

    saving_start_date: Optional[datetime] = None
    new_revenue_start_date: Optional[datetime] = None
    est_start_date: Optional[datetime] = None

    est_sav_kpi_achieved: Optional[str] = None

    priority: Optional[str] = None
    status: Optional[str] = None

    offer_id: Optional[int] = None
    activity_type: Optional[str] = None
    excluded_from_wip: bool = False


class StrategyItemCreate(StrategyItemBase):
    """Body for creating a strategy item. client_id comes from path."""


class StrategyItemUpdate(BaseModel):
    year: Optional[int] = None
    section: Optional[str] = None
    row_index: Optional[int] = None

    member_level_solutions: Optional[str] = None
    details: Optional[str] = None
    solution_type: Optional[str] = None
    sdg: Optional[str] = None
    key_results: Optional[str] = None

    solution_details_1: Optional[str] = None
    solution_details_2: Optional[str] = None
    solution_details_3: Optional[str] = None

    engagement_form: Optional[str] = None
    contract_signed: Optional[str] = None

    saving_achieved: Optional[float] = None
    new_revenue_achieved: Optional[float] = None
    est_saving_pa: Optional[float] = None
    est_revenue_pa: Optional[float] = None
    est_sav_rev_over_duration: Optional[float] = None

    saving_start_date: Optional[datetime] = None
    new_revenue_start_date: Optional[datetime] = None
    est_start_date: Optional[datetime] = None

    est_sav_kpi_achieved: Optional[str] = None

    priority: Optional[str] = None
    status: Optional[str] = None

    excluded_from_wip: Optional[bool] = None


class StrategyItemResponse(StrategyItemBase):
    id: int
    client_id: int
    offer_id: Optional[int] = None
    activity_type: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @field_serializer(
        "saving_start_date",
        "new_revenue_start_date",
        "est_start_date",
        "created_at",
        "updated_at",
    )
    def serialize_datetime(self, dt: Optional[datetime], _info):
        if dt is None:
            return None
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True


# --- Testimonials (member savings testimonials, optional link to 1st Month Savings invoice) ---

TESTIMONIAL_STATUSES = ("Draft", "Sent for approval", "Approved")


class TestimonialResponse(BaseModel):
    id: int
    business_name: str
    file_name: str
    file_id: str
    invoice_number: Optional[str] = None
    status: str
    testimonial_type: Optional[str] = None
    testimonial_solution_type_id: Optional[str] = None
    testimonial_savings: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at", "updated_at")
    def serialize_datetime(self, dt: Optional[datetime], _info):
        if dt is None:
            return None
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True


class TestimonialUpdate(BaseModel):
    status: Optional[str] = None
    invoice_number: Optional[str] = None


class TestimonialCheckApprovedResponse(BaseModel):
    has_approved: bool
    count: int = 0


# --- Testimonial solution content (for doc template placeholders, overridable via API) ---


class TestimonialSolutionContentUpdate(BaseModel):
    """Fields that can be updated per solution type (all optional)."""
    key_outcome_metrics: Optional[str] = None
    key_challenge_of_solution: Optional[str] = None
    key_approach_of_solution: Optional[str] = None
    key_outcome_of_solution: Optional[str] = None
    key_outcome_dotpoints_1: Optional[str] = None
    key_outcome_dotpoints_2: Optional[str] = None
    key_outcome_dotpoints_3: Optional[str] = None
    key_outcome_dotpoints_4: Optional[str] = None
    key_outcome_dotpoints_5: Optional[str] = None
    conclusion: Optional[str] = None
    esg_scope_for_solution: Optional[str] = None
    sdg_impact_for_solution: Optional[str] = None


class TestimonialSolutionContentItem(BaseModel):
    """Merged content for one solution type (defaults + overrides)."""
    solution_type: str
    solution_type_label: str
    key_outcome_metrics: str = ""
    key_challenge_of_solution: str = ""
    key_approach_of_solution: str = ""
    key_outcome_of_solution: str = ""
    key_outcome_dotpoints_1: str = ""
    key_outcome_dotpoints_2: str = ""
    key_outcome_dotpoints_3: str = ""
    key_outcome_dotpoints_4: str = ""
    key_outcome_dotpoints_5: str = ""
    conclusion: str = ""
    esg_scope_for_solution: str = ""
    sdg_impact_for_solution: str = ""


# --- Autonomous follow-up sequences ---


class AutonomousSequenceStartRequest(BaseModel):
    sequence_type: str = "gas_base2_followup_v1"  # or ci_electricity_base2_followup_v1 (same cadence)
    offer_id: int
    client_id: Optional[int] = None
    crm_activity_id: Optional[int] = None
    anchor_at: datetime
    timezone: str = "Australia/Brisbane"  # Ignored for scheduling; sequences always use fixed AEST (Brisbane)
    # Accept either naming from webhook payloads.
    email_id: Optional[str] = None
    email_ID: Optional[str] = None
    context: Dict[str, Any] = {}


class AutonomousSequenceTemplateStepBase(BaseModel):
    step_index: int
    day_number: int
    channel: str
    send_time_local: str
    prompt_text: Optional[str] = None
    retell_agent_id: Optional[str] = None
    is_active: bool = True


class AutonomousSequenceTemplateStepCreate(AutonomousSequenceTemplateStepBase):
    pass


class AutonomousSequenceTemplateStepUpdate(BaseModel):
    step_index: Optional[int] = None
    day_number: Optional[int] = None
    channel: Optional[str] = None
    send_time_local: Optional[str] = None
    prompt_text: Optional[str] = None
    retell_agent_id: Optional[str] = None
    is_active: Optional[bool] = None


class AutonomousSequenceTemplateBase(BaseModel):
    sequence_type: str
    display_name: str
    description: Optional[str] = None
    timezone: str = "Australia/Brisbane"
    is_active: bool = True
    is_restartable: bool = True


class AutonomousSequenceTemplateCreate(AutonomousSequenceTemplateBase):
    steps: List[AutonomousSequenceTemplateStepCreate] = []


class AutonomousSequenceTemplateUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    timezone: Optional[str] = None
    is_active: Optional[bool] = None
    is_restartable: Optional[bool] = None


class AutonomousSequenceTemplateStepResponse(BaseModel):
    id: int
    template_id: int
    step_index: int
    day_number: int
    channel: str
    send_time_local: str
    prompt_text: Optional[str] = None
    retell_agent_id: Optional[str] = None
    is_active: bool = True
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at", "updated_at")
    def serialize_template_step_dt(self, dt: datetime, _info):
        return to_melbourne_iso(dt)

    @field_validator("is_active", mode="before")
    @classmethod
    def _is_active_bool(cls, v: Optional[object]) -> bool:
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return bool(v)
        return True

    class Config:
        from_attributes = True


class AutonomousSequenceTemplateResponse(BaseModel):
    id: int
    sequence_type: str
    display_name: str
    description: Optional[str] = None
    timezone: str
    is_active: bool = True
    is_restartable: bool = True
    created_at: datetime
    updated_at: datetime
    steps: List[AutonomousSequenceTemplateStepResponse] = []

    @field_serializer("created_at", "updated_at")
    def serialize_template_dt(self, dt: datetime, _info):
        return to_melbourne_iso(dt)

    @field_validator("is_active", "is_restartable", mode="before")
    @classmethod
    def _template_flags_bool(cls, v: Optional[object]) -> bool:
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, int):
            return bool(v)
        return True

    class Config:
        from_attributes = True


class AutonomousSequenceStepResponse(BaseModel):
    id: int
    step_index: int
    day_number: int
    channel: str
    step_status: str
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retell_agent_id: Optional[str] = None
    last_outcome_summary: Optional[str] = None

    @field_serializer("scheduled_at", "started_at", "completed_at")
    def serialize_dt(self, dt: Optional[datetime], _info):
        return to_melbourne_iso(dt) if dt else None

    class Config:
        from_attributes = True


class AutonomousSequenceRunResponse(BaseModel):
    id: int
    sequence_type: str
    offer_id: int
    client_id: Optional[int] = None
    run_status: str
    stop_reason: Optional[str] = None
    anchor_at: datetime
    timezone: str
    created_at: datetime
    updated_at: datetime
    business_name: Optional[str] = None
    email_ID: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    context: Dict[str, Any] = {}
    steps: List[AutonomousSequenceStepResponse] = []

    @field_serializer("anchor_at", "created_at", "updated_at")
    def serialize_run_dt(self, dt: datetime, _info):
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True


class AutonomousSequenceRunPatchRequest(BaseModel):
    """Replace stored n8n/Retell context (contact details, lane, etc.)."""

    context: Dict[str, Any]


class AutonomousSequenceStepScheduleItem(BaseModel):
    step_id: int
    scheduled_at: datetime


class AutonomousSequenceStepsSchedulePatchRequest(BaseModel):
    """Reschedule pending steps (ready / to_start only). Times are interpreted as ISO-8601 instants (UTC or offset)."""

    updates: List[AutonomousSequenceStepScheduleItem]


class AutonomousSequenceRunListItem(BaseModel):
    id: int
    offer_id: int
    business_name: Optional[str] = None
    sequence_type: str
    run_status: str
    stop_reason: Optional[str] = None
    anchor_at: datetime
    next_step_channel: Optional[str] = None
    next_step_at: Optional[datetime] = None
    steps_done: int = 0
    steps_total: int = 0

    @field_serializer("anchor_at", "next_step_at")
    def serialize_item_dt(self, dt: Optional[datetime], _info):
        return to_melbourne_iso(dt) if dt else None


class AutonomousSequenceInboundRequest(BaseModel):
    offer_id: int
    run_id: Optional[int] = None
    channel: str = "email"
    raw_text: Optional[str] = None
    transcript: Optional[str] = None
    external_id: Optional[str] = None
    intent: Optional[str] = None
    sentiment_negative: bool = False
    agreement_signed: bool = False