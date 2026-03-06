"""
Database models
"""
from sqlalchemy import Column, Integer, String, DateTime, Date, Text, ForeignKey, Float
from sqlalchemy.sql import func
from datetime import datetime
from database import Base

# SQLite does not have a native JSON type; use Text for JSON metadata for compatibility.
JSON_COLUMN_TYPE = Text


class Client(Base):
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, index=True)
    business_name = Column(String(255), nullable=False, index=True)
    external_business_id = Column(String(255), nullable=True, index=True)
    primary_contact_email = Column(String(255), nullable=True)
    gdrive_folder_url = Column(Text, nullable=True)
    stage = Column(String(50), nullable=False, default="lead")
    stage_changed_at = Column(DateTime, nullable=True)
    owner_email = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Advocate / referral: member who referred this lead, or business name if lead not yet in CRM
    referred_by_client_id = Column(Integer, ForeignKey("clients.id"), nullable=True, index=True)
    referred_by_business_name = Column(String(255), nullable=True)  # when lead hasn't eventuated
    referred_by_active = Column(Integer, nullable=False, default=1)  # 1=active, 0=inactive

    # Advocacy meeting details (stored on dashboard, no n8n)
    advocacy_meeting_date = Column(Date, nullable=True)
    advocacy_meeting_time = Column(String(20), nullable=True)  # e.g. "11:03 AM"
    advocacy_meeting_completed = Column(Integer, nullable=False, default=0)  # 0=no, 1=yes


class ClientReferral(Base):
    """
    Multiple advocate/referral links per client. This client (the lead) was referred by
    one or more members (advocate_client_id) and/or we track business names (advocate_business_name)
    when the lead hasn't eventuated yet.
    """
    __tablename__ = "client_referrals"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False, index=True)  # the lead who was referred
    advocate_client_id = Column(Integer, ForeignKey("clients.id"), nullable=True, index=True)  # member who referred
    advocate_business_name = Column(String(255), nullable=True)  # when lead not yet in CRM
    active = Column(Integer, nullable=False, default=1)  # 1=active, 0=inactive
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=True)
    picture = Column(String, nullable=True)  # optional - from Google profile
    created_at = Column(DateTime, default=datetime.utcnow)


class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    due_date = Column(DateTime, nullable=True)
    status = Column(String, default="not_started", nullable=False)
    assigned_to = Column(String, nullable=True)
    assigned_by = Column(String, nullable=True)
    business_id = Column(Integer, nullable=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=True)
    category = Column(String(50), nullable=False, default="task")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)
    last_notification_sent_at = Column(DateTime, nullable=True)


class TaskHistory(Base):
    __tablename__ = "task_history"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("tasks.id"), nullable=False)
    user_email = Column(String, nullable=True)
    action = Column(String, nullable=False)  # e.g., "task_created", "status_changed", "field_updated"
    field = Column(String, nullable=True)  # which field changed
    old_value = Column(String, nullable=True)
    new_value = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

class ClientStatusNote(Base):
    __tablename__ = "client_status_notes"
    
    id = Column(Integer, primary_key=True, index=True)
    business_name = Column(String(255), nullable=False, index=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=True, index=True)
    note = Column(Text, nullable=False)
    user_email = Column(String(255), nullable=False)
    note_type = Column(String(50), nullable=False, default="general")
    related_task_id = Column(Integer, ForeignKey("tasks.id"), nullable=True)
    # Optional link to an offer so we can log offer-driven events without overloading free-text notes.
    related_offer_id = Column(Integer, ForeignKey("offers.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class Offer(Base):
    __tablename__ = "offers"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=True, index=True)
    business_name = Column(String(255), nullable=True, index=True)
    utility_type = Column(String(100), nullable=True)
    utility_type_identifier = Column(String(100), nullable=True)
    identifier = Column(String(255), nullable=True)
    status = Column(String(50), nullable=False, default="requested")
    # Detailed, linear pipeline stage for this offer (comparison → engagement → contract).
    # Kept as a simple string for compatibility; see OfferPipelineStage enum for allowed values.
    pipeline_stage = Column(String(50), nullable=True)
    estimated_value = Column(Integer, nullable=True)
    # From Base 2 / comparison (e.g. DMA): optional so other webhooks don't need to send
    annual_savings = Column(Float, nullable=True)
    current_cost = Column(Float, nullable=True)
    new_cost = Column(Float, nullable=True)
    created_by = Column(String(255), nullable=True)
    external_record_id = Column(String(255), nullable=True)
    document_link = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class OfferActivity(Base):
    """
    Structured artefacts/events linked to an offer (e.g. quote request, Base 2 review,
    comparison, GHG offer, engagement form, discrepancy email). Kept separate from
    ClientStatusNote which remains for free-text notes.
    """
    __tablename__ = "offer_activities"

    id = Column(Integer, primary_key=True, index=True)
    offer_id = Column(Integer, ForeignKey("offers.id"), nullable=False, index=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=True, index=True)
    activity_type = Column(String(50), nullable=False, index=True)
    document_link = Column(Text, nullable=True)
    external_id = Column(String(255), nullable=True)
    metadata_ = Column("metadata", JSON_COLUMN_TYPE, nullable=True)  # JSON stored as text if needed
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    created_by = Column(String(255), nullable=True)


class StrategyItem(Base):
    """
    Normalised rows for the Strategy & WIP template per client and year.

    Each record represents a single line item in one of the sections
    (e.g. Past Achievements Annual, In Progress, Objective, Advocate, Summary).
    """

    __tablename__ = "strategy_items"

    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    # Section key, e.g. "past_achievements_annual", "in_progress", "objective", "advocate", "summary"
    section = Column(String(50), nullable=False, index=True)
    # For ordering within a section
    row_index = Column(Integer, nullable=False, default=0)

    member_level_solutions = Column(String(255), nullable=True)
    details = Column(String(255), nullable=True)  # e.g. year label in Past Achievements
    solution_type = Column(String(100), nullable=True)
    sdg = Column(String(50), nullable=True)
    key_results = Column(Text, nullable=True)

    solution_details_1 = Column(Text, nullable=True)
    solution_details_2 = Column(Text, nullable=True)
    solution_details_3 = Column(Text, nullable=True)

    engagement_form = Column(String(100), nullable=True)
    contract_signed = Column(String(100), nullable=True)

    # Monetary fields – stored as floats for flexibility (amounts may be fractional).
    saving_achieved = Column(Float, nullable=True)
    new_revenue_achieved = Column(Float, nullable=True)
    est_saving_pa = Column(Float, nullable=True)
    est_revenue_pa = Column(Float, nullable=True)
    est_sav_rev_over_duration = Column(Float, nullable=True)

    # Dates – stored as datetimes; UI can send ISO date strings.
    saving_start_date = Column(DateTime, nullable=True)
    new_revenue_start_date = Column(DateTime, nullable=True)
    est_start_date = Column(DateTime, nullable=True)

    est_sav_kpi_achieved = Column(String(50), nullable=True)

    priority = Column(String(50), nullable=True)
    status = Column(String(50), nullable=True)

    # Link to CRM: when this row was auto-created from an offer/activity, we store references
    # so status updates can be applied and the UI can link to the offer.
    offer_id = Column(Integer, ForeignKey("offers.id"), nullable=True, index=True)
    offer_activity_id = Column(Integer, ForeignKey("offer_activities.id"), nullable=True, index=True, unique=True)
    activity_type = Column(String(50), nullable=True, index=True)  # e.g. "engagement_form", "comparison"
    # When True: still linked to offer but hidden from Strategy & WIP list/export (offer remains tracked).
    excluded_from_wip = Column(Integer, nullable=False, default=0)  # SQLite boolean as 0/1

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Testimonial(Base):
    """
    Member testimonial document (e.g. 1-page savings confirmation).
    Optional link to a 1st Month Savings invoice. Status: Draft, Sent for approval, Approved.
    """
    __tablename__ = "testimonials"

    id = Column(Integer, primary_key=True, index=True)
    business_name = Column(String(255), nullable=False, index=True)
    file_name = Column(String(512), nullable=False)
    file_id = Column(String(255), nullable=False)  # Google Drive file ID
    invoice_number = Column(String(100), nullable=True, index=True)  # Optional link to 1st Month Savings
    status = Column(String(50), nullable=False, default="Draft", index=True)  # Draft | Sent for approval | Approved
    testimonial_type = Column(String(255), nullable=True)  # e.g. C&I Electricity Reviews
    testimonial_solution_type_id = Column(String(100), nullable=True)  # e.g. ci_electricity
    testimonial_savings = Column(String(255), nullable=True)  # Free-text savings summary
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)