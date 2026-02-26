"""
Database models
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey
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
    estimated_value = Column(Integer, nullable=True)
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