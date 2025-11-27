"""
Pydantic schemas for API requests and responses
"""
from pydantic import BaseModel, field_serializer
from typing import Optional
from datetime import datetime
from utils.timezone import to_melbourne_iso


class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None
    due_date: Optional[datetime] = None
    status: str = "not_started"
    assigned_to: Optional[str] = None
    assigned_by: Optional[str] = None
    business_id: Optional[int] = None


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    due_date: Optional[datetime] = None
    status: Optional[str] = None
    assigned_to: Optional[str] = None
    assigned_by: Optional[str] = None
    business_id: Optional[int] = None


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


class ClientStatusNoteUpdate(BaseModel):
    note: str


class ClientStatusNoteResponse(BaseModel):
    id: int
    business_name: str
    note: str
    user_email: str
    created_at: datetime
    updated_at: datetime

    @field_serializer('created_at', 'updated_at')
    def serialize_datetime(self, dt: datetime, _info):
        """Convert UTC datetime to Melbourne timezone before serialization"""
        return to_melbourne_iso(dt)

    class Config:
        from_attributes = True