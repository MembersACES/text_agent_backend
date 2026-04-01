"""
Email service for task notifications via n8n webhook
"""
from sqlalchemy.orm import Session
from models import Task, User
from datetime import datetime, date as date_type
from typing import Any, Dict, List, Optional
import httpx
import logging

# n8n webhook URL
WEBHOOK_URL = "https://membersaces.app.n8n.cloud/webhook/tasks/notify"

# Daily batch runs may send many Gmail actions; allow longer than single-event calls.
_BATCH_WEBHOOK_TIMEOUT = 120.0


async def send_notification(
    notification_type: str, payload: dict, *, timeout: float = 10.0
) -> bool:
    """Send notification to n8n webhook"""
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                WEBHOOK_URL,
                json={
                    "notification_type": notification_type,
                    **payload,
                },
            )
            response.raise_for_status()
            logging.info(f"Notification sent successfully: {notification_type}")
            return True
    except Exception as e:
        logging.error(f"Failed to send notification to webhook: {str(e)}")
        return False


def _assignee_display_name(db: Session, email: Optional[str]) -> Optional[str]:
    if not email:
        return None
    user = db.query(User).filter(User.email == email).first()
    return user.name if user else None


def _due_today_item(task: Task, db: Session) -> Dict[str, Any]:
    email = task.assigned_to
    return {
        "task_id": task.id,
        "task_title": task.title,
        "task_description": task.description or "No description provided",
        "task_due_date": task.due_date.strftime("%B %d, %Y") if task.due_date else "Not set",
        "task_status": task.status,
        "user_email": email,
        "user_name": _assignee_display_name(db, email),
    }


def _overdue_item(task: Task, db: Session) -> Dict[str, Any]:
    email = task.assigned_to
    days_overdue = (
        (date_type.today() - task.due_date.date()).days if task.due_date else 0
    )
    return {
        "task_id": task.id,
        "task_title": task.title,
        "task_description": task.description or "No description provided",
        "task_due_date": task.due_date.strftime("%B %d, %Y") if task.due_date else "Not set",
        "days_overdue": days_overdue,
        "task_status": task.status,
        "user_email": email,
        "user_name": _assignee_display_name(db, email),
    }


async def send_new_task_email(assigned_to_email: str, creator_email: str, task: Task, db: Session):
    """Send email notification when a new task is created"""
    if not assigned_to_email:
        logging.warning(f"Cannot send new task email: no assigned_to email for task {task.id}")
        return

    # Get user details
    assigned_to_user = db.query(User).filter(User.email == assigned_to_email).first()
    creator_user = db.query(User).filter(User.email == creator_email).first()

    creator_name = creator_user.name if creator_user else creator_email
    assigned_to_name = assigned_to_user.name if assigned_to_user else assigned_to_email

    due_date_str = task.due_date.strftime("%B %d, %Y") if task.due_date else "Not set"

    payload = {
        "task_id": task.id,
        "task_title": task.title,
        "task_description": task.description or "No description provided",
        "task_due_date": due_date_str,
        "task_status": task.status,
        "assigned_to_email": assigned_to_email,
        "assigned_to_name": assigned_to_name,
        "creator_email": creator_email,
        "creator_name": creator_name,
        "created_at": task.created_at.isoformat() if task.created_at else None,
    }

    await send_notification("new_task", payload)


async def send_task_completed_email(assigned_by_email: str, assigned_to_email: str, task: Task, db: Session):
    """Send email notification when a task is marked as completed"""
    if not assigned_by_email:
        logging.warning(f"Cannot send completion email: no assigned_by email for task {task.id}")
        return

    # Get user details
    assigned_by_user = db.query(User).filter(User.email == assigned_by_email).first()
    assigned_to_user = db.query(User).filter(User.email == assigned_to_email).first()

    assigned_by_name = assigned_by_user.name if assigned_by_user else assigned_by_email
    assigned_to_name = assigned_to_user.name if assigned_to_user else assigned_to_email

    payload = {
        "task_id": task.id,
        "task_title": task.title,
        "task_description": task.description or "No description provided",
        "assigned_by_email": assigned_by_email,
        "assigned_by_name": assigned_by_name,
        "assigned_to_email": assigned_to_email,
        "assigned_to_name": assigned_to_name,
        "completed_at": datetime.now().isoformat(),
    }

    await send_notification("task_completed", payload)


async def check_due_tasks(db: Session):
    """Check for tasks due today or overdue and send notifications (batched n8n calls)."""
    today = date_type.today()
    today_start = datetime.combine(today, datetime.min.time())

    # Get tasks due today (not completed)
    due_today_tasks = db.query(Task).filter(
        Task.due_date.isnot(None),
        Task.status != "completed",
        Task.assigned_to.isnot(None),
    ).all()

    due_today_filtered: List[Task] = []
    for task in due_today_tasks:
        if task.due_date and task.due_date.date() == today:
            # Only send if we haven't sent a "due today" notification today
            if not task.last_notification_sent_at or task.last_notification_sent_at.date() < today:
                due_today_filtered.append(task)

    # Get overdue tasks (not completed)
    overdue_tasks = db.query(Task).filter(
        Task.due_date.isnot(None),
        Task.due_date < today_start,
        Task.status != "completed",
        Task.assigned_to.isnot(None),
    ).all()

    overdue_filtered: List[Task] = []
    for task in overdue_tasks:
        # Only send if we haven't sent an overdue notification today
        if not task.last_notification_sent_at or task.last_notification_sent_at.date() < today:
            overdue_filtered.append(task)

    due_with_email = [t for t in due_today_filtered if t.assigned_to]
    if due_with_email:
        items = [_due_today_item(t, db) for t in due_with_email]
        ok = await send_notification(
            "due_today_batch",
            {"items": items},
            timeout=_BATCH_WEBHOOK_TIMEOUT,
        )
        if ok:
            now = datetime.now()
            for t in due_with_email:
                t.last_notification_sent_at = now
            try:
                db.commit()
            except Exception as e:
                logging.error(f"Failed to commit due_today_batch notification timestamps: {e}")
                db.rollback()
        else:
            logging.error("due_today_batch webhook failed; not updating last_notification_sent_at")

    overdue_with_email = [t for t in overdue_filtered if t.assigned_to]
    if overdue_with_email:
        items = [_overdue_item(t, db) for t in overdue_with_email]
        ok = await send_notification(
            "overdue_batch",
            {"items": items},
            timeout=_BATCH_WEBHOOK_TIMEOUT,
        )
        if ok:
            now = datetime.now()
            for t in overdue_with_email:
                t.last_notification_sent_at = now
            try:
                db.commit()
            except Exception as e:
                logging.error(f"Failed to commit overdue_batch notification timestamps: {e}")
                db.rollback()
        else:
            logging.error("overdue_batch webhook failed; not updating last_notification_sent_at")

    logging.info(
        f"Daily task check completed: {len(due_today_filtered)} due today, {len(overdue_filtered)} overdue"
    )
