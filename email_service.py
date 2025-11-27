"""
Email service for task notifications via n8n webhook
"""
from sqlalchemy.orm import Session
from models import Task, User
from datetime import datetime, date as date_type
import httpx
import logging

# n8n webhook URL
WEBHOOK_URL = "https://membersaces.app.n8n.cloud/webhook/tasks/notify"


async def send_notification(notification_type: str, payload: dict):
    """Send notification to n8n webhook"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                WEBHOOK_URL,
                json={
                    "notification_type": notification_type,
                    **payload
                }
            )
            response.raise_for_status()
            logging.info(f"Notification sent successfully: {notification_type}")
            return True
    except Exception as e:
        logging.error(f"Failed to send notification to webhook: {str(e)}")
        return False


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
        "created_at": task.created_at.isoformat() if task.created_at else None
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
        "completed_at": datetime.now().isoformat()
    }
    
    await send_notification("task_completed", payload)


async def send_due_today_email(user_email: str, task: Task):
    """Send email notification for tasks due today"""
    user = None  # Could fetch from User model if needed
    
    payload = {
        "task_id": task.id,
        "task_title": task.title,
        "task_description": task.description or "No description provided",
        "task_due_date": task.due_date.strftime("%B %d, %Y") if task.due_date else "Not set",
        "task_status": task.status,
        "user_email": user_email,
        "user_name": user.name if user else None
    }
    
    await send_notification("due_today", payload)


async def send_overdue_email(user_email: str, task: Task):
    """Send email notification for overdue tasks"""
    user = None  # Could fetch from User model if needed
    
    days_overdue = (date_type.today() - task.due_date.date()).days if task.due_date else 0
    
    payload = {
        "task_id": task.id,
        "task_title": task.title,
        "task_description": task.description or "No description provided",
        "task_due_date": task.due_date.strftime("%B %d, %Y") if task.due_date else "Not set",
        "days_overdue": days_overdue,
        "task_status": task.status,
        "user_email": user_email,
        "user_name": user.name if user else None
    }
    
    await send_notification("overdue", payload)


async def check_due_tasks(db: Session):
    """Check for tasks due today or overdue and send notifications"""
    today = date_type.today()
    today_start = datetime.combine(today, datetime.min.time())
    
    # Get tasks due today (not completed)
    due_today_tasks = db.query(Task).filter(
        Task.due_date.isnot(None),
        Task.status != "completed",
        Task.assigned_to.isnot(None)
    ).all()
    
    due_today_filtered = []
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
        Task.assigned_to.isnot(None)
    ).all()
    
    overdue_filtered = []
    for task in overdue_tasks:
        # Only send if we haven't sent an overdue notification today
        if not task.last_notification_sent_at or task.last_notification_sent_at.date() < today:
            overdue_filtered.append(task)
    
    # Send due today emails
    for task in due_today_filtered:
        if task.assigned_to:
            try:
                await send_due_today_email(task.assigned_to, task)
                task.last_notification_sent_at = datetime.now()
                db.commit()
            except Exception as e:
                logging.error(f"Failed to send due today email for task {task.id}: {str(e)}")
                db.rollback()
    
    # Send overdue emails
    for task in overdue_filtered:
        if task.assigned_to:
            try:
                await send_overdue_email(task.assigned_to, task)
                task.last_notification_sent_at = datetime.now()
                db.commit()
            except Exception as e:
                logging.error(f"Failed to send overdue email for task {task.id}: {str(e)}")
                db.rollback()
    
    logging.info(f"Daily task check completed: {len(due_today_filtered)} due today, {len(overdue_filtered)} overdue")
