"""
Task history logging utilities
"""
from models import TaskHistory
from sqlalchemy.orm import Session
import logging


def log_task_history(
    db: Session,
    task_id: int,
    user_email: str,
    action: str,
    field: str = None,
    old: any = None,
    new: any = None
):
    """Create a task history entry"""
    try:
        entry = TaskHistory(
            task_id=task_id,
            user_email=user_email,
            action=action,
            field=field,
            old_value=str(old) if old is not None else None,
            new_value=str(new) if new is not None else None,
        )
        db.add(entry)
        db.commit()
        logging.info(f"Task history logged: task_id={task_id}, action={action}, field={field}")
    except Exception as e:
        logging.error(f"Failed to log task history: {str(e)}")
        db.rollback()
        raise

