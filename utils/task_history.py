"""
Clean + Correct Task History Logging
"""
from models import TaskHistory
from sqlalchemy.orm import Session
import logging


def log_field_change(
    db: Session,
    task_id: int,
    user_email: str,
    field: str,
    old: any,
    new: any
):
    """
    Log a field change ONLY when it actually changed.
    Prevents useless empty 'No field changes recorded.' rows.
    """
    if old == new:
        return  # skip logging no-op changes

    try:
        entry = TaskHistory(
            task_id=task_id,
            user_email=user_email,
            action="field_updated",
            field=field,
            old_value=str(old) if old is not None else None,
            new_value=str(new) if new is not None else None,
        )
        db.add(entry)
        db.commit()
        logging.info(f"[HISTORY] {field}: {old} → {new}")
    except Exception as e:
        logging.error(f"Failed to log field change: {str(e)}")
        db.rollback()


def log_status_change(
    db: Session,
    task_id: int,
    user_email: str,
    old_status: str,
    new_status: str
):
    """
    ALWAYS log status changes properly.
    """
    if old_status == new_status:
        return

    try:
        entry = TaskHistory(
            task_id=task_id,
            user_email=user_email,
            action="status_changed",
            field="status",
            old_value=old_status,
            new_value=new_status,
        )
        db.add(entry)
        db.commit()
        logging.info(f"[HISTORY] Status: {old_status} → {new_status}")
    except Exception as e:
        logging.error(f"Failed to log status change: {str(e)}")
        db.rollback()


def log_task_created(db: Session, task_id: int, user_email: str):
    try:
        entry = TaskHistory(
            task_id=task_id,
            user_email=user_email,
            action="task_created",
        )
        db.add(entry)
        db.commit()
    except Exception as e:
        db.rollback()
        logging.error(f"Failed to log task creation: {str(e)}")


def log_task_deleted(db: Session, task_id: int, user_email: str):
    try:
        entry = TaskHistory(
            task_id=task_id,
            user_email=user_email,
            action="task_deleted",
        )
        db.add(entry)
        db.commit()
    except Exception as e:
        db.rollback()
        logging.error(f"Failed to log task deletion: {str(e)}")
