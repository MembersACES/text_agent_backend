import logging
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from models import Testimonial

logger = logging.getLogger(__name__)


def get_testimonials_for_solution_type(
    db: Session,
    solution_type_id: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Return recent testimonials matching a given solution_type_id.

    CRM rows first, then Testimonial List sheet rows of the same type
    (so the content page still fills when n8n wrote the sheet but CRM
    lives on another environment).
    """
    if not solution_type_id:
        return []

    wanted = solution_type_id.strip()
    try:
        db_items = (
            db.query(Testimonial)
            .filter(Testimonial.testimonial_solution_type_id == wanted)
            .order_by(Testimonial.created_at.desc())
            .all()
        )
    except Exception as e:
        logger.error("Failed to fetch testimonials for solution_type %s: %s", wanted, e)
        db_items = []

    try:
        from tools.testimonial_sheet import (
            get_all_testimonials_from_sheet,
            merge_db_and_sheet_testimonials,
        )

        sheet_items = [
            item
            for item in get_all_testimonials_from_sheet()
            if (item.get("testimonial_solution_type_id") or "") == wanted
        ]
        merged = merge_db_and_sheet_testimonials(db_items, sheet_items)
    except Exception as e:
        logger.error("Failed to merge sheet testimonials for solution_type %s: %s", wanted, e)
        from tools.testimonial_sheet import merge_db_and_sheet_testimonials

        merged = merge_db_and_sheet_testimonials(db_items, [])

    if limit and limit > 0:
        return merged[:limit]
    return merged

