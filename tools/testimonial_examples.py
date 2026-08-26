import logging
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from models import Testimonial

logger = logging.getLogger(__name__)


def get_testimonials_for_solution_type(
    db: Session,
    solution_type_id: str | None = None,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Return recent testimonials, optionally filtered to a solution_type_id.

    CRM rows first, then Testimonial List sheet rows of the same type
    (so the content page still fills when n8n wrote the sheet but CRM
    lives on another environment). Omit solution_type_id to return every
    filed testimonial (used by the content-page All tab).
    """
    wanted = (solution_type_id or "").strip()
    try:
        query = db.query(Testimonial)
        if wanted:
            query = query.filter(Testimonial.testimonial_solution_type_id == wanted)
        db_items = query.order_by(Testimonial.created_at.desc()).all()
    except Exception as e:
        logger.error("Failed to fetch testimonials for solution_type %s: %s", wanted or "*", e)
        db_items = []

    try:
        from tools.testimonial_sheet import (
            get_all_testimonials_from_sheet,
            merge_db_and_sheet_testimonials,
        )

        sheet_items = get_all_testimonials_from_sheet()
        if wanted:
            sheet_items = [
                item
                for item in sheet_items
                if (item.get("testimonial_solution_type_id") or "") == wanted
            ]
        merged = merge_db_and_sheet_testimonials(db_items, sheet_items)
    except Exception as e:
        logger.error("Failed to merge sheet testimonials for solution_type %s: %s", wanted or "*", e)
        from tools.testimonial_sheet import merge_db_and_sheet_testimonials

        merged = merge_db_and_sheet_testimonials(db_items, [])

    if limit and limit > 0:
        return merged[:limit]
    return merged

