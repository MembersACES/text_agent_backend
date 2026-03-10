import logging
from typing import List

from sqlalchemy.orm import Session

from models import Testimonial

logger = logging.getLogger(__name__)


def get_testimonials_for_solution_type(
    db: Session,
    solution_type_id: str,
    limit: int = 5,
) -> List[Testimonial]:
    """
    Return recent testimonials matching a given solution_type_id.

    Used to show real examples for each testimonial content type.
    """
    if not solution_type_id:
        return []

    try:
        q = (
            db.query(Testimonial)
            .filter(Testimonial.testimonial_solution_type_id == solution_type_id)
            .order_by(Testimonial.created_at.desc())
        )
        if limit and limit > 0:
            q = q.limit(limit)
        return q.all()
    except Exception as e:
        logger.error("Failed to fetch testimonials for solution_type %s: %s", solution_type_id, e)
        return []

