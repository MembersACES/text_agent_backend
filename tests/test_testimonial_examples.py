"""Unit tests for listing testimonials (all vs one solution type)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tools.testimonial_examples import get_testimonials_for_solution_type


def _row(id_, solution_type_id, business_name, invoice_number=None):
    return SimpleNamespace(
        id=id_,
        business_name=business_name,
        file_name=f"{business_name}.pdf",
        file_id=f"file-{id_}",
        invoice_number=invoice_number,
        status="Approved",
        testimonial_type=solution_type_id,
        testimonial_solution_type_id=solution_type_id,
        testimonial_savings=None,
        created_at="2026-01-01",
        updated_at="2026-01-01",
    )


def test_omitting_solution_type_returns_every_row():
    db = MagicMock()
    query = db.query.return_value
    query.filter.return_value = query
    query.order_by.return_value.all.return_value = [
        _row(1, "ci_gas", "Moama Bowling Club", "RA5714"),
        _row(2, "ci_electricity", "Peninsula Villages Limited"),
    ]

    with patch("tools.testimonial_sheet.get_all_testimonials_from_sheet", return_value=[]):
        items = get_testimonials_for_solution_type(db, solution_type_id=None, limit=50)

    assert [row["business_name"] for row in items] == [
        "Moama Bowling Club",
        "Peninsula Villages Limited",
    ]
    assert items[0]["invoice_number"] == "RA5714"
    query.filter.assert_not_called()
    from schemas import TestimonialResponse as TestimonialPayload

    TestimonialPayload.model_validate(items[0])


def test_solution_type_filter_uses_id():
    db = MagicMock()
    query = db.query.return_value
    filtered = query.filter.return_value
    filtered.order_by.return_value.all.return_value = [
        _row(1, "ci_gas", "Moama Bowling Club", "RA5714"),
    ]

    with patch("tools.testimonial_sheet.get_all_testimonials_from_sheet", return_value=[]):
        items = get_testimonials_for_solution_type(db, solution_type_id="ci_gas", limit=20)

    assert len(items) == 1
    assert items[0]["business_name"] == "Moama Bowling Club"
    query.filter.assert_called_once()


def test_adopt_sheet_row_creates_crm_testimonial():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from database import Base
    from models import Testimonial
    from tools.testimonial_sheet import adopt_sheet_row_to_crm

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()

    sheet_row = {
        "id": -4,
        "business_name": "Bentleigh RSL Sub Branch",
        "file_name": "Testimonial for Bentleigh RSL Sub Branch - C&I Gas Reviews",
        "file_id": "drive-file-abc",
        "invoice_number": None,
        "status": "Approved",
        "testimonial_type": "C&I Gas Reviews",
        "testimonial_solution_type_id": "ci_gas",
        "testimonial_savings": None,
    }

    with patch("tools.testimonial_sheet.get_all_testimonials_from_sheet", return_value=[sheet_row]):
        created = adopt_sheet_row_to_crm(db, 4)

    db.commit()
    assert created is not None
    assert created.id > 0
    assert created.business_name == "Bentleigh RSL Sub Branch"
    assert created.file_id == "drive-file-abc"
    assert created.testimonial_solution_type_id == "ci_gas"
    assert db.query(Testimonial).count() == 1

    with patch("tools.testimonial_sheet.get_all_testimonials_from_sheet", return_value=[sheet_row]):
        again = adopt_sheet_row_to_crm(db, 4)
    assert again.id == created.id
    assert db.query(Testimonial).count() == 1
