"""Tests for Airtable invoice row → ActivityRecord ETL."""
from datetime import date

from services.climate_activity_etl import (
    EtlContext,
    default_fy_period,
    invoice_row_to_activity_record,
    transform_invoice_rows,
)


def test_default_fy_period_fy26():
    start, end = default_fy_period("FY26")
    assert start == date(2025, 7, 1)
    assert end == date(2026, 6, 30)


def test_electricity_row_maps_to_activity_record():
    ctx = EtlContext(
        entity_id="parramatta-leagues-club",
        client_id=1,
        loa_client_id="recafZHjICWMdueoo",
        site_id="NEEE001316",
        utility_type="C&I Electricity",
        period_start=date(2025, 7, 1),
        period_end=date(2026, 6, 30),
    )
    row = {
        "record_id": "recInvoice123",
        "kWh": "1,842,500",
        "Invoice Review Period": "2025-08-01",
        "Invoice PDF": "https://drive.google.com/file/d/abc/view",
    }
    res = invoice_row_to_activity_record(row, ctx)
    assert not res.skipped
    assert res.body["activity_type"] == "electricity_grid"
    assert res.body["scope"] == 2
    assert res.body["quantity"] == 1842500.0
    assert res.body["unit"] == "kWh"
    assert res.body["entity_id"] == "parramatta-leagues-club"
    assert res.body["client_id"] == "recafZHjICWMdueoo"
    assert len(res.body["evidence_refs"]) == 1


def test_skips_row_without_quantity():
    ctx = EtlContext(
        entity_id="test-entity",
        client_id=2,
        loa_client_id=None,
        site_id="NMI1",
        utility_type="C&I Gas",
        period_start=date(2025, 7, 1),
        period_end=date(2026, 6, 30),
    )
    res = invoice_row_to_activity_record({"record_id": "recX"}, ctx)
    assert res.skipped
    assert res.skip_reason == "missing or zero quantity"


def test_transform_invoice_rows_counts():
    ctx = EtlContext(
        entity_id="test",
        client_id=1,
        loa_client_id=None,
        site_id="NMI",
        utility_type="C&I Electricity",
        period_start=date(2025, 7, 1),
        period_end=date(2026, 6, 30),
    )
    rows = [
        {"record_id": "rec1", "kWh": 100},
        {"record_id": "rec2"},
    ]
    results, diag = transform_invoice_rows(rows, ctx)
    assert diag["produced"] == 1
    assert diag["skipped"] == 1
    assert len(results) == 2
