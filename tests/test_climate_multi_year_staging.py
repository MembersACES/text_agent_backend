"""
Staging is NOT financial-year scoped.

The financial year is a *view* decision made at compute time
(prograde_b4_client.collect_v1_bodies filters by the FY window), not a load
decision. Staging holds every year the source has, which is what makes
prior-year comparatives, baseline years and trend estimation possible — AASB S2
requires comparatives, and a target needs a base year.

These tests exist because the ETL used to drop any invoice outside the requested
FY, which made those things structurally impossible and silently discarded data.
"""
from datetime import date

from services.climate_activity_etl import (
    EtlContext,
    invoice_row_to_activity_record,
    oil_invoice_to_records,
    transform_invoice_rows,
)


def _ctx(utility_type: str = "C&I Electricity") -> EtlContext:
    """Context requesting FY26. Rows from other years must still be staged."""
    return EtlContext(
        entity_id="frankston-rsl",
        client_id=1,
        loa_client_id="recX",
        site_id="VEEE0U1Y2S",
        utility_type=utility_type,
        period_start=date(2025, 7, 1),
        period_end=date(2026, 6, 30),
    )


def test_invoice_from_a_different_fy_is_still_staged():
    """An FY23 invoice must survive a sync that asked for FY26."""
    row = {
        "record_id": "rec001",
        "Invoice Review Period": "01/08/2022-31/08/2022",
        "Consumption (kWh)": "12,500",
    }
    res = invoice_row_to_activity_record(row, _ctx())
    assert not res.skipped, res.skip_reason
    assert res.status == "draft"
    # The row keeps its OWN period, not the requested window.
    assert res.body["reporting_period"]["start"] == "2022-08-01"
    assert res.body["reporting_period"]["end"] == "2022-08-31"


def test_row_period_is_never_overwritten_by_the_requested_window():
    row = {
        "record_id": "rec002",
        "Invoice Review Period": "01/09/2023-30/09/2023",
        "Consumption (kWh)": "7,100",
    }
    res = invoice_row_to_activity_record(row, _ctx())
    assert res.body["reporting_period"]["end"] == "2023-09-30"


def test_undated_row_is_flagged_not_guessed():
    """
    A row with no readable billing period cannot be attributed to a year.

    It is staged so it stays visible and fixable, but marked "undated" so compute
    excludes it. Previously it was stamped with whatever FY was requested, and
    because record_id carries no period, re-syncing under a different FY label
    silently moved the row into that year.
    """
    row = {"record_id": "rec003", "Consumption (kWh)": "4,200"}
    res = invoice_row_to_activity_record(row, _ctx())
    assert not res.skipped
    assert res.status == "undated"
    assert "period_unknown" in res.body["data_quality"]["flags"]


def test_record_ids_stay_unique_across_years():
    """Multi-year staging must not collide — one row in, one record_id out."""
    rows = [
        {"record_id": "rec001", "Invoice Review Period": "01/08/2022-31/08/2022", "kWh": 100},
        {"record_id": "rec002", "Invoice Review Period": "01/12/2025-31/12/2025", "kWh": 200},
        {"record_id": "rec003", "Invoice Review Period": "01/09/2023-30/09/2023", "kWh": 300},
    ]
    results, _ = transform_invoice_rows(rows, _ctx())
    ids = [r.record_id for r in results if not r.skipped]
    assert len(ids) == 3
    assert len(set(ids)) == 3


def test_diagnostics_report_years_covered_and_undated_count():
    rows = [
        {"record_id": "rec001", "Invoice Review Period": "01/08/2022-31/08/2022", "kWh": 100},
        {"record_id": "rec002", "Invoice Review Period": "01/12/2025-31/12/2025", "kWh": 200},
        {"record_id": "rec003", "Invoice Review Period": "01/09/2023-30/09/2023", "kWh": 300},
        {"record_id": "rec004", "kWh": 400},          # undated
        {"record_id": "rec005"},                      # no quantity -> genuinely skipped
    ]
    results, diag = transform_invoice_rows(rows, _ctx())
    assert diag["produced"] == 4
    assert diag["skipped"] == 1
    assert diag["undated"] == 1
    # An invoice belongs to the FY its billing period ENDS in.
    assert diag["financial_years"] == {"FY23": 1, "FY24": 1, "FY26": 1}


def test_missing_quantity_is_still_skipped():
    """Dropping the FY filter must not stop real validation from skipping rows."""
    res = invoice_row_to_activity_record({"record_id": "rec009"}, _ctx())
    assert res.skipped
    assert res.skip_reason == "missing or zero quantity"


def test_oil_invoice_from_another_year_is_staged_and_split():
    """The oil path had its own copy of the FY filter — check it went too."""
    row = {
        "record_id": "recOil1",
        "Invoice Review Period": "01/03/2023-31/03/2023",
        "Product / Description 1": "Fresh Canola Fry Oil 20L",
        "Quantity 1": "60",
        "Product / Description 2": "WASTE OIL COLLECTED",
        "Quantity 2": "45",
    }
    results = oil_invoice_to_records(row, _ctx("Oil"))
    kept = [r for r in results if not r.skipped]
    assert len(kept) == 2, [r.skip_reason for r in results]
    assert {r.body["scope_3_category"] for r in kept} == {1, 5}
    for r in kept:
        assert r.status == "draft"
        assert r.body["reporting_period"]["end"] == "2023-03-31"


def test_undated_oil_invoice_is_flagged():
    row = {
        "record_id": "recOil2",
        "Product / Description 1": "Fresh Canola Fry Oil 20L",
        "Quantity 1": "20",
    }
    kept = [r for r in oil_invoice_to_records(row, _ctx("Oil")) if not r.skipped]
    assert kept and all(r.status == "undated" for r in kept)
    assert all("period_unknown" in r.body["data_quality"]["flags"] for r in kept)
