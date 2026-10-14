"""tools.sme_gas_sheet against a fake Sheets API, using the real East Malvern row (25 Sep 2026)."""

import pytest

import tools.sme_gas_sheet as sheet

HEADERS = [
    "MRIN", "Client Name", "Retailer", "Invoice Review Period", "Invoice Review Number of Days", "Supply Address",
    "General Usage Quantity", "General Usage Rate", "General Usage Peak Quantity", "General Usage Peak Rate",
    "General Usage Next Quantity", "General Usage Next Rate", "Block 1 Consumption", "Block 1 Rate",
    "Block 2 Consumption", "Block 2 Rate", "Block 3 Consumption", "Block 3 Rate", "Block 4 Consumption", "Block 4 Rate",
    "Daily Supply Charge Quantity", "Daily Supply Charge Rate", "Usage Discount %:", "Usage Discount $:",
    "Supply Discount %:", "Supply Discount $:", "Total Discount %:", "Total Discount $:", "Invoice Total:",
    "Webview Link", "Invoice Number", "Issue Date", "Due Date", "Plan Name", "Read Type", "Price Change on Invoice",
    "Rates Period", "Rates Period Days", "Block 1 Threshold", "Block 2 Threshold", "Block 3 Threshold",
    "Block 4 Threshold", "Block 1 Amount $", "Block 2 Amount $", "Block 3 Amount $", "Block 4 Amount $",
    "Daily Supply Amount $", "Plan Discount Included $", "Other Charges/Credits", "Other Charges/Credits $", "GST $",
    "Invoice Total incl GST", "Balance Brought Forward $", "Rates Basis", "Reconciles?", "Invoice Total MJ (whole bill)",
]

EAST_MALVERN = [
    "53102023079", "EAST MALVERN RSL CLUB", "Origin Energy", "26/05/2026-27/07/2026", "63",
    "STANLEY GROSE DR MALVERN EAST VIC 3145", "", "", "", "", "", "", "6750", "3.839", "20250", "3.509", "1520", "2.898",
    "", "", "27", "1.30889", "", "", "", "", "", "", "2132.46",
    "https://drive.google.com/file/d/1NME6J3KenRef03h9zU-5rPeiV9uq0Nzu/view?usp=drivesdk", "132584766",
    "29/07/2026", "18/08/2026", "Origin Standing", "Actual", "Y", "01/07/2026-27/07/2026", "27", "First 6750",
    "Next 6750 - 27000", "Remaining", "", "259.13", "710.57", "43.97", "", "35.34", "", "", "", "213.25", "2345.71",
    "0", "c/MJ incl GST", "Y", "66547",
]

OTHER = ["53302013763", "GLENROY RSL SUB - BRANCH INC"] + [""] * (len(HEADERS) - 2)


class _Call:
    def __init__(self, fn):
        self._fn = fn

    def execute(self):
        return self._fn()


class FakeSheets:
    """Serves the tab and records every range requested, so we can prove no full-tab read happens."""

    def __init__(self, data_rows):
        self.grid = [HEADERS] + data_rows
        self.requested = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def batchGet(self, spreadsheetId, ranges, valueRenderOption):
        self.requested.extend(ranges)
        return _Call(lambda: {"valueRanges": [self._range(r) for r in ranges]})

    def _range(self, a1):
        ref = a1.split("!", 1)[1]
        if ref == "A2:B":
            return {"values": [r[:2] for r in self.grid[1:]]}
        row = int(ref.split(":")[0][1:])
        return {"values": [self.grid[row - 1]]}


@pytest.fixture(autouse=True)
def _clear_cache():
    sheet._INDEX_CACHE.update({"ts": 0.0, "keys": None, "headers": None})


def test_reads_only_index_and_matching_rows():
    fake = FakeSheets([OTHER, EAST_MALVERN])
    out = sheet.get_sme_gas_invoice_from_sheet(mrin="53102023079", service=fake)
    assert out["source"]["rows"] == [3]
    assert out["source"]["match"] == "exact"
    # header row, the two key columns, then exactly the one matching row
    assert fake.requested == [
        "'5th Sheet - Small Gas'!A1:BZ1",
        "'5th Sheet - Small Gas'!A2:B",
        "'5th Sheet - Small Gas'!A3:BZ3",
    ]


def test_legacy_shape_uses_whole_bill_pair():
    out = sheet.get_sme_gas_invoice_from_sheet(mrin="53102023079", service=FakeSheets([EAST_MALVERN]))
    d = out["gas_sme_invoicedetails"]
    assert d["usage"]["general_usage_quantity"] == "66547"   # whole bill MJ ...
    assert d["invoice_review_days"] == "63"                   # ... with whole bill days
    assert d["usage"]["block_3"] == {"consumption": "1520", "rate": "2.898", "amount": "43.97", "threshold": "Remaining"}
    assert d["supply_charge"] == {"rate": "1.30889", "quantity_days": "27", "amount": "35.34"}
    assert d["total_invoice_cost"] == "2132.46"
    assert d["invoice_review_period"] == "26/05/2026-27/07/2026"
    assert d["period_start"] == "2026-05-26" and d["period_end"] == "2026-07-27"


def test_normalised_flags_the_misread_block_rate():
    out = sheet.get_sme_gas_invoice_from_sheet(mrin="53102023079", service=FakeSheets([EAST_MALVERN]))
    latest = out["normalised"]["latest"]
    codes = [f["code"] for f in latest["flags"]]
    assert "block_amount_mismatch" in codes
    assert latest["annual_basis"]["source"] == "whole_bill_mj_over_invoice_days"
    assert out["normalised"]["annual_gj"] == pytest.approx(66.547 * 365 / 63)
    assert out["normalised"]["class"] == "sme"
    # energy rate comes from the printed amounts, so the misread rate does not change it
    assert latest["energy_rate_aud_per_gj"] == pytest.approx((259.13 + 710.57 + 43.97) / 28.52)


def test_checksum_mrin_matches_but_last_digit_swap_does_not():
    fake = FakeSheets([EAST_MALVERN])
    assert sheet.get_sme_gas_invoice_from_sheet(mrin="5310202307", service=fake)["source"]["match"] == "checksum"
    sheet._INDEX_CACHE.update({"ts": 0.0, "keys": None, "headers": None})
    assert "error" in sheet.get_sme_gas_invoice_from_sheet(mrin="53102023078", service=FakeSheets([EAST_MALVERN]))


def test_business_name_lookup():
    out = sheet.get_sme_gas_invoice_from_sheet(business_name="east malvern rsl club", service=FakeSheets([OTHER, EAST_MALVERN]))
    assert out["source"]["match"] == "name_exact"
    assert out["gas_sme_invoicedetails"]["mrin"] == "53102023079"


MOAMA_ROW = ["52210174393", "MOAMA BOWLING CLUB", "Origin Energy", "16/04/2026-31/05/2026", "46"] + [""] * (len(HEADERS) - 5)


def test_rows_shifting_after_index_never_returns_another_site():
    """Reproduces 25 Sep: a new row is written above East Malvern while the index is cached."""
    fake = FakeSheets([OTHER, EAST_MALVERN])
    sheet.get_sme_gas_invoice_from_sheet(mrin="53102023079", service=fake)  # warms the index cache
    fake.grid.insert(1, MOAMA_ROW)  # row 3 is now Glenroy, East Malvern moved to row 4
    out = sheet.get_sme_gas_invoice_from_sheet(mrin="53102023079", service=fake)
    assert out["gas_sme_invoicedetails"]["mrin"] == "53102023079"
    assert out["gas_sme_invoicedetails"]["client_name"] == "EAST MALVERN RSL CLUB"
    assert out["source"]["rows"] == [4]
