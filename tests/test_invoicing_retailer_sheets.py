from tools.invoicing_retailer_sheets import (
    _commission_up_to_date_title,
    summarise_commission_up_to_date,
)


def test_summarise_sums_total_commission_and_counts_mrin_rows():
    header = ["MRIN", "Customer Name", "Total Commission"]
    rows = [
        ["5300000001", "Acme", 40],
        ["5300000002", "Beta", "$60.00"],
        ["", "skip empty", 999],
        ["Total", "totals row", 1000],
    ]
    data, err = summarise_commission_up_to_date(header, rows)
    assert err is None
    assert data is not None
    assert data["row_count"] == 2
    assert data["total_commission"] == 100.0
    assert data["row_label"] == "MRIN"


def test_summarise_uses_nmi_label_for_electricity():
    header = ["NMI", "Client", "Total Commission"]
    rows = [["4100000001", "Acme", 12.5]]
    data, err = summarise_commission_up_to_date(header, rows)
    assert err is None
    assert data is not None
    assert data["row_count"] == 1
    assert data["total_commission"] == 12.5
    assert data["row_label"] == "NMI"


def test_summarise_missing_total_commission_column():
    data, err = summarise_commission_up_to_date(["MRIN", "Name"], [["1", "Acme"]])
    assert data is None
    assert err == "total_commission_column_not_found"


def test_commission_up_to_date_title_prefers_exact_then_gas_prefix():
    tabs = [
        {"name": "Commission Figures", "gid": "1703322444"},
        {"name": "Gas Commission Up to Date", "gid": "0"},
        {"name": "Invoices Sent", "gid": "99"},
    ]
    assert _commission_up_to_date_title(tabs) == "Gas Commission Up to Date"

    tabs_exact = [
        {"name": "Commission Up to Date", "gid": "0"},
        {"name": "Gas Commission Up to Date", "gid": "1"},
    ]
    assert _commission_up_to_date_title(tabs_exact) == "Commission Up to Date"
