from tools.invoicing_direct_invoices import (
    a1_column,
    extract_drive_file_id,
    list_direct_client_stream_ids,
    normalize_direct_status,
    parse_ledger_rows,
)


def test_normalize_status_maps_unpaid_and_paid():
    assert normalize_direct_status("Unpaid", default="Sent") == "Sent"
    assert normalize_direct_status("paid") == "Paid"
    assert normalize_direct_status("Generated") == "Generated"
    assert normalize_direct_status("", default="Sent") == "Sent"
    assert normalize_direct_status("", default="Generated") == "Generated"


def test_extract_drive_file_id():
    assert (
        extract_drive_file_id(
            "https://drive.google.com/file/d/1ra0hmdAANTdyJYThmABC/view"
        )
        == "1ra0hmdAANTdyJYThmABC"
    )
    assert extract_drive_file_id("1ra0hmdAANTdyJYThmABC") == "1ra0hmdAANTdyJYThmABC"
    assert extract_drive_file_id("") == ""


def test_a1_column():
    assert a1_column(0) == "A"
    assert a1_column(11) == "L"
    assert a1_column(25) == "Z"
    assert a1_column(26) == "AA"


def test_parse_automation_invoices_sent():
    header = [
        "Invoice Number",
        "Client",
        "Contract Key",
        "Payment #",
        "Period (m)",
        "Invoice Period",
        "Invoice Date",
        "Due Date",
        "Gst",
        "Invoice Total",
        "Payment Date",
        "Status",
    ]
    rows = [
        [
            "INV3022",
            "Honest To Goodness",
            "DEPOSIT__Honest To Goodness__INV3022",
            "1",
            "1",
            "Initial Deposit",
            "20/02/2026",
            "20/02/2026",
            "$105.00",
            "$1,155.00",
            "20/02/2026",
            "Paid",
        ],
        [
            "INV3023",
            "Honest To Goodness",
            "ONE_OFF__Honest To Goodness__INV3023",
            "",
            "",
            "API Integration - Zoho",
            "24/04/2026",
            "08/05/2026",
            "$300.00",
            "$3,300.00",
            "",
            "Unpaid",
        ],
        ["INV3029"],
    ]
    invoices = parse_ledger_rows(header, rows, default_status="Sent")
    assert len(invoices) == 2
    paid = next(row for row in invoices if row["invoice_number"] == "INV3022")
    unpaid = next(row for row in invoices if row["invoice_number"] == "INV3023")
    assert paid["status"] == "Paid"
    assert paid["total_amount"] == 1155.0
    assert unpaid["status"] == "Sent"
    assert unpaid["line_items"][0]["solution_label"] == "API Integration - Zoho"


def test_parse_solar_invoices_sent_without_status():
    header = [
        "Invoice Date",
        "Invoice Number",
        "Invoice Key",
        "Quote #",
        "Offer ID",
        "Client ID (CRM)",
        "Client",
        "Contract Name",
        "Contract Email",
        "Invoice Date",
        "Due Date",
        "GST",
        "Invoice Total",
        "Signed Document URL",
        "Source Recorded At",
        "PDF URL",
    ]
    rows = [
        [
            "22/04/2026",
            "INV6193",
            "SOLARQUOTE_1719",
            "1719",
            "178",
            "1",
            "Frankston RSL Sub Branch Inc",
            "Brett Rowlands",
            "browlands@frankstonrsl.com.au",
            "22/04/2026",
            "22/04/2026",
            "$222.21",
            "$2,444.26",
            "https://drive.google.com/file/d/signed123/view",
            "2026-04-21T07:38:58.736633+00:00",
            "https://drive.google.com/file/d/pdf456abcde/view",
        ]
    ]
    invoices = parse_ledger_rows(header, rows, default_status="Sent")
    assert len(invoices) == 1
    row = invoices[0]
    assert row["invoice_number"] == "INV6193"
    assert row["business_name"] == "Frankston RSL Sub Branch Inc"
    assert row["status"] == "Sent"
    assert row["total_amount"] == 2444.26
    assert row["invoice_file_id"] == "pdf456abcde"


def test_direct_stream_ids_cover_dashboard_direct_clients():
    ids = set(list_direct_client_stream_ids())
    assert ids == {
        "one-month-savings",
        "automation-services",
        "equipment-rental",
        "solar-cleaning",
        "cleaning-scrubber",
    }
