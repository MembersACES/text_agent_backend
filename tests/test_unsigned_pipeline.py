"""Unit tests for unsigned utility pipeline (no live Sheets / Drive)."""
from services.unsigned_pipeline import (
    aggregate_by_state,
    annualise_usage,
    attach_signed_status,
    base1_matches_group,
    build_pipeline,
    guess_state,
    group_invoice_rows,
    postcode_to_state,
    resolve_utility_types,
    row_webview_link,
    sites_from_base1,
    summary_csv_bytes,
)


def test_postcode_to_state():
    assert postcode_to_state("3000") == "VIC"
    assert postcode_to_state("2000") == "NSW"
    assert postcode_to_state("4000") == "QLD"
    assert postcode_to_state("2600") == "ACT"
    assert postcode_to_state("0800") == "NT"
    assert postcode_to_state("5000") == "SA"
    assert postcode_to_state("6000") == "WA"
    assert postcode_to_state("7000") == "TAS"
    assert postcode_to_state("") == ""


def test_guess_state_from_address_and_explicit():
    assert guess_state("12 Smith St, Richmond VIC 3121") == "VIC"
    assert guess_state("1 George St Sydney 2000") == "NSW"
    assert guess_state("somewhere", "queensland") == "QLD"
    assert guess_state("") == "Unknown"
    assert guess_state("no digits here") == "Unknown"


def test_resolve_utility_types_segment():
    assert resolve_utility_types("gas", "all") == ["C&I Gas", "SME Gas"]
    assert resolve_utility_types("gas", "ci") == ["C&I Gas"]
    assert resolve_utility_types("gas", "sme") == ["SME Gas"]
    assert "Waste" in resolve_utility_types("all", "all")
    assert resolve_utility_types("waste", "ci") == ["Waste"]


def test_annualise_usage():
    annual, method = annualise_usage(100.0, 30)
    assert method == "annualised"
    assert abs(annual - (100.0 / 30 * 365)) < 1e-6
    billed, method2 = annualise_usage(50.0, 0)
    assert method2 == "sum_billed"
    assert billed == 50.0
    none, method3 = annualise_usage(0.0, 10)
    assert none is None
    assert method3 == "none"


def test_row_webview_link_is_case_insensitive():
    assert row_webview_link({"Webview Link": "https://drive.google.com/file/d/abc/view"})
    assert row_webview_link({"webview link": "https://x"}) == "https://x"
    assert row_webview_link({"Invoice PDF": "https://y"}) == "https://y"
    assert row_webview_link({"Webview Link": "null"}) == ""


def test_group_invoice_rows_gas_load_and_latest():
    rows = [
        {
            "MRIN": "5320123456",
            "Client Name": "Acme Bakery",
            "Site Address": "1 High St Melbourne VIC 3000",
            "Invoice Review Period": "01/01/2026-31/01/2026",
            "Energy Charge Quantity in GJ": "80",
            "Webview Link": "https://drive.google.com/file/d/aaa/view",
        },
        {
            "MRIN": "5320123456",
            "Client Name": "Acme Bakery",
            "Site Address": "1 High St Melbourne VIC 3000",
            "Invoice Review Period": "01/12/2025-31/12/2025",
            "Energy Charge Quantity in GJ": "70",
            "Webview Link": "",
        },
    ]
    sites = group_invoice_rows("C&I Gas", rows, ("MRIN",), "Invoice Review Period")
    assert len(sites) == 1
    site = sites[0]
    assert site["identifier"] == "5320123456"
    assert site["state"] == "VIC"
    assert site["invoice_count"] == 2
    assert site["pdf_count"] == 1
    assert site["load_method"] == "annualised"
    assert site["annual_load"] is not None
    assert site["latest_invoice"]["link"].endswith("aaa/view")


def test_unsigned_filter_keeps_gas_when_electricity_signed():
    sites = group_invoice_rows(
        "C&I Gas",
        [
            {
                "MRIN": "1",
                "Client Name": "Signed Elec Pty",
                "Site Address": "Brisbane QLD 4000",
                "GJ": "10",
                "Webview Link": "https://drive.google.com/file/d/x/view",
            }
        ],
        ("MRIN",),
        "Invoice Review Period",
    )
    file_ids = [
        {
            "business_name": "Signed Elec Pty",
            "record_id": "rec1",
            "sc_ci_e_status": "Signed via ACES",
            "sc_ci_g_status": "",
            "sc_sme_e_status": "",
            "sc_sme_g_status": "",
            "sc_waste_status": "",
            "sc_oil_status": "",
            "sc_dma_status": "",
            "sc_ci_e_file": "",
            "sc_ci_g_file": "",
            "sc_sme_e_file": "",
            "sc_sme_g_file": "",
            "sc_waste_file": "",
            "sc_oil_file": "",
            "sc_dma_file": "",
        }
    ]
    attach_signed_status(
        sites,
        file_ids_rows=file_ids,
        crm_clients=[{"id": 9, "business_name": "Signed Elec Pty", "external_business_id": "rec1", "stage": "existing_client"}],
    )
    assert sites[0]["signed"] is False
    assert "C&I Electricity" in sites[0]["signed_utilities"]
    assert sites[0]["match_method"] == "record_id"


def test_drops_signed_gas():
    payload = build_pipeline(
        utility_group="gas",
        unsigned_only=True,
        include_base1=False,
        invoice_rows_by_utility={
            "C&I Gas": [
                {
                    "MRIN": "99",
                    "Client Name": "Done Deal",
                    "Site Address": "Perth WA 6000",
                    "GJ": "5",
                    "Webview Link": "https://drive.google.com/file/d/z/view",
                }
            ],
            "SME Gas": [],
        },
        file_ids_rows=[
            {
                "business_name": "Done Deal",
                "record_id": "recZ",
                "sc_ci_g_status": "Signed via ACES",
                "sc_ci_e_status": "",
                "sc_sme_e_status": "",
                "sc_sme_g_status": "",
                "sc_waste_status": "",
                "sc_oil_status": "",
                "sc_dma_status": "",
                "sc_ci_e_file": "",
                "sc_ci_g_file": "",
                "sc_sme_e_file": "",
                "sc_sme_g_file": "",
                "sc_waste_file": "",
                "sc_oil_file": "",
                "sc_dma_file": "",
            }
        ],
        crm_clients=[],
        base1_rows=[],
    )
    assert payload["totals"]["site_count"] == 0


def test_build_pipeline_load_by_state_and_pdf_modes():
    rows = {
        "C&I Gas": [
            {
                "MRIN": "A1",
                "Client Name": "North Site",
                "Site Address": "Newcastle NSW 2300",
                "Invoice Review Period": "01/01/2026-31/01/2026",
                "GJ": "31",
                "Webview Link": "https://drive.google.com/file/d/one/view",
            },
            {
                "MRIN": "A1",
                "Client Name": "North Site",
                "Site Address": "Newcastle NSW 2300",
                "Invoice Review Period": "01/12/2025-31/12/2025",
                "GJ": "31",
                "Webview Link": "https://drive.google.com/file/d/two/view",
            },
        ],
        "SME Gas": [],
    }
    all_pdfs = build_pipeline(
        utility_group="gas",
        unsigned_only=True,
        pdfs="all",
        include_base1=False,
        invoice_rows_by_utility=rows,
        file_ids_rows=[],
        crm_clients=[],
        base1_rows=[],
    )
    latest = build_pipeline(
        utility_group="gas",
        unsigned_only=True,
        pdfs="latest",
        include_base1=False,
        invoice_rows_by_utility=rows,
        file_ids_rows=[],
        crm_clients=[],
        base1_rows=[],
    )
    assert all_pdfs["totals"]["site_count"] == 1
    assert all_pdfs["totals"]["pdf_count"] == 2
    assert latest["totals"]["pdf_count"] == 1
    assert all_pdfs["by_state"][0]["state"] == "NSW"
    assert all_pdfs["sites"][0]["unit"] == "GJ"


def test_utilities_without_file_ids_flag_are_unsigned():
    payload = build_pipeline(
        utility_group="water",
        unsigned_only=True,
        include_base1=False,
        invoice_rows_by_utility={
            "Water": [
                {
                    "Account Number": "W-1",
                    "Client Name": "Wet Co",
                    "Supply Address": "Hobart TAS 7000",
                    "kL": "12",
                    "Webview Link": "https://drive.google.com/file/d/w/view",
                }
            ]
        },
        file_ids_rows=[],
        crm_clients=[],
        base1_rows=[],
    )
    site = payload["sites"][0]
    assert site["signed"] is False
    assert site["has_contract_flag"] is False
    assert site["unit"] == "kL"


def test_base1_overlay_skips_crm_and_matches_gas():
    sites = sites_from_base1(
        [
            {
                "Company Name": "Fresh Lead",
                "Contact Email": "a@x.com",
                "State": "VIC",
                "Utility Types": "Gas, Electricity",
                "Google Drive Folder": "https://drive.google.com/drive/folders/abc",
                "Timestamp": "2026-01-02",
            },
            {
                "Company Name": "Already Member",
                "Contact Email": "b@x.com",
                "State": "NSW",
                "Utility Types": "Gas",
                "Google Drive Folder": "https://drive.google.com/drive/folders/zzz",
                "Timestamp": "2026-01-01",
            },
            {
                "Company Name": "Waste Only",
                "State": "QLD",
                "Utility Types": "Waste",
                "Timestamp": "2026-01-03",
            },
        ],
        utility_group="gas",
        existing_names={"already member"},
        existing_emails=set(),
    )
    assert len(sites) == 1
    assert sites[0]["business_name"] == "Fresh Lead"
    assert sites[0]["state"] == "VIC"
    assert sites[0]["source"] == "base1"
    assert sites[0]["pdf_count"] == 1


def test_base1_matches_group():
    assert base1_matches_group("C&I Gas", "gas")
    assert base1_matches_group("electricity nmi", "electricity")
    assert not base1_matches_group("Waste only", "gas")
    assert base1_matches_group("anything", "all")


def test_aggregate_mixed_units_not_summed():
    sites = [
        {
            "state": "VIC",
            "unit": "GJ",
            "annual_load": 10,
            "quoteable": True,
            "invoice_count": 1,
            "invoices": [{"link": "a", "missing": False}],
            "latest_invoice": {"link": "a", "missing": False},
        },
        {
            "state": "VIC",
            "unit": "kWh",
            "annual_load": 1000,
            "quoteable": True,
            "invoice_count": 1,
            "invoices": [{"link": "b", "missing": False}],
            "latest_invoice": {"link": "b", "missing": False},
        },
    ]
    rows = aggregate_by_state(sites, "all")
    assert rows[0]["load_by_unit"]["GJ"] == 10
    assert rows[0]["load_by_unit"]["kWh"] == 1000
    assert rows[0]["site_count"] == 2


def test_thin_data_excluded_from_headline_load():
    rows = {
        "C&I Gas": [
            {
                "MRIN": "THIN",
                "Client Name": "Short Bill",
                "Site Address": "Melbourne VIC 3000",
                "Invoice Review Period": "01/01/2026-31/01/2026",
                "GJ": "31",
                "Retailer": "Origin",
                "Webview Link": "https://drive.google.com/file/d/thin/view",
            },
            {
                "MRIN": "SOLID",
                "Client Name": "Long History",
                "Site Address": "Geelong VIC 3220",
                "Invoice Review Period": "01/01/2026-30/06/2026",
                "GJ": "180",
                "Retailer": "Alinta",
                "Webview Link": "https://drive.google.com/file/d/solid/view",
            },
        ],
        "SME Gas": [],
    }
    payload = build_pipeline(
        utility_group="gas",
        unsigned_only=True,
        include_base1=False,
        invoice_rows_by_utility=rows,
        file_ids_rows=[],
        crm_clients=[],
        base1_rows=[],
    )
    by_id = {s["identifier"]: s for s in payload["sites"]}
    assert by_id["THIN"]["thin_data"] is True
    assert by_id["THIN"]["quoteable"] is False
    assert by_id["SOLID"]["quoteable"] is True
    assert payload["totals"]["quoteable_site_count"] == 1
    assert payload["totals"]["thin_site_count"] == 1
    assert abs(payload["totals"]["load_by_unit"]["GJ"] - by_id["SOLID"]["annual_load"]) < 0.01
    assert "Headline load" in payload["summary"]
    assert by_id["THIN"]["retailer"] == "Origin"


def test_exclude_retailer_and_state_filters():
    rows = {
        "C&I Gas": [
            {
                "MRIN": "1",
                "Client Name": "A",
                "Site Address": "Sydney NSW 2000",
                "Invoice Review Period": "01/01/2026-30/06/2026",
                "GJ": "100",
                "Retailer": "Origin",
                "Webview Link": "https://drive.google.com/file/d/a/view",
            },
            {
                "MRIN": "2",
                "Client Name": "B",
                "Site Address": "Sydney NSW 2000",
                "Invoice Review Period": "01/01/2026-30/06/2026",
                "GJ": "100",
                "Retailer": "Alinta",
                "Webview Link": "https://drive.google.com/file/d/b/view",
            },
        ],
        "SME Gas": [],
    }
    payload = build_pipeline(
        utility_group="gas",
        unsigned_only=True,
        include_base1=False,
        exclude_retailers=["Origin"],
        invoice_rows_by_utility=rows,
        file_ids_rows=[],
        crm_clients=[],
        base1_rows=[],
    )
    assert [s["identifier"] for s in payload["sites"]] == ["2"]


def test_base1_not_in_quoteable_load():
    payload = build_pipeline(
        utility_group="gas",
        unsigned_only=True,
        include_base1=True,
        invoice_rows_by_utility={"C&I Gas": [], "SME Gas": []},
        file_ids_rows=[],
        crm_clients=[],
        base1_rows=[
            {
                "Company Name": "Lead Co",
                "State": "VIC",
                "Utility Types": "Gas",
                "Google Drive Folder": "https://drive.google.com/drive/folders/x",
                "Timestamp": "2026-01-01",
            }
        ],
    )
    assert payload["totals"]["site_count"] == 1
    assert payload["totals"]["quoteable_site_count"] == 0
    assert payload["totals"]["base1_site_count"] == 1
    assert payload["sites"][0]["quoteable"] is False


def test_summary_csv_contains_sites():
    payload = {
        "sites": [
            {
                "state": "VIC",
                "utility_type": "C&I Gas",
                "business_name": "Acme",
                "identifier": "1",
                "annual_load": 12.5,
                "unit": "GJ",
                "load_method": "annualised",
                "invoice_count": 2,
                "pdf_count": 1,
                "signed": False,
                "has_contract_flag": True,
                "source": "member_aces",
                "match_method": "name",
                "latest_invoice": {"label": "Jan", "link": "https://x"},
            }
        ],
        "by_state": [{"state": "VIC", "site_count": 1, "invoice_count": 2, "pdf_count": 1, "load_by_unit": {"GJ": 12.5}}],
    }
    text = summary_csv_bytes(payload).decode("utf-8")
    assert "Acme" in text
    assert "12.5" in text
