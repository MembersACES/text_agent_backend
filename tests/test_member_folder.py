"""Unit tests for member/distributor folder helpers (no live Drive/Airtable)."""

from services.member_folder import merge_linked_ids
from tools.distributor_agreement import (
    distributor_folder_name,
    extract_distribution_agreement,
)
from tools.member_folder_drive import (
    member_site_folder_name,
    member_wip_spreadsheet_name,
    staging_loa_filename_matches,
)


def test_staging_loa_matches_invoice_api_cleaned_name():
    assert staging_loa_filename_matches(
        "Specialised Linen Services Brisbane Pty Ltd_LOA_2026-08-25.pdf",
        "Specialised Linen Services (Brisbane) Pty Ltd",
    )
    assert staging_loa_filename_matches(
        "Acme Pty Ltd_LOA_Unknown.pdf",
        "Acme Pty Ltd",
    )
    assert staging_loa_filename_matches(
        "Joe's Cafe_LOA_2026-01-01.pdf",
        "Joe's Cafe",
    )
    assert staging_loa_filename_matches(
        "Joes Cafe_LOA_2026-01-01.pdf",
        "Joe's Cafe",
    )
    assert not staging_loa_filename_matches(
        "Other Co Pty Ltd_LOA_2026-08-25.pdf",
        "Acme Pty Ltd",
    )


def test_member_wip_spreadsheet_name():
    assert member_wip_spreadsheet_name("Acme Pty Ltd") == "Acme Pty Ltd Work in Progress"


def test_member_site_folder_name():
    assert member_site_folder_name("Acme Pty Ltd", "Acme") == "Acme Pty Ltd - Acme"
    assert member_site_folder_name("Acme Pty Ltd", "") == "Acme Pty Ltd - N/A"
    assert member_site_folder_name("Acme Pty Ltd", "  ") == "Acme Pty Ltd - N/A"


def test_distributor_folder_name_strips_pty_ltd():
    assert (
        distributor_folder_name("Specialist Energy Management Pty Ltd")
        == "A - Specialist Energy Management"
    )
    assert distributor_folder_name("A - Reddrop") == "A - Reddrop"
    assert distributor_folder_name("", "Sercon") == "A - Sercon"


def test_merge_linked_ids_avoids_duplicates():
    assert merge_linked_ids(None, "recABC") == ["recABC"]
    assert merge_linked_ids(["recABC"], "recABC") == ["recABC"]
    assert merge_linked_ids(["recABC"], "recDEF") == ["recABC", "recDEF"]
    assert merge_linked_ids("recABC", "recDEF") == ["recABC", "recDEF"]


SAMPLE_AGREEMENT = """
DISTRIBUTION AGREEMENT
Distributor: Specialist Energy Management Pty Ltd
ABN:
Adress: 448, Lilydale VIC 3140],
Email: nigel@specialistenergy.com.au.
Phone: 61 3 8751 2288
Mobile: 04 33 283 333
Start date: 1/9/2026
Area: Australia.
Initial term: 24 months.
You are exclusive to us.
Name: _______________________________
Position: _____________________________
Date: ________________________________
Name: Nigel Daviot
Position: Director
Date: _______24/8/2026_________________________
"""


def test_extract_distribution_agreement_from_text(monkeypatch):
    monkeypatch.setattr(
        "tools.distributor_agreement._pdf_to_text",
        lambda _b: SAMPLE_AGREEMENT,
    )
    monkeypatch.setattr("tools.distributor_agreement._pdf_page_images", lambda _b: [])
    out = extract_distribution_agreement(b"%PDF-fake")
    assert out["distributor_business"] == "Specialist Energy Management Pty Ltd"
    assert out["email"] == "nigel@specialistenergy.com.au"
    assert out["phone"] == "61 3 8751 2288"
    assert out["state"] == "VIC"
    assert out["postcode"] == "3140"
    assert out["initial_term_months"] == "24"
    assert out["exclusivity"] == "Y"
    assert out["folder_name"] == "A - Specialist Energy Management"
    assert out["contact_name"] == "Nigel Daviot"
    assert out["contact_position"] == "Director"
    assert "24/8/2026" in out["signed_date"]


def test_extract_distribution_agreement_from_scan(monkeypatch):
    monkeypatch.setattr("tools.distributor_agreement._pdf_to_text", lambda _b: "")
    monkeypatch.setattr(
        "tools.distributor_agreement._pdf_page_images",
        lambda _b: [b"fake-scan-bytes"],
    )
    monkeypatch.setattr(
        "tools.distributor_agreement._extract_from_page_images",
        lambda _b: {
            "distributor_business": "Specialist Energy Management Pty Ltd",
            "trading_as": "",
            "abn": "27159882649",
            "acn": "",
            "contact_name": "Nigel Daviot",
            "contact_position": "Director",
            "email": "nigel@specialistenergy.com.au",
            "phone": "61 3 8751 2288",
            "mobile": "04 33 283 333",
            "address": "P.O. Box 448, Lilydale VIC 3140",
            "state": "VIC",
            "postcode": "3140",
            "start_date": "1/9/2026",
            "signed_date": "24/8/2026",
            "initial_term_months": "24",
            "territory": "Australia",
            "exclusivity": "Y",
            "notes": "",
        },
    )
    out = extract_distribution_agreement(b"%PDF-scan")
    assert out["distributor_business"] == "Specialist Energy Management Pty Ltd"
    assert out["abn"] == "27159882649"
    assert out["address"].startswith("P.O. Box 448")
    assert out["folder_name"] == "A - Specialist Energy Management"
    assert out["contact_name"] == "Nigel Daviot"
    assert "scanned" in " ".join(out["extraction_warnings"]).lower()


def test_list_distributor_master_rows(monkeypatch):
    from tools.distributor_agreement import list_distributor_master_rows

    class FakeValues:
        def get(self, **_kwargs):
            return self

        def execute(self):
            return {
                "values": [
                    ["Distributor Business", "Email", "Drive Folder URL"],
                    ["Alpha Pty Ltd", "a@x.com", "https://drive.google.com/a"],
                    ["Beta Co", "b@x.com", "https://drive.google.com/b"],
                ]
            }

    class FakeSheets:
        def spreadsheets(self):
            return self

        def values(self):
            return FakeValues()

    monkeypatch.setattr(
        "tools.distributor_agreement.resolve_distributor_master_sheet",
        lambda _token: ("sheet123", "Sheet1"),
    )
    monkeypatch.setattr(
        "tools.distributor_agreement._user_sheets_service",
        lambda _token: FakeSheets(),
    )
    out = list_distributor_master_rows(user_access_token="token")
    assert out["spreadsheet_id"] == "sheet123"
    assert out["tab"] == "Sheet1"
    assert [r["Distributor Business"] for r in out["rows"]] == ["Beta Co", "Alpha Pty Ltd"]
