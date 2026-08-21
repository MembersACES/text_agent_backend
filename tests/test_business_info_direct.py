"""Unit tests for direct Airtable/Sheets business-info paths (no live APIs)."""
from unittest.mock import MagicMock, patch

from tools import business_info


def test_get_file_ids_prefers_sheets_over_n8n():
    with patch.object(business_info, "get_file_ids_from_sheets", return_value={"LOA File ID": "abc"}):
        with patch.object(business_info, "get_file_ids_from_n8n") as n8n_mock:
            result = business_info.get_file_ids("Test Co")
    assert result == {"LOA File ID": "abc"}
    n8n_mock.assert_not_called()


def test_get_file_ids_falls_back_to_n8n_when_sheets_empty():
    with patch.object(business_info, "get_file_ids_from_sheets", return_value={}):
        with patch.object(business_info, "get_file_ids_from_n8n", return_value={"WIP": "wip1"}):
            result = business_info.get_file_ids("Test Co")
    assert result == {"WIP": "wip1"}


def test_search_business_info_from_airtable_builds_linked_details():
    loa = {
        "id": "recTEST123",
        "fields": {
            "Business Name": "Frankston RSL",
            "Trading As": "Frankston",
            "Business ABN": "123",
            "Site Address": "1 Main St",
        },
    }
    with patch("services.airtable_client.USE_AIRTABLE_DIRECT", True):
        with patch("services.airtable_client.AIRTABLE_API_KEY", "key"):
            with patch(
                "services.airtable_client.get_loa_record_by_business_name",
                return_value=loa,
            ):
                with patch(
                    "services.airtable_client.build_business_info_from_loa",
                    return_value={
                        "record_ID": "recTEST123",
                        "business_details": {"name": "Frankston RSL"},
                        "contact_information": {},
                        "representative_details": {},
                        "gdrive": {"folder_url": "https://drive.example/folder"},
                    },
                ):
                    with patch(
                        "services.airtable_client.get_linked_utility_records",
                        return_value=(
                            {"C&I Electricity": ["61020123456", "61020987654"]},
                            {"C&I Electricity": ["Origin", "AGL"]},
                            {},
                        ),
                    ):
                        data = business_info._search_business_info_from_airtable("Frankston RSL")
    assert data is not None
    assert data["record_ID"] == "recTEST123"
    assert data["Linked_Details"]["linked_utilities"]["C&I Electricity"] == "61020123456, 61020987654"
    assert data["Linked_Details"]["utility_retailers"]["C&I Electricity"] == ["Origin", "AGL"]


def test_get_business_information_uses_airtable_when_available():
    airtable_payload = {
        "record_ID": "rec1",
        "business_details": {"name": "Test Co"},
        "contact_information": {},
        "representative_details": {},
        "gdrive": {"folder_url": None},
        "Linked_Details": {"linked_utilities": {}, "utility_retailers": {}},
    }
    with patch.object(business_info, "_search_business_info_from_airtable", return_value=airtable_payload):
        with patch.object(business_info, "_search_business_info_from_n8n") as n8n_mock:
            with patch.object(business_info, "get_file_ids", return_value={"LOA File ID": "file123"}):
                result = business_info.get_business_information("Test Co")
    n8n_mock.assert_not_called()
    assert result["record_ID"] == "rec1"
    assert "_processed_file_ids" in result
    assert result["_processed_file_ids"].get("business_LOA")


def test_get_business_information_falls_back_to_n8n():
    n8n_payload = {
        "record_ID": "rec2",
        "business_details": {"name": "Fallback Co"},
        "contact_information": {},
        "representative_details": {},
        "gdrive": {"folder_url": None},
        "Linked_Details": {"linked_utilities": {}, "utility_retailers": {}},
    }
    with patch.object(business_info, "_search_business_info_from_airtable", return_value=None):
        with patch.object(business_info, "_search_business_info_from_n8n", return_value=n8n_payload):
            with patch.object(business_info, "get_file_ids", return_value={}):
                result = business_info.get_business_information("Fallback Co")
    assert result["record_ID"] == "rec2"


def test_find_sheet_header_row():
    values = [
        ["", ""],
        ["Business Name", "LOA File ID"],
        ["Acme Pty Ltd", "abc"],
    ]
    assert business_info._find_sheet_header_row(values) == 1


def test_get_file_ids_from_sheets_matches_business_name():
    mock_service = MagicMock()
    mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": [
            ["Business Name", "LOA File ID"],
            ["Acme Pty Ltd", "abc123"],
            ["Other Co", "zzz"],
        ]
    }
    with patch.object(business_info, "get_sheets_service", return_value=mock_service):
        row = business_info.get_file_ids_from_sheets("Acme Pty Ltd")
    assert row["Business Name"] == "Acme Pty Ltd"
    assert row["LOA File ID"] == "abc123"
