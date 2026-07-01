"""Unit tests for LOA return_business_details direct Sheets path."""
from unittest.mock import MagicMock, patch

from tools import loa_business_details as loa


def test_get_return_business_details_prefers_sheets():
    row = {"Business Name": "Test Co", "Trading As": "Test", "row_number": 2}
    with patch.object(loa, "get_latest_loa_business_details_from_sheets", return_value=row):
        with patch.object(loa, "get_latest_loa_business_details_from_n8n") as n8n_mock:
            result = loa.get_return_business_details()
    assert result == [row]
    n8n_mock.assert_not_called()


def test_get_return_business_details_falls_back_to_n8n():
    n8n_row = [{"Business Name": "N8N Co"}]
    with patch.object(loa, "get_latest_loa_business_details_from_sheets", return_value=None):
        with patch.object(loa, "get_latest_loa_business_details_from_n8n", return_value=n8n_row):
            result = loa.get_return_business_details()
    assert result == n8n_row


def test_row_dict_from_sheet_maps_headers():
    headers = ["Business Name", "Trading As", "Date"]
    row = ["Acme Pty Ltd", "Acme", "01/01/26"]
    d = loa._row_dict_from_sheet(headers, row)
    assert d["Business Name"] == "Acme Pty Ltd"
    assert d["Date"] == "01/01/26"


def test_get_latest_loa_from_sheets_reads_row_two():
    mock_service = MagicMock()
    mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = [
        {"values": [["Business Name", "Trading As", "Date"]]},
        {"values": [["Latest Co", "Latest", "24/06/2026"]]},
    ]
    with patch.object(loa, "get_sheets_service", return_value=mock_service):
        row = loa.get_latest_loa_business_details_from_sheets()
    assert row is not None
    assert row["Business Name"] == "Latest Co"
    assert row["row_number"] == 2
