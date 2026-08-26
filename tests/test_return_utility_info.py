"""Unit tests for return_utility_info direct Sheets path."""
from unittest.mock import MagicMock, patch

from tools import return_utility_info as util


def test_get_return_utility_info_prefers_sheets():
    row = {"NMI": "123", "Client Name": "Test Co", "row_number": 2}
    with patch.object(util, "get_latest_utility_info_from_sheets", return_value=row):
        with patch.object(util, "get_return_utility_info_from_n8n") as n8n_mock:
            result = util.get_return_utility_info("ELECTRICITY_CI", "Acme")
    assert result == [row]
    n8n_mock.assert_not_called()


def test_get_return_utility_info_falls_back_to_n8n():
    n8n_row = [{"NMI": "999"}]
    with patch.object(util, "get_latest_utility_info_from_sheets", return_value=None):
        with patch.object(util, "get_return_utility_info_from_n8n", return_value=n8n_row):
            result = util.get_return_utility_info("WASTE", "Acme")
    assert result == n8n_row


def test_loa_type_delegates_to_business_details():
    loa_row = [{"Business Name": "LOA Co", "row_number": 2}]
    with patch("tools.return_utility_info.get_return_business_details", return_value=loa_row):
        result = util.get_return_utility_info("LOA", "LOA Co")
    assert result == loa_row


def test_get_latest_utility_info_reads_row_two():
    mock_service = MagicMock()
    mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = [
        {"values": [["NMI", "Client Name", "Retailer"]]},
        {"values": [["4102007927", "GOSFORD SAILING CLUB LTD", "Origin"]]},
    ]
    with patch("tools.return_utility_info.get_sheets_service", return_value=mock_service):
        row = util.get_latest_utility_info_from_sheets("ELECTRICITY_SME")
    assert row is not None
    assert row["NMI"] == "4102007927"
    assert row["row_number"] == 2


def test_unknown_utility_type_returns_empty():
    assert util.get_return_utility_info("UNKNOWN_TYPE") == []
