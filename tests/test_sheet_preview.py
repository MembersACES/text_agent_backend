"""Unit tests for Member ACES Data sheet preview."""
from unittest.mock import MagicMock, patch

from tools import sheet_preview as preview


def test_get_sheet_preview_loa_reads_top_rows():
    mock_service = MagicMock()
    mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = [
        {
            "values": [
                [
                    "Business Name",
                    "Business ABN",
                    "Trading As",
                    "Contact Name",
                    "Contact  Email  :",
                    "Contact Number:",
                    "Date",
                ]
            ]
        },
        {
            "values": [
                ["Frankston RSL", "12 345 678 901", "RSL", "Jane", "j@rsl.com", "0400", "01/01/2026"],
                ["Old Client", "99", "Old", "Bob", "b@old.com", "0401", "01/01/2025"],
            ]
        },
    ]
    with patch("tools.sheet_preview.get_sheets_service", return_value=mock_service):
        result = preview.get_sheet_preview("LOA", row_count=5)

    assert result["utility_type"] == "LOA"
    assert result["latest_row_number"] == 2
    assert len(result["rows"]) == 2
    assert result["rows"][0]["is_latest"] is True
    assert result["rows"][0]["cells"]["Business Name"] == "Frankston RSL"
    assert result["rows"][1]["row_number"] == 3


def test_unknown_utility_type_returns_empty_rows():
    result = preview.get_sheet_preview("NOT_A_REAL_TYPE")
    assert result["rows"] == []
    assert result["tab"] is None


def test_get_sheet_preview_electricity_ci_columns():
    mock_service = MagicMock()
    mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = [
        {"values": [["NMI", "Client Name", "Retailer", "Site Address", "Other"]]},
        {"values": [["4102007927", "Test Club", "Origin", "1 Main St", "x"]]},
    ]
    with patch("tools.sheet_preview.get_sheets_service", return_value=mock_service):
        result = preview.get_sheet_preview("ELECTRICITY_CI", row_count=3)

    assert "NMI" in result["columns"]
    assert result["rows"][0]["cells"]["NMI"] == "4102007927"


def test_row_fingerprint_stable():
    row = {
        "cells": {
            "Business Name": "Acme",
            "Business ABN": "1",
        }
    }
    assert preview.row_fingerprint(row, ["Business Name", "Business ABN"]) == "Acme|1"
