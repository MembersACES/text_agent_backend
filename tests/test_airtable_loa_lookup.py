"""Unit tests for LOA lookup helpers (no live Airtable)."""
from unittest.mock import patch

from services import airtable_client


def test_resolve_loa_record_id_returns_none_when_zero_matches():
    with patch.object(airtable_client, "get_loa_records_by_business_name", return_value=[]):
        assert airtable_client.resolve_loa_record_id("Unknown Co") is None


def test_resolve_loa_record_id_returns_id_when_one_match():
    with patch.object(
        airtable_client,
        "get_loa_records_by_business_name",
        return_value=[{"id": "recABC123", "fields": {}}],
    ):
        assert airtable_client.resolve_loa_record_id("Solo Co") == "recABC123"


def test_resolve_loa_record_id_returns_none_when_ambiguous():
    with patch.object(
        airtable_client,
        "get_loa_records_by_business_name",
        return_value=[
            {"id": "rec1", "fields": {}},
            {"id": "rec2", "fields": {}},
        ],
    ):
        assert airtable_client.resolve_loa_record_id("Ambiguous Co") is None


def test_get_loa_record_by_business_name_delegates_to_list():
    records = [{"id": "rec1", "fields": {"Site Address": "1 Main St"}}]
    with patch.object(airtable_client, "get_loa_records_by_business_name", return_value=records):
        assert airtable_client.get_loa_record_by_business_name("Test") == records[0]


def test_loa_record_candidate_summary():
    rec = {
        "id": "recXYZ",
        "fields": {
            "Trading As": "Trading Co",
            "Site Address": "10 High St",
            "Business ABN": "12 345 678 901",
        },
    }
    summary = airtable_client.loa_record_candidate_summary(rec)
    assert summary["record_id"] == "recXYZ"
    assert summary["trading_name"] == "Trading Co"
    assert summary["site_address"] == "10 High St"
    assert summary["abn"] == "12 345 678 901"


def test_get_loa_record_by_id_delegates_to_fetch():
    with patch.object(airtable_client, "_fetch_record", return_value={"id": "rec1"}) as mock_fetch:
        result = airtable_client.get_loa_record_by_id("rec1")
        assert result == {"id": "rec1"}
        mock_fetch.assert_called_once_with(airtable_client.LOA_TABLE_ID, "rec1")
