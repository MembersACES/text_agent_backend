"""Unit tests for member EOI/WIP direct Sheets paths."""
from unittest.mock import patch

from tools import member_documents


def test_get_eoi_ids_prefers_sheets():
    rows = [{"EOI Type": "Demand Response", "EOI File ID": "abc123456789"}]
    with patch.object(member_documents, "get_eoi_ids_from_sheets", return_value=rows):
        with patch.object(member_documents, "get_eoi_ids_from_n8n") as n8n_mock:
            result = member_documents.get_eoi_ids("Test Co")
    assert result == rows
    n8n_mock.assert_not_called()


def test_get_eoi_ids_falls_back_to_n8n():
    with patch.object(member_documents, "get_eoi_ids_from_sheets", return_value=[]):
        with patch.object(member_documents, "get_eoi_ids_from_n8n", return_value=[{"EOI Type": "X"}]):
            result = member_documents.get_eoi_ids("Test Co")
    assert len(result) == 1


def test_get_member_wip_from_sheets_shape():
    with patch.object(member_documents, "get_file_ids_from_sheets", return_value={"WIP": "sheetId123456789012345"}):
        with patch.object(member_documents, "_read_additional_documents", return_value=[{"File Name": "Doc", "File ID": "f1"}]):
            with patch.object(
                member_documents,
                "_signed_ef_row_from_member_wip",
                return_value={"EF Type": "Solar", "Business": "Test", "Signed Date": "01/01/26", "File Name": "ef.pdf"},
            ):
                with patch.object(
                    member_documents,
                    "_engagement_forms_from_central_sheet",
                    return_value=[{"fileId": "ef1", "name": "ef.pdf"}],
                ):
                    data = member_documents.get_member_wip_from_sheets("Test Co")
    assert data is not None
    assert data["ok"] is True
    assert len(data["additional_documents"]) == 1
    assert len(data["engagement_forms"]) == 1


def test_extract_google_id_from_url():
    assert member_documents._extract_google_id(
        "https://docs.google.com/spreadsheets/d/abc123/edit"
    ) == "abc123"
    assert member_documents._extract_google_id("abc123456789012345678") == "abc123456789012345678"
