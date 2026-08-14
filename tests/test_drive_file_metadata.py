"""Unit tests for Drive file-id extraction and metadata batching (no live Drive)."""
from unittest.mock import MagicMock, patch

from tools.drive_file_metadata import extract_drive_file_id, get_drive_file_times


def test_extract_drive_file_id_from_file_url():
    assert (
        extract_drive_file_id(
            "https://drive.google.com/file/d/abc1234567890/view?usp=drivesdk"
        )
        == "abc1234567890"
    )


def test_extract_drive_file_id_from_spreadsheet_url():
    assert (
        extract_drive_file_id("https://docs.google.com/spreadsheets/d/sheetId1234567890/edit")
        == "sheetId1234567890"
    )


def test_extract_drive_file_id_from_raw_id():
    assert extract_drive_file_id("abc1234567890xyz") == "abc1234567890xyz"


def test_extract_drive_file_id_ignores_status_text():
    assert extract_drive_file_id("Signed via ACES") == ""
    assert extract_drive_file_id("") == ""
    assert extract_drive_file_id(None) == ""


def test_get_drive_file_times_empty():
    assert get_drive_file_times([]) == {}
    assert get_drive_file_times(["Signed via ACES"]) == {}


def test_get_drive_file_times_batches_unique_ids():
    drive = MagicMock()
    captured: dict = {}

    def new_batch(callback=None):
        captured["callback"] = callback
        batch = MagicMock()

        def execute():
            captured["callback"](
                "0",
                {
                    "id": "abc1234567890",
                    "createdTime": "2026-08-01T03:00:00.000Z",
                    "modifiedTime": None,
                },
                None,
            )

        batch.execute.side_effect = execute
        captured["batch"] = batch
        return batch

    drive.new_batch_http_request.side_effect = new_batch

    with patch("tools.drive_file_metadata.get_drive_service", return_value=drive):
        result = get_drive_file_times(
            [
                "https://drive.google.com/file/d/abc1234567890/view",
                "abc1234567890",
            ]
        )

    assert result["abc1234567890"]["created_time"] == "2026-08-01T03:00:00.000Z"
    assert captured["batch"].add.call_count == 1
