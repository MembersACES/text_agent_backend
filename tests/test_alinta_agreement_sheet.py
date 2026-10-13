from tools.alinta_agreement_sheet import (
    ELECTRICITY_COLUMNS,
    GAS_COLUMNS,
    append_tracker_row,
    gmail_ids_from_webhook,
    tracker_row,
)


def test_gas_row_matches_headers_and_leaves_the_queue_open():
    row = tracker_row(
        GAS_COLUMNS,
        {
            "sent_at": "2026-10-13 09:04",
            "sent_by": "morgan.h@acesolutions.com.au",
            "client_id": "18",
            "company_name": "Gladius Hotels Pty Ltd",
            "mirn": "5310123456",
            "request_kind": "Retention",
            "email_subject": "Agreement Request: G-C&I (GJ) 19000 Retention Gladius MIRN 5310123456",
            "gmail_thread_id": "thread-1",
            "gmail_message_id": "msg-1",
            "price_per_gj": "$14.70",
            "days_waiting": "=NETWORKDAYS(B2,TODAY())",
            "status": "",
        },
    )
    assert len(row) == len(GAS_COLUMNS)
    assert row[GAS_COLUMNS.index("status")] == "Waiting"
    assert row[GAS_COLUMNS.index("days_waiting")] == 0
    assert row[GAS_COLUMNS.index("mirn")] == "5310123456"
    assert row[GAS_COLUMNS.index("gmail_thread_id")] == "thread-1"
    assert row[GAS_COLUMNS.index("received_at")] == ""
    assert row[GAS_COLUMNS.index("returned_price_per_gj")] == ""
    assert row[GAS_COLUMNS.index("check_result")] == ""
    assert row[GAS_COLUMNS.index("ready_at")] == ""


def test_electricity_row_uses_nmis_not_mirn():
    row = tracker_row(
        ELECTRICITY_COLUMNS,
        {"nmis": "4311234567, 4311234568", "peak_rate_c_kwh": "12.5"},
    )
    assert "mirn" not in ELECTRICITY_COLUMNS
    assert row[ELECTRICITY_COLUMNS.index("nmis")] == "4311234567, 4311234568"
    assert row[ELECTRICITY_COLUMNS.index("returned_nmis")] == ""
    assert row[ELECTRICITY_COLUMNS.index("days_waiting")] == 0


def test_gmail_ids_from_webhook_body():
    assert gmail_ids_from_webhook("") == ("", "")
    assert gmail_ids_from_webhook("sent") == ("", "")
    assert gmail_ids_from_webhook('{"ok": true, "threadId": "t1", "messageId": "m1"}') == ("t1", "m1")
    assert gmail_ids_from_webhook('[{"threadId": "t2", "id": "m2"}]') == ("t2", "m2")
    assert gmail_ids_from_webhook('{"id": "not-a-gmail-id"}') == ("", "")


def test_append_reports_a_missing_sheets_client(monkeypatch):
    monkeypatch.setattr("tools.business_info.get_sheets_service", lambda: None)
    error = append_tracker_row("Gas", GAS_COLUMNS, {"company_name": "Acme"})
    assert error == "Google Sheets is not configured"
