"""Staff collision email uses the existing n8n Gmail send webhook."""

from __future__ import annotations

import partner_collision_notify as notify


def test_recipients_default_and_env_override(monkeypatch):
    monkeypatch.delenv(notify.NOTIFY_EMAILS_ENV, raising=False)
    assert notify.collision_notify_recipients() == ["members@acesolutions.com.au"]
    monkeypatch.setenv(notify.NOTIFY_EMAILS_ENV, "a@acesolutions.com.au, b@acesolutions.com.au")
    assert notify.collision_notify_recipients() == [
        "a@acesolutions.com.au",
        "b@acesolutions.com.au",
    ]
    monkeypatch.setenv(notify.NOTIFY_EMAILS_ENV, "  ")
    assert notify.collision_notify_recipients() == []


def test_email_body_has_full_internal_detail():
    content = notify.build_collision_staff_email(
        {
            "timestamp": "2026-09-18 03:00:00 UTC",
            "distributor_name": "Test Partner",
            "distributor_slug": "test-partner",
            "partner_id": 2,
            "submitted_by_email": "morganhaas69@gmail.com",
            "submitted_business_name": "Sunshine RSL Sub-Branch Inc",
            "existing_client_id": 75,
            "existing_business_name": "Sunshine RSL Sub-Branch Inc",
            "match_kind": notify.MATCH_ACES,
            "folder_url": "https://drive.google.com/drive/folders/folder-3",
            "collision_id": 9,
        }
    )
    assert "Sunshine RSL Sub-Branch Inc" in content["subject"]
    for body in (content["body_text"], content["html_body"]):
        assert "Test Partner" in body
        assert "test-partner" in body
        assert "morganhaas69@gmail.com" in body
        assert "client id 75" in body
        assert notify.MATCH_ACES in body
        assert "https://drive.google.com/drive/folders/folder-3" in body
        assert "2026-09-18 03:00:00 UTC" in body


def test_other_distributor_match_is_labelled():
    content = notify.build_collision_staff_email(
        {
            "distributor_name": "Test Partner",
            "submitted_business_name": "Shared Name",
            "existing_client_id": 10,
            "existing_business_name": "Shared Name",
            "match_kind": notify.MATCH_OTHER_DISTRIBUTOR,
            "matched_partner_name": "Other Co",
        }
    )
    assert notify.MATCH_OTHER_DISTRIBUTOR in content["body_text"]
    assert "Other Co" in content["body_text"]
    assert notify.MATCH_ACES not in content["body_text"]


def test_send_posts_to_existing_gmail_webhook(monkeypatch):
    posted = []

    class _Resp:
        status_code = 200
        text = "ok"

    def fake_post(url, json=None, timeout=None):
        posted.append({"url": url, "json": json, "timeout": timeout})
        return _Resp()

    monkeypatch.delenv("PARTNER_BASE1_SKIP_N8N", raising=False)
    monkeypatch.delenv("PARTNER_COLLISION_SKIP_NOTIFY", raising=False)
    monkeypatch.setenv(notify.NOTIFY_EMAILS_ENV, "members@acesolutions.com.au")
    monkeypatch.setattr(
        notify,
        "share_folder_email_webhook_url",
        lambda: "https://n8n.example/send-email",
    )
    monkeypatch.setattr(notify.httpx, "post", fake_post)
    notify.notify_staff_of_lead_collision(
        {
            "distributor_name": "Test Partner",
            "submitted_business_name": "Sunshine RSL Sub-Branch Inc",
            "existing_client_id": 75,
            "match_kind": notify.MATCH_ACES,
            "folder_url": "https://drive.google.com/drive/folders/abc",
        }
    )
    assert len(posted) == 1
    assert posted[0]["url"] == "https://n8n.example/send-email"
    assert posted[0]["json"]["to"] == "members@acesolutions.com.au"
    assert posted[0]["json"]["event"] == "partner_lead_collision"
    assert "Sunshine RSL Sub-Branch Inc" in posted[0]["json"]["subject"]
