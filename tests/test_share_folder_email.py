"""Unit tests for branded Shared Folder emails."""

from unittest.mock import MagicMock, patch

from tools.share_folder_email import build_share_folder_email, send_share_folder_emails


def test_build_share_folder_email_contains_brand_and_link():
    content = build_share_folder_email(
        business_name="Frankston RSL Sub Branch Inc",
        folder_url="https://drive.google.com/drive/folders/abc",
        file_names=["Quote.pdf", "Summary.docx"],
        sender_name="Max H",
        sender_email="max.h@acesolutions.com.au",
    )
    assert "Carbon Zero Australasia" in content["subject"]
    assert "Frankston RSL" in content["subject"]
    assert "Open shared documents" in content["html_body"]
    assert "https://drive.google.com/drive/folders/abc" in content["html_body"]
    assert "Quote.pdf" in content["html_body"]
    assert "Max H" in content["html_body"]
    assert "Frankston RSL" in content["body_text"]


def test_send_share_folder_emails_posts_once_per_recipient():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "ok"
    with patch("tools.share_folder_email.httpx.post", return_value=mock_response) as post:
        results = send_share_folder_emails(
            recipients=["a@x.com", "b@x.com"],
            business_name="Test Co",
            folder_url="https://drive.google.com/drive/folders/abc",
            file_names=["Doc.pdf"],
            sender_name="Max",
            sender_email="max.h@acesolutions.com.au",
        )
    assert [row["action"] for row in results] == ["sent", "sent"]
    assert post.call_count == 2
    first_payload = post.call_args_list[0].kwargs["json"]
    assert first_payload["to"] == "a@x.com"
    assert first_payload["event"] == "share_folder_email"
    assert "Carbon Zero Australasia" in first_payload["subject"]
