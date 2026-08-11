"""Unit tests for invoicing Drive helpers (no live Drive calls)."""

from __future__ import annotations

from tools.invoicing_access import (
    is_email_in_invoicing_allowlist,
    parse_invoicing_allowlist,
)
from tools.invoicing_drive import (
    get_category_config,
    infer_invoice_number,
    oms_business_key,
    parse_oms_filename,
)


def test_parse_allowlist():
    assert parse_invoicing_allowlist("a@x.com, B@Y.COM") == ["a@x.com", "b@y.com"]


def test_allowlist_empty_denies(monkeypatch):
    monkeypatch.delenv("INVOICING_ALLOWED_EMAILS", raising=False)
    monkeypatch.delenv("NEXT_PUBLIC_INVOICING_ALLOWED_EMAILS", raising=False)
    assert is_email_in_invoicing_allowlist("a@x.com") is False


def test_allowlist_match(monkeypatch):
    monkeypatch.setenv("INVOICING_ALLOWED_EMAILS", "a@x.com,b@y.com")
    assert is_email_in_invoicing_allowlist("A@X.com") is True
    assert is_email_in_invoicing_allowlist("other@x.com") is False


def test_get_allowlist_raw_source(monkeypatch):
    from tools.invoicing_access import get_invoicing_allowlist_raw

    monkeypatch.delenv("INVOICING_ALLOWED_EMAILS", raising=False)
    monkeypatch.delenv("NEXT_PUBLIC_INVOICING_ALLOWED_EMAILS", raising=False)
    raw, source = get_invoicing_allowlist_raw()
    assert raw == ""
    assert source == ""

    monkeypatch.setenv("INVOICING_ALLOWED_EMAILS", "a@x.com")
    raw, source = get_invoicing_allowlist_raw()
    assert "a@x.com" in raw
    assert source == "INVOICING_ALLOWED_EMAILS"


def test_category_keys():
    assert get_category_config("automation_services") is not None
    assert get_category_config("one_month_savings") is not None
    assert get_category_config("bogus") is None


def test_oms_filename_parse():
    biz, inv = parse_oms_filename("Acme Pty Ltd - INV-1001.pdf")
    assert biz == "Acme Pty Ltd"
    assert inv == "INV-1001"
    assert parse_oms_filename("no-dash.pdf") == (None, None)


def test_oms_business_key_stable():
    assert oms_business_key("Acme") == oms_business_key("  ACME ")
    assert oms_business_key("Acme").startswith("oms_")


def test_infer_invoice_number_simple():
    assert infer_invoice_number("Acme - INV-42.pdf") == "INV-42"
    assert infer_invoice_number("random notes.pdf") is None
