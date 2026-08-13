"""Unit tests for invoicing Drive helpers (no live Drive calls)."""

from __future__ import annotations

from tools.invoicing_access import (
    is_email_in_invoicing_allowlist,
    parse_invoicing_allowlist,
)
from tools.invoicing_drive import (
    get_category_config,
    infer_invoice_number,
    list_category_keys,
    oms_business_key,
    parse_oms_filename,
    retailer_synthetic_id,
    _sort_documents_newest_first,
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
    for key in (
        "alinta_ci_electricity",
        "alinta_ci_gas",
        "origin_ci_electricity",
        "origin_ci_gas",
        "trojan_oil",
        "momentum_ci_electricity",
    ):
        cfg = get_category_config(key)
        assert cfg is not None
        assert cfg.discovery == "pdfs_in_parent"
    assert "trojan_oil" in list_category_keys()
    assert retailer_synthetic_id("trojan_oil") == "retailer_trojan_oil"


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


def test_sort_documents_newest_first():
    docs = [
        {"name": "old.pdf", "created_time": "2025-01-01T00:00:00.000Z"},
        {"name": "new.pdf", "created_time": "2026-08-01T00:00:00.000Z"},
        {"name": "mid.pdf", "created_time": "2026-03-15T00:00:00.000Z"},
    ]
    _sort_documents_newest_first(docs)
    assert [d["name"] for d in docs] == ["new.pdf", "mid.pdf", "old.pdf"]
