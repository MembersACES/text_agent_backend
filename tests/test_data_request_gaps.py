"""
Chasing a coverage gap by email.

The Data disclosure page finds the months an entity has no invoice for; this is
the other half — turning that finding into a request the retailer can act on.

Two things had to be true before the button was worth having:

1. The email must name the missing periods. The C&I electricity template asked
   only for "the most recent invoice", and every Centurion gap is C&I
   electricity — so the button would have sent an email that does not ask for
   the data we are missing.
2. The internal copy must be on every retailer, not most of them.

Both are pinned here, along with the rule that a request sent the old way (no
months supplied) still reads exactly as it always did.
"""
import json
import sys
import types
from datetime import date

import pytest

# supplier_data_request pulls in requests + business_info at import time; neither
# is exercised by template rendering, so stub them rather than drag in the world.
sys.modules.setdefault("requests", types.ModuleType("requests"))
if "tools.business_info" not in sys.modules:
    _bi = types.ModuleType("tools.business_info")
    _bi.get_business_information = lambda *a, **k: {}
    sys.modules["tools.business_info"] = _bi

from tools.supplier_data_request import (  # noqa: E402
    EMAIL_TEMPLATES,
    RETAILER_EMAILS,
    _invoice_attachment_clause,
    _missing_months_clause,
)
from services.climate_data_gaps import _evidence_uri_of, month_label  # noqa: E402

INTERNAL_CC = "data.quote@fornrg.com"
TEMPLATE_VARS = {
    "business_name": "Centurion Transport",
    "nmi": "2002323086",
    "mrin": "5323746742",
    "account_number": "ACC-1",
}


# --- the rider --------------------------------------------------------------

@pytest.mark.parametrize("empty", [None, [], ["", "   "]])
def test_no_months_means_no_rider(empty):
    """A request sent the old way must read exactly as it always did."""
    assert _missing_months_clause(empty) == ""


def test_rider_names_every_month():
    out = _missing_months_clause(["Jul 2025", "Sep 2025", "Jan 2026"])
    assert "Jul 2025, Sep 2025, Jan 2026" in out
    assert out.startswith(" ")  # appends inside the <li>, so it needs the space


def test_rider_is_capped_so_a_broken_report_cannot_flood_the_email():
    out = _missing_months_clause([f"M{i}" for i in range(40)])
    assert out.count(",") == 12
    assert "and earlier" in out


# --- templates --------------------------------------------------------------

@pytest.mark.parametrize("key", sorted(EMAIL_TEMPLATES))
def test_every_template_renders_with_and_without_the_rider(key):
    cfg = EMAIL_TEMPLATES[key]
    rider = _missing_months_clause(["Jul 2025", "Sep 2025"])
    without = cfg["template"].format(
        **TEMPLATE_VARS, missing_months_clause="", invoice_attachment_clause=""
    )
    with_ = cfg["template"].format(
        **TEMPLATE_VARS, missing_months_clause=rider, invoice_attachment_clause=""
    )
    cfg["subject"].format(**TEMPLATE_VARS)
    assert "{" not in without and "}" not in without
    assert len(with_) - len(without) == len(rider)
    assert "Jul 2025, Sep 2025" in with_


@pytest.mark.parametrize("key", sorted(EMAIL_TEMPLATES))
def test_every_template_asks_for_invoices(key):
    """The rider hangs off the invoice line; no invoice line, no ask."""
    assert "invoice" in EMAIL_TEMPLATES[key]["template"].lower()


def test_the_five_supported_request_types_all_have_a_template():
    for service_type in ("electricity_ci", "electricity_sme", "gas_ci", "gas_sme", "waste"):
        assert f"{service_type}_data" in EMAIL_TEMPLATES


@pytest.mark.parametrize("key", sorted(EMAIL_TEMPLATES))
def test_invoice_is_not_promised_as_attached_until_the_workflow_attaches_it(key):
    """The webhook stores invoice_url and does not attach the file yet."""
    assert _invoice_attachment_clause(None) == ""
    assert _invoice_attachment_clause("   ") == ""
    assert _invoice_attachment_clause("https://drive.google.com/file/d/abc/view") == ""
    rendered = EMAIL_TEMPLATES[key]["template"].format(
        **TEMPLATE_VARS, missing_months_clause="", invoice_attachment_clause=""
    )
    assert "A recent invoice for this account, for reference" not in rendered


def test_attachment_line_appears_once_the_workflow_can_attach(monkeypatch):
    import tools.supplier_data_request as sdr

    monkeypatch.setattr(sdr, "ATTACH_HELD_INVOICE", True)
    clause = sdr._invoice_attachment_clause("https://drive.google.com/file/d/abc/view")
    assert "A recent invoice for this account" in clause
    assert sdr._invoice_attachment_clause(None) == ""


# --- internal copy ----------------------------------------------------------

@pytest.mark.parametrize("retailer", sorted(RETAILER_EMAILS))
def test_every_retailer_copies_the_internal_address(retailer):
    assert INTERNAL_CC in RETAILER_EMAILS[retailer]["email"], retailer


# --- what the gap report hands over ----------------------------------------

def test_month_label_is_what_a_retailer_reads():
    assert month_label("2025-07") == "Jul 2025"
    assert month_label("2026-01") == "Jan 2026"


@pytest.mark.parametrize("junk", ["", "nonsense", "2025-13", None])
def test_month_label_never_raises(junk):
    month_label(junk)


class _Row:
    def __init__(self, body):
        self.body_json = body


def test_invoice_link_is_pulled_off_the_staged_row():
    row = _Row(json.dumps({"evidence_refs": [
        {"evidence_uri": "https://drive.google.com/file/d/abc/view"},
    ]}))
    assert _evidence_uri_of(row) == "https://drive.google.com/file/d/abc/view"


def test_first_http_ref_wins_and_non_links_are_skipped():
    row = _Row(json.dumps({"evidence_refs": [
        {"evidence_uri": "airtable:recXYZ"},
        {"evidence_uri": "  https://drive.google.com/file/d/two/view  "},
    ]}))
    assert _evidence_uri_of(row) == "https://drive.google.com/file/d/two/view"


@pytest.mark.parametrize("body", ["", "{}", "not json", None, '{"evidence_refs": []}'])
def test_a_row_with_no_usable_link_returns_none_rather_than_exploding(body):
    assert _evidence_uri_of(_Row(body)) is None


# --- the whole call path ----------------------------------------------------

class _FakeResponse:
    status_code = 200
    text = '{"status": "success", "request_id": "req-1"}'

    def json(self):
        return {"status": "success", "request_id": "req-1"}


def test_the_request_that_actually_goes_out(monkeypatch):
    """
    End to end: a Centurion C&I electricity gap, sent.

    Pins the three things Morgan asked for - the retailer it goes to, the
    internal copy, and an invoice link travelling with it - plus the months.
    """
    import tools.supplier_data_request as sdr

    sent = {}

    def _post(url, json=None, headers=None, timeout=None):
        sent["url"] = url
        sent["payload"] = json
        return _FakeResponse()

    monkeypatch.setattr(sdr.requests, "post", _post, raising=False)

    result = sdr.supplier_data_request(
        supplier_name="AGL",
        business_name="Centurion Transport",
        service_type="electricity_ci",
        account_identifier="2002323086",
        identifier_type="NMI",
        missing_months=["Jul 2025", "Sep 2025", "Jan 2026"],
        invoice_url="https://drive.google.com/file/d/abc/view",
    )

    payload = sent["payload"]
    assert INTERNAL_CC in payload["supplier_email"]
    assert payload["invoice_url"] == "https://drive.google.com/file/d/abc/view"
    assert payload["missing_months"] == ["Jul 2025", "Sep 2025", "Jan 2026"]
    assert "2002323086" in payload["email_subject"]
    assert "Jul 2025, Sep 2025, Jan 2026" in payload["email_body"]
    assert "A recent invoice for this account" not in payload["email_body"]
    assert "{" not in payload["email_body"]
    assert result.startswith("✅") or "successfully sent" in result


def test_a_request_with_no_gap_reads_as_it_always_did(monkeypatch):
    import tools.supplier_data_request as sdr

    sent = {}
    monkeypatch.setattr(
        sdr.requests,
        "post",
        lambda url, json=None, headers=None, timeout=None: (
            sent.update(payload=json) or _FakeResponse()
        ),
        raising=False,
    )

    sdr.supplier_data_request(
        supplier_name="AGL",
        business_name="Centurion Transport",
        service_type="electricity_ci",
        account_identifier="2002323086",
        identifier_type="NMI",
    )
    body = sent["payload"]["email_body"]
    assert "in particular" not in body
    assert "Specifically, we hold no invoice" not in body
    assert "A recent invoice for this account" not in body
    assert sent["payload"]["invoice_url"] == ""
    assert sent["payload"]["missing_months"] == []
