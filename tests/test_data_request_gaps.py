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
    _invoice_ask,
    _invoice_attachment_clause,
    place_missing_invoice_ask,
)
from services.climate_data_gaps import _evidence_uri_of, month_label  # noqa: E402

INTERNAL_CC = "data.quote@fornrg.com"
TEMPLATE_VARS = {
    "business_name": "Centurion Transport",
    "nmi": "2002323086",
    "mrin": "5323746742",
    "account_number": "ACC-1",
}


# --- the ask ----------------------------------------------------------------

@pytest.mark.parametrize("empty", [None, [], ["", "   "]])
def test_no_months_means_the_template_is_left_alone(empty):
    """A request sent the old way must read exactly as it always did."""
    body = "<li>Copy of the most recent invoice</li><p>Kind Regards,</p>"
    assert _invoice_ask(empty) == ""
    assert place_missing_invoice_ask(body, empty) == body


def test_the_invoice_bullet_is_replaced_with_the_missing_periods():
    body = "<ul><li>Copy of the most recent invoice</li></ul><p>Kind Regards,</p><p>NOTE: confidential</p>"
    out = place_missing_invoice_ask(body, ["Jul 2025", "Sep 2025", "Jan 2026"])
    assert "Copy of the most recent invoice" not in out
    assert "Copies of the invoices for these periods, which we do not hold: Jul 2025, Sep 2025, Jan 2026" in out
    assert out.index("Jul 2025") < out.index("Kind Regards")
    assert "Specifically, we hold no invoice" not in out


def test_a_saved_template_does_not_hide_the_ask_under_the_footer():
    """The email that went to Momentum: generic bullet, months dumped after the note."""
    body = """<p>Can you please provide the below information:</p>
<ul>
<li>12 Months Interval Data</li>
<li>Contract End Date</li>
<li>Direct Metering Agreement End Date</li>
<li>Copy of the most recent invoice</li>
</ul>
<p>Please see attached:</p>
<ul>
<li>The Letter of Authority</li>
</ul>
<p>Kind Regards,</p>
<p>NOTE: This email, including any attachments, is strictly confidential.</p>"""
    out = place_missing_invoice_ask(
        body,
        ["Jul 2025", "Aug 2025", "Sep 2025", "Oct 2025", "Nov 2025", "Dec 2025", "Jan 2026", "Feb 2026", "Mar 2026"],
    )
    assert "Copy of the most recent invoice" not in out
    assert out.index("Mar 2026") < out.index("Please see attached")
    assert out.index("Mar 2026") < out.index("Kind Regards")
    assert not out.strip().endswith("Mar 2026</p>") and "NOTE:" in out


def test_the_ask_is_capped_so_a_broken_report_cannot_flood_the_email():
    out = _invoice_ask([f"M{i}" for i in range(40)])
    assert "M0, M1, M2, M3, M4, M5, M6, M7, M8, M9, M10, M11, and earlier" in out
    assert "M12" not in out


# --- templates --------------------------------------------------------------

@pytest.mark.parametrize("key", sorted(EMAIL_TEMPLATES))
def test_every_template_renders_and_the_gap_rewrites_the_invoice_bullet(key):
    cfg = EMAIL_TEMPLATES[key]
    rendered = cfg["template"].format(**TEMPLATE_VARS, invoice_attachment_clause="")
    cfg["subject"].format(**TEMPLATE_VARS)
    assert "{" not in rendered and "}" not in rendered
    asked = place_missing_invoice_ask(rendered, ["Jul 2025", "Sep 2025"])
    assert "Jul 2025, Sep 2025" in asked
    assert asked.index("Jul 2025") < asked.index("Kind Regards")


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
        **TEMPLATE_VARS, invoice_attachment_clause=""
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
    body = payload["email_body"]
    assert "Copy of the most recent invoice" not in body
    assert "Copies of the invoices for these periods, which we do not hold: Jul 2025, Sep 2025, Jan 2026" in body
    assert body.index("Jul 2025") < body.index("Kind Regards")
    assert "Specifically, we hold no invoice" not in body
    assert "A recent invoice for this account" not in body
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
    assert "Copy of the most recent invoice" in body
    assert "Copies of the invoices for these periods" not in body
    assert "Specifically, we hold no invoice" not in body
    assert "A recent invoice for this account" not in body
    assert sent["payload"]["invoice_url"] == ""
    assert sent["payload"]["missing_months"] == []
