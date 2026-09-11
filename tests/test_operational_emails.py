"""Operational email templates and recipient directories."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base
from services.operational_email_defaults import emails_look_placeholder
from services.operational_emails import (
    assert_editor_allowed,
    create_recipient,
    editor_domain_allowed,
    find_data_request_recipient,
    list_recipients,
    list_templates,
    render_tokens,
    seed_operational_emails,
    update_recipient,
    update_template,
)


def _db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def test_editor_domains():
    assert editor_domain_allowed("alex@acesolutions.com.au")
    assert editor_domain_allowed("pat@czeroanz.com")
    assert not editor_domain_allowed("someone@gmail.com")
    try:
        assert_editor_allowed("x@gmail.com")
        assert False
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 403


def test_seed_creates_templates_and_placeholders():
    db = _db()
    result = seed_operational_emails(db)
    assert result["templates"] > 0
    assert result["recipients"] > 0
    keys = {row.key for row in list_templates(db)}
    assert "data_request.electricity_ci" in keys
    assert "quote_request.default" in keys
    assert "signed_agreement.default" in keys
    eois = list_recipients(db, "eoi")
    placeholders = [row for row in eois if row.is_placeholder]
    assert any(row.key == "Cleaning Robot" for row in placeholders)
    cleaning = next(row for row in eois if row.key == "Cleaning Robot")
    assert cleaning.is_placeholder == 1
    dma = next(row for row in eois if row.key == "Direct Meter Agreement")
    assert dma.is_placeholder == 0
    second = seed_operational_emails(db)
    assert second == {"templates": 0, "recipients": 0}


def test_seed_does_not_overwrite_edits():
    db = _db()
    seed_operational_emails(db)
    updated = update_template(
        db,
        "data_request.electricity_ci",
        html_body="<p>Please send interval data only for {{business_name}}</p>",
        updated_by="alex@acesolutions.com.au",
    )
    assert "interval data only" in updated.html_body
    seed_operational_emails(db)
    row = next(t for t in list_templates(db, "data_request") if t.key == "data_request.electricity_ci")
    assert "interval data only" in row.html_body
    assert row.updated_by == "alex@acesolutions.com.au"


def test_render_tokens_and_html_blocks():
    html = render_tokens(
        "<p>Hello {{business_name}}</p>{{identifier_html}}",
        {"business_name": "Acme", "identifier_html": "<p>NMI: 1</p>"},
    )
    assert html == "<p>Hello Acme</p><p>NMI: 1</p>"
    assert render_tokens("Hi {{missing}}", {}) == "Hi "


def test_data_request_recipient_lookup():
    db = _db()
    seed_operational_emails(db)
    email, name, is_default = find_data_request_recipient(db, "Origin Energy", "electricity_ci")
    assert "originenergy.com.au" in email.lower() or "fornrg.com" in email.lower()
    assert is_default is False
    unknown_email, _resolved, default = find_data_request_recipient(db, "Unknown Retailer XYZ", "electricity_ci")
    assert default is True
    assert "members@acesolutions.com.au" in unknown_email


def test_add_and_update_recipient_emails():
    db = _db()
    seed_operational_emails(db)
    row = create_recipient(
        db,
        flow="signed_contract",
        key="AGL C&I Electricity",
        display_name="AGL",
        emails=["contracts@agl.com.au", "data.quote@fornrg.com"],
        group_name="C&I Electricity",
        updated_by="pat@czeroanz.com",
    )
    assert row.key == "AGL C&I Electricity"
    updated = update_recipient(
        db,
        row.id,
        emails=["new.person@agl.com.au", "data.quote@fornrg.com"],
        updated_by="alex@acesolutions.com.au",
    )
    assert "new.person@agl.com.au" in updated.emails_json
    assert updated.updated_by == "alex@acesolutions.com.au"


def test_placeholder_detection():
    assert emails_look_placeholder(["cleantech@supplier.com"], "Cleaning Robot")
    assert emails_look_placeholder(["members@acesolutions.com.au"], "New Placeholder Template")
    assert not emails_look_placeholder(["data.quote@fornrg.com"], "Direct Meter Agreement")
