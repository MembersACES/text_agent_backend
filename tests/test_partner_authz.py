"""Partner tenancy gate: staff-inert when tables are empty; collision never stamps partner_id."""

from __future__ import annotations

from datetime import datetime

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import auth_domain
import partner_authz
from database import Base
from models import Client, Partner, PartnerAuditEvent, PartnerLeadCollision, PartnerUser
from partner_authz import PartnerPrincipal, PartnerTool, admit_partner_lead, log_partner_write


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def _principal(**overrides) -> PartnerPrincipal:
    values = dict(
        partner_id=1,
        partner_user_id=1,
        email="nigel@specialistenergy.com.au",
        tools=(PartnerTool.BASE1.value,),
    )
    values.update(overrides)
    return PartnerPrincipal(**values)


def _seed_partner(db, *, active=1, user_active=1, tools='["base1"]', email="nigel@specialistenergy.com.au"):
    partner = Partner(
        name="Specialist Energy",
        slug="specialist-energy",
        enabled_tools=tools,
        active=active,
    )
    db.add(partner)
    db.flush()
    user = PartnerUser(
        partner_id=partner.id,
        email=email,
        active=user_active,
    )
    db.add(user)
    db.commit()
    db.refresh(partner)
    db.refresh(user)
    return partner, user


def _client(db, business_name, partner_id=None):
    row = Client(
        business_name=business_name,
        stage="lead",
        partner_id=partner_id,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def test_empty_tables_lookup_returns_none():
    db = _session()
    assert partner_authz.lookup_active_partner_principal("nigel@specialistenergy.com.au", db=db) is None
    assert partner_authz.lookup_active_partner_principal("pat@acesolutions.com.au", db=db) is None


def test_empty_tables_enforce_still_403s_off_domain(monkeypatch):
    db = _session()
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    original = partner_authz.lookup_active_partner_principal

    def lookup(email, db_arg=None):
        return original(email, db=db)

    monkeypatch.setattr(partner_authz, "lookup_active_partner_principal", lookup)
    with pytest.raises(HTTPException) as exc:
        auth_domain.apply_email_domain_policy(
            {"email": "nigel@specialistenergy.com.au"},
            "verify_google_token",
        )
    assert exc.value.status_code == 403


def test_staff_unchanged_with_empty_tables(monkeypatch):
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    idinfo = {"email": "pat@acesolutions.com.au", "sub": "1"}
    assert auth_domain.apply_email_domain_policy(idinfo, "verify_google_token") is idinfo
    assert "aces_auth" not in idinfo


def test_staff_does_not_enter_partner_lookup(monkeypatch):
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")

    def boom(email, db=None):
        raise AssertionError("staff must not call partner lookup")

    monkeypatch.setattr(partner_authz, "lookup_active_partner_principal", boom)
    idinfo = {"email": "a@czeroanz.com"}
    assert auth_domain.apply_email_domain_policy(idinfo, "verify_roster_access") is idinfo


def test_active_partner_allowed_under_enforce(monkeypatch):
    db = _session()
    partner, user = _seed_partner(db)
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    original = partner_authz.lookup_active_partner_principal

    def lookup(email, db_arg=None):
        return original(email, db=db)

    monkeypatch.setattr(partner_authz, "lookup_active_partner_principal", lookup)
    idinfo = {"email": "nigel@specialistenergy.com.au", "sub": "nigel"}
    out = auth_domain.apply_email_domain_policy(idinfo, "verify_google_token")
    assert out is not idinfo
    assert out["aces_auth"]["role"] == "partner"
    assert out["aces_auth"]["partner_id"] == partner.id
    assert out["aces_auth"]["partner_user_id"] == user.id
    assert "base1" in out["aces_auth"]["tools"]
    assert "aces_auth" not in idinfo


def test_inactive_partner_is_denied(monkeypatch):
    db = _session()
    _seed_partner(db, active=0)
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    original = partner_authz.lookup_active_partner_principal

    def lookup(email, db_arg=None):
        return original(email, db=db)

    monkeypatch.setattr(partner_authz, "lookup_active_partner_principal", lookup)
    with pytest.raises(HTTPException) as exc:
        auth_domain.apply_email_domain_policy(
            {"email": "nigel@specialistenergy.com.au"},
            "verify_google_token",
        )
    assert exc.value.status_code == 403


def test_inactive_partner_user_is_denied(monkeypatch):
    db = _session()
    _seed_partner(db, user_active=0)
    assert (
        partner_authz.lookup_active_partner_principal("nigel@specialistenergy.com.au", db=db)
        is None
    )


def test_require_partner_tool_is_authorisation():
    principal = _principal(tools=(PartnerTool.BASE1.value,))
    partner_authz.require_partner_tool(principal, PartnerTool.BASE1)
    with pytest.raises(HTTPException) as exc:
        partner_authz.require_partner_tool(principal, PartnerTool.BASE2)
    assert exc.value.status_code == 403


def test_collision_does_not_stamp_partner_id():
    db = _session()
    partner, _user = _seed_partner(db)
    existing = _client(db, "Acme Pty Ltd", partner_id=None)
    principal = _principal(partner_id=partner.id, partner_user_id=_user.id)
    result = admit_partner_lead(db, principal, "acme pty ltd", payload={"nmi": "1"})
    db.commit()
    db.refresh(existing)
    assert result.outcome == "collision"
    assert result.public == {"status": "received"}
    assert result.client_id is None
    assert existing.partner_id is None
    collision = db.query(PartnerLeadCollision).one()
    assert collision.existing_client_id == existing.id
    assert collision.partner_id == partner.id
    assert db.query(PartnerAuditEvent).filter_by(action="lead_collision").count() == 0


def test_own_lead_is_not_a_collision_and_does_not_restamp():
    db = _session()
    partner, _user = _seed_partner(db)
    existing = _client(db, "Their Lead", partner_id=partner.id)
    principal = _principal(partner_id=partner.id, partner_user_id=_user.id)
    result = admit_partner_lead(db, principal, "Their Lead")
    db.commit()
    db.refresh(existing)
    assert result.outcome == "own"
    assert result.client_id == existing.id
    assert existing.partner_id == partner.id
    assert db.query(PartnerLeadCollision).count() == 0


def test_other_partner_lead_is_collision():
    db = _session()
    partner, _user = _seed_partner(db)
    other = Partner(name="Other", slug="other", enabled_tools="[]", active=1)
    db.add(other)
    db.commit()
    db.refresh(other)
    existing = _client(db, "Shared Name", partner_id=other.id)
    principal = _principal(partner_id=partner.id, partner_user_id=_user.id)
    result = admit_partner_lead(db, principal, "Shared Name")
    db.commit()
    db.refresh(existing)
    assert result.outcome == "collision"
    assert existing.partner_id == other.id


def test_create_outcome_does_not_insert_client():
    db = _session()
    partner, _user = _seed_partner(db)
    principal = _principal(partner_id=partner.id, partner_user_id=_user.id)
    result = admit_partner_lead(db, principal, "Brand New Co")
    db.commit()
    assert result.outcome == "create"
    assert db.query(Client).count() == 0


def test_audit_write_records_partner_id_email_action_target():
    db = _session()
    partner, _user = _seed_partner(db)
    principal = _principal(partner_id=partner.id, partner_user_id=_user.id, email=_user.email)
    event = log_partner_write(
        db,
        principal,
        action="base1_submit",
        target_type="client",
        target_id=99,
        path="/api/partner/base1",
    )
    db.commit()
    stored = db.query(PartnerAuditEvent).one()
    assert stored.id == event.id
    assert stored.partner_id == partner.id
    assert stored.email == _user.email
    assert stored.action == "base1_submit"
    assert stored.target_type == "client"
    assert stored.target_id == "99"
    assert stored.path == "/api/partner/base1"


def test_partner_client_public_exact_keys():
    db = _session()
    row = _client(db, "Visible Co", partner_id=None)
    row.owner_email = "internal@acesolutions.com.au"
    row.stage = "qualified"
    db.commit()
    payload = partner_authz.partner_client_public(row)
    assert set(payload) == partner_authz.PARTNER_CLIENT_KEYS
    assert set(payload) == {"id", "business_name", "primary_contact_email", "created_at"}
    from schemas import ClientResponse, PartnerClientResponse

    assert set(PartnerClientResponse.model_fields) == partner_authz.PARTNER_CLIENT_KEYS
    leaked = {
        "owner_email",
        "notes",
        "stage",
        "stage_changed_at",
        "commission",
        "margin",
        "gdrive_folder_url",
        "updated_at",
        "partner_id",
        "referred_by_client_id",
        "referred_by_business_name",
    }
    assert leaked.isdisjoint(PartnerClientResponse.model_fields)
    assert "owner_email" in ClientResponse.model_fields
    assert "stage" in ClientResponse.model_fields
    assert payload["business_name"] == "Visible Co"
    assert "owner_email" not in payload
    assert "stage" not in payload


def test_validate_base1_files_accepts_pdf_jpg_png_heic():
    heic = b"\x00\x00\x00\x18ftypheic" + b"\x00" * 8
    partner_authz.validate_base1_files(
        [
            ("a.pdf", b"%PDF-1.4\n%", "application/pdf"),
            ("b.jpg", b"\xff\xd8\xff\xdb", "image/jpeg"),
            ("c.png", b"\x89PNG\r\n\x1a\n" + b"\x00" * 8, "image/png"),
            ("d.heic", heic, "image/heic"),
        ]
    )


def test_validate_base1_files_rejects_bad_type_and_oversize():
    with pytest.raises(HTTPException) as empty:
        partner_authz.validate_base1_files([])
    assert empty.value.status_code == 400
    with pytest.raises(HTTPException) as exe:
        partner_authz.validate_base1_files([("malware.pdf", b"MZ\x90\x00", "application/pdf")])
    assert exe.value.status_code == 400
    too_big = b"%PDF-1.4\n" + (b"x" * (partner_authz.BASE1_MAX_FILE_BYTES + 1))
    with pytest.raises(HTTPException) as size:
        partner_authz.validate_base1_files([("big.pdf", too_big, "application/pdf")])
    assert size.value.status_code == 400
    many = [
        (f"{i}.pdf", b"%PDF-1.4\n%", "application/pdf")
        for i in range(partner_authz.BASE1_MAX_FILES + 1)
    ]
    with pytest.raises(HTTPException) as count:
        partner_authz.validate_base1_files(many)
    assert count.value.status_code == 400


def test_base1_rate_limit_per_partner():
    db = _session()
    partner, user = _seed_partner(db)
    principal = _principal(partner_id=partner.id, partner_user_id=user.id, email=user.email)
    for _ in range(partner_authz.BASE1_MAX_SUBMISSIONS_PER_HOUR):
        log_partner_write(
            db,
            principal,
            action=partner_authz.BASE1_RATE_ACTION,
            target_type="partner",
            target_id=partner.id,
            path="/api/partner/base1",
        )
    db.commit()
    with pytest.raises(HTTPException) as exc:
        partner_authz.assert_base1_rate_limit(db, partner.id)
    assert exc.value.status_code == 429
    assert exc.value.detail["code"] == "rate_limited"
    assert "try again shortly" in exc.value.detail["message"].lower()
    other = Partner(name="Other", slug="other-rate", enabled_tools="[]", active=1)
    db.add(other)
    db.commit()
    db.refresh(other)
    partner_authz.assert_base1_rate_limit(db, other.id)


def test_persist_collision_files_writes_drive_refs(monkeypatch):
    db = _session()
    partner, user = _seed_partner(db)
    partner.drive_folder_id = "partner-drive-root"
    db.commit()
    existing = _client(db, "Acme Pty Ltd", partner_id=None)
    principal = _principal(partner_id=partner.id, partner_user_id=user.id, email=user.email)
    result = admit_partner_lead(db, principal, "Acme Pty Ltd")
    collision = db.query(PartnerLeadCollision).one()

    created = []

    def fake_find_or_create(parent_id, name, drive=None):
        created.append((parent_id, name))
        return f"folder-{len(created)}", True

    def fake_upload(data, name, dest_id, mimetype="application/octet-stream"):
        return {"id": f"file-{name}", "url": f"https://drive.example/{name}"}

    monkeypatch.setattr("tools.member_folder_drive.find_or_create_folder", fake_find_or_create)
    monkeypatch.setattr("tools.member_folder_drive.upload_bytes_to_folder", fake_upload)
    refs = partner_authz.persist_collision_files(
        partner,
        collision,
        [("bill.pdf", b"%PDF-1.4\n%", "application/pdf")],
    )
    assert refs[0]["id"] == "file-bill.pdf"
    assert refs[0]["name"] == "bill.pdf"
    assert refs[0]["folder_id"] == "folder-2"
    event = log_partner_write(
        db,
        principal,
        action="lead_collision",
        target_type="partner_lead_collision",
        target_id=collision.id,
        path="/api/partner/base1",
        detail={"file_count": 1, "files": refs},
    )
    db.commit()
    stored = db.query(PartnerAuditEvent).filter_by(action="lead_collision").one()
    assert stored.id == event.id
    assert '"file-bill.pdf"' in stored.detail_json
    assert existing.partner_id is None

