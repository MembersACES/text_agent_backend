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
    assert db.query(PartnerAuditEvent).filter_by(action="lead_collision").count() == 1


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
