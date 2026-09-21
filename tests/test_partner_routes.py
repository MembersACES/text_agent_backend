"""Partner HTTP surface: tenancy, serializer keys, Base 1 limits."""

from __future__ import annotations

from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from google.oauth2 import id_token
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database
import models  # noqa: F401 — register tables on Base
import partner_authz
from database import Base, get_db
from models import Client, Partner, PartnerAuditEvent, PartnerLeadCollision, PartnerUser
from partner_authz import (
    BASE1_MAX_SUBMISSIONS_PER_HOUR,
    BASE1_RATE_ACTION,
    BASE1_RATE_LIMIT_MESSAGE,
    COLLISION_HTTP_STATUS,
    COLLISION_PUBLIC,
    PARTNER_CLIENT_KEYS,
)
from partner_collision_notify import MATCH_ACES, MATCH_OTHER_DISTRIBUTOR

PARTNER_EMAIL = "nigel@specialistenergy.com.au"
STAFF_EMAIL = "pat@acesolutions.com.au"
AUTH = {"Authorization": "Bearer partner-route-token"}
PDF = ("%PDF-1.4\n%", "bill.pdf", "application/pdf")


def _engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return engine


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    monkeypatch.setenv("AUTH_ALLOWED_EMAIL_DOMAINS", "acesolutions.com.au,czeroanz.com")
    monkeypatch.setenv("PARTNER_BASE1_SKIP_N8N", "1")
    engine = _engine()
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    from main import app
    import partner_routes

    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setattr(database, "SessionLocal", TestingSessionLocal)
    monkeypatch.setattr(partner_authz, "SessionLocal", TestingSessionLocal)
    monkeypatch.setattr(
        partner_routes,
        "persist_collision_files",
        lambda partner, collision, files: [
            {
                "id": "drive-file-1",
                "name": files[0][0] if files else "bill.pdf",
                "url": "https://drive.example/file/drive-file-1",
                "folder_id": "drive-folder-collision",
                "folder_url": "https://drive.google.com/drive/folders/drive-folder-collision",
            }
        ],
    )
    monkeypatch.setattr(
        id_token,
        "verify_oauth2_token",
        lambda *args, **kwargs: {
            "email": PARTNER_EMAIL,
            "email_verified": True,
            "sub": "partner-sub",
            "aud": "test",
        },
    )
    db = TestingSessionLocal()
    partner = Partner(
        name="Specialist Energy",
        slug="specialist-energy",
        enabled_tools='["base1"]',
        drive_folder_id="partner-drive-root",
        active=1,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(partner)
    db.flush()
    user = PartnerUser(
        partner_id=partner.id,
        email=PARTNER_EMAIL,
        active=1,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(user)
    db.commit()
    db.refresh(partner)
    client = TestClient(app, raise_server_exceptions=False)
    try:
        yield client, TestingSessionLocal, partner
    finally:
        app.dependency_overrides.clear()
        db.close()


def _add_client(SessionLocal, **kwargs):
    db = SessionLocal()
    now = datetime.utcnow()
    values = dict(
        stage="lead",
        created_at=now,
        updated_at=now,
    )
    values.update(kwargs)
    row = Client(**values)
    db.add(row)
    db.commit()
    db.refresh(row)
    client_id = row.id
    db.close()
    return client_id


def _pdf_upload():
    data, name, mime = PDF
    return {"files": (name, data, mime)}


def test_partner_me(api):
    client, _SessionLocal, partner = api
    res = client.get("/api/partner/me", headers=AUTH)
    assert res.status_code == 200
    body = res.json()
    assert body["email"] == PARTNER_EMAIL
    assert body["partner_id"] == partner.id
    assert body["tools"] == ["base1"]


def test_list_clients_is_own_tenant_only_and_exact_keys(api):
    client, SessionLocal, partner = api
    own_id = _add_client(
        SessionLocal,
        business_name="Ours",
        primary_contact_email="ours@example.com",
        partner_id=partner.id,
        owner_email="hidden@acesolutions.com.au",
    )
    other = Partner(
        name="Other Co",
        slug="other-co",
        enabled_tools="[]",
        active=1,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db = SessionLocal()
    db.add(other)
    db.commit()
    db.refresh(other)
    other_id = other.id
    db.close()
    _add_client(SessionLocal, business_name="Theirs", partner_id=other_id)
    _add_client(SessionLocal, business_name="ACES Member", partner_id=None, owner_email="pat@acesolutions.com.au")

    res = client.get("/api/partner/clients", headers=AUTH)
    assert res.status_code == 200
    rows = res.json()
    assert [row["id"] for row in rows] == [own_id]
    assert set(rows[0]) == PARTNER_CLIENT_KEYS
    assert "owner_email" not in rows[0]
    assert "stage" not in rows[0]
    assert "partner_id" not in rows[0]


def test_get_own_client_and_cross_tenant_is_404(api):
    client, SessionLocal, partner = api
    own_id = _add_client(SessionLocal, business_name="Ours", partner_id=partner.id)
    aces_id = _add_client(SessionLocal, business_name="ACES Member", partner_id=None)
    res = client.get(f"/api/partner/clients/{own_id}", headers=AUTH)
    assert res.status_code == 200
    body = res.json()
    assert set(body) == PARTNER_CLIENT_KEYS
    assert body["id"] == own_id
    hidden = client.get(f"/api/partner/clients/{aces_id}", headers=AUTH)
    assert hidden.status_code == 404
    missing = client.get("/api/partner/clients/999999", headers=AUTH)
    assert missing.status_code == 404


def test_staff_list_clients_still_includes_partner_owned(api, monkeypatch):
    client, SessionLocal, partner = api
    _add_client(SessionLocal, business_name="Partner Lead", partner_id=partner.id)
    _add_client(SessionLocal, business_name="ACES Member", partner_id=None)
    monkeypatch.setattr(
        id_token,
        "verify_oauth2_token",
        lambda *args, **kwargs: {
            "email": STAFF_EMAIL,
            "email_verified": True,
            "sub": "staff-sub",
            "aud": "test",
        },
    )
    res = client.get("/api/clients", headers={"Authorization": "Bearer staff-token"})
    assert res.status_code == 200
    names = {row["business_name"] for row in res.json()}
    assert "Partner Lead" in names
    assert "ACES Member" in names


def test_staff_token_cannot_use_partner_routes(api, monkeypatch):
    client, _SessionLocal, _partner = api
    monkeypatch.setattr(
        id_token,
        "verify_oauth2_token",
        lambda *args, **kwargs: {
            "email": STAFF_EMAIL,
            "email_verified": True,
            "sub": "staff-sub",
            "aud": "test",
        },
    )
    res = client.get("/api/partner/me", headers={"Authorization": "Bearer staff-token"})
    assert res.status_code == 403


def test_base1_create_own_lead(api):
    client, SessionLocal, partner = api
    res = client.post(
        "/api/partner/base1",
        headers=AUTH,
        data={"companyName": "New Distributor Lead", "email": "lead@example.com"},
        files=[("files", ("bill.pdf", b"%PDF-1.4\n%", "application/pdf"))],
    )
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert "client_id" in body
    db = SessionLocal()
    row = db.query(Client).filter(Client.id == body["client_id"]).one()
    assert row.business_name == "New Distributor Lead"
    assert row.partner_id == partner.id
    assert db.query(PartnerAuditEvent).filter_by(action="base1_submit").count() == 1
    db.close()


def test_base1_collision_neutral_failure(api, monkeypatch):
    client, SessionLocal, partner = api
    captured = []
    monkeypatch.setattr(
        "partner_routes.notify_staff_of_lead_collision",
        captured.append,
    )
    _add_client(SessionLocal, business_name="Existing ACES", partner_id=None)
    res = client.post(
        "/api/partner/base1",
        headers=AUTH,
        data={"companyName": "Existing ACES"},
        files=[("files", ("bill.pdf", b"%PDF-1.4\n%", "application/pdf"))],
    )
    assert res.status_code == COLLISION_HTTP_STATUS
    assert res.json() == {"detail": COLLISION_PUBLIC}
    db = SessionLocal()
    row = db.query(Client).filter(Client.business_name == "Existing ACES").one()
    assert row.partner_id is None
    collision = db.query(PartnerLeadCollision).one()
    assert collision.existing_client_id == row.id
    assert '"drive-file-1"' in (collision.files_json or "")
    audit = db.query(PartnerAuditEvent).filter_by(action="lead_collision").one()
    assert '"drive-file-1"' in (audit.detail_json or "")
    db.close()
    assert len(captured) == 1
    assert captured[0]["match_kind"] == MATCH_ACES
    assert captured[0]["existing_client_id"] == row.id
    assert captured[0]["distributor_name"] == partner.name
    assert captured[0]["submitted_business_name"] == "Existing ACES"
    assert captured[0]["folder_url"]


def test_base1_other_distributor_collision_same_body(api, monkeypatch):
    client, SessionLocal, _partner = api
    captured = []
    monkeypatch.setattr(
        "partner_routes.notify_staff_of_lead_collision",
        captured.append,
    )
    other = Partner(
        name="Other Co",
        slug="other-co-collision",
        enabled_tools="[]",
        active=1,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db = SessionLocal()
    db.add(other)
    db.commit()
    db.refresh(other)
    other_id = other.id
    db.close()
    _add_client(SessionLocal, business_name="Shared Name", partner_id=other_id)
    res = client.post(
        "/api/partner/base1",
        headers=AUTH,
        data={"companyName": "Shared Name"},
        files=[("files", ("bill.pdf", b"%PDF-1.4\n%", "application/pdf"))],
    )
    assert res.status_code == COLLISION_HTTP_STATUS
    assert res.json() == {"detail": COLLISION_PUBLIC}
    db = SessionLocal()
    row = db.query(Client).filter(Client.business_name == "Shared Name").one()
    assert row.partner_id == other_id
    db.close()
    assert len(captured) == 1
    assert captured[0]["match_kind"] == MATCH_OTHER_DISTRIBUTOR
    assert captured[0]["matched_partner_name"] == "Other Co"
    assert captured[0]["existing_client_id"] == row.id


def test_base1_rejects_disallowed_file(api):
    client, _SessionLocal, _partner = api
    res = client.post(
        "/api/partner/base1",
        headers=AUTH,
        data={"companyName": "File Check"},
        files=[("files", ("notes.txt", b"hello", "text/plain"))],
    )
    assert res.status_code == 400


def test_base1_rate_limited_per_partner(api):
    client, SessionLocal, partner = api
    db = SessionLocal()
    now = datetime.utcnow()
    for _ in range(BASE1_MAX_SUBMISSIONS_PER_HOUR):
        db.add(
            PartnerAuditEvent(
                partner_id=partner.id,
                email=PARTNER_EMAIL,
                action=BASE1_RATE_ACTION,
                target_type="partner",
                target_id=str(partner.id),
                path="/api/partner/base1",
                created_at=now,
            )
        )
    db.commit()
    db.close()
    res = client.post(
        "/api/partner/base1",
        headers=AUTH,
        data={"companyName": "Rate Limited Co"},
        files=[("files", ("bill.pdf", b"%PDF-1.4\n%", "application/pdf"))],
    )
    assert res.status_code == 429
    body = res.json()["detail"]
    assert body["code"] == "rate_limited"
    assert body["message"] == BASE1_RATE_LIMIT_MESSAGE


def test_base1_requires_tool(api):
    client, SessionLocal, partner = api
    db = SessionLocal()
    row = db.query(Partner).filter(Partner.id == partner.id).one()
    row.enabled_tools = "[]"
    db.commit()
    db.close()
    res = client.post(
        "/api/partner/base1",
        headers=AUTH,
        data={"companyName": "No Tools Co"},
        files=[("files", ("bill.pdf", b"%PDF-1.4\n%", "application/pdf"))],
    )
    assert res.status_code == 403
