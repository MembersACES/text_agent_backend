"""Staff distributor admin: create, link, invite, tools, deactivate. Partner token is refused."""

from __future__ import annotations

from datetime import datetime

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from google.oauth2 import id_token
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database
import models  # noqa: F401
import partner_authz
from database import Base, get_db
from models import Partner, PartnerUser
from partner_authz import lookup_active_partner_principal

STAFF_EMAIL = "pat@acesolutions.com.au"
PARTNER_EMAIL = "nigel@specialistenergy.com.au"
STAFF_AUTH = {"Authorization": "Bearer staff-token"}
PARTNER_AUTH = {"Authorization": "Bearer partner-token"}

ADMIN_ROUTES = {
    ("GET", "/api/distributors/partners"),
    ("POST", "/api/distributors/partners"),
    ("PATCH", "/api/distributors/partners/{partner_id}"),
    ("POST", "/api/distributors/partners/{partner_id}/deactivate"),
    ("POST", "/api/distributors/partners/{partner_id}/users"),
    ("POST", "/api/distributors/partners/{partner_id}/users/{user_id}/deactivate"),
}


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
    engine = _engine()
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    ensured: list[str] = []

    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    def fake_verify(token, *args, **kwargs):
        if token == "staff-token":
            return {
                "email": STAFF_EMAIL,
                "email_verified": True,
                "sub": "staff-sub",
                "aud": "test",
            }
        return {
            "email": PARTNER_EMAIL,
            "email_verified": True,
            "sub": "partner-sub",
            "aud": "test",
        }

    def fake_confirm(folder_id):
        if folder_id == "existing-folder":
            return (
                {
                    "id": folder_id,
                    "name": "A - Acme",
                    "display_name": "Acme",
                    "folder_id": folder_id,
                    "folder_url": "https://drive.google.com/drive/folders/existing-folder",
                },
                None,
                200,
            )
        if folder_id == "nested":
            return None, "not_distributor_entity", 400
        if folder_id == "missing":
            return None, "distributor_not_found", 404
        return None, "Drive is not configured.", 503

    def fake_ensure(partner):
        if not (partner.drive_folder_id or "").strip():
            partner.drive_folder_id = f"provisioned-{partner.slug}"
        ensured.append(partner.drive_folder_id)
        return partner.drive_folder_id

    from main import app
    import partner_admin_routes

    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setattr(database, "SessionLocal", TestingSessionLocal)
    monkeypatch.setattr(partner_authz, "SessionLocal", TestingSessionLocal)
    monkeypatch.setattr(id_token, "verify_oauth2_token", fake_verify)
    monkeypatch.setattr(partner_admin_routes, "confirm_distributor_entity", fake_confirm)
    monkeypatch.setattr(partner_admin_routes, "ensure_partner_drive_folder", fake_ensure)

    client = TestClient(app, raise_server_exceptions=False)
    try:
        yield client, TestingSessionLocal, ensured
    finally:
        app.dependency_overrides.clear()


def _seed_login(SessionLocal, email=PARTNER_EMAIL, active=1, partner_active=1, slug="specialist-energy"):
    db = SessionLocal()
    now = datetime.utcnow()
    partner = Partner(
        name="Specialist Energy",
        slug=slug,
        enabled_tools='["base1"]',
        drive_folder_id="partner-drive-root",
        active=partner_active,
        created_at=now,
        updated_at=now,
    )
    db.add(partner)
    db.flush()
    user = PartnerUser(
        partner_id=partner.id,
        email=email,
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(user)
    db.commit()
    partner_id, user_id = partner.id, user.id
    db.close()
    return partner_id, user_id


def test_admin_routes_are_staff_in_the_generated_catalogue():
    from tests.test_partner_catalogue import ROUTE_CASES

    found = {(method, path): kind for method, path, kind in ROUTE_CASES}
    missing = ADMIN_ROUTES - set(found)
    assert not missing
    assert {found[key] for key in ADMIN_ROUTES} == {"staff"}


def test_partner_principal_is_refused_on_every_admin_route(api):
    client, SessionLocal, _ensured = api
    partner_id, user_id = _seed_login(SessionLocal)
    cases = [
        ("GET", "/api/distributors/partners", None),
        ("POST", "/api/distributors/partners", {"name": "Acme", "slug": "acme", "enabled_tools": []}),
        ("PATCH", f"/api/distributors/partners/{partner_id}", {"enabled_tools": ["base1"]}),
        ("POST", f"/api/distributors/partners/{partner_id}/deactivate", None),
        ("POST", f"/api/distributors/partners/{partner_id}/users", {"email": "new@example.com"}),
        (
            "POST",
            f"/api/distributors/partners/{partner_id}/users/{user_id}/deactivate",
            None,
        ),
    ]
    for method, path, body in cases:
        kwargs = {"headers": PARTNER_AUTH}
        if body is not None:
            kwargs["json"] = body
        res = client.request(method, path, **kwargs)
        assert res.status_code == 403, f"{method} {path} -> {res.status_code}"


def test_create_provisions_drive_folder_immediately(api):
    client, SessionLocal, ensured = api
    res = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Acme Energy", "slug": "Acme-Energy", "enabled_tools": ["base1", "base1"]},
    )
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["slug"] == "acme-energy"
    assert body["enabled_tools"] == ["base1"]
    assert body["drive_folder_id"] == "provisioned-acme-energy"
    assert body["active"] is True
    assert ensured == ["provisioned-acme-energy"]
    db = SessionLocal()
    row = db.query(Partner).filter(Partner.slug == "acme-energy").one()
    assert row.drive_folder_id == "provisioned-acme-energy"
    db.close()


def test_create_links_existing_distributor_folder(api):
    client, _SessionLocal, ensured = api
    res = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={
            "name": "Acme",
            "slug": "acme",
            "enabled_tools": ["base2"],
            "drive_folder_id": "existing-folder",
        },
    )
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["drive_folder_id"] == "existing-folder"
    assert ensured == ["existing-folder"]


def test_create_rejects_nested_or_unknown_folder_without_a_row(api):
    client, SessionLocal, ensured = api
    nested = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Acme", "slug": "acme", "enabled_tools": [], "drive_folder_id": "nested"},
    )
    missing = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Acme", "slug": "acme", "enabled_tools": [], "drive_folder_id": "missing"},
    )
    assert nested.status_code == 400
    assert missing.status_code == 404
    assert ensured == []
    db = SessionLocal()
    assert db.query(Partner).count() == 0
    db.close()


def test_drive_failure_does_not_leave_a_partner(api, monkeypatch):
    import partner_admin_routes

    def boom(partner):
        raise HTTPException(status_code=503, detail="Drive unavailable")

    monkeypatch.setattr(partner_admin_routes, "ensure_partner_drive_folder", boom)
    client, SessionLocal, _ensured = api
    res = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Acme", "slug": "acme", "enabled_tools": []},
    )
    assert res.status_code == 503
    db = SessionLocal()
    assert db.query(Partner).count() == 0
    db.close()


def test_duplicate_slug_and_unknown_tool(api):
    client, _SessionLocal, _ensured = api
    first = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Acme", "slug": "acme", "enabled_tools": []},
    )
    assert first.status_code == 201
    dup = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Other", "slug": "acme", "enabled_tools": []},
    )
    unknown = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Other", "slug": "other", "enabled_tools": ["crm"]},
    )
    assert dup.status_code == 409
    assert unknown.status_code == 400


def test_toggle_tools_invite_and_deactivate(api):
    client, SessionLocal, _ensured = api
    created = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Acme", "slug": "acme", "enabled_tools": ["base1"]},
    )
    partner_id = created.json()["id"]
    tools = client.patch(
        f"/api/distributors/partners/{partner_id}",
        headers=STAFF_AUTH,
        json={"enabled_tools": ["base2"]},
    )
    assert tools.status_code == 200
    assert tools.json()["enabled_tools"] == ["base2"]
    assert tools.json()["drive_folder_id"] == "provisioned-acme"

    staff = client.post(
        f"/api/distributors/partners/{partner_id}/users",
        headers=STAFF_AUTH,
        json={"email": "pat@acesolutions.com.au"},
    )
    assert staff.status_code == 400

    invited = client.post(
        f"/api/distributors/partners/{partner_id}/users",
        headers=STAFF_AUTH,
        json={"email": "Nigel@SpecialistEnergy.com.au"},
    )
    assert invited.status_code == 201, invited.text
    users = invited.json()["users"]
    assert users[0]["email"] == "nigel@specialistenergy.com.au"
    assert users[0]["active"] is True
    user_id = users[0]["id"]

    again = client.post(
        f"/api/distributors/partners/{partner_id}/users",
        headers=STAFF_AUTH,
        json={"email": "nigel@specialistenergy.com.au"},
    )
    assert again.status_code == 409

    other = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={"name": "Other", "slug": "other", "enabled_tools": []},
    )
    taken = client.post(
        f"/api/distributors/partners/{other.json()['id']}/users",
        headers=STAFF_AUTH,
        json={"email": "nigel@specialistenergy.com.au"},
    )
    assert taken.status_code == 409

    second = client.post(
        f"/api/distributors/partners/{partner_id}/users",
        headers=STAFF_AUTH,
        json={"email": "ada@example.com"},
    )
    assert second.status_code == 201

    stopped_user = client.post(
        f"/api/distributors/partners/{partner_id}/users/{user_id}/deactivate",
        headers=STAFF_AUTH,
    )
    assert stopped_user.status_code == 200
    by_email = {row["email"]: row["active"] for row in stopped_user.json()["users"]}
    assert by_email["nigel@specialistenergy.com.au"] is False
    assert by_email["ada@example.com"] is True

    db = SessionLocal()
    assert lookup_active_partner_principal("nigel@specialistenergy.com.au", db=db) is None
    ada = lookup_active_partner_principal("ada@example.com", db=db)
    assert ada is not None
    assert ada.tools == ("base2",)
    db.close()

    stopped = client.post(
        f"/api/distributors/partners/{partner_id}/deactivate",
        headers=STAFF_AUTH,
    )
    assert stopped.status_code == 200
    assert stopped.json()["active"] is False
    assert stopped.json()["deactivated_at"]
    db = SessionLocal()
    assert lookup_active_partner_principal("ada@example.com", db=db) is None
    db.close()

    invite_after = client.post(
        f"/api/distributors/partners/{partner_id}/users",
        headers=STAFF_AUTH,
        json={"email": "more@example.com"},
    )
    assert invite_after.status_code == 400


def test_link_existing_partner_to_drive_folder(api):
    client, SessionLocal, ensured = api
    partner_id, _user_id = _seed_login(SessionLocal)
    res = client.patch(
        f"/api/distributors/partners/{partner_id}",
        headers=STAFF_AUTH,
        json={"drive_folder_id": "existing-folder"},
    )
    assert res.status_code == 200, res.text
    assert res.json()["drive_folder_id"] == "existing-folder"
    assert ensured == ["existing-folder"]

    other = client.post(
        "/api/distributors/partners",
        headers=STAFF_AUTH,
        json={
            "name": "Other",
            "slug": "other",
            "enabled_tools": [],
            "drive_folder_id": "existing-folder",
        },
    )
    assert other.status_code == 409


def test_confirm_requires_a_top_level_distributor_folder(monkeypatch):
    from tools import distributor_folders as folders

    monkeypatch.setattr(folders, "_drive_or_error", lambda: (object(), None))
    monkeypatch.setattr(folders, "get_distributors_parent_id", lambda: "root")

    def resolve(_drive, folder_id, parent_id):
        assert parent_id == "root"
        if folder_id == "dist":
            return {
                "supplier": {"id": "dist", "name": "A - Acme", "webViewLink": "https://drive/x"},
                "current": {},
                "path": [],
            }, None
        if folder_id == "nested":
            return {
                "supplier": {"id": "dist", "name": "A - Acme"},
                "current": {},
                "path": [],
            }, None
        return None, "not_found"

    monkeypatch.setattr(folders, "_resolve_under_supplier", resolve)
    payload, err, status = folders.confirm_distributor_entity("dist")
    assert err is None and status == 200
    assert payload["id"] == "dist"
    assert payload["display_name"] == "Acme"
    _payload, err, status = folders.confirm_distributor_entity("nested")
    assert err == "not_distributor_entity" and status == 400
    _payload, err, status = folders.confirm_distributor_entity("missing")
    assert err == "distributor_not_found" and status == 404
