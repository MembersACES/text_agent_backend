"""Partner route table: refuse everything except PARTNER_ALLOW.

The allow set is the only hand-maintained list. Cases are generated from
app.routes so a new path fails this test until it is either refused or
explicitly allowed.
"""

from __future__ import annotations

import re

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from google.oauth2 import id_token

from partner_allow import PARTNER_ALLOW
from partner_authz import PartnerPrincipal, PartnerTool

PARTNER_EMAIL = "nigel@specialistenergy.com.au"
PARTNER_TOKEN = "partner-catalogue-token"

STAFF_DEP_NAMES = {
    "verify_google_token",
    "verify_google_access_token",
    "verify_google_access_token_optional",
    "verify_roster_access",
    "get_current_user_with_db",
    "get_current_user_with_db_or_backend_api_key",
    "get_current_user_with_db_or_tasks_api_key",
    "_require_aces_staff",
}
OIDC_SECRET_DEP_NAMES = {
    "verify_cloud_scheduler_oidc",
    "verify_autonomous_inbound_secret",
}
PARTNER_DEP_NAMES = {"verify_partner_token"}
SKIP_METHODS = {"HEAD", "OPTIONS"}


def _dep_names(dependant) -> set[str]:
    names: set[str] = set()
    stack = [dependant]
    seen: set[int] = set()
    while stack:
        dep = stack.pop()
        ident = id(dep)
        if ident in seen:
            continue
        seen.add(ident)
        call = getattr(dep, "call", None)
        if call is not None:
            names.add(getattr(call, "__name__", "") or "")
        stack.extend(getattr(dep, "dependencies", None) or [])
    return names


def _classify(route: APIRoute) -> str:
    names = _dep_names(route.dependant)
    if names & PARTNER_DEP_NAMES:
        return "partner"
    if names & OIDC_SECRET_DEP_NAMES:
        return "oidc_secret"
    if names & STAFF_DEP_NAMES:
        return "staff"
    return "public"


def _concrete_path(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "1", path)


def _iter_route_cases():
    from main import app

    cases = []
    seen = set()
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        methods = (route.methods or set()) - SKIP_METHODS
        kind = _classify(route)
        for method in sorted(methods):
            key = (method, route.path)
            if key in seen:
                continue
            seen.add(key)
            cases.append((method, route.path, kind))
    return cases


ROUTE_CASES = _iter_route_cases()


@pytest.fixture
def catalogue_client(monkeypatch):
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    monkeypatch.setenv("AUTH_ALLOWED_EMAIL_DOMAINS", "acesolutions.com.au,czeroanz.com")
    monkeypatch.setenv("AUTONOMOUS_INBOUND_SECRET", "catalogue-inbound-secret")
    monkeypatch.setenv("PARTNER_BASE1_SKIP_N8N", "1")

    principal = PartnerPrincipal(
        partner_id=1,
        partner_user_id=1,
        email=PARTNER_EMAIL,
        tools=(PartnerTool.BASE1.value,),
    )

    def fake_lookup(email, db=None):
        if (email or "").strip().lower() == PARTNER_EMAIL:
            return principal
        return None

    monkeypatch.setattr("partner_authz.lookup_active_partner_principal", fake_lookup)
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
    from main import app

    return TestClient(app, raise_server_exceptions=False)


def test_allow_constant_is_the_six_line_v1_surface():
    assert PARTNER_ALLOW == {
        ("GET", "/api/partner/me"),
        ("GET", "/api/partner/clients"),
        ("GET", "/api/partner/clients/{client_id}"),
        ("POST", "/api/partner/base1"),
    }
    registered = {(method, path) for method, path, _kind in ROUTE_CASES}
    missing = PARTNER_ALLOW - registered
    assert not missing, f"PARTNER_ALLOW entries missing from app.routes: {missing}"


def test_partner_token_refused_on_every_non_allow_route(catalogue_client):
    headers = {"Authorization": f"Bearer {PARTNER_TOKEN}"}
    failures: list[str] = []
    for method, path, kind in ROUTE_CASES:
        if (method, path) in PARTNER_ALLOW:
            continue
        if path.startswith("/api/partner/"):
            failures.append(f"{method} {path} is a partner route not in PARTNER_ALLOW")
            continue
        url = _concrete_path(path)
        request_kwargs: dict = {"headers": headers}
        if method in {"POST", "PUT", "PATCH"}:
            request_kwargs["json"] = {}
        partner = catalogue_client.request(method, url, **request_kwargs)
        if kind == "staff":
            if partner.status_code != 403:
                failures.append(
                    f"{method} {path}: staff route expected 403, got {partner.status_code}"
                )
            continue
        if kind == "oidc_secret":
            if partner.status_code != 401:
                failures.append(
                    f"{method} {path}: oidc/secret expected 401, got {partner.status_code}"
                )
            continue
        anon_kwargs = {}
        if method in {"POST", "PUT", "PATCH"}:
            anon_kwargs["json"] = {}
        anonymous = catalogue_client.request(method, url, **anon_kwargs)
        if partner.status_code < 400 and anonymous.status_code >= 400:
            failures.append(
                f"{method} {path}: partner token opened a public-classified route "
                f"(partner={partner.status_code} anon={anonymous.status_code})"
            )
    assert not failures, "\n".join(failures)
