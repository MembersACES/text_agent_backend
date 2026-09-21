"""Cloud Scheduler OIDC verification for cron routes."""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from starlette.requests import Request

import auth_domain
import auth_scheduler


def _bind(
    path="/api/tasks/check-due-cron",
    host="text-agent-backend-672026052958.australia-southeast2.run.app",
    scheme="https",
):
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": scheme,
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [(b"host", host.encode())],
        "client": ("127.0.0.1", 123),
        "server": (host, 443),
    }
    return auth_domain.bind_request_context(Request(scope))


def test_scheduler_missing_bearer_401():
    with pytest.raises(HTTPException) as exc:
        auth_scheduler.verify_cloud_scheduler_oidc(authorization=None)
    assert exc.value.status_code == 401


def test_scheduler_accepts_matching_sa_and_audience(monkeypatch):
    origin = "https://text-agent-backend-672026052958.australia-southeast2.run.app"
    path = "/api/tasks/check-due-cron"
    tokens = _bind(path=path, host="text-agent-backend-672026052958.australia-southeast2.run.app")

    def fake_verify(token, request, audience=None):
        assert token == "oidc-token"
        return {
            "email": "672026052958-compute@developer.gserviceaccount.com",
            "email_verified": True,
            "aud": origin,
        }

    monkeypatch.setattr(auth_scheduler.id_token, "verify_oauth2_token", fake_verify)
    try:
        info = auth_scheduler.verify_cloud_scheduler_oidc(authorization="Bearer oidc-token")
        assert info["email"].endswith("@developer.gserviceaccount.com")
    finally:
        auth_domain.reset_request_context(tokens)


def test_scheduler_rejects_wrong_sa(monkeypatch):
    tokens = _bind()

    def fake_verify(token, request, audience=None):
        return {
            "email": "you@gmail.com",
            "email_verified": True,
            "aud": "https://text-agent-backend-672026052958.australia-southeast2.run.app",
        }

    monkeypatch.setattr(auth_scheduler.id_token, "verify_oauth2_token", fake_verify)
    try:
        with pytest.raises(HTTPException) as exc:
            auth_scheduler.verify_cloud_scheduler_oidc(authorization="Bearer oidc-token")
        assert exc.value.status_code == 401
    finally:
        auth_domain.reset_request_context(tokens)


def test_scheduler_rejects_wrong_audience(monkeypatch):
    tokens = _bind()
    monkeypatch.delenv("SCHEDULER_OIDC_AUDIENCE", raising=False)
    monkeypatch.setenv(
        "SCHEDULER_OIDC_AUDIENCE",
        "https://text-agent-backend-672026052958.australia-southeast2.run.app",
    )

    def fake_verify(token, request, audience=None):
        return {
            "email": "672026052958-compute@developer.gserviceaccount.com",
            "email_verified": True,
            "aud": "https://evil.example",
        }

    monkeypatch.setattr(auth_scheduler.id_token, "verify_oauth2_token", fake_verify)
    try:
        with pytest.raises(HTTPException) as exc:
            auth_scheduler.verify_cloud_scheduler_oidc(authorization="Bearer oidc-token")
        assert exc.value.status_code == 401
    finally:
        auth_domain.reset_request_context(tokens)


def test_scheduler_rejects_invalid_jwt(monkeypatch):
    def fake_verify(token, request, audience=None):
        raise ValueError("bad token")

    monkeypatch.setattr(auth_scheduler.id_token, "verify_oauth2_token", fake_verify)
    with pytest.raises(HTTPException) as exc:
        auth_scheduler.verify_cloud_scheduler_oidc(authorization="Bearer not-a-jwt")
    assert exc.value.status_code == 401


def test_scheduler_http_origin_matches_https_audience(monkeypatch):
    tokens = _bind(
        path="/api/tasks/check-due-cron",
        host="text-agent-backend-672026052958.australia-southeast2.run.app",
        scheme="http",
    )
    monkeypatch.delenv("SCHEDULER_OIDC_AUDIENCE", raising=False)

    def fake_verify(token, request, audience=None):
        assert audience is None
        return {
            "email": "672026052958-compute@developer.gserviceaccount.com",
            "email_verified": "true",
            "aud": "https://text-agent-backend-672026052958.australia-southeast2.run.app",
        }

    monkeypatch.setattr(auth_scheduler.id_token, "verify_oauth2_token", fake_verify)
    try:
        info = auth_scheduler.verify_cloud_scheduler_oidc(authorization="Bearer oidc-token")
        assert info["email_verified"] == "true"
    finally:
        auth_domain.reset_request_context(tokens)
