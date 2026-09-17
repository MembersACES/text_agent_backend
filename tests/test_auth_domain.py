"""Staff Google ID-token domain policy."""

from __future__ import annotations

import logging

import pytest
from fastapi import HTTPException
from starlette.requests import Request

import auth_domain


def _reset_flags(monkeypatch):
    monkeypatch.setattr(auth_domain, "_empty_env_logged", False)
    monkeypatch.setattr(auth_domain, "_invalid_mode_logged", False)


def test_parse_domains_unset_uses_default():
    assert auth_domain.parse_allowed_email_domains(None) == [
        "acesolutions.com.au",
        "czeroanz.com",
    ]


def test_parse_domains_empty_uses_default_and_logs(monkeypatch, caplog):
    _reset_flags(monkeypatch)
    with caplog.at_level(logging.WARNING):
        domains = auth_domain.parse_allowed_email_domains("  ")
    assert domains == ["acesolutions.com.au", "czeroanz.com"]
    assert auth_domain.LOG_EMPTY_ENV in caplog.text


def test_email_domain_allowed(monkeypatch):
    monkeypatch.setenv("AUTH_ALLOWED_EMAIL_DOMAINS", "acesolutions.com.au,czeroanz.com")
    assert auth_domain.email_domain_allowed("pat@acesolutions.com.au")
    assert auth_domain.email_domain_allowed("a@czeroanz.com")
    assert not auth_domain.email_domain_allowed("you@gmail.com")
    assert not auth_domain.email_domain_allowed("")
    assert not auth_domain.email_domain_allowed(None)


def test_mode_default_is_log(monkeypatch):
    monkeypatch.delenv("AUTH_DOMAIN_MODE", raising=False)
    assert auth_domain.get_auth_domain_mode() == "log"


def test_mode_enforce(monkeypatch):
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    assert auth_domain.get_auth_domain_mode() == "enforce"


def test_mode_invalid_falls_back_to_log(monkeypatch, caplog):
    _reset_flags(monkeypatch)
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "yes")
    with caplog.at_level(logging.WARNING):
        assert auth_domain.get_auth_domain_mode() == "log"
    assert auth_domain.LOG_MODE_INVALID in caplog.text


def test_policy_allows_staff_unchanged(monkeypatch):
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    idinfo = {"email": "morgan.h@acesolutions.com.au", "sub": "1"}
    assert auth_domain.apply_email_domain_policy(idinfo, "verify_google_token") is idinfo


def test_policy_log_only_lets_gmail_through(monkeypatch, caplog):
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "log")
    monkeypatch.setenv("AUTH_ALLOWED_EMAIL_DOMAINS", "acesolutions.com.au,czeroanz.com")
    idinfo = {"email": "you@gmail.com"}
    with caplog.at_level(logging.WARNING):
        out = auth_domain.apply_email_domain_policy(idinfo, "verify_google_token")
    assert out is idinfo
    assert auth_domain.LOG_DENY in caplog.text
    assert "auth_path=verify_google_token" in caplog.text
    assert "email=you@gmail.com" in caplog.text
    assert "mode=log" in caplog.text


def test_policy_enforce_403s_gmail(monkeypatch, caplog):
    monkeypatch.setenv("AUTH_DOMAIN_MODE", "enforce")
    idinfo = {"email": "you@gmail.com"}
    with caplog.at_level(logging.WARNING):
        with pytest.raises(HTTPException) as exc:
            auth_domain.apply_email_domain_policy(idinfo, "verify_roster_access")
    assert exc.value.status_code == 403
    assert auth_domain.LOG_DENY in caplog.text
    assert "auth_path=verify_roster_access" in caplog.text
    assert "mode=enforce" in caplog.text


def test_bind_request_context_sets_path():
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": "/api/clients",
        "raw_path": b"/api/clients",
        "query_string": b"",
        "headers": [(b"host", b"example.run.app")],
        "client": ("127.0.0.1", 123),
        "server": ("example.run.app", 443),
    }
    request = Request(scope)
    tokens = auth_domain.bind_request_context(request)
    try:
        assert auth_domain.current_request_path() == "/api/clients"
        assert auth_domain.current_request_origin().endswith("example.run.app")
        assert auth_domain.current_request_url().endswith("/api/clients")
    finally:
        auth_domain.reset_request_context(tokens)
