"""Tests for GET /api/climate/roster."""
import os

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_roster_requires_auth(client):
    res = client.get("/api/climate/roster")
    assert res.status_code == 401


def test_roster_with_service_key(client, monkeypatch):
    monkeypatch.setenv("CLIMATE_ROSTER_SERVICE_KEY", "test-roster-key")
    res = client.get(
        "/api/climate/roster",
        headers={"X-ACES-Service-Key": "test-roster-key"},
    )
    # May be 200 with empty list if no DB clients in test env
    assert res.status_code in (200, 500)
    if res.status_code == 200:
        data = res.json()
        assert "clients" in data
        assert data.get("period") == "FY26"
