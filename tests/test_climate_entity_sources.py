"""Tests for entity activity-sources builder and API."""
import os

import pytest
from fastapi.testclient import TestClient

from main import app
@pytest.fixture
def client():
    return TestClient(app)


def test_activity_sources_api_requires_auth(client):
    res = client.get("/api/climate/entities/parramatta-leagues-club/activity-sources")
    assert res.status_code == 401


def test_activity_sources_api_not_found(client, monkeypatch):
    monkeypatch.setenv("CLIMATE_ROSTER_SERVICE_KEY", "test-roster-key")
    res = client.get(
        "/api/climate/entities/no-such-entity-slug/activity-sources",
        headers={"X-ACES-Service-Key": "test-roster-key"},
    )
    assert res.status_code == 404
