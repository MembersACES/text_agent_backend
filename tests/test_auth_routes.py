"""Route-level checks for the staff-auth PR (debug dump gone, crons require OIDC)."""

from fastapi.testclient import TestClient

from main import app


client = TestClient(app)


def test_client_status_debug_all_removed():
    res = client.get("/api/client-status/debug/all")
    assert res.status_code == 404


def test_check_due_cron_requires_scheduler_oidc():
    res = client.post("/api/tasks/check-due-cron")
    assert res.status_code == 401


def test_consumables_cron_requires_scheduler_oidc():
    res = client.post("/api/pudu/consumables/baseline-redetect-all-sites-cron")
    assert res.status_code == 401
