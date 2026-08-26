"""
Regression test for the connection-pool-exhaustion fix.

build_entity_activity_sources runs a slow live-Airtable fan-out. It must
release its DB connection (db.close()) BEFORE making any Airtable call, so a
multi-minute request can't pin a pooled connection and 500 unrelated
endpoints with QueuePool timeouts.

This test asserts the ordering invariant: every Airtable call happens only
after the session is closed, and the result is still well-formed.
"""
import types

import services.climate_entity_sources as ces


class _FakeDB:
    """Minimal stand-in for a SQLAlchemy Session that records close()."""

    def __init__(self):
        self.closed = False

    def query(self, *args, **kwargs):  # pragma: no cover - should not run here
        raise AssertionError(
            "No db.query expected: test clients have entity_group_id=None"
        )

    def close(self):
        self.closed = True


def _client(**overrides):
    base = dict(
        id=1,
        business_name="Acme Pty Ltd",
        external_business_id="recLOA123",
        entity_group_id=None,
        reporting_entity="acme",
    )
    base.update(overrides)
    return types.SimpleNamespace(**base)


def test_session_closed_before_any_airtable_call(monkeypatch):
    db = _FakeDB()
    events = []

    # DB-backed helpers (must run while the session is open).
    monkeypatch.setattr(
        ces, "clients_in_disclosure_rollup", lambda _db, _slug: [_client()]
    )

    def _list_records(_db, **_kw):
        events.append(("db_read", db.closed))
        return []

    monkeypatch.setattr(ces, "list_activity_records", _list_records)
    monkeypatch.setattr(
        ces, "disclosure_source_for_client", lambda *_a, **_k: "group_member"
    )

    # Airtable calls — each asserts the connection is already released.
    monkeypatch.setattr(ces.airtable_client, "AIRTABLE_API_KEY", "test-key")
    monkeypatch.setattr(
        ces.airtable_client, "USE_AIRTABLE_DIRECT", True, raising=False
    )

    def _loa_by_id(_record_id):
        events.append(("airtable", db.closed))
        assert db.closed, "DB session must be closed before any Airtable call"
        return None

    monkeypatch.setattr(ces.airtable_client, "get_loa_record_by_id", _loa_by_id)
    monkeypatch.setattr(
        ces.airtable_client, "get_loa_record_by_business_name", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        ces.airtable_client,
        "get_linked_utility_records",
        lambda *_a, **_k: ({}, {}, {}),
    )

    out = ces.build_entity_activity_sources(db, "acme")

    # Connection was released, and the payload is intact.
    assert db.closed is True
    assert out["found"] is True
    assert out["aces_client_id"] == 1
    assert out["aces_client_ids"] == [1]
    assert out["business_name"] == "Acme Pty Ltd"

    # Ordering: the DB read happened before close; Airtable strictly after.
    assert ("db_read", False) in events
    assert ("airtable", True) in events
    assert events.index(("db_read", False)) < events.index(("airtable", True))
