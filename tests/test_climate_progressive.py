"""
Tests for the progressive-loading endpoints (manifest + per-site detail).

These split the one slow activity-sources request into a fast manifest plus
many quick per-site calls, so large entities (e.g. aligned-leisure) stop
returning 504. Both functions must release the DB connection BEFORE any
Airtable call, same as build_entity_activity_sources.
"""
import types

import services.climate_entity_sources as ces


class _FakeDB:
    def __init__(self):
        self.closed = False

    def query(self, *a, **k):  # pragma: no cover - clients have entity_group_id=None
        raise AssertionError("no db.query expected in this test")

    def close(self):
        self.closed = True


def _client(**kw):
    base = dict(
        id=7,
        business_name="Aligned Leisure",
        external_business_id="recLOA",
        entity_group_id=None,
        reporting_entity="aligned-leisure",
    )
    base.update(kw)
    return types.SimpleNamespace(**base)


def test_manifest_lists_sites_and_closes_before_airtable(monkeypatch):
    db = _FakeDB()
    events = []

    monkeypatch.setattr(ces, "clients_in_disclosure_rollup", lambda _db, _s: [_client()])
    monkeypatch.setattr(
        ces, "list_activity_records",
        lambda _db, **k: events.append(("db_read", db.closed)) or [],
    )
    monkeypatch.setattr(ces, "disclosure_source_for_client", lambda *a, **k: "group_member")

    monkeypatch.setattr(ces.airtable_client, "AIRTABLE_API_KEY", "key")
    monkeypatch.setattr(ces.airtable_client, "USE_AIRTABLE_DIRECT", True, raising=False)
    monkeypatch.setattr(
        ces, "_resolve_loa_for_client",
        lambda client, *, airtable_configured: ({"id": "recLOA"}, "recLOA"),
    )

    def _linked(_loa):
        events.append(("airtable", db.closed))
        assert db.closed, "session must be closed before Airtable LOA lookup"
        return ({"C&I Electricity": ["NMI123"]}, {"C&I Electricity": ["RetA"]}, {"C&I Electricity": [{}]})

    monkeypatch.setattr(ces.airtable_client, "get_linked_utility_records", _linked)

    out = ces.build_entity_activity_manifest(db, "aligned-leisure")

    assert db.closed is True
    assert out["found"] is True
    assert out["site_count"] == 1
    site = out["sites"][0]
    assert site["site_key"] == "C&I Electricity|NMI123"
    assert site["utility_type"] == "C&I Electricity"
    assert site["identifier"] == "NMI123"
    assert site["member_aces_client_id"] == 7
    assert site["has_staged_activity"] is False
    # no per-site invoice/etl payload in the manifest (that's the detail call)
    assert "airtable_invoices" not in site
    # ordering: DB read happened before close; Airtable strictly after
    assert events.index(("db_read", False)) < events.index(("airtable", True))


def test_site_detail_closes_before_airtable_and_returns_bundle(monkeypatch):
    db = _FakeDB()
    events = []

    # one staged record that matches this site
    monkeypatch.setattr(
        ces, "list_activity_records",
        lambda _db, **k: events.append(("db_read", db.closed)) or [object()],
    )
    monkeypatch.setattr(
        ces, "activity_record_to_summary",
        lambda _row: {"site_id": "NMI123", "source_utility_type": "C&I Electricity", "record_id": "rec1"},
    )

    monkeypatch.setattr(ces.airtable_client, "AIRTABLE_API_KEY", "key")
    monkeypatch.setattr(ces.airtable_client, "USE_AIRTABLE_DIRECT", True, raising=False)

    def _rows(_ut, _ident, **k):
        events.append(("airtable", db.closed))
        assert db.closed, "session must be closed before Airtable invoice fetch"
        return {"rows": [{"record_id": "inv1", "row_number": 1}], "total_count": 1, "diagnostics": {}}

    monkeypatch.setattr(ces.airtable_client, "get_utility_invoice_rows_by_identifier", _rows)

    out = ces.build_entity_site_detail(
        db,
        "aligned-leisure",
        "C&I Electricity",
        "NMI123",
        member_aces_client_id=7,
        member_loa_record_id="recLOA",
        include_etl_preview=False,  # keep the ETL transform out of this unit test
    )

    assert db.closed is True
    assert out["site_key"] == "C&I Electricity|NMI123"
    assert out["airtable_invoices"]["configured"] is True
    assert out["airtable_invoices"]["total_count"] == 1
    assert len(out["staged_activity_records"]) == 1
    assert out["staged_activity_records"][0]["record_id"] == "rec1"
    assert events.index(("db_read", False)) < events.index(("airtable", True))
