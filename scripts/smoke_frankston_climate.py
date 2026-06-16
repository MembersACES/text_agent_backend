#!/usr/bin/env python3
"""Local smoke tests for Frankston climate integration (IMMEDIATE_ACTION Phase 1-2)."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

BACKEND_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(BACKEND_ROOT / ".env", override=True)

BASE = os.environ.get("SMOKE_BASE_URL", "http://127.0.0.1:8000")
KEY = (os.environ.get("CLIMATE_ROSTER_SERVICE_KEY") or "").strip()
ENTITY = "frankston-rsl"
PERIOD = "FY26"
TIMEOUT = 300  # activity-sources can be slow against live Airtable


def hdrs() -> dict[str, str]:
    if not KEY:
        raise SystemExit("CLIMATE_ROSTER_SERVICE_KEY not set in .env")
    return {"X-ACES-Service-Key": KEY}


def get(path: str, **params) -> tuple[int, dict | list | str]:
    url = f"{BASE}{path}"
    t0 = time.perf_counter()
    r = requests.get(url, headers=hdrs(), params=params or None, timeout=TIMEOUT)
    elapsed = time.perf_counter() - t0
    try:
        body = r.json()
    except ValueError:
        body = r.text[:500]
    print(f"  GET {path} -> {r.status_code} ({elapsed:.1f}s)")
    return r.status_code, body


def ok(label: str, cond: bool, detail: str = "") -> bool:
    mark = "PASS" if cond else "FAIL"
    suffix = f" — {detail}" if detail else ""
    print(f"  [{mark}] {label}{suffix}")
    return cond


def section(title: str) -> None:
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")


def main() -> int:
    print(f"Smoke target: {BASE}")
    failures = 0

    section("0. Health")
    try:
        r = requests.get(f"{BASE}/docs", timeout=10)
        failures += not ok("Backend reachable", r.status_code == 200, f"HTTP {r.status_code}")
    except requests.RequestException as e:
        print(f"  [FAIL] Backend unreachable: {e}")
        return 1

    section("1. Climate roster v2")
    code, body = get("/api/climate/roster", period=PERIOD, q="frankston")
    if not isinstance(body, dict):
        failures += 1
        print(f"  Unexpected body: {body}")
    else:
        clients = body.get("clients") or []
        frankston = [c for c in clients if (c.get("reporting_entity") or "") == ENTITY]
        failures += not ok("Roster returns frankston-rsl", len(frankston) >= 1)
        if frankston:
            row = frankston[0]
            failures += not ok(
                "Roster deduped (one row)",
                len(frankston) == 1,
                f"got {len(frankston)} rows",
            )
            failures += not ok(
                "member_count >= 2",
                (row.get("member_count") or 0) >= 2,
                f"member_count={row.get('member_count')}",
            )
            failures += not ok(
                "activity_record_count present",
                "activity_record_count" in row,
                str(row.get("activity_record_count")),
            )
            failures += not ok(
                "entity_group_slug set",
                row.get("entity_group_slug") == ENTITY,
                str(row.get("entity_group_slug")),
            )
            print(f"  roster row: {json.dumps(row, indent=2)[:800]}")

    section("2. Entity group summary")
    code, body = get(f"/api/entity-groups/{ENTITY}/summary")
    if isinstance(body, dict):
        failures += not ok("Summary HTTP 200", code == 200)
        failures += not ok(
            "group_reporting_entity",
            body.get("group_reporting_entity") == ENTITY,
            str(body.get("group_reporting_entity")),
        )
        failures += not ok(
            "members_in_climate_rollup >= 2",
            (body.get("members_in_climate_rollup") or 0) >= 2,
            str(body.get("members_in_climate_rollup")),
        )
        print(
            f"  summary: members={body.get('member_count')} "
            f"rollup={body.get('members_in_climate_rollup')} "
            f"staged={body.get('staged_activity_total')}"
        )
    else:
        failures += 1

    section("3. Entity group detail")
    code, body = get(f"/api/entity-groups/{ENTITY}")
    if isinstance(body, dict):
        failures += not ok("Detail HTTP 200", code == 200)
        failures += not ok(
            "group reporting_entity on detail",
            (body.get("reporting_entity") or "") == ENTITY,
            str(body.get("reporting_entity")),
        )
        members = body.get("members") or []
        failures += not ok("At least 2 group members", len(members) >= 2, f"count={len(members)}")
        box_hill = [m for m in members if "box hill" in (m.get("business_name") or "").lower()]
        if box_hill:
            bh = box_hill[0]
            failures += not ok(
                "Box Hill has null member reporting_entity (inherit path)",
                not (bh.get("reporting_entity") or "").strip(),
                f"reporting_entity={bh.get('reporting_entity')}",
            )
            print(f"  Box Hill client id={bh.get('id')}")
        frankston_members = [m for m in members if m.get("reporting_entity") == ENTITY]
        failures += not ok("Frankston member has reporting_entity", len(frankston_members) >= 1)

    section("4. Activity-sources (live Airtable — may take several minutes)")
    code, body = get(
        f"/api/climate/entities/{ENTITY}/activity-sources",
        period=PERIOD,
        invoice_sample_limit=1,
    )
    if isinstance(body, dict) and body.get("found"):
        failures += not ok("activity-sources HTTP 200", code == 200)
        failures += not ok(
            "site_count >= 6",
            (body.get("site_count") or 0) >= 6,
            f"site_count={body.get('site_count')}",
        )
        utility_types = sorted({s.get("utility_type") for s in body.get("sites") or []})
        failures += not ok("Oil in sites", "Oil" in utility_types, str(utility_types))
        failures += not ok("Waste in sites", "Waste" in utility_types, str(utility_types))
        failures += not ok(
            "members includes 2+ rollup members",
            len(body.get("members") or []) >= 2,
            str(len(body.get("members") or [])),
        )
        sources = {m.get("disclosure_source") for m in body.get("members") or []}
        failures += not ok(
            "group_inherit member present",
            "group_inherit" in sources,
            str(sources),
        )
        retailers_empty = [
            s for s in body.get("sites") or [] if not (s.get("retailer") or "").strip()
        ]
        failures += not ok(
            "all sites have retailer",
            len(retailers_empty) == 0,
            f"{len(retailers_empty)} empty: "
            + ", ".join(f"{s.get('utility_type')}/{s.get('identifier')}" for s in retailers_empty[:5]),
        )
        oil_sites = [s for s in body.get("sites") or [] if s.get("utility_type") == "Oil"]
        if oil_sites:
            oil_staged = sum(len(s.get("staged_activity_records") or []) for s in oil_sites)
            print(f"  Oil staged records attached: {oil_staged}")
        waste_sites = [s for s in body.get("sites") or [] if s.get("utility_type") == "Waste"]
        if waste_sites:
            inv = waste_sites[0].get("airtable_invoices") or {}
            print(f"  Waste invoice total_count: {inv.get('total_count')}")
        print(f"  utility types: {utility_types}")
        print(f"  staged total: {body.get('staged_activity_record_count')} orphaned: {body.get('orphaned_staged_record_count')}")
        for s in body.get("sites") or []:
            print(
                f"    - {s.get('utility_type')} | {s.get('identifier')} | "
                f"retailer={s.get('retailer')!r} | member={s.get('member_aces_client_id')}"
            )
    else:
        failures += not ok("activity-sources found", False, str(body)[:200])

    section("5. Per-client linked-utilities (Frankston client 1, in-process)")
    sys.path.insert(0, str(BACKEND_ROOT))
    from database import SessionLocal
    from models import Client
    from services.climate_entity_sources import build_client_linked_utilities

    db = SessionLocal()
    try:
        client = db.query(Client).filter(Client.id == 1).first()
        if not client:
            failures += not ok("Frankston client id=1 exists", False)
        else:
            lu = build_client_linked_utilities(db, client)
            failures += not ok(
                "site_count >= 4 for Frankston LOA",
                (lu.get("site_count") or 0) >= 4,
                f"site_count={lu.get('site_count')}",
            )
            types = sorted({s.get("utility_type") for s in lu.get("sites") or []})
            failures += not ok("Oil in linked-utilities", "Oil" in types, str(types))
            failures += not ok("Waste in linked-utilities", "Waste" in types, str(types))
            failures += not ok(
                "effective_reporting_entity",
                lu.get("effective_reporting_entity") == ENTITY,
                str(lu.get("effective_reporting_entity")),
            )
            print(f"  types: {types}")
    finally:
        db.close()

    section("6. Auth guard")
    r = requests.get(f"{BASE}/api/climate/roster", timeout=10)
    failures += not ok("Roster without key returns 401", r.status_code == 401, f"HTTP {r.status_code}")

    section("7. Direct LOA resolver (unit-level, no HTTP)")
    from services import airtable_client

    if airtable_client.AIRTABLE_API_KEY and airtable_client.USE_AIRTABLE_DIRECT:
        loa = airtable_client.get_loa_record_by_id("recjQ6GSka3b4kiB1")
        if loa:
            linked, retailers, _ = airtable_client.get_linked_utility_records(loa)
            failures += not ok("LOA has Oil key", "Oil" in linked, str(list(linked.keys())))
            failures += not ok("LOA has Waste key", "Waste" in linked, str(list(linked.keys())))
            if "Oil" in retailers:
                failures += not ok(
                    "Oil retailer from rollup",
                    bool((retailers.get("Oil") or [""])[0]),
                    str(retailers.get("Oil")),
                )
            if "Waste" in retailers:
                failures += not ok(
                    "Waste retailer from rollup",
                    bool((retailers.get("Waste") or [""])[0]),
                    str(retailers.get("Waste")),
                )
            print(f"  LOA utility keys: {sorted(linked.keys())}")
        else:
            failures += not ok("Fetch Frankston LOA", False)
    else:
        print("  [SKIP] Airtable direct not configured")

    section("RESULT")
    if failures:
        print(f"\n{failures} check(s) FAILED")
        return 1
    print("\nAll checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
