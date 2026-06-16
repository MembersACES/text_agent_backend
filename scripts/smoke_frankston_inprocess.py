#!/usr/bin/env python3
"""In-process smoke tests (new code path, no HTTP auth / timeout limits)."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))
from dotenv import load_dotenv

load_dotenv(BACKEND_ROOT / ".env", override=True)

ENTITY = "frankston-rsl"


def ok(label: str, cond: bool, detail: str = "") -> int:
    mark = "PASS" if cond else "FAIL"
    suffix = f" — {detail}" if detail else ""
    print(f"  [{mark}] {label}{suffix}")
    return 0 if cond else 1


def main() -> int:
    fast = "--fast" in sys.argv or os.environ.get("SMOKE_FAST") == "1"
    if fast:
        print("(fast mode — skipping activity-sources invoice fetches)\n")
    failures = 0
    from database import SessionLocal
    from models import Client, EntityGroup
    from services import airtable_client
    from services.climate_entity_sources import (
        build_client_linked_utilities,
        build_entity_activity_sources,
    )
    from services.climate_store import list_climate_roster
    from services.entity_groups import build_entity_group_summary, clients_in_disclosure_rollup

    db = SessionLocal()
    try:
        print("=" * 60)
        print("DB: entity_groups + clients")
        print("=" * 60)
        groups = db.query(EntityGroup).all()
        for g in groups:
            print(
                f"  group id={g.id} slug={g.slug!r} reporting_entity={g.reporting_entity!r} "
                f"abn={g.primary_abn!r}"
            )
        rollup = clients_in_disclosure_rollup(db, ENTITY)
        print(f"  rollup members ({len(rollup)}):")
        for c in rollup:
            print(
                f"    id={c.id} name={c.business_name!r} reporting_entity={c.reporting_entity!r} "
                f"group_id={c.entity_group_id}"
            )

        print("\n" + "=" * 60)
        print("Roster v2 (in-process)")
        print("=" * 60)
        rows = list_climate_roster(db, query="frankston")
        frankston = [r for r in rows if r.get("reporting_entity") == ENTITY]
        failures += ok("one roster row", len(frankston) == 1, f"got {len(frankston)}")
        if frankston:
            r = frankston[0]
            failures += ok("member_count>=2", (r.get("member_count") or 0) >= 2)
            failures += ok("primary_abn set", bool(r.get("primary_abn")))

        print("\n" + "=" * 60)
        print("Group summary (in-process)")
        print("=" * 60)
        group = db.query(EntityGroup).filter(EntityGroup.slug == ENTITY).first()
        if group:
            summary = build_entity_group_summary(db, group)
            failures += ok(
                "group_reporting_entity",
                summary.get("group_reporting_entity") == ENTITY,
                str(summary.get("group_reporting_entity")),
            )
            failures += ok(
                "members_in_climate_rollup>=2",
                (summary.get("members_in_climate_rollup") or 0) >= 2,
                str(summary.get("members_in_climate_rollup")),
            )
            print(json.dumps(summary, indent=2))
        else:
            failures += ok("frankston-rsl group exists", False)

        print("\n" + "=" * 60)
        print("LOA resolver (live Airtable)")
        print("=" * 60)
        if airtable_client.AIRTABLE_API_KEY and airtable_client.USE_AIRTABLE_DIRECT:
            t0 = time.perf_counter()
            loa = airtable_client.get_loa_record_by_id("recjQ6GSka3b4kiB1")
            linked, retailers, _ = airtable_client.get_linked_utility_records(loa)
            print(f"  LOA resolve: {time.perf_counter()-t0:.1f}s keys={sorted(linked.keys())}")
            failures += ok("Oil present", "Oil" in linked)
            failures += ok("Waste present", "Waste" in linked)
            for ut in ["Oil", "Waste", "C&I Gas", "C&I Electricity"]:
                if ut in linked:
                    print(f"    {ut}: ids={linked[ut]} retailers={retailers.get(ut)}")
        else:
            print("  SKIP: Airtable not configured")

        print("\n" + "=" * 60)
        print("Linked-utilities client 1")
        print("=" * 60)
        c1 = db.query(Client).filter(Client.id == 1).first()
        if c1:
            lu = build_client_linked_utilities(db, c1)
            types = sorted({s["utility_type"] for s in lu.get("sites") or []})
            failures += ok("site_count>=6", (lu.get("site_count") or 0) >= 6, str(lu.get("site_count")))
            failures += ok("Oil+Waste", "Oil" in types and "Waste" in types, str(types))
            failures += ok("effective slug", lu.get("effective_reporting_entity") == ENTITY)

        if fast:
            print("\n" + "=" * 60)
            print("Activity-sources — SKIPPED (use without --fast; expect 3–8 min)")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print("Activity-sources (no ETL preview, live Airtable — slow)")
            print("=" * 60)
            t0 = time.perf_counter()
            payload = build_entity_activity_sources(
                db, ENTITY, include_etl_preview=False, invoice_sample_limit=1, invoice_fetch_limit=5
            )
            elapsed = time.perf_counter() - t0
            print(f"  completed in {elapsed:.1f}s")
            if payload.get("found"):
                failures += ok("site_count>=6", (payload.get("site_count") or 0) >= 6)
                types = sorted({s["utility_type"] for s in payload.get("sites") or []})
                failures += ok("Oil in bundle", "Oil" in types, str(types))
                failures += ok("Waste in bundle", "Waste" in types)
                failures += ok("2+ members", len(payload.get("members") or []) >= 2)
                sources = {m["disclosure_source"] for m in payload.get("members") or []}
                failures += ok("group_inherit", "group_inherit" in sources, str(sources))
                empty_retailers = [
                    s for s in payload.get("sites") or [] if not (s.get("retailer") or "").strip()
                ]
                failures += ok(
                    "all retailers filled", len(empty_retailers) == 0, f"{len(empty_retailers)} empty"
                )
                print(
                    f"  staged={payload.get('staged_activity_record_count')} "
                    f"orphaned={payload.get('orphaned_staged_record_count')}"
                )
                for s in payload.get("sites") or []:
                    staged = len(s.get("staged_activity_records") or [])
                    inv = (s.get("airtable_invoices") or {}).get("total_count")
                    print(
                        f"    {s['utility_type']} | {s['identifier']} | retailer={s.get('retailer')!r} | "
                        f"member={s.get('member_aces_client_id')} | staged={staged} | invoices={inv}"
                    )
            else:
                failures += ok("found", False, str(payload.get("message")))

    finally:
        db.close()

    print("\n" + "=" * 60)
    if failures:
        print(f"RESULT: {failures} FAILED")
        return 1
    print("RESULT: ALL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
