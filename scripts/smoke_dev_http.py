#!/usr/bin/env python3
"""HTTP smoke against deployed dev backend (no local DB)."""
import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

BACKEND_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(BACKEND_ROOT / ".env", override=True)

BASE = os.environ.get(
    "SMOKE_BASE_URL",
    "https://text-agent-backend-dev-672026052958.australia-southeast2.run.app",
)
KEY = (os.environ.get("CLIMATE_ROSTER_SERVICE_KEY") or "").strip()
ENTITY = "frankston-rsl"
FAST = "--fast" in sys.argv


def main() -> int:
    if not KEY:
        print("CLIMATE_ROSTER_SERVICE_KEY missing")
        return 1
    H = {"X-ACES-Service-Key": KEY}
    failures = 0
    print(f"=== DEV BACKEND SMOKE: {BASE} ===\n")

    r = requests.get(f"{BASE}/docs", timeout=15)
    ok = r.status_code == 200
    print(f"[{'PASS' if ok else 'FAIL'}] reachable")
    failures += not ok

    r = requests.get(
        f"{BASE}/api/climate/roster",
        headers=H,
        params={"q": "frankston", "period": "FY26"},
        timeout=30,
    )
    print(f"roster HTTP {r.status_code}")
    if r.ok:
        rows = [c for c in r.json().get("clients", []) if c.get("reporting_entity") == ENTITY]
        if rows:
            print(json.dumps(rows[0], indent=2)[:1200])
            failures += 0 if rows[0].get("reporting_entity") == ENTITY else 1
            print(f"[{'PASS' if rows else 'FAIL'}] roster frankston-rsl row")
        else:
            print("[FAIL] no frankston roster row")
            failures += 1

    if FAST:
        print("\n(fast mode — skipping activity-sources)")
        return failures

    print("\nactivity-sources (slow)...")
    t0 = time.time()
    try:
        r = requests.get(
            f"{BASE}/api/climate/entities/{ENTITY}/activity-sources",
            headers=H,
            params={"period": "FY26", "invoice_sample_limit": 0},
            timeout=360,
        )
        print(f"HTTP {r.status_code} in {time.time() - t0:.0f}s")
        if r.ok:
            p = r.json()
            types = sorted({s.get("utility_type") for s in p.get("sites", [])})
            print(
                json.dumps(
                    {
                        "site_count": p.get("site_count"),
                        "members": p.get("members"),
                        "entity_group_slug": p.get("entity_group_slug"),
                        "utility_types": types,
                    },
                    indent=2,
                )[:2000]
            )
            failures += (p.get("site_count") or 0) < 6
            failures += "Oil" not in types or "Waste" not in types
            for s in p.get("sites", []):
                print(
                    f"  {s.get('utility_type')} | {s.get('identifier')} | retailer={s.get('retailer')!r}"
                )
    except requests.Timeout:
        print(f"[TIMEOUT] after {time.time() - t0:.0f}s")
        failures += 1

    print(f"\n{'FAILED' if failures else 'ALL PASSED'} ({failures} failures)")
    return failures


if __name__ == "__main__":
    sys.exit(main())
