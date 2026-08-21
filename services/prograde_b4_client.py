"""
prograde_b4_client — forward staged activity to the ACES Climate API (B4).

Reads activity_record.v1 bodies already staged in climate_activity_records for a
reporting_entity + period and POSTs them to B4's /api/climate/reports/from-v1,
which runs the PC1 calc and returns real tCO2e. Optionally commits (locks) the
report to defensible.

Zero new dependencies (stdlib urllib). Config via env:
    B4_BASE_URL   e.g. https://aces-climate-api-xxxx.a.run.app   (no trailing slash)
    B4_API_KEY    service key matching the B4 API_KEY (optional if B4 has none)
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Optional

from sqlalchemy.orm import Session

from models import ClimateActivityRecord
from services.climate_activity_etl import default_fy_period


class B4PushError(Exception):
    pass


def _b4_base() -> str:
    base = (os.getenv("B4_BASE_URL") or "").rstrip("/")
    if not base:
        raise B4PushError("B4_BASE_URL is not set")
    return base


def collect_v1_bodies(db: Session, reporting_entity: str, period_label: str) -> list[dict]:
    """Staged activity_record.v1 bodies for this entity, within the FY period."""
    fy_start, fy_end = default_fy_period(period_label)
    rows = (
        db.query(ClimateActivityRecord)
        .filter(ClimateActivityRecord.entity_id == reporting_entity)
        .filter(ClimateActivityRecord.reporting_period_start >= fy_start)
        .filter(ClimateActivityRecord.reporting_period_start <= fy_end)
        .all()
    )
    bodies: list[dict] = []
    for r in rows:
        try:
            bodies.append(json.loads(r.body_json))
        except (TypeError, json.JSONDecodeError):
            # fall back to reconstructing the minimal calc fields from columns
            bodies.append({
                "record_id": r.record_id, "entity_id": r.entity_id,
                "site_id": r.site_id, "activity_type": r.activity_type,
                "scope": r.scope, "quantity": r.quantity, "unit": r.unit,
                "reporting_period": {
                    "start": r.reporting_period_start.isoformat() if r.reporting_period_start else None,
                    "end": r.reporting_period_end.isoformat() if r.reporting_period_end else None,
                },
            })
    return bodies


def push_report_to_b4(
    db: Session,
    reporting_entity: str,
    period_label: str = "FY26",
    *,
    commit: bool = False,
    jurisdiction: Optional[str] = None,
    user_email: str = "system@acesolutions.com.au",
) -> dict:
    """
    Forward staged v1 activity to B4 and return the computed report
    (totals_tco2e, run_id, hash_chain, status, skipped_activity).
    """
    bodies = collect_v1_bodies(db, reporting_entity, period_label)
    if not bodies:
        raise B4PushError(f"no staged activity for {reporting_entity} / {period_label}")
    if jurisdiction:
        for b in bodies:
            b.setdefault("jurisdiction", jurisdiction)

    payload = json.dumps({
        "entity_id": reporting_entity,
        "period": period_label,
        "records": bodies,
        "commit": commit,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{_b4_base()}/api/climate/reports/from-v1",
        data=payload, method="POST",
        headers={
            "Content-Type": "application/json",
            "X-User-Email": user_email,
            "X-API-Key": os.getenv("B4_API_KEY", ""),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")
        raise B4PushError(f"B4 returned {e.code}: {detail}")
    except urllib.error.URLError as e:
        raise B4PushError(f"cannot reach B4 at {_b4_base()}: {e.reason}")
