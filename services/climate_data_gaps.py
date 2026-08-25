"""
climate_data_gaps — what's MISSING from an entity's activity data.

Everything else in the climate pipeline answers "what have we got". This answers
"what haven't we got", which is the question that actually costs you a report.

Motivated by a real incident: an FY26 total moved from ~2,277 to 1,587 tCO2e and
nobody could tell whether that was a bug or correct. It was correct — invoices
with no readable date used to be silently swept into whatever FY was requested,
and are now excluded. But the only way to find that out was reading source code.
This module surfaces it on screen instead.

Deliberately DB-only apart from resolving the site list. Per-site Airtable
invoice fetches are what pushed the old activity-sources endpoint into Cloud Run
504s, so we don't do them here. That means we can say "nothing has been brought
in for this site" but not "Airtable has 12 invoices we haven't collected" — for
that, open the site in the workspace.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session

from models import ClimateActivityRecord
from services.climate_activity_etl import UTILITY_ACTIVITY_MAP, default_fy_period
from services.entity_groups import clients_in_disclosure_rollup

# Severity ordering — worst first, so the UI can sort on it without knowing the words.
SEVERITY_RANK = {
    "nothing_staged": 0,
    "undated": 1,
    "month_gaps": 2,
    "not_countable": 3,
    "ok": 4,
}


def _fy_months(fy_start: date, fy_end: date) -> list[str]:
    """The 12 'YYYY-MM' labels in a financial year, in order."""
    out: list[str] = []
    y, m = fy_start.year, fy_start.month
    while (y, m) <= (fy_end.year, fy_end.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _months_spanned(start: Optional[date], end: Optional[date]) -> list[str]:
    """
    Every month a billing period touches.

    A bill covering 15 Nov – 14 Dec counts for BOTH months: treating it as one
    month would report a false gap for the other.
    """
    if not start:
        return []
    if not end or end < start:
        end = start
    out: list[str] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
        if len(out) > 24:  # defensive: a nonsense period shouldn't loop forever
            break
    return out


def _site_key(utility_type: str, identifier: str) -> str:
    return f"{(utility_type or '').strip()}|{(identifier or '').strip()}"


def build_entity_data_gaps(
    db: Session,
    entity_id: str,
    *,
    period_label: str = "FY26",
    sites: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Gap report for one reporting entity and financial year.

    `sites` is the site list from build_entity_activity_manifest. Pass it in when
    the caller already has it (saves a second Airtable resolution); omit it and
    the report covers only sites that already have staged rows — still useful,
    but it cannot then tell you about a linked site with nothing brought in.
    """
    slug = (entity_id or "").strip().lower()
    if not slug:
        raise ValueError("entity_id required")

    fy_start, fy_end = default_fy_period(period_label)
    fy_months = _fy_months(fy_start, fy_end)

    clients = clients_in_disclosure_rollup(db, slug)
    if not clients:
        return {
            "found": False,
            "entity_id": slug,
            "period": period_label,
            "message": "No CRM client in disclosure rollup for this reporting_entity",
        }

    # One query for every staged row on the entity — all years, not just this FY,
    # so we can report what other years hold without a second round trip.
    rows = (
        db.query(ClimateActivityRecord)
        .filter(ClimateActivityRecord.entity_id == slug)
        .all()
    )

    # ---- accumulate per site -------------------------------------------------
    by_site: dict[str, dict[str, Any]] = {}
    other_years: dict[str, int] = {}
    undated_total = 0

    for r in rows:
        ut = (r.source_utility_type or "").strip()
        ident = (r.site_id or "").strip()
        key = _site_key(ut, ident)
        s = by_site.setdefault(
            key,
            {
                "utility_type": ut,
                "identifier": ident,
                "staged_in_period": 0,
                "undated": 0,
                "months_present": set(),
                "activity_types": set(),
            },
        )

        if (r.status or "") == "undated":
            s["undated"] += 1
            undated_total += 1
            continue

        start, end = r.reporting_period_start, r.reporting_period_end
        if start and fy_start <= start <= fy_end:
            s["staged_in_period"] += 1
            if r.activity_type:
                s["activity_types"].add(r.activity_type)
            for mth in _months_spanned(start, end):
                if mth in fy_months:
                    s["months_present"].add(mth)
        elif start:
            # A different financial year — worth reporting, since multi-year
            # staging means prior years are now genuinely available.
            fy = f"FY{((start.year + 1) if start.month >= 7 else start.year) % 100:02d}"
            other_years[fy] = other_years.get(fy, 0) + 1

    # ---- fold in the linked-site list so empty sites appear ------------------
    linked_keys: set[str] = set()
    if sites:
        for site in sites:
            ut = str(site.get("utility_type") or "").strip()
            ident = str(site.get("identifier") or "").strip()
            if not ut or not ident:
                continue
            key = _site_key(ut, ident)
            linked_keys.add(key)
            s = by_site.setdefault(
                key,
                {
                    "utility_type": ut,
                    "identifier": ident,
                    "staged_in_period": 0,
                    "undated": 0,
                    "months_present": set(),
                    "activity_types": set(),
                },
            )
            s["retailer"] = str(site.get("retailer") or "").strip()
            s["member_business_name"] = site.get("member_business_name")

    # ---- classify ------------------------------------------------------------
    out_sites: list[dict[str, Any]] = []
    for key, s in by_site.items():
        ut = s["utility_type"]
        countable = ut in UTILITY_ACTIVITY_MAP
        present = sorted(s["months_present"])
        missing = [m for m in fy_months if m not in s["months_present"]]

        if not countable:
            severity, headline = (
                "not_countable",
                f"{ut or 'Unknown utility'} can't be counted yet — linked on the LOA but no emission factor",
            )
        elif s["staged_in_period"] == 0 and s["undated"] == 0:
            severity, headline = (
                "nothing_staged",
                "Nothing brought in for this period",
            )
        elif s["undated"] > 0:
            severity, headline = (
                "undated",
                f"{s['undated']} invoice(s) have no readable date — excluded from the total until fixed in Airtable",
            )
        elif missing:
            severity, headline = (
                "month_gaps",
                f"No invoice for {len(missing)} of 12 months: {', '.join(missing[:4])}"
                + (" …" if len(missing) > 4 else ""),
            )
        else:
            severity, headline = ("ok", "Complete — all 12 months present")

        out_sites.append(
            {
                "utility_type": ut,
                "identifier": s["identifier"],
                "retailer": s.get("retailer", ""),
                "member_business_name": s.get("member_business_name"),
                "countable": countable,
                "staged_in_period": s["staged_in_period"],
                "undated": s["undated"],
                "activity_types": sorted(s["activity_types"]),
                "months_present": present,
                "months_missing": missing,
                "coverage_pct": round(100.0 * len(present) / max(1, len(fy_months)), 1),
                "severity": severity,
                "headline": headline,
                "still_linked": (not sites) or key in linked_keys,
            }
        )

    # Worst first, then biggest gap, then name — so the top of the list is the work.
    out_sites.sort(
        key=lambda s: (
            SEVERITY_RANK.get(s["severity"], 9),
            -len(s["months_missing"]),
            s["utility_type"],
            s["identifier"],
        )
    )

    counts: dict[str, int] = {}
    for s in out_sites:
        counts[s["severity"]] = counts.get(s["severity"], 0) + 1

    # Orphans: staged rows for a site that is no longer on the LOA. These keep
    # being counted, because a sync never deletes — only rebuild-staged does.
    orphans = [s for s in out_sites if not s["still_linked"] and s["staged_in_period"] > 0]

    return {
        "found": True,
        "entity_id": slug,
        "period": period_label,
        "period_start": fy_start.isoformat(),
        "period_end": fy_end.isoformat(),
        "fy_months": fy_months,
        "member_count": len(clients),
        "site_count": len(out_sites),
        "staged_in_period": sum(s["staged_in_period"] for s in out_sites),
        "undated_total": undated_total,
        "counts_by_severity": counts,
        "other_years_available": dict(sorted(other_years.items())),
        "orphan_site_count": len(orphans),
        "needs_attention": sum(
            v for k, v in counts.items() if k in ("nothing_staged", "undated", "month_gaps")
        ),
        "sites": out_sites,
        "notes": [
            "Undated invoices are staged but excluded from the total — fix the date in Airtable and refresh.",
            "Month gaps are based on the billing periods that were brought in, not on Airtable directly.",
            "Sites marked not countable are linked on the LOA but have no emission factor mapping yet.",
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_roster_data_gaps(
    db: Session,
    *,
    period_label: str = "FY26",
    limit: int = 100,
) -> dict[str, Any]:
    """
    One line per reporting entity — the overview the Data Disclosure page opens on.

    DB-only and deliberately cheap: no Airtable, no per-site work. Entities whose
    every site looks clean still appear, so "nothing to see" is a positive signal
    rather than an absence.
    """
    fy_start, fy_end = default_fy_period(period_label)

    slugs = [
        s[0]
        for s in db.query(ClimateActivityRecord.entity_id).distinct().all()
        if s and s[0]
    ]

    entities: list[dict[str, Any]] = []
    for slug in sorted(slugs)[: max(1, limit)]:
        try:
            rep = build_entity_data_gaps(db, slug, period_label=period_label)
        except Exception as e:  # pragma: no cover - one bad entity must not kill the page
            logging.warning("[data-gaps] %s failed: %s", slug, e)
            continue
        if not rep.get("found"):
            continue
        entities.append(
            {
                "entity_id": slug,
                "site_count": rep["site_count"],
                "staged_in_period": rep["staged_in_period"],
                "undated_total": rep["undated_total"],
                "needs_attention": rep["needs_attention"],
                "counts_by_severity": rep["counts_by_severity"],
                "other_years_available": rep["other_years_available"],
            }
        )

    entities.sort(key=lambda e: (-e["needs_attention"], e["entity_id"]))
    return {
        "period": period_label,
        "period_start": fy_start.isoformat(),
        "period_end": fy_end.isoformat(),
        "entity_count": len(entities),
        "entities_needing_attention": sum(1 for e in entities if e["needs_attention"] > 0),
        "undated_total": sum(e["undated_total"] for e in entities),
        "entities": entities,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
