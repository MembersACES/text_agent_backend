"""
Resolve reporting_entity → LOA linked utilities + Airtable invoice samples + staged SQL activity.

Used by Prograde dashboard (Scope 1 & 2 activity integration) and Marcus integration review.
"""
from __future__ import annotations

import logging
from typing import Any, Optional
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from models import Client, EntityGroup
from services import airtable_client
from services.entity_groups import (
    clients_in_disclosure_rollup,
    disclosure_source_for_client,
    effective_reporting_entity,
)
from services.climate_activity_etl import (
    EtlContext,
    UTILITY_ACTIVITY_MAP,
    default_fy_period,
    transform_invoice_rows,
)
from services.climate_store import activity_record_to_summary, list_activity_records

# Utilities ACES ETL can map today (others still listed from LOA for Marcus review).
ETL_SUPPORTED_UTILITIES = frozenset(UTILITY_ACTIVITY_MAP.keys())


def _etl_mapping_summary(utility_type: str) -> dict[str, Any]:
    cfg = UTILITY_ACTIVITY_MAP.get(utility_type)
    if not cfg:
        return {
            "etl_supported": False,
            "note": "Not in climate_activity_etl yet — linked on LOA only",
        }
    out: dict[str, Any] = {
        "etl_supported": True,
        "activity_type": cfg.get("activity_type"),
        "scope": cfg.get("scope"),
        "unit": cfg.get("unit"),
        "quantity_fields": list(cfg.get("quantity_fields") or []),
    }
    if cfg.get("tou_sum_field_groups"):
        out["tou_sum_field_groups"] = cfg["tou_sum_field_groups"]
    if cfg.get("mj_fields"):
        out["mj_fields"] = cfg["mj_fields"]
    return out


def _sites_from_loa(
    linked_utilities: dict,
    utility_retailers: dict,
    linked_utility_extra: dict,
) -> list[dict[str, Any]]:
    sites: list[dict[str, Any]] = []
    for utility_type, identifiers in (linked_utilities or {}).items():
        if not isinstance(identifiers, list):
            if isinstance(identifiers, str) and identifiers.strip():
                identifiers = [x.strip() for x in identifiers.split(",") if x.strip()]
            else:
                continue
        retailers = utility_retailers.get(utility_type) or []
        extras = linked_utility_extra.get(utility_type) or []
        if not isinstance(retailers, list):
            retailers = [str(retailers)] if retailers else []
        if not isinstance(extras, list):
            extras = []

        for idx, ident in enumerate(identifiers):
            ident_str = str(ident or "").strip()
            if not ident_str:
                continue
            extra = extras[idx] if idx < len(extras) and isinstance(extras[idx], dict) else {}
            retailer = ""
            if idx < len(retailers):
                retailer = str(retailers[idx] or "").strip()
            if not retailer and extra.get("retailer"):
                retailer = str(extra.get("retailer") or "").strip()
            sites.append(
                {
                    "utility_type": utility_type,
                    "identifier": ident_str,
                    "retailer": retailer,
                    "loa_extra": extra,
                    "etl_mapping": _etl_mapping_summary(utility_type),
                }
            )
    return sites


def _invoice_payload(
    utility_type: str,
    identifier: str,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    if not airtable_client.AIRTABLE_API_KEY or not getattr(airtable_client, "USE_AIRTABLE_DIRECT", False):
        return {
            "configured": False,
            "error": "Airtable direct mode not configured",
            "total_count": 0,
            "sample_rows": [],
            "diagnostics": {},
        }
    try:
        payload = airtable_client.get_utility_invoice_rows_by_identifier(
            utility_type,
            identifier,
            max_records=max(1, min(sample_limit, 20)),
        )
        rows = payload.get("rows", []) if isinstance(payload, dict) else []
        diagnostics = payload.get("diagnostics", {}) if isinstance(payload, dict) else {}
        total = payload.get("total_count", len(rows)) if isinstance(payload, dict) else len(rows)
        sample = []
        for row in rows[:sample_limit]:
            if not isinstance(row, dict):
                continue
            sample.append(
                {
                    "record_id": row.get("record_id"),
                    "field_keys": sorted(k for k in row.keys() if k != "row_number"),
                    "row": row,
                }
            )
        return {
            "configured": True,
            "total_count": int(total or 0),
            "sample_rows": sample,
            "diagnostics": diagnostics,
        }
    except Exception as exc:
        return {
            "configured": True,
            "error": str(exc),
            "total_count": 0,
            "sample_rows": [],
            "diagnostics": {},
        }


def _etl_preview(
    entity_id: str,
    client_id: int,
    loa_record_id: Optional[str],
    utility_type: str,
    identifier: str,
    invoice_rows: list[dict],
    period_label: str,
) -> dict[str, Any]:
    if utility_type not in ETL_SUPPORTED_UTILITIES:
        return {"supported": False, "preview": [], "diagnostics": {}}
    period_start, period_end = default_fy_period(period_label)
    ctx = EtlContext(
        entity_id=entity_id,
        client_id=client_id,
        loa_client_id=loa_record_id,
        site_id=identifier,
        utility_type=utility_type,
        period_start=period_start,
        period_end=period_end,
    )
    results, diagnostics = transform_invoice_rows(invoice_rows, ctx)
    preview = []
    for res in results[:25]:
        if res.skipped:
            preview.append(
                {
                    "skipped": True,
                    "source_row_id": res.source_row_id,
                    "reason": res.skip_reason,
                }
            )
        else:
            preview.append(
                {
                    "skipped": False,
                    "record_id": res.record_id,
                    "activity_type": res.body.get("activity_type"),
                    "scope": res.body.get("scope"),
                    "quantity": res.body.get("quantity"),
                    "unit": res.body.get("unit"),
                    "reporting_period": res.body.get("reporting_period"),
                }
            )
    return {
        "supported": True,
        "would_produce": sum(1 for r in results if not r.skipped),
        "would_skip": sum(1 for r in results if r.skipped),
        "preview": preview,
        "diagnostics": diagnostics,
    }


def _resolve_loa_for_client(
    client: Client,
    *,
    airtable_configured: bool,
) -> tuple[Optional[dict], Optional[str]]:
    loa_record_id = (client.external_business_id or "").strip() or None
    if not airtable_configured:
        return None, loa_record_id
    loa_record = None
    if loa_record_id:
        loa_record = airtable_client.get_loa_record_by_id(loa_record_id)
    if not loa_record and client.business_name:
        loa_record = airtable_client.get_loa_record_by_business_name(client.business_name)
    if loa_record:
        loa_record_id = loa_record.get("id") or loa_record_id
    return loa_record, loa_record_id


def _merge_loa_utility_maps(
    linked_utilities: dict,
    utility_retailers: dict,
    linked_utility_extra: dict,
    new_linked: dict,
    new_retailers: dict,
    new_extra: dict,
) -> None:
    for app_key, identifiers in (new_linked or {}).items():
        if not isinstance(identifiers, list):
            continue
        existing_ids = linked_utilities.setdefault(app_key, [])
        existing_retailers = utility_retailers.setdefault(app_key, [])
        existing_extras = linked_utility_extra.setdefault(app_key, [])
        seen = {str(x).strip() for x in existing_ids if str(x).strip()}
        retailers = new_retailers.get(app_key) or []
        extras = new_extra.get(app_key) or []
        if not isinstance(retailers, list):
            retailers = [str(retailers)] if retailers else []
        if not isinstance(extras, list):
            extras = []
        for idx, ident in enumerate(identifiers):
            ident_str = str(ident or "").strip()
            if not ident_str or ident_str in seen:
                continue
            seen.add(ident_str)
            existing_ids.append(ident_str)
            retailer = ""
            if idx < len(retailers):
                retailer = str(retailers[idx] or "").strip()
            extra: dict = {}
            if idx < len(extras) and isinstance(extras[idx], dict):
                extra = extras[idx]
            existing_retailers.append(retailer)
            existing_extras.append(extra)


def build_entity_activity_sources(
    db: Session,
    entity_id: str,
    *,
    period_label: str = "FY26",
    invoice_sample_limit: int = 3,
    invoice_fetch_limit: int = 50,
    include_etl_preview: bool = True,
) -> dict[str, Any]:
    """
    Full activity-source bundle for a reporting_entity slug (A1 kebab-case).

    NOTE: every database read is performed up front, then the DB session is
    released (db.close()) BEFORE the slow live-Airtable fan-out. A single
    request here can run for minutes; holding a pooled connection that long
    exhausted the (small) connection pool and made unrelated endpoints fail
    with QueuePool timeouts. Code after db.close() only reads already-loaded
    scalar columns on detached ORM objects (no lazy loads) and calls Airtable.
    """
    slug = (entity_id or "").strip().lower()
    if not slug:
        raise ValueError("entity_id required")

    clients = clients_in_disclosure_rollup(db, slug)
    if not clients:
        return {
            "found": False,
            "entity_id": slug,
            "period": period_label,
            "message": "No CRM client in disclosure rollup for this reporting_entity",
        }

    primary_client = clients[0]
    airtable_configured = bool(
        airtable_client.AIRTABLE_API_KEY and getattr(airtable_client, "USE_AIRTABLE_DIRECT", False)
    )

    group_by_id: dict[int, EntityGroup] = {}
    for client in clients:
        if client.entity_group_id and client.entity_group_id not in group_by_id:
            group = db.query(EntityGroup).filter(EntityGroup.id == client.entity_group_id).first()
            if group:
                group_by_id[group.id] = group

    entity_group_slug: str | None = None
    for group in group_by_id.values():
        if (group.reporting_entity or "").strip().lower() == slug:
            entity_group_slug = group.slug
            break

    # ------------------------------------------------------------------
    # DB reads — performed while the pooled connection is still checked out.
    # ------------------------------------------------------------------
    staged_all: list = []
    staged_by_key: dict[str, list[dict]] = {}
    for client in clients:
        for row in list_activity_records(db, client_id=client.id, limit=200):
            staged_all.append(row)
            summary = activity_record_to_summary(row)
            sid = (summary.get("site_id") or "").strip()
            ut = (summary.get("source_utility_type") or "").strip()
            if sid and ut:
                key = f"{ut}|{sid}"
                staged_by_key.setdefault(key, []).append(summary)

    members_payload: list[dict[str, Any]] = []
    for client in clients:
        group = group_by_id.get(client.entity_group_id) if client.entity_group_id else None
        members_payload.append(
            {
                "aces_client_id": client.id,
                "business_name": client.business_name,
                "loa_record_id": (client.external_business_id or "").strip() or None,
                "disclosure_source": disclosure_source_for_client(client, slug, group),
            }
        )

    # Release the pooled DB connection BEFORE the slow Airtable work. Only
    # already-loaded scalar attributes are read on the detached objects below.
    db.close()

    # ------------------------------------------------------------------
    # Live Airtable fan-out — NO DB connection held below this line.
    # ------------------------------------------------------------------
    linked_utilities: dict = {}
    utility_retailers: dict = {}
    linked_utility_extra: dict = {}
    loa_record_ids: list[str] = []
    site_by_key: dict[str, dict[str, Any]] = {}

    if airtable_configured:
        for client in clients:
            loa_record, loa_record_id = _resolve_loa_for_client(
                client, airtable_configured=airtable_configured
            )
            if loa_record_id and loa_record_id not in loa_record_ids:
                loa_record_ids.append(loa_record_id)
            if loa_record:
                new_linked, new_retailers, new_extra = airtable_client.get_linked_utility_records(
                    loa_record
                )
                _merge_loa_utility_maps(
                    linked_utilities,
                    utility_retailers,
                    linked_utility_extra,
                    new_linked,
                    new_retailers,
                    new_extra,
                )
                for site in _sites_from_loa(new_linked, new_retailers, new_extra):
                    key = f"{site['utility_type']}|{site['identifier']}"
                    if key in site_by_key:
                        continue
                    site_by_key[key] = {
                        **site,
                        "member_aces_client_id": client.id,
                        "member_business_name": client.business_name,
                        "member_loa_record_id": loa_record_id,
                    }

    sites = list(site_by_key.values()) if site_by_key else _sites_from_loa(
        linked_utilities, utility_retailers, linked_utility_extra
    )
    primary_loa_record_id = loa_record_ids[0] if loa_record_ids else None

    utility_bundles: list[dict[str, Any]] = []
    matched_staged_keys: set[str] = set()
    for site in sites:
        ut = site["utility_type"]
        ident = site["identifier"]
        member_client_id = site.get("member_aces_client_id") or primary_client.id
        member_loa_id = site.get("member_loa_record_id") or primary_loa_record_id
        inv = _invoice_payload(ut, ident, sample_limit=invoice_sample_limit)
        fetch_rows: list[dict] = []
        if inv.get("configured") and not inv.get("error"):
            try:
                full = airtable_client.get_utility_invoice_rows_by_identifier(
                    ut,
                    ident,
                    max_records=max(1, min(invoice_fetch_limit, 100)),
                )
                fetch_rows = full.get("rows", []) if isinstance(full, dict) else []
            except Exception:
                fetch_rows = [
                    s.get("row", {})
                    for s in inv.get("sample_rows", [])
                    if isinstance(s, dict) and isinstance(s.get("row"), dict)
                ]
        etl_block = (
            _etl_preview(
                slug,
                member_client_id,
                member_loa_id,
                ut,
                ident,
                fetch_rows,
                period_label,
            )
            if include_etl_preview
            else {"supported": ut in ETL_SUPPORTED_UTILITIES, "preview": []}
        )
        key = f"{ut}|{ident}"
        staged_for_site = staged_by_key.get(key, [])
        if staged_for_site:
            matched_staged_keys.add(key)
        utility_bundles.append(
            {
                **site,
                "aces_client_id": member_client_id,
                "member_aces_client_id": member_client_id,
                "member_business_name": site.get("member_business_name"),
                "loa_record_id": member_loa_id,
                "airtable_invoices": inv,
                "etl_preview": etl_block,
                "staged_activity_records": staged_for_site,
            }
        )

    orphaned_staged_records: list[dict] = []
    for key, summaries in staged_by_key.items():
        if key not in matched_staged_keys:
            orphaned_staged_records.extend(summaries)

    by_type: dict[str, list[dict]] = {}
    for bundle in utility_bundles:
        ut = bundle["utility_type"]
        by_type.setdefault(ut, []).append(bundle)

    return {
        "found": True,
        "entity_id": slug,
        "period": period_label,
        "entity_group_slug": entity_group_slug,
        "members": members_payload,
        "aces_client_id": primary_client.id,
        "aces_client_ids": [c.id for c in clients],
        "business_name": primary_client.business_name,
        "loa_record_id": primary_loa_record_id,
        "loa_record_ids": loa_record_ids,
        "airtable_configured": airtable_configured,
        "site_count": len(utility_bundles),
        "staged_activity_record_count": len(staged_all),
        "orphaned_staged_record_count": len(orphaned_staged_records),
        "orphaned_staged_records": orphaned_staged_records,
        "raw_loa": {
            "linked_utilities": linked_utilities,
            "utility_retailers": utility_retailers,
            "linked_utility_extra": linked_utility_extra,
        },
        "utilities_by_type": by_type,
        "sites": utility_bundles,
        "integration_notes": {
            "scope_1_2_sources": [
                "C&I Gas / SME Gas → natural_gas (Scope 1)",
                "C&I Electricity / SME Electricity → electricity_grid (Scope 2)",
                "Oil → liquid_fuel (Scope 1)",
            ],
            "blocked": [
                "Waste — no tonnes in Airtable (bins only)",
                "Cleaning — not in ETL adapter yet",
            ],
            "b4_boundary": "activity_record.v1 staged in climate_activity_records; Prograde ingest post-Tuesday",
        },
    }


def build_client_linked_utilities(db: Session, client: Client) -> dict[str, Any]:
    """Per-client LOA linked utilities — same resolver as activity-sources (CRM Climate tab)."""
    group: EntityGroup | None = None
    if client.entity_group_id:
        group = db.query(EntityGroup).filter(EntityGroup.id == client.entity_group_id).first()

    effective_slug = effective_reporting_entity(client, group)
    airtable_configured = bool(
        airtable_client.AIRTABLE_API_KEY and getattr(airtable_client, "USE_AIRTABLE_DIRECT", False)
    )

    linked_utilities: dict = {}
    utility_retailers: dict = {}
    linked_utility_extra: dict = {}
    loa_record_id: str | None = None

    if airtable_configured:
        loa_record, loa_record_id = _resolve_loa_for_client(
            client, airtable_configured=airtable_configured
        )
        if loa_record:
            linked_utilities, utility_retailers, linked_utility_extra = (
                airtable_client.get_linked_utility_records(loa_record)
            )
    else:
        loa_record_id = (client.external_business_id or "").strip() or None

    sites = _sites_from_loa(linked_utilities, utility_retailers, linked_utility_extra)
    disclosure_source = "none"
    if effective_slug:
        disclosure_source = disclosure_source_for_client(client, effective_slug, group)

    return {
        "client_id": client.id,
        "business_name": client.business_name,
        "loa_record_id": loa_record_id,
        "reporting_entity": (client.reporting_entity or "").strip().lower() or None,
        "effective_reporting_entity": effective_slug,
        "disclosure_source": disclosure_source,
        "entity_group_slug": group.slug if group else None,
        "group_reporting_entity": (group.reporting_entity or "").strip().lower() if group else None,
        "airtable_configured": airtable_configured,
        "linked_utilities": linked_utilities,
        "utility_retailers": utility_retailers,
        "linked_utility_extra": linked_utility_extra,
        "sites": sites,
        "site_count": len(sites),
    }


def build_entity_activity_manifest(
    db: Session,
    entity_id: str,
    *,
    period_label: str = "FY26",
) -> dict[str, Any]:
    """
    Fast 'manifest' for progressive loading: entity + members + the list of
    linked sites, WITHOUT the per-site Airtable invoice fetch / ETL preview.

    Pairs with build_entity_site_detail: the frontend loads this first (a few
    seconds), renders one row per site, then fetches each site's detail in
    parallel — so no single request approaches the Cloud Run timeout and large
    entities stop returning 504. Mirrors the DB-reads-then-close-then-Airtable
    discipline of build_entity_activity_sources; code after db.close() reads
    only already-loaded scalar columns on detached objects.
    """
    slug = (entity_id or "").strip().lower()
    if not slug:
        raise ValueError("entity_id required")

    clients = clients_in_disclosure_rollup(db, slug)
    if not clients:
        return {
            "found": False,
            "entity_id": slug,
            "period": period_label,
            "message": "No CRM client in disclosure rollup for this reporting_entity",
        }

    primary_client = clients[0]
    airtable_configured = bool(
        airtable_client.AIRTABLE_API_KEY and getattr(airtable_client, "USE_AIRTABLE_DIRECT", False)
    )

    group_by_id: dict[int, EntityGroup] = {}
    for client in clients:
        if client.entity_group_id and client.entity_group_id not in group_by_id:
            group = db.query(EntityGroup).filter(EntityGroup.id == client.entity_group_id).first()
            if group:
                group_by_id[group.id] = group

    entity_group_slug: str | None = None
    for group in group_by_id.values():
        if (group.reporting_entity or "").strip().lower() == slug:
            entity_group_slug = group.slug
            break

    # Counts only here — which sites already have staged SQL activity, so the UI
    # can flag them. The per-site detail endpoint returns the actual records.
    staged_keys: set[str] = set()
    staged_total = 0
    for client in clients:
        for row in list_activity_records(db, client_id=client.id, limit=200):
            staged_total += 1
            summary = activity_record_to_summary(row)
            sid = (summary.get("site_id") or "").strip()
            sut = (summary.get("source_utility_type") or "").strip()
            if sid and sut:
                staged_keys.add(f"{sut}|{sid}")

    members_payload: list[dict[str, Any]] = []
    for client in clients:
        group = group_by_id.get(client.entity_group_id) if client.entity_group_id else None
        members_payload.append(
            {
                "aces_client_id": client.id,
                "business_name": client.business_name,
                "loa_record_id": (client.external_business_id or "").strip() or None,
                "disclosure_source": disclosure_source_for_client(client, slug, group),
            }
        )

    primary_client_id = primary_client.id
    primary_client_name = primary_client.business_name
    all_client_ids = [c.id for c in clients]

    # Release the pooled DB connection before the LOA Airtable lookups.
    db.close()

    linked_utilities: dict = {}
    utility_retailers: dict = {}
    linked_utility_extra: dict = {}
    loa_record_ids: list[str] = []
    site_by_key: dict[str, dict[str, Any]] = {}

    if airtable_configured:
        # PERF: the per-client Airtable lookups (LOA resolve + linked-utility fetch) were the manifest's
        # main wall-time cost (sequential -> ~2 min for multi-member entities, which the synchronous
        # embed turned into a frozen tab). They run AFTER db.close() and only do HTTP — no shared DB
        # session — so we fan them out across a small thread pool. The MERGE stays sequential below
        # (preserves site order, avoids dict races). Worker cap is deliberately conservative to stay
        # under Airtable's rate limit; single-member entities run inline (no pool overhead).
        from concurrent.futures import ThreadPoolExecutor

        def _fetch_for_client(client):
            loa_record, loa_record_id = _resolve_loa_for_client(
                client, airtable_configured=airtable_configured
            )
            if loa_record:
                nl, nr, ne = airtable_client.get_linked_utility_records(loa_record)
            else:
                nl, nr, ne = {}, {}, {}
            return client, loa_record, loa_record_id, nl, nr, ne

        if len(clients) > 1:
            with ThreadPoolExecutor(max_workers=min(3, len(clients))) as _ex:
                _results = list(_ex.map(_fetch_for_client, clients))
        else:
            _results = [_fetch_for_client(c) for c in clients]

        for client, loa_record, loa_record_id, new_linked, new_retailers, new_extra in _results:
            if loa_record_id and loa_record_id not in loa_record_ids:
                loa_record_ids.append(loa_record_id)
            if loa_record:
                _merge_loa_utility_maps(
                    linked_utilities,
                    utility_retailers,
                    linked_utility_extra,
                    new_linked,
                    new_retailers,
                    new_extra,
                )
                for site in _sites_from_loa(new_linked, new_retailers, new_extra):
                    key = f"{site['utility_type']}|{site['identifier']}"
                    if key in site_by_key:
                        continue
                    site_by_key[key] = {
                        **site,
                        "member_aces_client_id": client.id,
                        "member_business_name": client.business_name,
                        "member_loa_record_id": loa_record_id,
                    }

    raw_sites = list(site_by_key.values()) if site_by_key else _sites_from_loa(
        linked_utilities, utility_retailers, linked_utility_extra
    )
    primary_loa_record_id = loa_record_ids[0] if loa_record_ids else None

    sites: list[dict[str, Any]] = []
    for site in raw_sites:
        ut = site["utility_type"]
        ident = site["identifier"]
        member_client_id = site.get("member_aces_client_id") or primary_client_id
        member_loa_id = site.get("member_loa_record_id") or primary_loa_record_id
        key = f"{ut}|{ident}"
        sites.append(
            {
                "site_key": key,
                "utility_type": ut,
                "identifier": ident,
                "retailer": site.get("retailer", ""),
                "loa_extra": site.get("loa_extra", {}),
                "etl_mapping": site.get("etl_mapping", {}),
                "etl_supported": ut in ETL_SUPPORTED_UTILITIES,
                "aces_client_id": member_client_id,
                "member_aces_client_id": member_client_id,
                "member_business_name": site.get("member_business_name"),
                "loa_record_id": member_loa_id,
                "has_staged_activity": key in staged_keys,
            }
        )

    # ACES enrichment: attach signed-contract status per site so any consumer (incl. the
    # Prograde Disc Engine) can show "contract on file" instead of a blind "unsigned" badge.
    # One cached FILE_IDS read for the whole manifest; never fatal to the manifest itself.
    contracts_available = False
    contract_sheet_id = None
    try:
        from services.signed_contract_dry_run import load_contract_index, normalize_business_name
        _ci = load_contract_index()
        _cidx = _ci["index"]
        contract_sheet_id = _ci.get("sheet_id")
        for s in sites:
            _bn = s.get("member_business_name") or primary_client_name
            _cmap = _cidx.get(normalize_business_name(_bn), {}) if _bn else {}
            s["contract"] = _cmap.get(s["utility_type"])  # {file_id,status,link} or None
        contracts_available = True
    except Exception as _ce:  # pragma: no cover - defensive
        logging.info("[manifest] contract enrichment skipped: %s", _ce)
        for s in sites:
            s.setdefault("contract", None)

    return {
        "found": True,
        "entity_id": slug,
        "period": period_label,
        "entity_group_slug": entity_group_slug,
        "contracts_available": contracts_available,
        "contract_sheet_id": contract_sheet_id,
        "members": members_payload,
        "aces_client_id": primary_client_id,
        "aces_client_ids": all_client_ids,
        "business_name": primary_client_name,
        "loa_record_id": primary_loa_record_id,
        "loa_record_ids": loa_record_ids,
        "airtable_configured": airtable_configured,
        "site_count": len(sites),
        "staged_activity_record_count": staged_total,
        "sites": sites,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_entity_site_detail(
    db: Session,
    entity_id: str,
    utility_type: str,
    identifier: str,
    *,
    member_aces_client_id: Optional[int] = None,
    member_loa_record_id: Optional[str] = None,
    period_label: str = "FY26",
    invoice_sample_limit: int = 3,
    invoice_fetch_limit: int = 50,
    include_etl_preview: bool = True,
) -> dict[str, Any]:
    """
    Per-site detail for progressive loading: Airtable invoice sample + full
    fetch + ETL preview + staged SQL activity for ONE (utility_type, identifier).
    Returns in a few seconds so the frontend can fan these out in parallel after
    build_entity_activity_manifest, never hitting the 504. DB read (staged
    records) happens first, then the connection is released before Airtable.
    """
    slug = (entity_id or "").strip().lower()
    ut = (utility_type or "").strip()
    ident = (identifier or "").strip()
    if not slug or not ut or not ident:
        raise ValueError("entity_id, utility_type and identifier are required")

    staged_for_site: list[dict] = []
    member_business_name: Optional[str] = None
    if member_aces_client_id:
        _client = db.query(Client).filter(Client.id == int(member_aces_client_id)).first()
        member_business_name = _client.business_name if _client else None
        for row in list_activity_records(db, client_id=int(member_aces_client_id), limit=200):
            summary = activity_record_to_summary(row)
            sid = (summary.get("site_id") or "").strip()
            sut = (summary.get("source_utility_type") or "").strip()
            if sid == ident and sut == ut:
                staged_for_site.append(summary)
    # Release the pooled DB connection before the Airtable work.
    db.close()

    # ACES enrichment: this site's signed-contract status (cached index; never fatal).
    contract = None
    if member_business_name:
        try:
            from services.signed_contract_dry_run import contracts_for
            contract = contracts_for(member_business_name).get(ut)
        except Exception as _ce:  # pragma: no cover - defensive
            logging.info("[site-detail] contract enrichment skipped: %s", _ce)

    # ACES enrichment (waste only): the invoice PDF Drive links + missing-invoice flag live ONLY in
    # the raw dump sheet, not in the Airtable invoice rows above. Attach them additively so the Disc
    # Engine gets the same PDFs our ACES Waste page shows, without changing the existing invoice source.
    waste_documents = None
    if ut.strip().lower() == "waste":
        try:
            from services.waste_invoice_dump import waste_documents_for_account
            waste_documents = waste_documents_for_account(ident)
        except Exception as _we:  # pragma: no cover - defensive
            logging.info("[site-detail] waste-documents enrichment skipped: %s", _we)

    inv = _invoice_payload(ut, ident, sample_limit=invoice_sample_limit)
    fetch_rows: list[dict] = []
    if inv.get("configured") and not inv.get("error"):
        try:
            full = airtable_client.get_utility_invoice_rows_by_identifier(
                ut,
                ident,
                max_records=max(1, min(invoice_fetch_limit, 100)),
            )
            fetch_rows = full.get("rows", []) if isinstance(full, dict) else []
        except Exception:
            fetch_rows = [
                s.get("row", {})
                for s in inv.get("sample_rows", [])
                if isinstance(s, dict) and isinstance(s.get("row"), dict)
            ]

    etl_block = (
        _etl_preview(
            slug,
            int(member_aces_client_id) if member_aces_client_id else 0,
            member_loa_record_id,
            ut,
            ident,
            fetch_rows,
            period_label,
        )
        if include_etl_preview
        else {"supported": ut in ETL_SUPPORTED_UTILITIES, "preview": []}
    )

    return {
        "site_key": f"{ut}|{ident}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "utility_type": ut,
        "identifier": ident,
        "aces_client_id": member_aces_client_id,
        "member_aces_client_id": member_aces_client_id,
        "loa_record_id": member_loa_record_id,
        "etl_supported": ut in ETL_SUPPORTED_UTILITIES,
        "member_business_name": member_business_name,
        "contract": contract,
        "airtable_invoices": inv,
        "waste_invoice_documents": waste_documents,
        "etl_preview": etl_block,
        "staged_activity_records": staged_for_site,
    }
