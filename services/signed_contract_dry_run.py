"""
Read-only diagnostic: match CRM clients to FILE_IDS sheet rows and report
"Signed via ACES" contract status vs current client stage.

Used by scripts/dry_run_signed_contract_stage.py and GET /api/admin/signed-contract-dry-run.
No database or sheet writes.
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from models import Client
from tools.business_info import (
    FILE_IDS_SHEET_ID,
    FILE_IDS_SHEET_NAME,
    get_sheets_service,
)

ROW_KEYS = [
    "business_name",
    "record_id",
    "sc_ci_e_status",
    "sc_sme_e_status",
    "sc_ci_g_status",
    "sc_sme_g_status",
    "sc_waste_status",
    "sc_oil_status",
    "sc_dma_status",
    "sc_ci_e_file",
    "sc_sme_e_file",
    "sc_ci_g_file",
    "sc_sme_g_file",
    "sc_waste_file",
    "sc_oil_file",
    "sc_dma_file",
]

HEADER_TO_KEY: dict[str, str] = {
    "business name": "business_name",
    "record id": "record_id",
    "record_id": "record_id",
    "airtable record id": "record_id",
    "sc c&i e status:": "sc_ci_e_status",
    "sc c&i e status": "sc_ci_e_status",
    "sc sme e status:": "sc_sme_e_status",
    "sc sme e status": "sc_sme_e_status",
    "sc c&i g status:": "sc_ci_g_status",
    "sc c&i g status": "sc_ci_g_status",
    "sc sme g status:": "sc_sme_g_status",
    "sc sme g status": "sc_sme_g_status",
    "sc waste status:": "sc_waste_status",
    "sc waste status": "sc_waste_status",
    "sc oil status:": "sc_oil_status",
    "sc oil status": "sc_oil_status",
    "sc dma status:": "sc_dma_status",
    "sc dma status": "sc_dma_status",
    "sc c&i e": "sc_ci_e_file",
    "sc sme e": "sc_sme_e_file",
    "sc c&i g": "sc_ci_g_file",
    "sc sme g": "sc_sme_g_file",
    "sc waste": "sc_waste_file",
    "sc oil": "sc_oil_file",
    "sc dma": "sc_dma_file",
}

STATUS_FILE_PAIRS: list[tuple[str, str, str]] = [
    ("sc_ci_e_status", "sc_ci_e_file", "C&I Electricity"),
    ("sc_sme_e_status", "sc_sme_e_file", "SME Electricity"),
    ("sc_ci_g_status", "sc_ci_g_file", "C&I Gas"),
    ("sc_sme_g_status", "sc_sme_g_file", "SME Gas"),
    ("sc_waste_status", "sc_waste_file", "Waste"),
    ("sc_oil_status", "sc_oil_file", "Oil"),
    ("sc_dma_status", "sc_dma_file", "DMA"),
]


def _normalize_header(h: Any) -> str:
    if h is None:
        return ""
    s = str(h).strip().lower()
    return re.sub(r"\s+", " ", s)


def _header_without_parens(h: str) -> str:
    s = re.sub(r"\s*\([^)]*\)", "", h)
    return re.sub(r"\s+", " ", s).strip()


def _build_header_map(base: dict[str, str]) -> dict[str, str]:
    out = dict(base)
    for header, key in base.items():
        stripped = _header_without_parens(header)
        if stripped and stripped not in out:
            out[stripped] = key
    return out


def _resolve_header_key(header: str, header_map: dict[str, str]) -> str | None:
    if not header:
        return None
    key = header_map.get(header)
    if key:
        return key
    return header_map.get(_header_without_parens(header))


def _find_header_row_index(values: list[list[Any]], header_map: dict[str, str]) -> int:
    best_idx = 0
    best_score = -1
    scan = min(5, len(values))
    for idx in range(scan):
        row = values[idx]
        score = sum(
            1
            for cell in row
            if _resolve_header_key(_normalize_header(cell), header_map)
        )
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx


def _row_has_content(obj: dict[str, str]) -> bool:
    return any((v or "").strip() for v in obj.values())


def normalize_business_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def _cell_str(raw: Any) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def read_data_from_airtable_tab() -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Bulk-read FILE_IDS sheet tab once. Returns (rows, meta)."""
    if not FILE_IDS_SHEET_ID:
        raise RuntimeError("FILE_IDS_SHEET_ID is not set")

    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service (check SERVICE_ACCOUNT_*)")

    tab = FILE_IDS_SHEET_NAME or "Data from Airtable"
    range_str = f"'{tab}'!A1:AZ5000"
    resp = service.spreadsheets().values().get(
        spreadsheetId=FILE_IDS_SHEET_ID,
        range=range_str,
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()

    values = resp.get("values", [])
    warnings: list[str] = []
    if not values:
        return [], {
            "sheet_id": FILE_IDS_SHEET_ID,
            "sheet_tab": tab,
            "record_id_column_detected": False,
            "warnings": ["Sheet tab returned no rows"],
        }

    header_map = _build_header_map(HEADER_TO_KEY)
    header_idx = _find_header_row_index(values, header_map)
    raw_headers = values[header_idx]
    resolved_keys = {
        _resolve_header_key(_normalize_header(h), header_map)
        for h in raw_headers
        if _resolve_header_key(_normalize_header(h), header_map)
    }
    record_id_detected = "record_id" in resolved_keys
    if not record_id_detected:
        warnings.append(
            "No record_id column detected in sheet headers; ID joins fall back to business name only."
        )

    rows: list[dict[str, str]] = []
    for row in values[header_idx + 1 :]:
        obj: dict[str, str] = {k: "" for k in ROW_KEYS}
        for i, raw in enumerate(row):
            if i >= len(raw_headers):
                break
            h = _normalize_header(raw_headers[i])
            key = _resolve_header_key(h, header_map)
            if key:
                obj[key] = _cell_str(raw)
        if _row_has_content(obj):
            rows.append(obj)

    return rows, {
        "sheet_id": FILE_IDS_SHEET_ID,
        "sheet_tab": tab,
        "record_id_column_detected": record_id_detected,
        "warnings": warnings,
    }


def load_clients(db: Session) -> list[dict[str, Any]]:
    rows = (
        db.query(Client.id, Client.business_name, Client.external_business_id, Client.stage)
        .order_by(Client.business_name.asc())
        .all()
    )
    return [
        {
            "id": r.id,
            "business_name": r.business_name,
            "external_business_id": r.external_business_id,
            "stage": r.stage,
        }
        for r in rows
    ]


def _status_tokens(status_raw: str) -> list[str]:
    if not status_raw.strip():
        return []
    return [t.strip() for t in status_raw.split(",") if t.strip()]


def _token_is_signed_via_aces(token: str) -> bool:
    t = token.lower()
    if "existing" in t:
        return False
    return "signed via aces" in t


def compute_signed_utilities(sheet_row: dict[str, str]) -> tuple[bool, list[str]]:
    signed: list[str] = []
    for status_key, file_key, label in STATUS_FILE_PAIRS:
        status_raw = sheet_row.get(status_key, "")
        file_raw = sheet_row.get(file_key, "")
        tokens = _status_tokens(status_raw)
        if not tokens:
            if file_raw.strip():
                continue
            continue
        for token in tokens:
            if _token_is_signed_via_aces(token):
                signed.append(label)
                break
    return (len(signed) > 0, signed)


@dataclass
class ClientMatch:
    client_id: int
    business_name: str
    external_business_id: str | None
    stage: str
    matched_by: str  # "id" | "name" | "none"
    sheet_business_name: str
    sheet_record_id: str
    has_signed: bool
    signed_utilities: list[str] = field(default_factory=list)
    name_collision_count: int = 0


@dataclass
class SheetOrphan:
    sheet_business_name: str
    sheet_record_id: str
    has_signed: bool
    signed_utilities: list[str] = field(default_factory=list)


def join_clients_to_sheet(
    clients: list[dict[str, Any]],
    sheet_rows: list[dict[str, str]],
) -> tuple[list[ClientMatch], list[SheetOrphan], dict[str, int], list[dict[str, Any]]]:
    by_record_id: dict[str, dict[str, str]] = {}
    by_name: dict[str, list[dict[str, str]]] = {}

    for row in sheet_rows:
        rid = row.get("record_id", "").strip()
        if rid:
            by_record_id[rid] = row
        name = normalize_business_name(row.get("business_name", ""))
        if name:
            by_name.setdefault(name, []).append(row)

    duplicate_name_collisions = [
        {
            "normalized_name": norm,
            "sheet_row_count": len(rows),
            "sheet_business_names": [r.get("business_name", "") for r in rows],
        }
        for norm, rows in by_name.items()
        if len(rows) > 1
    ]

    matched_sheet_ids: set[int] = set()
    matches: list[ClientMatch] = []
    matched_by_id = 0
    matched_by_name = 0

    for client in clients:
        cid = int(client["id"])
        cname = client.get("business_name") or ""
        ext_id = (client.get("external_business_id") or "").strip() or None
        stage = (client.get("stage") or "lead").strip()

        sheet_row: dict[str, str] | None = None
        matched_by = "none"
        name_collision_count = 0

        if ext_id:
            if ext_id in by_record_id:
                sheet_row = by_record_id[ext_id]
                matched_by = "id"
                matched_by_id += 1
        else:
            norm = normalize_business_name(cname)
            candidates = by_name.get(norm) if norm else None
            if candidates:
                sheet_row = candidates[0]
                matched_by = "name"
                matched_by_name += 1
                name_collision_count = len(candidates)

        if sheet_row is None:
            matches.append(
                ClientMatch(
                    client_id=cid,
                    business_name=cname,
                    external_business_id=ext_id,
                    stage=stage,
                    matched_by="none",
                    sheet_business_name="",
                    sheet_record_id="",
                    has_signed=False,
                    signed_utilities=[],
                )
            )
            continue

        matched_sheet_ids.add(id(sheet_row))
        has_signed, signed_utils = compute_signed_utilities(sheet_row)
        matches.append(
            ClientMatch(
                client_id=cid,
                business_name=cname,
                external_business_id=ext_id,
                stage=stage,
                matched_by=matched_by,
                sheet_business_name=sheet_row.get("business_name", ""),
                sheet_record_id=sheet_row.get("record_id", ""),
                has_signed=has_signed,
                signed_utilities=signed_utils,
                name_collision_count=name_collision_count,
            )
        )

    orphans: list[SheetOrphan] = []
    for row in sheet_rows:
        if id(row) in matched_sheet_ids:
            continue
        has_signed, signed_utils = compute_signed_utilities(row)
        orphans.append(
            SheetOrphan(
                sheet_business_name=row.get("business_name", ""),
                sheet_record_id=row.get("record_id", ""),
                has_signed=has_signed,
                signed_utilities=signed_utils,
            )
        )

    join_counts = {
        "matched_by_id": matched_by_id,
        "matched_by_name": matched_by_name,
    }
    return matches, orphans, join_counts, duplicate_name_collisions


def compute_client_signed_status(sheet_row: dict[str, str] | None) -> tuple[bool, list[str]]:
    """Return (has_signed_via_aces, utility_labels) for a matched sheet row."""
    if not sheet_row:
        return False, []
    return compute_signed_utilities(sheet_row)


def match_clients_to_sheet(
    clients: list[dict[str, Any]],
    sheet_rows: list[dict[str, str]],
) -> list[ClientMatch]:
    """Join clients to sheet rows and compute signed-via-ACES status per client."""
    matches, _, _, _ = join_clients_to_sheet(clients, sheet_rows)
    return matches


def recompute_signed_contracts(
    db: Session, *, promote_signed: bool = False
) -> dict[str, Any]:
    """
    Bulk-read FILE_IDS sheet once; update has_signed_contract on all clients.
    By default never changes client stage. When promote_signed=True, members who are
    signed via ACES and still lead/qualified are promoted to existing_client.
    """
    sheet_rows, sheet_meta = read_data_from_airtable_tab()
    client_dicts = load_clients(db)
    matches = match_clients_to_sheet(client_dicts, sheet_rows)
    match_by_client_id = {m.client_id: m for m in matches}

    clients = db.query(Client).order_by(Client.id.asc()).all()
    now = datetime.utcnow()

    prev_flags = {c.id: bool(c.has_signed_contract) for c in clients}
    total_flagged = 0
    signed_lead_or_qualified = 0
    newly_flagged = 0
    newly_signed_lead_or_qualified = 0
    flagged_matched_by_id = 0
    flagged_matched_by_name = 0
    promoted_to_existing = 0

    for client in clients:
        m = match_by_client_id.get(client.id)
        if m is not None and m.matched_by != "none":
            has_signed = m.has_signed
            utilities = list(m.signed_utilities)
        else:
            has_signed = False
            utilities = []

        client.has_signed_contract = 1 if has_signed else 0
        client.signed_contract_utilities = (
            json.dumps(utilities) if utilities else None
        )
        client.signed_contract_checked_at = now

        if has_signed:
            total_flagged += 1
            if m and m.matched_by == "id":
                flagged_matched_by_id += 1
            elif m and m.matched_by == "name":
                flagged_matched_by_name += 1
            stage = (client.stage or "lead").strip()
            if stage in ("lead", "qualified"):
                if promote_signed:
                    client.stage = "existing_client"
                    client.stage_changed_at = now
                    promoted_to_existing += 1
                else:
                    signed_lead_or_qualified += 1
            if not prev_flags.get(client.id):
                newly_flagged += 1
                if stage in ("lead", "qualified"):
                    newly_signed_lead_or_qualified += 1

    db.commit()

    return {
        "meta": {
            **sheet_meta,
            "synced_at": now.isoformat() + "Z",
            "clients_updated": len(clients),
        },
        "total_flagged": total_flagged,
        "signed_but_lead_or_qualified": signed_lead_or_qualified,
        "newly_flagged": newly_flagged,
        "newly_signed_but_lead_or_qualified": newly_signed_lead_or_qualified,
        "promoted_to_existing": promoted_to_existing,
        "flagged_matched_by_id": flagged_matched_by_id,
        "flagged_matched_by_name": flagged_matched_by_name,
        "join_stats": {
            "matched_by_id": sum(1 for m in matches if m.matched_by == "id"),
            "matched_by_name": sum(1 for m in matches if m.matched_by == "name"),
            "clients_unmatched": sum(1 for m in matches if m.matched_by == "none"),
        },
    }


def run_signed_contract_dry_run(db: Session) -> dict[str, Any]:
    """
    Run the full read-only diagnostic against the configured DB and FILE_IDS sheet.
    Returns a JSON-serializable report dict.
    """
    clients = load_clients(db)
    sheet_rows, sheet_meta = read_data_from_airtable_tab()
    matches, orphans, join_counts, duplicate_name_collisions = join_clients_to_sheet(
        clients, sheet_rows
    )

    matched_clients = [m for m in matches if m.matched_by != "none"]
    unmatched_clients = [m for m in matches if m.matched_by == "none"]
    signed_lead_qualified = [
        m for m in matches if m.has_signed and m.stage in ("lead", "qualified")
    ]
    signed_existing = [m for m in matches if m.has_signed and m.stage == "existing_client"]

    total_clients = len(clients)
    match_rate = (len(matched_clients) / total_clients * 100) if total_clients else 0.0

    name_matched_with_collision = [
        m for m in matches if m.matched_by == "name" and m.name_collision_count > 1
    ]

    return {
        "meta": {
            **sheet_meta,
            "total_clients": total_clients,
            "total_sheet_rows": len(sheet_rows),
        },
        "counts": {
            "signed_but_lead_or_qualified": len(signed_lead_qualified),
            "signed_existing_client": len(signed_existing),
            "sheet_rows_no_client": len(orphans),
            "clients_no_sheet_row": len(unmatched_clients),
        },
        "join_stats": {
            "matched_by_id": join_counts["matched_by_id"],
            "matched_by_name": join_counts["matched_by_name"],
            "clients_matched": len(matched_clients),
            "clients_total": total_clients,
            "match_rate_percent": round(match_rate, 2),
        },
        "duplicate_name_collisions": duplicate_name_collisions,
        "clients_matched_by_name_with_collision": [
            {
                "client_id": m.client_id,
                "business_name": m.business_name,
                "stage": m.stage,
                "matched_by": m.matched_by,
                "name_collision_count": m.name_collision_count,
                "sheet_business_name": m.sheet_business_name,
            }
            for m in name_matched_with_collision
        ],
        "signed_but_lead_or_qualified": [
            {
                "client_id": m.client_id,
                "business_name": m.business_name,
                "stage": m.stage,
                "matched_by": m.matched_by,
                "signed_utilities": m.signed_utilities,
                "external_business_id": m.external_business_id,
                "sheet_business_name": m.sheet_business_name,
                "sheet_record_id": m.sheet_record_id,
                "name_collision_count": m.name_collision_count,
            }
            for m in signed_lead_qualified
        ],
        "signed_existing_client": [
            {
                "client_id": m.client_id,
                "business_name": m.business_name,
                "stage": m.stage,
                "matched_by": m.matched_by,
                "signed_utilities": m.signed_utilities,
            }
            for m in signed_existing
        ],
        "clients_no_sheet_row": [
            {
                "client_id": m.client_id,
                "business_name": m.business_name,
                "stage": m.stage,
                "external_business_id": m.external_business_id,
            }
            for m in unmatched_clients
        ],
        "sheet_rows_no_client_sample": [
            asdict(o)
            for o in orphans[:50]
        ],
        "sheet_rows_no_client_total": len(orphans),
    }


def get_contracts_for_business(business_name: str) -> dict:
    """Signed-contract file IDs + status per utility for one business, matched by name
    against the FILE_IDS 'Data from Airtable' tab. Read-only; reuses read_data_from_airtable_tab.
    Returns {business_name, matched, record_id, contracts:{<utility>:{file_id,status,link}}, sheet_id}."""
    rows, meta = read_data_from_airtable_tab()
    target = normalize_business_name(business_name)
    matched = None
    for r in rows:
        if normalize_business_name(r.get("business_name", "")) == target:
            matched = r
            break
    contracts: dict[str, dict] = {}
    if matched:
        for status_key, file_key, label in STATUS_FILE_PAIRS:
            fid = (matched.get(file_key) or "").strip()
            status = (matched.get(status_key) or "").strip()
            if fid or status:
                contracts[label] = {
                    "file_id": fid or None,
                    "status": status or None,
                    "link": f"https://drive.google.com/file/d/{fid}/view" if fid else None,
                }
    return {
        "business_name": business_name,
        "matched": matched is not None,
        "record_id": (matched.get("record_id") if matched else None),
        "contracts": contracts,
        "sheet_id": meta.get("sheet_id"),
    }
