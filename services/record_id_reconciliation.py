"""
Reconcile Airtable LOA record IDs into column AG (record_id) on the
"Data from Airtable" Google Sheet tab.

Preview by default; writes only column AG when apply=True.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from googleapiclient.errors import HttpError

from services import airtable_client
from services.signed_contract_dry_run import (
    _build_header_map,
    _find_header_row_index,
    _normalize_header,
    _resolve_header_key,
    normalize_business_name,
)
from tools.business_info import (
    FILE_IDS_SHEET_ID,
    FILE_IDS_SHEET_NAME,
    get_sheets_service,
)

RECORD_ID_COLUMN = "AG"
RECORD_ID_COL_INDEX = 32  # 0-based; AG = 33rd column

SHEET_HEADER_TO_KEY: dict[str, str] = {
    "business name": "business_name",
    "google drive folder id": "drive_folder_id",
    "google drive folder link": "drive_folder_link",
    "record_id": "record_id",
    "record id": "record_id",
    "airtable record id": "record_id",
}


def _cell_str(raw: Any) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def _normalize_drive_folder_id(raw: Any) -> str:
    return airtable_client._normalize_drive_folder_id(raw)


def _sheet_drive_folder_id(row: dict[str, str]) -> str:
    direct = _normalize_drive_folder_id(row.get("drive_folder_id"))
    if direct:
        return direct
    return _normalize_drive_folder_id(row.get("drive_folder_link"))


def _col_letter(n: int) -> str:
    s = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def inspect_live_headers() -> dict[str, Any]:
    """Return the live header row with column letters (Step 0)."""
    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service")
    tab = FILE_IDS_SHEET_NAME or "Data from Airtable"
    resp = service.spreadsheets().values().get(
        spreadsheetId=FILE_IDS_SHEET_ID,
        range=f"'{tab}'!A1:AZ1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    row = resp.get("values", [[]])[0] if resp.get("values") else []
    columns = []
    for i, h in enumerate(row):
        columns.append({
            "column": _col_letter(i),
            "index": i + 1,
            "header": _cell_str(h),
        })
    return {
        "sheet_id": FILE_IDS_SHEET_ID,
        "sheet_tab": tab,
        "header_cell_count": len(row),
        "columns": columns,
        "has_abn_column": any(
            "abn" in c["header"].lower() for c in columns if c["header"]
        ),
        "has_drive_folder_id": any(
            c["header"].lower() == "google drive folder id" for c in columns
        ),
        "record_id_column": RECORD_ID_COLUMN,
        "record_id_header": next(
            (c["header"] for c in columns if c["column"] == RECORD_ID_COLUMN),
            "",
        ),
    }


@dataclass
class SheetRow:
    sheet_row_number: int  # 1-based row in the spreadsheet
    business_name: str
    record_id: str
    drive_folder_id: str
    match_key: str = ""


@dataclass
class PlannedWrite:
    sheet_row_number: int
    business_name: str
    current_record_id: str
    new_record_id: str
    match_method: str
    airtable_trading_as: str = ""
    airtable_abn: str = ""


@dataclass
class ReconciliationReport:
    sheet_meta: dict[str, Any] = field(default_factory=dict)
    airtable_count: int = 0
    sheet_row_count: int = 0
    already_correct: int = 0
    would_write: int = 0
    matched_by_existing_ag: int = 0
    matched_by_name: int = 0
    matched_by_folder_tiebreak: int = 0
    unresolved_airtable: list[dict[str, str]] = field(default_factory=list)
    name_collisions: list[dict[str, Any]] = field(default_factory=list)
    sheet_orphans: list[dict[str, str]] = field(default_factory=list)
    ag_conflicts: list[dict[str, str]] = field(default_factory=list)
    planned_writes: list[PlannedWrite] = field(default_factory=list)
    apply_result: Optional[dict[str, Any]] = None


def read_sheet_rows_for_reconciliation() -> tuple[list[SheetRow], dict[str, Any]]:
    """Bulk-read the Data from Airtable tab; return rows with 1-based sheet row numbers."""
    if not FILE_IDS_SHEET_ID:
        raise RuntimeError("FILE_IDS_SHEET_ID is not set")
    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service")

    tab = FILE_IDS_SHEET_NAME or "Data from Airtable"
    range_str = f"'{tab}'!A1:AZ5000"
    resp = service.spreadsheets().values().get(
        spreadsheetId=FILE_IDS_SHEET_ID,
        range=range_str,
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    values = resp.get("values", [])
    if not values:
        return [], {"sheet_tab": tab, "header_row_number": 1, "warnings": ["empty tab"]}

    header_map = _build_header_map(SHEET_HEADER_TO_KEY)
    header_idx = _find_header_row_index(values, header_map)
    raw_headers = values[header_idx]
    header_row_number = header_idx + 1

    rows: list[SheetRow] = []
    for offset, raw in enumerate(values[header_idx + 1 :]):
        obj: dict[str, str] = {}
        for i, cell in enumerate(raw):
            if i >= len(raw_headers):
                break
            h = _normalize_header(raw_headers[i])
            key = _resolve_header_key(h, header_map)
            if key:
                obj[key] = _cell_str(cell)
        business_name = obj.get("business_name", "")
        record_id = obj.get("record_id", "")
        drive_folder_id = _sheet_drive_folder_id(obj)
        if not business_name and not record_id and not drive_folder_id:
            continue
        rows.append(
            SheetRow(
                sheet_row_number=header_row_number + 1 + offset,
                business_name=business_name,
                record_id=record_id,
                drive_folder_id=drive_folder_id,
            )
        )

    meta = {
        "sheet_id": FILE_IDS_SHEET_ID,
        "sheet_tab": tab,
        "header_row_number": header_row_number,
        "record_id_column": RECORD_ID_COLUMN,
        "sheet_row_count": len(rows),
    }
    return rows, meta


def _pick_name_candidates(
    airtable_rec: dict[str, str],
    by_name: dict[str, list[SheetRow]],
) -> list[SheetRow]:
    names = []
    for field in ("business_name", "trading_as"):
        n = normalize_business_name(airtable_rec.get(field) or "")
        if n and n not in names:
            names.append(n)
    candidates: list[SheetRow] = []
    seen_rows: set[int] = set()
    for n in names:
        for row in by_name.get(n, []):
            if row.sheet_row_number not in seen_rows:
                candidates.append(row)
                seen_rows.add(row.sheet_row_number)
    return candidates


def reconcile_record_ids(*, apply: bool = False) -> ReconciliationReport:
    """
    Match Airtable LOA records to sheet rows and optionally write column AG.
    """
    report = ReconciliationReport()
    report.sheet_meta = inspect_live_headers()

    if not airtable_client.AIRTABLE_API_KEY:
        raise RuntimeError("AIRTABLE_API_KEY is not set")

    sheet_rows, sheet_meta = read_sheet_rows_for_reconciliation()
    report.sheet_row_count = len(sheet_rows)
    report.sheet_meta.update(sheet_meta)

    loa_records = airtable_client.list_all_loa_records()
    report.airtable_count = len(loa_records)
    airtable_ids = {r["record_id"] for r in loa_records if r.get("record_id")}

    by_ag: dict[str, SheetRow] = {}
    by_name: dict[str, list[SheetRow]] = {}
    for row in sheet_rows:
        if row.record_id:
            by_ag.setdefault(row.record_id, row)
        name_key = normalize_business_name(row.business_name)
        if name_key:
            by_name.setdefault(name_key, []).append(row)

    matched_sheet_rows: set[int] = set()
    planned: list[PlannedWrite] = []

    for rec in loa_records:
        rid = rec.get("record_id") or ""
        if not rid:
            continue

        # (1) rec id already in AG
        row_by_ag = by_ag.get(rid)
        if row_by_ag:
            matched_sheet_rows.add(row_by_ag.sheet_row_number)
            if row_by_ag.record_id == rid:
                report.already_correct += 1
                report.matched_by_existing_ag += 1
            continue

        candidates = _pick_name_candidates(rec, by_name)
        # Exclude rows already claimed by another Airtable record via AG
        candidates = [c for c in candidates if c.sheet_row_number not in matched_sheet_rows]

        chosen: Optional[SheetRow] = None
        method = ""

        if len(candidates) == 1:
            chosen = candidates[0]
            method = "business_name"
            report.matched_by_name += 1
        elif len(candidates) > 1:
            folder_id = _normalize_drive_folder_id(rec.get("drive_folder_id"))
            if folder_id:
                folder_matches = [
                    c for c in candidates if c.drive_folder_id == folder_id
                ]
                if len(folder_matches) == 1:
                    chosen = folder_matches[0]
                    method = "drive_folder_tiebreak"
                    report.matched_by_folder_tiebreak += 1
                else:
                    report.name_collisions.append({
                        "airtable_record_id": rid,
                        "business_name": rec.get("business_name") or "",
                        "trading_as": rec.get("trading_as") or "",
                        "abn": rec.get("abn") or "",
                        "drive_folder_id": folder_id,
                        "candidate_sheet_rows": [
                            {
                                "sheet_row_number": c.sheet_row_number,
                                "business_name": c.business_name,
                                "drive_folder_id": c.drive_folder_id,
                                "record_id": c.record_id,
                            }
                            for c in candidates
                        ],
                        "folder_match_count": len(folder_matches),
                        "reason": (
                            "multiple sheet rows share this business name; "
                            "drive folder tie-break did not resolve to exactly one row"
                        ),
                    })
            else:
                report.name_collisions.append({
                    "airtable_record_id": rid,
                    "business_name": rec.get("business_name") or "",
                    "trading_as": rec.get("trading_as") or "",
                    "abn": rec.get("abn") or "",
                    "drive_folder_id": "",
                    "candidate_sheet_rows": [
                        {
                            "sheet_row_number": c.sheet_row_number,
                            "business_name": c.business_name,
                            "drive_folder_id": c.drive_folder_id,
                            "record_id": c.record_id,
                        }
                        for c in candidates
                    ],
                    "folder_match_count": 0,
                    "reason": (
                        "multiple sheet rows share this business name; "
                        "no Airtable drive folder ID available for tie-break"
                    ),
                })
        else:
            report.unresolved_airtable.append({
                "record_id": rid,
                "business_name": rec.get("business_name") or "",
                "trading_as": rec.get("trading_as") or "",
                "abn": rec.get("abn") or "",
                "drive_folder_id": rec.get("drive_folder_id") or "",
                "reason": "no sheet row with matching business or trading name",
            })
            continue

        if not chosen:
            continue

        matched_sheet_rows.add(chosen.sheet_row_number)

        if chosen.record_id == rid:
            report.already_correct += 1
            continue

        if chosen.record_id and chosen.record_id != rid:
            report.ag_conflicts.append({
                "sheet_row_number": str(chosen.sheet_row_number),
                "business_name": chosen.business_name,
                "current_record_id": chosen.record_id,
                "proposed_record_id": rid,
                "match_method": method,
                "reason": "column AG already contains a different record id; not overwriting",
            })
            continue

        planned.append(
            PlannedWrite(
                sheet_row_number=chosen.sheet_row_number,
                business_name=chosen.business_name,
                current_record_id=chosen.record_id,
                new_record_id=rid,
                match_method=method,
                airtable_trading_as=rec.get("trading_as") or "",
                airtable_abn=rec.get("abn") or "",
            )
        )

    # Sheet rows with no Airtable match
    planned_rows = {p.sheet_row_number for p in planned}
    for row in sheet_rows:
        if row.sheet_row_number in matched_sheet_rows or row.sheet_row_number in planned_rows:
            continue
        if row.record_id and row.record_id not in airtable_ids:
            report.ag_conflicts.append({
                "sheet_row_number": str(row.sheet_row_number),
                "business_name": row.business_name,
                "current_record_id": row.record_id,
                "proposed_record_id": "",
                "match_method": "",
                "reason": "AG record id not found in Airtable LOA table",
            })
            continue
        if row.business_name or row.drive_folder_id:
            report.sheet_orphans.append({
                "sheet_row_number": str(row.sheet_row_number),
                "business_name": row.business_name,
                "record_id": row.record_id,
                "drive_folder_id": row.drive_folder_id,
            })

    report.planned_writes = planned
    report.would_write = len(planned)

    if apply and planned:
        report.apply_result = _apply_writes(planned, sheet_meta.get("sheet_tab", ""))

    return report


def _apply_writes(writes: list[PlannedWrite], tab: str) -> dict[str, Any]:
    """batchUpdate column AG only for the planned rows."""
    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service")

    data = []
    for w in writes:
        range_name = f"'{tab}'!{RECORD_ID_COLUMN}{w.sheet_row_number}"
        data.append({"range": range_name, "values": [[w.new_record_id]]})

    try:
        resp = service.spreadsheets().values().batchUpdate(
            spreadsheetId=FILE_IDS_SHEET_ID,
            body={
                "valueInputOption": "RAW",
                "data": data,
            },
        ).execute()
        return {
            "updated_cells": resp.get("totalUpdatedCells", 0),
            "updated_rows": len(writes),
            "ranges": [d["range"] for d in data],
        }
    except HttpError as e:
        raise RuntimeError(f"Google Sheets batchUpdate failed: {e}") from e


def report_to_dict(report: ReconciliationReport) -> dict[str, Any]:
    d = asdict(report)
    return d
