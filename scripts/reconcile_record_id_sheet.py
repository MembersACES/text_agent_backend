#!/usr/bin/env python3
"""
Backfill column AG (record_id) on the "Data from Airtable" sheet from Airtable LOA records.

Preview by default (no writes). Pass --apply to write column AG only.

Requires:
  - AIRTABLE_API_KEY (and optionally AIRTABLE_BASE_ID)
  - SERVICE_ACCOUNT_FILE or SERVICE_ACCOUNT_JSON with Sheets write access

Does NOT use the database or Cloud SQL.

Run from backend root:
  python scripts/reconcile_record_id_sheet.py
  python scripts/reconcile_record_id_sheet.py --inspect-headers
  python scripts/reconcile_record_id_sheet.py --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

_BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

load_dotenv(_BACKEND_ROOT / ".env")

from services.record_id_reconciliation import (  # noqa: E402
    inspect_live_headers,
    reconcile_record_ids,
)


def _print_headers() -> None:
    info = inspect_live_headers()
    print(f"Sheet: {info['sheet_id']} / {info['sheet_tab']}")
    print(f"Header cells: {info['header_cell_count']}")
    print(f"record_id column: {info['record_id_column']} ({info['record_id_header']!r})")
    print(f"ABN column present: {info['has_abn_column']}")
    print(f"Google Drive Folder ID column present: {info['has_drive_folder_id']}")
    print()
    for col in info["columns"]:
        if col["header"]:
            print(f"  {col['column']:>3} ({col['index']:>2}): {col['header']!r}")
    print()


def _print_preview(report) -> None:
    meta = report.sheet_meta
    print("=" * 60)
    print("RECORD ID RECONCILIATION - PREVIEW")
    print("=" * 60)
    print(f"Sheet tab: {meta.get('sheet_tab')}")
    print(f"Airtable LOA records: {report.airtable_count}")
    print(f"Sheet data rows:      {report.sheet_row_count}")
    print()
    print("Match breakdown:")
    print(f"  Already correct (AG matches):     {report.already_correct}")
    print(f"  Matched via existing AG:          {report.matched_by_existing_ag}")
    print(f"  Would write - matched by name:    {report.matched_by_name}")
    print(f"  Would write - folder tie-break:   {report.matched_by_folder_tiebreak}")
    print(f"  Total planned writes (empty AG):  {report.would_write}")
    print()
    print(f"Unresolved Airtable records:        {len(report.unresolved_airtable)}")
    print(f"Name collisions (manual):           {len(report.name_collisions)}")
    print(f"AG conflicts (not overwriting):     {len(report.ag_conflicts)}")
    print(f"Sheet rows with no Airtable match:  {len(report.sheet_orphans)}")
    print()

    if report.planned_writes:
        print("-" * 60)
        print(f"PLANNED WRITES ({len(report.planned_writes)} rows, column AG only)")
        print("-" * 60)
        for w in report.planned_writes[:50]:
            print(
                f"  row {w.sheet_row_number:>4}  AG: {w.current_record_id or '(empty)'} -> {w.new_record_id}"
                f"  [{w.match_method}]  {w.business_name!r}"
            )
        if len(report.planned_writes) > 50:
            print(f"  ... and {len(report.planned_writes) - 50} more")
        print()

    if report.name_collisions:
        print("-" * 60)
        print("NAME COLLISIONS - manual assignment required")
        print("-" * 60)
        for item in report.name_collisions[:20]:
            print(
                f"  Airtable {item['airtable_record_id']}  {item['business_name']!r}"
                f"  folder={item['drive_folder_id']!r}  candidates={len(item['candidate_sheet_rows'])}"
            )
            for c in item["candidate_sheet_rows"]:
                print(
                    f"    sheet row {c['sheet_row_number']}: {c['business_name']!r}"
                    f"  folder={c['drive_folder_id']!r}  ag={c['record_id'] or '(empty)'}"
                )
            print(f"    reason: {item['reason']}")
        if len(report.name_collisions) > 20:
            print(f"  ... and {len(report.name_collisions) - 20} more")
        print()

    if report.unresolved_airtable:
        print("-" * 60)
        print("UNRESOLVED AIRTABLE (no sheet row by name)")
        print("-" * 60)
        for item in report.unresolved_airtable[:20]:
            print(
                f"  {item['record_id']}  {item['business_name']!r}"
                f"  trading_as={item['trading_as']!r}"
            )
        if len(report.unresolved_airtable) > 20:
            print(f"  ... and {len(report.unresolved_airtable) - 20} more")
        print()

    if report.ag_conflicts:
        print("-" * 60)
        print("AG CONFLICTS (skipped)")
        print("-" * 60)
        for item in report.ag_conflicts[:15]:
            print(
                f"  sheet row {item['sheet_row_number']}: {item['business_name']!r}"
                f"  current={item['current_record_id']!r}  reason={item['reason']}"
            )
        if len(report.ag_conflicts) > 15:
            print(f"  ... and {len(report.ag_conflicts) - 15} more")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill record_id (column AG) from Airtable LOA records."
    )
    parser.add_argument(
        "--inspect-headers",
        action="store_true",
        help="Print live sheet header row and exit (Step 0).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write column AG only (default is preview, no writes).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit full report as JSON after the human-readable summary.",
    )
    args = parser.parse_args()

    if args.inspect_headers:
        _print_headers()
        return 0

    try:
        report = reconcile_record_ids(apply=args.apply)
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    _print_headers()
    _print_preview(report)

    if args.apply:
        if report.apply_result:
            print("=" * 60)
            print("APPLY COMPLETE")
            print("=" * 60)
            print(json.dumps(report.apply_result, indent=2))
        else:
            print("No writes performed (nothing to update).")
    else:
        print("Preview only - no sheet writes. Re-run with --apply to write column AG.")

    if args.json:
        from services.record_id_reconciliation import report_to_dict

        print()
        print(json.dumps(report_to_dict(report), indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
