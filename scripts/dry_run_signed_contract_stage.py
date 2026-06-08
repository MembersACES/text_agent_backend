#!/usr/bin/env python3
"""
Read-only diagnostic CLI: match CRM clients to FILE_IDS sheet rows and report
"Signed via ACES" contract status vs current client stage.

Uses the same logic as GET /api/admin/signed-contract-dry-run (services/signed_contract_dry_run.py).
Does NOT write to the database or sheet.

Run from backend root:
  python scripts/dry_run_signed_contract_stage.py
"""
from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

_BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

load_dotenv(_BACKEND_ROOT / ".env")

from database import SessionLocal  # noqa: E402
from services.signed_contract_dry_run import run_signed_contract_dry_run  # noqa: E402


def _output_csv_path() -> Path:
    tmp = Path("/tmp")
    if tmp.is_dir():
        return tmp / "signed_contract_stage_dry_run.csv"
    return Path(tempfile.gettempdir()) / "signed_contract_stage_dry_run.csv"


def write_csv(path: Path, report: dict) -> None:
    fieldnames = [
        "bucket",
        "client_id",
        "client_business_name",
        "external_business_id",
        "stage",
        "matched_by",
        "sheet_business_name",
        "sheet_record_id",
        "has_signed",
        "signed_utilities",
        "name_collision_count",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for row in report.get("signed_but_lead_or_qualified", []):
            w.writerow(
                {
                    "bucket": "signed_but_lead_or_qualified",
                    "client_id": row["client_id"],
                    "client_business_name": row["business_name"],
                    "external_business_id": row.get("external_business_id") or "",
                    "stage": row["stage"],
                    "matched_by": row["matched_by"],
                    "sheet_business_name": row.get("sheet_business_name", ""),
                    "sheet_record_id": row.get("sheet_record_id", ""),
                    "has_signed": "yes",
                    "signed_utilities": "; ".join(row.get("signed_utilities", [])),
                    "name_collision_count": row.get("name_collision_count", 0),
                }
            )

        for row in report.get("signed_existing_client", []):
            w.writerow(
                {
                    "bucket": "signed_existing_client",
                    "client_id": row["client_id"],
                    "client_business_name": row["business_name"],
                    "external_business_id": "",
                    "stage": row["stage"],
                    "matched_by": row["matched_by"],
                    "sheet_business_name": "",
                    "sheet_record_id": "",
                    "has_signed": "yes",
                    "signed_utilities": "; ".join(row.get("signed_utilities", [])),
                    "name_collision_count": 0,
                }
            )

        for row in report.get("clients_no_sheet_row", []):
            w.writerow(
                {
                    "bucket": "client_no_sheet_row",
                    "client_id": row["client_id"],
                    "client_business_name": row["business_name"],
                    "external_business_id": row.get("external_business_id") or "",
                    "stage": row["stage"],
                    "matched_by": "none",
                    "sheet_business_name": "",
                    "sheet_record_id": "",
                    "has_signed": "no",
                    "signed_utilities": "",
                    "name_collision_count": 0,
                }
            )


def main() -> int:
    meta = {}
    print("Signed-via-ACES vs stage — read-only dry run")

    db = SessionLocal()
    try:
        report = run_signed_contract_dry_run(db)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    finally:
        db.close()

    meta = report.get("meta", {})
    counts = report.get("counts", {})
    join_stats = report.get("join_stats", {})

    print(f"  Sheet: {meta.get('sheet_id')} / tab {meta.get('sheet_tab')!r}")
    print(f"  Loaded {meta.get('total_clients')} clients from DB")
    print(f"  Loaded {meta.get('total_sheet_rows')} sheet rows (single bulk read)")
    for w in meta.get("warnings", []):
        print(f"  WARNING: {w}", file=sys.stderr)

    print("\n--- Join quality ---")
    print(f"  Matched by Airtable record ID: {join_stats.get('matched_by_id')}")
    print(f"  Matched by business name (fallback): {join_stats.get('matched_by_name')}")
    print(
        f"  Clients matched to a sheet row: {join_stats.get('clients_matched')} / "
        f"{join_stats.get('clients_total')} ({join_stats.get('match_rate_percent')}%)"
    )
    print(f"  Clients with NO sheet row: {counts.get('clients_no_sheet_row')}")
    print(f"  Sheet rows with NO client: {counts.get('sheet_rows_no_client')}")
    print(f"  Duplicate-name sheet collisions: {len(report.get('duplicate_name_collisions', []))}")
    print(
        f"  Clients name-matched with collision: "
        f"{len(report.get('clients_matched_by_name_with_collision', []))}"
    )

    signed_lq = report.get("signed_but_lead_or_qualified", [])
    print(f"\n{'=' * 72}")
    print(f"SIGNED via ACES but stage is lead or qualified ({len(signed_lq)})")
    print(f"{'=' * 72}")
    for row in signed_lq:
        utils = ", ".join(row.get("signed_utilities", [])) or "-"
        print(
            f"  [{row['client_id']}] {row['business_name']!r}  stage={row['stage']!r}  "
            f"match={row['matched_by']}  utilities={utils}"
        )

    csv_path = _output_csv_path()
    write_csv(csv_path, report)
    print(f"\nCSV written to: {csv_path}")
    print("(Read-only — no database or sheet writes performed.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
