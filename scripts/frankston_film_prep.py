#!/usr/bin/env python3
"""
Frankston RSL film prep — URLs, kanban leads, optional local autonomous run staging.

Usage (from text_agent_backend):
  python scripts/frankston_film_prep.py
  python scripts/frankston_film_prep.py --base-url http://localhost:8080
  python scripts/frankston_film_prep.py --list-kanban-leads
  python scripts/frankston_film_prep.py --stage-local-run 3

Local SQLite only for --stage-local-run. Does not touch Cloud SQL.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv

load_dotenv(BACKEND_ROOT / ".env")

DB_PATH = BACKEND_ROOT / os.getenv("SQLITE_DB_NAME", "aces-task-db.sqlite3")
CLIENT_ID = 1
LEGAL_NAME = "Frankston RSL Sub Branch Inc"
TRADING_NAME = "Frankston RSL"
HERO_MRIN = "5321568754"

COMPARISON_SNAPSHOT_RUN3 = {
    "lane": "ci_gas",
    "annual_savings": 18098.17,
    "current_cost": 97394.06,
    "new_cost": 79295.89,
    "annual_usage_gj": 4454.83,
    "energy_charge_pct": 85.06,
    "contracted_rate": 21.8626,
    "offer_rate": 17.8,
    "commission_aud_per_gj": 0.35,
}

DIRECT_LINKS = {
    "drive_client_folder": "https://drive.google.com/drive/folders/1g8Ns1a-C9P9L_DM12ayYewFSCrowJ5lq",
    "pdf_offer_22_big_gas": "https://drive.google.com/file/d/1ebPrl6d9_85xE2JBq6K0jgNZWPupv76k/view",
    "pdf_offer_18_loop": "https://drive.google.com/file/d/1pAoXz3pj3tRzzFCcV47jVYIer67pYNUA/view",
    "pdf_offer_42_small_gas": "https://drive.google.com/file/d/1QQOzZhrmeEfrwHeDAlyBtCBvTvVqNAk3/view",
    "sheet_offer_44_elec": "https://docs.google.com/spreadsheets/d/1hI4L_oFfFgomujK6DnVnarAZtlIb6HLPy59Eux2mSZI",
    "invoice_origin_5321568754": "https://drive.google.com/file/d/1oRj94Amh6hacWbhmpP_LEcZwwXndTGo6/view",
    "lead_a_review": "https://docs.google.com/spreadsheets/d/1RcCCH1-h-7ap6JuDefJZ3lb_RTkVqhcW/edit",
    "lead_a_drive": "https://drive.google.com/drive/folders/1D4f54eIIbLteBky_lfOsC_R0GAcka-LA",
}


def app_routes(base: str) -> dict[str, str]:
    b = base.rstrip("/")
    q_legal = "Frankston%20RSL%20Sub%20Branch%20Inc"
    return {
        "discrepancy_v1": f"{b}/resources/discrepancy-check?business_name={q_legal}&identifier={HERO_MRIN}&type=gas&mismatches_only=1",
        "crm_member": f"{b}/crm-member/{CLIENT_ID}",
        "base2": f"{b}/base-2?businessName=Frankston+RSL+Sub+Branch+Inc&clientId={CLIENT_ID}",
        "offer_22": f"{b}/offers/22",
        "offer_18": f"{b}/offers/18",
        "autonomous_run_3": f"{b}/autonomous-agent/3",
        "autonomous_run_8": f"{b}/autonomous-agent/8",
        "base1_kanban": f"{b}/base-1",
        "member_agent_one_pager": f"{b}/solution-range/one-pager/frankston-rsl-agent",
        "booking_alex_one_pager": f"{b}/solution-range/one-pager/inbound-booking-alex",
        "strategy_generator": f"{b}/initial-strategy-generator",
    }


def load_client_summary() -> dict | None:
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM clients WHERE id = ?", (CLIENT_ID,)).fetchone()
    if not row:
        conn.close()
        return None
    client = dict(row)
    offers = [
        dict(r)
        for r in conn.execute(
            "SELECT id, status, annual_savings, utility_type_identifier, identifier, pipeline_stage "
            "FROM offers WHERE client_id = ? ORDER BY id DESC LIMIT 12",
            (CLIENT_ID,),
        )
    ]
    runs = [
        dict(r)
        for r in conn.execute(
            """
            SELECT r.id, r.offer_id, r.run_status, r.sequence_type,
                   (SELECT COUNT(*) FROM autonomous_sequence_steps s WHERE s.run_id = r.id
                    AND s.step_status IN ('executed','completed')) AS steps_done,
                   (SELECT COUNT(*) FROM autonomous_sequence_steps s WHERE s.run_id = r.id) AS steps_total
            FROM autonomous_sequence_runs r
            WHERE r.client_id = ?
            ORDER BY r.id DESC
            """,
            (CLIENT_ID,),
        )
    ]
    conn.close()
    return {"client": client, "recent_offers": offers, "autonomous_runs": runs}


def list_kanban_leads(limit: int = 8) -> list[dict]:
    from tools.business_info import get_base1_landing_responses

    crm_names: set[str] = set()
    crm_emails: set[str] = set()
    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        for name, email in conn.execute(
            "SELECT business_name, primary_contact_email FROM clients"
        ):
            if name:
                crm_names.add(str(name).strip().lower())
            if email:
                crm_emails.add(str(email).strip().lower())
        conn.close()

    out: list[dict] = []
    for row in get_base1_landing_responses() or []:
        company = (row.get("Company Name") or "").strip()
        email = (row.get("Contact Email") or "").strip()
        if not company:
            continue
        if company.lower() in crm_names or (email and email.lower() in crm_emails):
            continue
        out.append(
            {
                "company": company,
                "state": row.get("State"),
                "timestamp": row.get("Timestamp"),
                "drive_folder_url": row.get("Google Drive Folder"),
                "base1_review_url": row.get("Base 1 Review"),
                "utility_types": row.get("Utility Types"),
            }
        )
    out.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    return out[:limit]


def stage_local_run(run_id: int, executed_steps: int = 3) -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"SQLite not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT id, context_json FROM autonomous_sequence_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if not row:
        conn.close()
        raise SystemExit(f"Run {run_id} not found in local DB")

    ctx = json.loads(row[1] or "{}")
    ctx["comparison_snapshot"] = COMPARISON_SNAPSHOT_RUN3
    ctx.setdefault("utility_lane", "ci_gas")
    ctx.setdefault("site_identifiers", [HERO_MRIN])
    ctx.setdefault("base2_trigger", "comparison_success")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "UPDATE autonomous_sequence_runs SET context_json = ?, updated_at = ? WHERE id = ?",
        (json.dumps(ctx), now, run_id),
    )

    steps = conn.execute(
        "SELECT id, step_index FROM autonomous_sequence_steps WHERE run_id = ? ORDER BY step_index",
        (run_id,),
    ).fetchall()
    for step_id, step_index in steps:
        if step_index < executed_steps:
            conn.execute(
                """
                UPDATE autonomous_sequence_steps
                SET step_status = 'executed', started_at = ?, completed_at = ?
                WHERE id = ?
                """,
                (now, now, step_id),
            )

    conn.commit()
    conn.close()
    print(f"Staged local run #{run_id}: comparison_snapshot set; first {executed_steps} steps -> executed")


def main() -> None:
    parser = argparse.ArgumentParser(description="Frankston RSL film prep helper")
    parser.add_argument(
        "--base-url",
        default=os.getenv("FILM_APP_BASE_URL", "http://localhost:8080"),
        help="Dashboard origin for route URLs",
    )
    parser.add_argument("--list-kanban-leads", action="store_true")
    parser.add_argument(
        "--stage-local-run",
        type=int,
        metavar="RUN_ID",
        help="Inject comparison_snapshot + executed steps into local SQLite only",
    )
    parser.add_argument(
        "--executed-steps",
        type=int,
        default=3,
        help="With --stage-local-run: number of steps to mark executed (default 3)",
    )
    args = parser.parse_args()

    if args.list_kanban_leads:
        print("=== Kanban candidates (non-CRM) ===")
        print(json.dumps(list_kanban_leads(), indent=2))
        return

    if args.stage_local_run:
        stage_local_run(args.stage_local_run, args.executed_steps)
        return

    print("=== Frankston RSL film prep ===")
    print(f"DB: {DB_PATH} ({'found' if DB_PATH.exists() else 'MISSING'})")
    print(f"Base URL: {args.base_url}\n")

    summary = load_client_summary()
    if summary:
        c = summary["client"]
        print("=== CRM client ===")
        print(f"  id={c.get('id')}  name={c.get('business_name')}")
        print(f"  stage={c.get('stage')}  gdrive={c.get('gdrive_folder_url')}")
        print("\n=== Recent offers (top 12) ===")
        for o in summary["recent_offers"]:
            sav = o.get("annual_savings")
            sav_s = f"${sav:,.2f}" if sav else "—"
            print(
                f"  #{o['id']}  {o.get('status')}  savings={sav_s}  "
                f"{o.get('utility_type_identifier') or '—'}  {o.get('identifier') or ''}"
            )
        print("\n=== Autonomous runs ===")
        for r in summary["autonomous_runs"]:
            print(
                f"  run #{r['id']}  offer #{r['offer_id']}  {r['run_status']}  "
                f"steps {r['steps_done']}/{r['steps_total']}  {r['sequence_type']}"
            )
    else:
        print("No local CRM data for client_id=1")

    print("\n=== App routes ===")
    for name, url in app_routes(args.base_url).items():
        print(f"  {name}: {url}")

    print("\n=== Direct asset links ===")
    for name, url in DIRECT_LINKS.items():
        print(f"  {name}: {url}")

    leads = list_kanban_leads(3)
    if leads:
        print("\n=== Suggested Lead A (latest non-CRM) ===")
        print(f"  company: {leads[0]['company']}")
        print(f"  review:  {leads[0].get('base1_review_url')}")
        print(f"  drive:   {leads[0].get('drive_folder_url')}")

    print("\n=== Optional: stage local autonomous run for sidebar B-roll ===")
    print("  python scripts/frankston_film_prep.py --stage-local-run 3")
    print("\nFull doc: text_agent_interface/docs/frankston-rsl-film-pack.md")


if __name__ == "__main__":
    main()
