#!/usr/bin/env python3
"""Mass-upload testimonial files from a Drive zip to deployed POST /api/testimonials/upload.

Default is a dry-run. After the new solution types are deployed, run with --execute.

    python scripts/bulk_upload_testimonials.py --zip "C:\\Users\\morga\\Downloads\\drive-download-20260824T231613Z-1-001.zip"
    python scripts/bulk_upload_testimonials.py --zip PATH --execute

Auth: Bearer BACKEND_API_KEY (upload) and a Google ID token for GET /api/clients so
business_name matches the CRM member record. Set GOOGLE_ID_TOKEN or pass --google-token.
BACKEND_API_KEY is loaded from .env. API host defaults to the Cloud Run dev backend.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import zipfile
from collections import Counter
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env", override=True)

from tools.testimonial_bulk_import import (  # noqa: E402
    ClassifiedEntry,
    CrmClient,
    SkipReason,
    match_crm_member,
    plan_zip_entries,
    solution_type_label,
)

DEFAULT_API = "https://text-agent-backend-dev-672026052958.australia-southeast2.run.app"
DEFAULT_ZIP = Path.home() / "Downloads" / "drive-download-20260824T231613Z-1-001.zip"


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _load_clients(api_base: str, token: str) -> list[CrmClient]:
    url = f"{api_base.rstrip('/')}/api/clients"
    response = requests.get(url, headers=_auth_headers(token), timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(
            f"GET /api/clients failed ({response.status_code}). "
            "Pass --google-token (a dashboard Google ID token) so names match CRM members."
        )
    data = response.json()
    rows = data if isinstance(data, list) else data.get("items") or data.get("clients") or []
    clients: list[CrmClient] = []
    for row in rows:
        name = str(row.get("business_name") or "").strip()
        if not name:
            continue
        clients.append(
            CrmClient(
                business_name=name,
                gdrive_folder_url=(row.get("gdrive_folder_url") or None),
            )
        )
    return clients


def _confirm_types_deployed(api_base: str, token: str, needed: set[str]) -> list[str]:
    url = f"{api_base.rstrip('/')}/api/testimonials/solution-content"
    response = requests.get(url, headers=_auth_headers(token), timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(
            f"GET /api/testimonials/solution-content failed ({response.status_code}). "
            "Deploy the backend with the new types before --execute."
        )
    present = {str(item.get("solution_type") or "") for item in response.json()}
    return sorted(needed - present)


def _upload_one(
    api_base: str,
    token: str,
    *,
    filename: str,
    file_bytes: bytes,
    business_name: str,
    solution_type_id: str,
    gdrive_folder_url: str | None,
    status: str,
) -> tuple[int, str]:
    url = f"{api_base.rstrip('/')}/api/testimonials/upload"
    form = {
        "business_name": business_name,
        "status": status,
        "testimonial_solution_type_id": solution_type_id,
        "testimonial_type": solution_type_label(solution_type_id),
    }
    if gdrive_folder_url:
        form["gdrive_folder_url"] = gdrive_folder_url
    files = {"file": (filename, file_bytes)}
    response = requests.post(url, headers=_auth_headers(token), data=form, files=files, timeout=180)
    detail = ""
    try:
        payload = response.json()
        detail = str(payload.get("detail") or payload.get("id") or payload)
    except Exception:
        detail = (response.text or "")[:300]
    return response.status_code, detail


def _print_plan(entries: list[ClassifiedEntry]) -> None:
    kept = [e for e in entries if e.preferred]
    skipped = [e for e in entries if not e.preferred]
    print(f"Zip entries classified: {len(entries)}")
    print(f"  upload candidates: {len(kept)}")
    print(f"  skipped:           {len(skipped)}")
    print("\nBy type (candidates):")
    counts = Counter(e.solution_type_id for e in kept)
    for type_id, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {n:3d}  {type_id:28s}  {solution_type_label(type_id)}")
    reasons = Counter(e.skip_reason.value for e in skipped if e.skip_reason)
    print("\nSkip reasons:")
    for reason, n in reasons.most_common():
        print(f"  {n:3d}  {reason}")
    print("\nCandidates:")
    for entry in kept:
        print(f"  [{entry.solution_type_id}] {entry.member_hint}  <-  {entry.zip_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zip", default=str(DEFAULT_ZIP), help="Path to the colleague testimonial zip")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("TESTIMONIAL_BULK_API_URL")
        or os.environ.get("SMOKE_BASE_URL")
        or DEFAULT_API,
        help="Deployed backend origin",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("BACKEND_API_KEY", ""),
        help="BACKEND_API_KEY for POST /api/testimonials/upload",
    )
    parser.add_argument(
        "--google-token",
        default=os.environ.get("GOOGLE_ID_TOKEN", ""),
        help="Google ID token for GET /api/clients (CRM name matching)",
    )
    parser.add_argument("--status", default="Approved", choices=("Draft", "Sent for approval", "Approved"))
    parser.add_argument("--limit", type=int, default=0, help="Max files to upload when executing (0 = all)")
    parser.add_argument("--only-type", default="", help="Only this solution_type_id")
    parser.add_argument("--sleep", type=float, default=0.4, help="Seconds between uploads")
    parser.add_argument("--execute", action="store_true", help="Upload to the deployed API (otherwise dry-run)")
    parser.add_argument("--json", action="store_true", help="Also print machine-readable plan JSON")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    zip_path = Path(args.zip)
    if not zip_path.is_file():
        print(f"Zip not found: {zip_path}")
        return 1

    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        entries = plan_zip_entries(names)
        _print_plan(entries)
        kept = [e for e in entries if e.preferred]
        if args.only_type:
            kept = [e for e in kept if e.solution_type_id == args.only_type]
            print(f"\nFiltered to --only-type {args.only_type}: {len(kept)}")

        if args.json:
            print(
                json.dumps(
                    [
                        {
                            "zip_path": e.zip_path,
                            "member_hint": e.member_hint,
                            "solution_type_id": e.solution_type_id,
                            "skip_reason": e.skip_reason.value if e.skip_reason else None,
                            "preferred": e.preferred,
                        }
                        for e in entries
                    ],
                    indent=2,
                )
            )

        if not args.execute:
            print(
                "\nDry-run only. After backend + interface are deployed with the new types:\n"
                f"  python scripts/bulk_upload_testimonials.py --zip \"{zip_path}\" --execute"
            )
            return 0

        upload_token = (args.api_key or args.google_token or "").strip()
        clients_token = (args.google_token or args.api_key or "").strip()
        if not upload_token:
            print("Need BACKEND_API_KEY or --google-token to upload.")
            return 1

        missing_types = _confirm_types_deployed(
            args.base_url,
            upload_token,
            {e.solution_type_id for e in kept if e.solution_type_id},
        )
        if missing_types:
            print("Deployed API is missing solution types (deploy backend first):")
            for type_id in missing_types:
                print(f"  - {type_id}")
            return 1

        try:
            clients = _load_clients(args.base_url, clients_token)
            print(f"\nLoaded {len(clients)} CRM members")
        except RuntimeError as exc:
            print(f"\n{exc}")
            print("Uploads will use filename hints; they may not appear on the member page.")
            clients = []

        if args.limit:
            kept = kept[: args.limit]

        failures = 0
        unmatched = 0
        uploaded = 0
        for entry in kept:
            match = match_crm_member(entry.member_hint, clients) if clients else None
            if match and match.ambiguous:
                print(f"SKIP ambiguous CRM match for {entry.member_hint!r} ({entry.zip_path})")
                unmatched += 1
                continue
            if match:
                business_name = match.business_name
                folder = match.gdrive_folder_url
            elif clients:
                print(f"SKIP no CRM match for {entry.member_hint!r} ({entry.zip_path})")
                unmatched += 1
                continue
            else:
                business_name = entry.member_hint
                folder = None

            file_bytes = archive.read(entry.zip_path)
            status, detail = _upload_one(
                args.base_url,
                upload_token,
                filename=entry.filename,
                file_bytes=file_bytes,
                business_name=business_name,
                solution_type_id=entry.solution_type_id or "",
                gdrive_folder_url=folder,
                status=args.status,
            )
            ok = 200 <= status < 300
            uploaded += int(ok)
            failures += int(not ok)
            mark = "OK" if ok else "FAIL"
            print(f"[{mark} {status}] {business_name} / {entry.solution_type_id} ← {entry.filename}  {detail}")
            if args.sleep:
                time.sleep(args.sleep)

        print(f"\nUploaded {uploaded}, failed {failures}, unmatched/ambiguous {unmatched}")
        return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
