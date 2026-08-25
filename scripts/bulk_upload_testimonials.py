#!/usr/bin/env python3
"""Mass-upload testimonial files from a Drive zip to deployed POST /api/testimonials/upload.

Default is a dry-run. After types are on the API, run with --execute.

    python scripts/bulk_upload_testimonials.py --zip PATH
    python scripts/bulk_upload_testimonials.py --zip PATH --execute

Uses BACKEND_API_KEY from .env (quote the value if it contains #).
CRM members are loaded from GET /api/clients, or from DATABASE_URL if the API
still rejects the key. Members are matched so Drive folders and dashboard names line up.
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


def _clean_secret(raw: str | None) -> str:
    return (raw or "").strip().strip('"').strip("'")


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _rows_to_clients(rows: list) -> list[CrmClient]:
    clients: list[CrmClient] = []
    for row in rows:
        name = str(row.get("business_name") or "").strip()
        if not name:
            continue
        folder = row.get("gdrive_folder_url")
        clients.append(
            CrmClient(
                business_name=name,
                gdrive_folder_url=(str(folder).strip() if folder else None),
            )
        )
    return clients


def _load_clients_from_api(api_base: str, token: str) -> list[CrmClient]:
    url = f"{api_base.rstrip('/')}/api/clients"
    response = requests.get(url, headers=_auth_headers(token), timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"GET /api/clients failed ({response.status_code})")
    data = response.json()
    raw = data if isinstance(data, list) else data.get("items") or data.get("clients") or []
    return _rows_to_clients(raw)


def _load_clients_from_db() -> list[CrmClient]:
    from database import SessionLocal
    from models import Client

    db = SessionLocal()
    try:
        rows = db.query(Client.business_name, Client.gdrive_folder_url).all()
        return _rows_to_clients(
            [{"business_name": name, "gdrive_folder_url": folder} for name, folder in rows]
        )
    finally:
        db.close()


def _load_clients(api_base: str, token: str) -> tuple[list[CrmClient], str]:
    try:
        clients = _load_clients_from_api(api_base, token)
        if clients:
            return clients, "API"
    except Exception as exc:
        print(f"API client list unavailable ({exc}); trying DATABASE_URL from .env")
    clients = _load_clients_from_db()
    if not clients:
        raise RuntimeError(
            "Could not load CRM members from the API or DATABASE_URL. "
            "Check BACKEND_API_KEY and that .env DATABASE_URL is the shared CRM database."
        )
    return clients, "DATABASE_URL"


def _confirm_types_deployed(api_base: str, token: str, needed: set[str]) -> list[str]:
    url = f"{api_base.rstrip('/')}/api/testimonials/solution-content"
    response = requests.get(url, headers=_auth_headers(token), timeout=60)
    if response.status_code == 401:
        raise RuntimeError(
            "API key was rejected (401). Quote BACKEND_API_KEY in .env if it contains #, "
            "and use the same key as the deployed backend."
        )
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
        default=_clean_secret(os.environ.get("BACKEND_API_KEY")),
        help="BACKEND_API_KEY for upload (loaded from .env)",
    )
    parser.add_argument(
        "--google-token",
        default=_clean_secret(os.environ.get("GOOGLE_ID_TOKEN")),
        help="Unused if BACKEND_API_KEY works; optional extra token",
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

        upload_token = _clean_secret(args.api_key or args.google_token)
        if not upload_token:
            print("BACKEND_API_KEY is missing from .env")
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
            clients, source = _load_clients(args.base_url, upload_token)
        except RuntimeError as exc:
            print(str(exc))
            return 1
        with_folder = sum(1 for c in clients if c.gdrive_folder_url)
        print(f"\nLoaded {len(clients)} CRM members from {source} ({with_folder} have a Drive folder)")
        fallback_folder = _clean_secret(os.environ.get("TESTIMONIAL_STORAGE_FOLDER_ID"))

        if args.limit:
            kept = kept[: args.limit]

        failures = 0
        unmatched = 0
        uploaded = 0
        for entry in kept:
            match = match_crm_member(entry.member_hint, clients)
            if match is None or match.ambiguous:
                reason = "ambiguous CRM match" if match and match.ambiguous else "no CRM match"
                print(f"SKIP {reason} for {entry.member_hint!r} ({entry.zip_path})")
                unmatched += 1
                continue
            business_name = match.business_name
            folder = match.gdrive_folder_url or fallback_folder or None
            if not folder:
                print(f"SKIP {business_name} has no Drive folder ({entry.zip_path})")
                unmatched += 1
                continue

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
            print(f"[{mark} {status}] {business_name} / {entry.solution_type_id} <- {entry.filename}  {detail}")
            if args.sleep:
                time.sleep(args.sleep)

        print(f"\nUploaded {uploaded}, failed {failures}, unmatched/skipped {unmatched}")
        return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
