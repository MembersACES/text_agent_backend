#!/usr/bin/env python3
"""Remove peninsula-villages-ci-electricity video job from DB + Drive (dev reset)."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(ROOT, ".env"))

SLUG = "peninsula-villages-ci-electricity"
SLUG_TOKEN = "peninsula-villages"


def trash_file(drive, file_id: str, label: str) -> None:
    try:
        drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
        print(f"  deleted {label}: {file_id}")
    except Exception as e:
        print(f"  WARN delete {label} {file_id}: {e}")


def trash_folder_tree(drive, folder_id: str, label: str) -> None:
    """Delete children then folder."""
    q = f"'{folder_id}' in parents and trashed=false"
    token = None
    while True:
        resp = (
            drive.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name, mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=100,
                pageToken=token,
            )
            .execute()
        )
        for f in resp.get("files") or []:
            fid = f["id"]
            if f.get("mimeType") == "application/vnd.google-apps.folder":
                trash_folder_tree(drive, fid, f.get("name", fid))
            else:
                trash_file(drive, fid, f.get("name", fid))
        token = resp.get("nextPageToken")
        if not token:
            break
    trash_file(drive, folder_id, label)


def list_folders_by_name(drive, parent_id: str, name_contains: str) -> list[dict]:
    q = (
        f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' "
        f"and trashed=false and name contains '{name_contains}'"
    )
    out: list[dict] = []
    token = None
    while True:
        resp = (
            drive.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=100,
                pageToken=token,
            )
            .execute()
        )
        out.extend(resp.get("files") or [])
        token = resp.get("nextPageToken")
        if not token:
            break
    return out


def list_files_by_name(drive, parent_id: str, name_contains: str) -> list[dict]:
    q = f"'{parent_id}' in parents and trashed=false and name contains '{name_contains}'"
    out: list[dict] = []
    token = None
    while True:
        resp = (
            drive.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name, mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=100,
                pageToken=token,
            )
            .execute()
        )
        out.extend(resp.get("files") or [])
        token = resp.get("nextPageToken")
        if not token:
            break
    return out


def main() -> int:
    from tools.one_month_savings import get_drive_service
    from tools.resources_drive_videos import get_resources_videos_folder_id
    from tools.video_creation_pack import get_video_creation_packs_parent_id

    drive = get_drive_service()
    if not drive:
        print("ERROR: Drive service unavailable")
        return 1

    packs_parent = get_video_creation_packs_parent_id()
    shared_root = "0AOEr_35z3HP9Uk9PVA"
    library = get_resources_videos_folder_id()

    print("=== Drive: pack folders ===")
    for parent, label in [(packs_parent, "packs"), (shared_root, "shared-root-orphans")]:
        folders = list_folders_by_name(drive, parent, SLUG_TOKEN)
        print(f"{label}: {len(folders)} folder(s)")
        for f in folders:
            trash_folder_tree(drive, f["id"], f["name"])

    print("=== Drive: library MP4s ===")
    if library:
        for f in list_files_by_name(drive, library, SLUG_TOKEN):
            if "mp4" in f.get("name", "").lower() or f.get("mimeType", "").startswith("video/"):
                trash_file(drive, f["id"], f["name"])

    print("=== SQLite: marketing_videos ===")
    import sqlite3

    db_path = os.path.join(ROOT, os.getenv("SQLITE_DB_NAME", "aces-task-db.sqlite3"))
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT id, slug, variant FROM marketing_videos WHERE slug = ?", (SLUG,)
    ).fetchall()
    print(f"  removing {len(rows)} row(s): {rows}")
    conn.execute("DELETE FROM marketing_videos WHERE slug = ?", (SLUG,))
    conn.execute(
        "UPDATE testimonials SET video_long_file_id = NULL, video_short_file_id = NULL "
        "WHERE business_name LIKE '%Peninsula%'"
    )
    conn.commit()
    conn.close()

    print("=== Done. Re-start video from /videos/create (Member testimonial tab). ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
