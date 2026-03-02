"""
One-off migration: add missing columns to SQLite schema so it matches
models.py (ClientStatusNote, Task). Safe to run multiple times.

Uses SQLITE_DB_NAME if set (same as database.py), else aces-task-db.sqlite3.
Run from backend dir: python add_client_id_to_notes.py
"""
import os
import sqlite3
from pathlib import Path
from typing import Optional

DB_NAME = os.getenv("SQLITE_DB_NAME", "aces-task-db.sqlite3")
DB_PATH = Path(__file__).resolve().parent / DB_NAME


def columns(cur, table: str) -> list[str]:
    cur.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]


def add_column_if_missing(cur, table: str, col: str, spec: str, backfill_sql: Optional[str] = None):
    if col in columns(cur, table):
        print(f"  {table}.{col} already exists.")
        return
    print(f"  Adding {table}.{col}...")
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {spec};")
    if backfill_sql:
        cur.execute(backfill_sql)
        print(f"  Backfilled {table}.{col}.")


def main():
    print(f"Using DB at: {DB_PATH}")
    if not DB_PATH.exists():
        print("Database file not found. Run the app once to create it, or fix DB_PATH.")
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ----- client_status_notes: match ClientStatusNote model -----
    print("client_status_notes:")
    add_column_if_missing(cur, "client_status_notes", "client_id", "INTEGER")
    add_column_if_missing(cur, "client_status_notes", "note_type", "TEXT", 
                         "UPDATE client_status_notes SET note_type = 'general' WHERE note_type IS NULL;")
    add_column_if_missing(cur, "client_status_notes", "related_task_id", "INTEGER")
    add_column_if_missing(cur, "client_status_notes", "related_offer_id", "INTEGER")
    add_column_if_missing(cur, "client_status_notes", "created_at", "TEXT",
                         "UPDATE client_status_notes SET created_at = datetime('now') WHERE created_at IS NULL;")
    add_column_if_missing(cur, "client_status_notes", "updated_at", "TEXT",
                         "UPDATE client_status_notes SET updated_at = datetime('now') WHERE updated_at IS NULL;")

    print("Backfilling client_status_notes.client_id...")
    cur.execute(
        """
        UPDATE client_status_notes
        SET client_id = (
          SELECT id FROM clients
          WHERE clients.business_name = client_status_notes.business_name
          LIMIT 1
        )
        WHERE client_id IS NULL;
        """
    )

    # ----- tasks: match Task model -----
    print("tasks:")
    add_column_if_missing(cur, "tasks", "client_id", "INTEGER")
    add_column_if_missing(cur, "tasks", "category", "TEXT",
                         "UPDATE tasks SET category = 'task' WHERE category IS NULL;")

    conn.commit()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
