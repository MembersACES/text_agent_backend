"""
One-off script to list Airtable bases and their table/field structure.
Run from backend root: python airtable_inspect.py
Requires: pip install python-dotenv requests (or use your existing deps)
"""
import os
import sys
import json

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests

AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
if not AIRTABLE_API_KEY:
    print("Missing AIRTABLE_API_KEY in environment (.env)")
    exit(1)

BASE_URL = "https://api.airtable.com/v0"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json",
}

def list_bases():
    """List all bases the token can access."""
    r = requests.get(f"{BASE_URL}/meta/bases", headers=HEADERS)
    r.raise_for_status()
    return r.json()

def get_base_schema(base_id):
    """Get tables and fields for a base."""
    r = requests.get(f"{BASE_URL}/meta/bases/{base_id}/tables", headers=HEADERS)
    r.raise_for_status()
    return r.json()

def safe_print(s: str) -> None:
    """Print string, replacing chars that Windows console can't encode."""
    enc = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        out = s.encode(enc, errors="replace").decode(enc)
    except (LookupError, TypeError):
        out = s.encode("ascii", errors="replace").decode("ascii")
    print(out)


def main():
    data = list_bases()
    bases = data.get("bases", [])
    safe_print(f"Found {len(bases)} base(s)\n")
    safe_print("=" * 60)

    for base in bases:
        base_id = base.get("id")
        name = base.get("name", "Unnamed")
        safe_print(f"\nBASE: {name}")
        safe_print(f"  ID: {base_id}")
        safe_print("-" * 60)

        try:
            schema = get_base_schema(base_id)
        except Exception as e:
            safe_print(f"  Error loading schema: {e}")
            continue

        tables = schema.get("tables", [])
        for t in tables:
            table_id = t.get("id")
            table_name = t.get("name", "Unnamed table")
            fields = t.get("fields", [])
            safe_print(f"\n  TABLE: {table_name}")
            safe_print(f"    id: {table_id}")
            safe_print(f"    fields ({len(fields)}):")
            for f in fields:
                fname = f.get("name", "?")
                ftype = f.get("type", "?")
                safe_print(f"      - {fname} ({ftype})")
        safe_print("")

if __name__ == "__main__":
    main()