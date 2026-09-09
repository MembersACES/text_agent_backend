#!/usr/bin/env python3
"""Sync one shared block of rules into every Retell voice agent's prompt.

Why this exists
---------------
Voice prompts do not live on the Retell agent — they live on the retell-llm
object behind it. Editing six agents by hand in the dashboard means six chances
to paste something slightly different, and no way to tell later which agent got
which version. This keeps the shared rules in one file in git and pushes them
into every agent, replacing only the managed section and leaving each agent's
own wording alone.

The managed section is delimited by the markers below. On the first run the
block is appended; on every run after that it is replaced in place, so this is
safe to run as often as you like.

Scope
-----
This NEVER touches every agent in the Retell account. There are ~100, spanning
unrelated products — robot maintenance, venue bookings, reception. It only
touches agents that an autonomous sequence actually uses, taken from
autonomous_sequence_type.retell_agent_id, or an explicit list you supply.

Retell also returns one entry per agent *version*, so the same underlying LLM
can appear twenty times in /list-agents. Work is deduplicated by llm_id.

Usage
-----
    export RETELL_API_KEY=...            # same key the backend uses
    export DATABASE_URL=...              # same DB the worker reads

    python sync_voice_prompts.py --from-db           # dry run, agents from the DB
    python sync_voice_prompts.py --from-db --diff    # see the exact changes
    python sync_voice_prompts.py --from-db --apply   # write

    python sync_voice_prompts.py --only agent_abc123 --apply   # one agent
    python sync_voice_prompts.py --agents ids.txt --apply      # a file of ids

Nothing is written without --apply, and nothing runs without a scope.
"""

from __future__ import annotations

import argparse
import difflib
import os
import re
import sys
from pathlib import Path

import requests

RETELL_BASE = os.getenv("RETELL_API_BASE_URL", "https://api.retellai.com").rstrip("/")
RETELL_LLM_TYPE = "retell-llm"

START = "### ─── ACES SHARED RULES — managed by sync_voice_prompts.py, do not edit here ───"
END = "### ─── END ACES SHARED RULES ───"

BLOCK_RE = re.compile(
    re.escape(START) + r".*?" + re.escape(END),
    re.DOTALL,
)


def api_key() -> str:
    key = (os.getenv("RETELL_API_KEY") or "").strip()
    if not key:
        sys.exit("RETELL_API_KEY is not set. Export the same key the backend uses.")
    return key


def request(method: str, path: str, json_body: dict | None = None):
    res = requests.request(
        method,
        f"{RETELL_BASE}{path}",
        headers={"Authorization": f"Bearer {api_key()}", "Content-Type": "application/json"},
        json=json_body,
        timeout=30,
    )
    if res.status_code >= 400:
        sys.exit(f"Retell {method} {path} failed [{res.status_code}]: {res.text[:400]}")
    return res.json() if res.text else None


def agent_ids_from_db() -> dict[str, str]:
    """Agent ids the autonomous sequences actually use, keyed to their sequence type.

    This is the authoritative list. Matching on agent *name* would be guesswork
    and would sweep in unrelated products.
    """
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        sys.exit("DATABASE_URL is not set — needed for --from-db. Or pass --agents / --only.")
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        sys.exit("SQLAlchemy is required for --from-db:  pip install sqlalchemy psycopg2-binary")

    engine = create_engine(url)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT sequence_type, retell_agent_id FROM autonomous_sequence_type "
                "WHERE retell_agent_id IS NOT NULL AND retell_agent_id <> ''"
            )
        ).fetchall()
    return {str(r[1]).strip(): str(r[0]) for r in rows if str(r[1]).strip()}


def backup_prompt(root: Path, stamp: str, agent_name: str, llm_id: str, prompt: str) -> Path:
    """Save the current prompt before touching it. No backup, no write."""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", agent_name).strip("_") or llm_id
    out = root / stamp
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{safe}__{llm_id}.txt"
    path.write_text(prompt or "", encoding="utf-8")
    return path


def restore_from(root: Path, apply_changes: bool) -> int:
    """Put back every prompt saved in a backup directory."""
    files = sorted(root.glob("*__*.txt"))
    if not files:
        sys.exit(f"No backup files found in {root}")
    print(f"Restoring from {root}  ({len(files)} prompt(s))\n")
    for path in files:
        llm_id = path.stem.split("__")[-1]
        prompt = path.read_text(encoding="utf-8")
        print(f"  {path.name}  ->  llm {llm_id}  ({len(prompt)} chars)")
        if apply_changes:
            request("PATCH", f"/update-retell-llm/{llm_id}", json_body={"general_prompt": prompt})
            print("      restored")
    if not apply_changes:
        print("\nDry run — nothing written. Re-run with --apply.")
    return 0


def engine_of(agent: dict) -> dict:
    eng = agent.get("response_engine")
    return eng if isinstance(eng, dict) else {}


def apply_block(prompt: str, block: str) -> str:
    """Replace the managed section, or append it if this agent has none yet."""
    managed = f"{START}\n\n{block.strip()}\n\n{END}"
    existing = prompt or ""
    if BLOCK_RE.search(existing):
        return BLOCK_RE.sub(lambda _: managed, existing)
    separator = "\n\n" if existing.strip() else ""
    return f"{existing.rstrip()}{separator}{managed}\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the changes (default is a dry run)")
    ap.add_argument("--from-db", action="store_true",
                    help="scope to agents used by autonomous_sequence_type (recommended)")
    ap.add_argument("--only", metavar="AGENT_ID", help="limit to a single agent")
    ap.add_argument("--agents", metavar="FILE", help="file of agent ids, one per line")
    ap.add_argument("--diff", action="store_true", help="print a full unified diff per agent")
    ap.add_argument("--backup-dir", default="prompt_backups",
                    help="where current prompts are saved before any write (default: prompt_backups)")
    ap.add_argument("--restore", metavar="DIR",
                    help="put back the prompts saved in a backup directory, then exit")
    ap.add_argument(
        "--block",
        default=str(Path(__file__).with_name("voice_shared_block.md")),
        help="file holding the shared rules",
    )
    args = ap.parse_args()

    if args.restore:
        return restore_from(Path(args.restore), args.apply)

    block_path = Path(args.block)
    if not block_path.is_file():
        sys.exit(f"Shared block file not found: {block_path}")
    block = block_path.read_text(encoding="utf-8").strip()
    if not block:
        sys.exit("Shared block file is empty — refusing to wipe every agent's prompt.")

    # Work out the scope first. Refuse to run without one.
    labels: dict[str, str] = {}
    if args.from_db:
        labels = agent_ids_from_db()
        allowed = set(labels)
        scope = f"{len(allowed)} agent(s) from autonomous_sequence_type"
    elif args.agents:
        path = Path(args.agents)
        if not path.is_file():
            sys.exit(f"Agent id file not found: {path}")
        allowed = {ln.strip() for ln in path.read_text().splitlines() if ln.strip() and not ln.startswith("#")}
        scope = f"{len(allowed)} agent(s) from {path.name}"
    elif args.only:
        allowed = {args.only.strip()}
        scope = "1 agent from --only"
    else:
        sys.exit(
            "Refusing to run without a scope. This account has ~100 agents across\n"
            "unrelated products. Use --from-db (recommended), --agents FILE, or --only ID."
        )
    if not allowed:
        sys.exit("Scope resolved to zero agents — nothing to do.")

    agents = [a for a in (request("GET", "/list-agents") or []) if str(a.get("agent_id")) in allowed]
    found = {str(a.get("agent_id")) for a in agents}
    for missing in sorted(allowed - found):
        print(f"  !! agent id not found in Retell: {missing}")

    print(f"Scope: {scope}")
    print(f"Shared block: {block_path.name}, {len(block.splitlines())} lines\n")

    from datetime import datetime
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    changed = skipped = unchanged = 0
    seen_llms: set[str] = set()

    for agent in sorted(agents, key=lambda a: str(a.get("agent_name") or "")):
        agent_id = str(agent.get("agent_id") or "")
        name = str(agent.get("agent_name") or agent_id)
        eng = engine_of(agent)
        llm_id = str(eng.get("llm_id") or "")

        if str(eng.get("type") or "") != RETELL_LLM_TYPE or not llm_id:
            print(f"  SKIP     {name}  (engine {eng.get('type')!r}, prompt not editable)")
            skipped += 1
            continue

        # Retell lists every version of an agent separately, all pointing at the
        # same LLM. Patch each LLM once.
        if llm_id in seen_llms:
            continue
        seen_llms.add(llm_id)

        label = labels.get(agent_id)
        if label:
            name = f"{name}  [{label}]"

        llm = request("GET", f"/get-retell-llm/{llm_id}") or {}
        current = llm.get("general_prompt") or ""
        updated = apply_block(current, block)

        if updated == current:
            print(f"  same     {name}")
            unchanged += 1
            continue

        had_block = bool(BLOCK_RE.search(current))
        action = "replace block" if had_block else "APPEND block (first time)"
        print(f"  CHANGE   {name}  [{action}]  {len(current)} -> {len(updated)} chars")

        if args.diff:
            for line in difflib.unified_diff(
                current.splitlines(), updated.splitlines(),
                fromfile=f"{name} (current)", tofile=f"{name} (new)", lineterm="", n=2,
            ):
                print("      " + line)

        if args.apply:
            saved = backup_prompt(Path(args.backup_dir), stamp, name, llm_id, current)
            print(f"           backed up -> {saved}")
            request("PATCH", f"/update-retell-llm/{llm_id}", json_body={"general_prompt": updated})
            print(f"           written to llm {llm_id}")
        changed += 1

    print(f"\n{changed} to change, {unchanged} already current, {skipped} skipped"
          f"  ({len(seen_llms)} distinct prompt(s) behind {len(agents)} agent entries)")
    if changed and not args.apply:
        print("Dry run — nothing written. Re-run with --apply.")
    elif changed and args.apply:
        print(f"\nOriginals saved in {Path(args.backup_dir) / stamp}")
        print(f"To undo:  python {Path(__file__).name} --restore \"{Path(args.backup_dir) / stamp}\" --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
