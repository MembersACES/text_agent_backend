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
Each agent is loaded by id (GET /get-agent/{id}), not by listing the account.

Usage
-----
    export RETELL_API_KEY=...            # same key the backend uses
    export DATABASE_URL=...              # same DB the worker reads

    python sync_voice_prompts.py --from-db                  # dry run (same as --dry-run)
    python sync_voice_prompts.py --from-db --dry-run        # explicit; identical to omitting --apply
    python sync_voice_prompts.py --from-db --apply          # write AND publish

    python sync_voice_prompts.py --only agent_abc123 --dry-run
    python sync_voice_prompts.py --only agent_abc123 --lane campaign --dry-run
    python sync_voice_prompts.py --only agent_abc123 agent_def456 --apply
    python sync_voice_prompts.py --agents ids.txt --apply

The "already holding" paragraph is composed per lane: gci_outbound_v1 never
mentions figures or an expiry; comparison lanes keep that wording.

Omitting --apply is a dry run: it publishes nothing, PATCHes nothing, and
creates no drafts. It prints each agent id and name, the published version
callers hear, whether that version is frozen or a draft is sitting on it,
and a unified diff of the managed prompt block.

--apply always publishes. A PATCH without publish leaves callers on the last
published version (Version 0, in the incident this script is meant to prevent).
Before writing, known superseded passages in the agent-owned body are replaced
with lane-specific wording (never deleted). After a write the script re-reads
that published version and reports agent id, version, and whether it contains
the new shared-block text and no leftover superseded copy.
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

# Phrases the live shared block must contain. Callers heard the old
# "best handled by one of our consultants" wording because a PATCH without
# publish left Version 0 live. After every sync we check the *published*
# prompt for these, not the draft GET returns.
NEW_TEXT_MARKERS = (
    "Amelia Williams",
    "business@acesolutions.com.au",
    "1300 849 908",
)
OLD_CONSULTANTS_WORDING = "best handled by one of our consultants"
ARRANGE_A_CALL_PHRASE = "I can arrange a call or a short online meeting"
# Spoken half of the campaign escalation rule. Same trigger, honest handoff:
# pass to Amelia by email. Do not delete this passage — replace it.
OLD_ESCALATION_SCRIPT = (
    'Use: "That\'s a good question and it\'s best handled by one of our consultants. '
    'I can arrange a call or a short online meeting so they can answer properly."'
)
LEFTOVER_SUPERSEDED_PHRASES = (
    OLD_ESCALATION_SCRIPT,
    ARRANGE_A_CALL_PHRASE,
    OLD_CONSULTANTS_WORDING,
)
HOLDING_TOKEN = "{{email_already_holding}}"
PAUSE_TOKEN = "{{figure_pause}}"
CAMPAIGN_SEQUENCE_TYPES = frozenset({"gci_outbound_v1"})
COMPARISON_HOLDING_PHRASE = "the figures and the expiry"
FORBIDDEN_CAMPAIGN_PHRASES = (
    COMPARISON_HOLDING_PHRASE,
    "a saving, a rate, an expiry",
)
LANE_COPY = {
    "comparison": {
        "email_already_holding": (
            '  "Everything\'s in the email I sent through - the figures and the expiry are\n'
            '   all in there. Have a look and I\'ll follow up with you."'
        ),
        "figure_pause": (
            "After any figure that matters - a saving, a rate, an expiry - pause and check\n"
            "they caught it. Do not just carry on."
        ),
        "escalation_script": (
            'Use: "That\'s a good question, and it\'s better handled by one of our consultants\n'
            "rather than me. I'll pass this on to Amelia Williams at ACES, and you can\n"
            'reach her directly on business@acesolutions.com.au."'
        ),
    },
    "campaign": {
        "email_already_holding": (
            '  "It\'s all in the email I sent through, have a look and I\'ll follow up with you."'
        ),
        "figure_pause": (
            "After any figure that matters, pause and check they caught it. Do not just carry on."
        ),
        "escalation_script": (
            'Use: "That\'s a good question, and for a business your size it\'s better handled by one\n'
            "of our consultants rather than me. I'll pass this on to Amelia Williams at ACES,\n"
            'and you can reach her directly on business@acesolutions.com.au."'
        ),
    },
}


def markers_missing(prompt: str, markers: tuple[str, ...] = NEW_TEXT_MARKERS) -> list[str]:
    text = prompt or ""
    return [marker for marker in markers if marker.lower() not in text.lower()]


def published_has_new_text(prompt: str, markers: tuple[str, ...] = NEW_TEXT_MARKERS) -> bool:
    return not markers_missing(prompt, markers)


def forbidden_campaign_phrases(prompt: str) -> list[str]:
    text = (prompt or "").lower()
    return [phrase for phrase in FORBIDDEN_CAMPAIGN_PHRASES if phrase.lower() in text]


def _passage_pattern(passage: str) -> re.Pattern[str]:
    """Literal words of a passage; whitespace (including wrapping) may vary."""
    words = (passage or "").split()
    if not words:
        return re.compile(r"(?!)")
    return re.compile(r"\s+".join(re.escape(word) for word in words), re.IGNORECASE)


def leftover_superseded(prompt: str) -> list[str]:
    text = prompt or ""
    return [phrase for phrase in LEFTOVER_SUPERSEDED_PHRASES if _passage_pattern(phrase).search(text)]


def rewrite_unmanaged_body(text: str, lane: str = "comparison") -> tuple[str, list[str]]:
    """Replace known superseded passages with lane-specific wording. Never delete a rule."""
    remaining = text or ""
    replaced: list[str] = []
    copy = LANE_COPY[lane]
    pattern = _passage_pattern(OLD_ESCALATION_SCRIPT)
    if pattern.search(remaining):
        remaining = pattern.sub(copy["escalation_script"], remaining, count=1)
        replaced.append("escalation_script")
    remaining = re.sub(r"[ \t]+\n", "\n", remaining)
    remaining = re.sub(r"\n{3,}", "\n\n", remaining)
    return remaining.strip(), replaced


def unmanaged_body(prompt: str) -> str:
    return BLOCK_RE.sub("", prompt or "").strip()


def published_prompt_ok(
    prompt: str,
    markers: tuple[str, ...] = NEW_TEXT_MARKERS,
    lane: str = "comparison",
) -> bool:
    if not published_has_new_text(prompt, markers):
        return False
    if leftover_superseded(prompt):
        return False
    if lane == "campaign" and forbidden_campaign_phrases(prompt):
        return False
    return True


def format_verify_line(
    agent_id: str,
    version: object,
    is_published: bool | None,
    prompt: str,
    markers: tuple[str, ...] = NEW_TEXT_MARKERS,
    lane: str = "comparison",
) -> str:
    missing = markers_missing(prompt, markers)
    contains = "YES" if not missing else f"NO — missing {missing}"
    published = "yes" if is_published else "no" if is_published is False else "?"
    old = ""
    leftover = leftover_superseded(prompt)
    if leftover:
        old = "  FAIL superseded wording still present"
    if lane == "campaign":
        forbidden = forbidden_campaign_phrases(prompt)
        if forbidden:
            old += f"  FORBIDDEN campaign holding copy still present: {forbidden}"
    return (
        f"  agent_id={agent_id}  version={version}  published={published}  "
        f"published contains new text: {contains}{old}"
    )


def api_key() -> str:
    key = (os.getenv("RETELL_API_KEY") or "").strip()
    if not key:
        sys.exit("RETELL_API_KEY is not set. Export the same key the backend uses.")
    return key


def _retell_request(method: str, path: str, json_body: dict | None = None, params: dict | None = None):
    return requests.request(
        method,
        f"{RETELL_BASE}{path}",
        headers={"Authorization": f"Bearer {api_key()}", "Content-Type": "application/json"},
        json=json_body,
        params=params,
        timeout=30,
    )


def request(method: str, path: str, json_body: dict | None = None, params: dict | None = None):
    res = _retell_request(method, path, json_body, params)
    if res.status_code >= 400:
        sys.exit(f"Retell {method} {path} failed [{res.status_code}]: {res.text[:400]}")
    return res.json() if res.text else None


def request_or_none(method: str, path: str, json_body: dict | None = None, params: dict | None = None):
    res = _retell_request(method, path, json_body, params)
    if res.status_code >= 400:
        return None
    return res.json() if res.text else None


def _sequence_types(url: str) -> dict[str, str]:
    from sqlalchemy import create_engine, text

    engine = create_engine(url)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT sequence_type, retell_agent_id FROM autonomous_sequence_type "
                "WHERE retell_agent_id IS NOT NULL AND retell_agent_id <> ''"
            )
        ).fetchall()
    return {str(r[1]).strip(): str(r[0]) for r in rows if str(r[1]).strip()}


def try_sequence_types() -> dict[str, str]:
    """Best-effort agent_id → sequence_type. Empty if DB is unavailable."""
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        return {}
    try:
        return _sequence_types(url)
    except Exception:
        return {}


def agent_ids_from_db() -> dict[str, str]:
    """Agent ids the autonomous sequences actually use, keyed to their sequence type.

    This is the authoritative list. Matching on agent *name* would be guesswork
    and would sweep in unrelated products.
    """
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        sys.exit("DATABASE_URL is not set — needed for --from-db. Or pass --agents / --only.")
    try:
        return _sequence_types(url)
    except ImportError:
        sys.exit("SQLAlchemy is required for --from-db:  pip install sqlalchemy psycopg2-binary")


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


def load_local_env() -> None:
    env_file = Path(__file__).resolve().parents[1] / ".env"
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(env_file)


def assert_template_is_lane_neutral(template: str) -> None:
    missing = [token for token in (HOLDING_TOKEN, PAUSE_TOKEN) if token not in (template or "")]
    if missing:
        raise ValueError(
            f"Shared block is missing {missing} — refusing to sync a one-size-fits-all holding line."
        )
    if COMPARISON_HOLDING_PHRASE in (template or ""):
        raise ValueError(
            "Shared block still hardcodes comparison holding copy; use {{email_already_holding}}."
        )


def lane_for_sequence(sequence_type: str | None, override: str | None = None) -> str:
    """Campaign first-touch must not claim a comparison was sent. Everything else does."""
    if override in LANE_COPY:
        return override
    if (sequence_type or "").strip() in CAMPAIGN_SEQUENCE_TYPES:
        return "campaign"
    return "comparison"


def compose_shared_block(template: str, lane: str) -> str:
    copy = LANE_COPY[lane]
    out = (template or "").replace(HOLDING_TOKEN, copy["email_already_holding"])
    out = out.replace(PAUSE_TOKEN, copy["figure_pause"])
    leftover = [token for token in (HOLDING_TOKEN, PAUSE_TOKEN) if token in out]
    if leftover:
        raise ValueError(f"Unsubstituted lane tokens remain: {leftover}")
    return out


def wrap_managed(block: str) -> str:
    return f"{START}\n\n{block.strip()}\n\n{END}"


def managed_section(prompt: str) -> str:
    match = BLOCK_RE.search(prompt or "")
    return match.group(0) if match else ""


def apply_block(prompt: str, block: str, lane: str = "comparison") -> str:
    """Rewrite superseded body wording, then replace or append the managed section."""
    managed = wrap_managed(block)
    body, _replaced = rewrite_unmanaged_body(unmanaged_body(prompt or ""), lane)
    separator = "\n\n" if body else ""
    return f"{body}{separator}{managed}\n"


def edit_state_label(latest_is_published: bool, latest_version: object, published_version: object) -> str:
    if latest_is_published:
        return f"FROZEN (published v{published_version}, no draft)"
    return f"DRAFT v{latest_version} sitting on published v{published_version}"


def normalize_agent_ids(values: list[str] | None) -> list[str]:
    """--only a b  and  --only a,b  both work."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        for part in str(raw).replace(",", " ").split():
            aid = part.strip()
            if aid and aid not in seen:
                seen.add(aid)
                out.append(aid)
    return out


def block_diff_lines(old_prompt: str, new_block: str, from_label: str, to_label: str) -> list[str]:
    old = managed_section(old_prompt)
    new = wrap_managed(new_block)
    if old.strip() == new.strip():
        return ["  managed block: identical"]
    header = (
        ["  managed block: APPEND (no shared-rules section on this prompt yet)"]
        if not old.strip()
        else ["  managed block: CHANGE"]
    )
    diff = difflib.unified_diff(
        old.splitlines(),
        new.splitlines(),
        fromfile=from_label,
        tofile=to_label,
        lineterm="",
        n=2,
    )
    return header + ["      " + line for line in diff]


def body_diff_lines(old_prompt: str, new_prompt: str, from_label: str, to_label: str) -> list[str]:
    old = unmanaged_body(old_prompt)
    new = unmanaged_body(new_prompt)
    if old.strip() == new.strip():
        return ["  unmanaged body: identical"]
    diff = difflib.unified_diff(
        old.splitlines(),
        new.splitlines(),
        fromfile=from_label,
        tofile=to_label,
        lineterm="",
        n=2,
    )
    return ["  unmanaged body: CHANGE"] + ["      " + line for line in diff]


def load_agent(agent_id: str, version: int | str | None = None) -> dict:
    params = {"version": version} if version is not None else None
    agent = request("GET", f"/get-agent/{agent_id}", params=params)
    if not isinstance(agent, dict):
        sys.exit(f"Unexpected get-agent response for {agent_id}")
    return agent


def try_load_agent(agent_id: str, version: int | str | None = None) -> dict | None:
    params = {"version": version} if version is not None else None
    agent = request_or_none("GET", f"/get-agent/{agent_id}", params=params)
    return agent if isinstance(agent, dict) else None


def llm_prompt(llm_id: str, version: object | None = None) -> str:
    params = None
    if version is not None and version != "":
        params = {"version": version}
    llm = request("GET", f"/get-retell-llm/{llm_id}", params=params) or {}
    return (llm.get("general_prompt") if isinstance(llm, dict) else "") or ""


def prompt_from_agent(agent: dict) -> str:
    eng = engine_of(agent)
    llm_id = str(eng.get("llm_id") or "").strip()
    if not llm_id:
        return ""
    return llm_prompt(llm_id, eng.get("version"))


def editable_agent(agent_id: str, agent: dict) -> dict:
    """Fork a draft if this version is published. Writes to a frozen LLM 400."""
    if not agent.get("is_published"):
        return agent
    base = agent.get("version")
    if base is None:
        print(f"  !! {agent_id}: published with no version — writing may fail")
        return agent
    created = request(
        "POST",
        f"/create-agent-version/{agent_id}",
        json_body={"base_version": int(base)},
    )
    new_version = created.get("version") if isinstance(created, dict) else None
    if new_version is None:
        print(f"  !! {agent_id}: create-agent-version returned no version")
        return agent
    print(f"           forked published v{base} -> draft v{new_version}")
    return load_agent(agent_id, int(new_version))


def publish_agent(agent_id: str, version: int) -> bool:
    request("POST", f"/publish-agent-version/{agent_id}", json_body={"version": int(version)})
    return True


def published_prompt_for(agent_id: str, version: int | None, llm_id: str, llm_version: object | None) -> str:
    """Read the prompt at the version we just published, not whatever GET defaults to."""
    if version is not None:
        agent = load_agent(agent_id, int(version))
        eng = engine_of(agent)
        llm_id = str(eng.get("llm_id") or llm_id).strip()
        llm_version = eng.get("version") if eng.get("version") is not None else llm_version
    return llm_prompt(llm_id, llm_version)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--apply",
        action="store_true",
        help="write and publish. Without this flag, or with --dry-run, nothing is written.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="read-only report (default when --apply is omitted). Never writes or publishes.",
    )
    ap.add_argument("--from-db", action="store_true",
                    help="scope to agents used by autonomous_sequence_type (recommended)")
    ap.add_argument(
        "--only",
        nargs="+",
        metavar="AGENT_ID",
        help="limit to these agent ids (one or more). Other agents are not touched. "
             "Accepts space- or comma-separated ids.",
    )
    ap.add_argument("--agents", metavar="FILE", help="file of agent ids, one per line")
    ap.add_argument("--diff", action="store_true", help="also print a full-prompt diff (managed-block diff is always shown)")
    ap.add_argument("--backup-dir", default="prompt_backups",
                    help="where current prompts are saved before any write (default: prompt_backups)")
    ap.add_argument("--restore", metavar="DIR",
                    help="put back the prompts saved in a backup directory, then exit")
    ap.add_argument(
        "--block",
        default=str(Path(__file__).with_name("voice_shared_block.md")),
        help="file holding the shared rules",
    )
    ap.add_argument(
        "--lane",
        choices=sorted(LANE_COPY),
        help="force campaign or comparison holding copy for every agent in this run. "
        "Default: gci_outbound_v1 -> campaign, everything else -> comparison.",
    )
    return ap


def main() -> int:
    args = build_parser().parse_args()
    if args.apply and args.dry_run:
        sys.exit("Pass --apply or --dry-run, not both.")

    load_local_env()

    if args.restore:
        return restore_from(Path(args.restore), args.apply)

    block_path = Path(args.block)
    if not block_path.is_file():
        sys.exit(f"Shared block file not found: {block_path}")
    template = block_path.read_text(encoding="utf-8").strip()
    if not template:
        sys.exit("Shared block file is empty — refusing to wipe every agent's prompt.")
    try:
        assert_template_is_lane_neutral(template)
    except ValueError as exc:
        sys.exit(str(exc))

    only_ids = normalize_agent_ids(args.only)
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
    elif only_ids:
        allowed = set(only_ids)
        scope = f"{len(allowed)} agent(s) from --only"
    else:
        sys.exit(
            "Refusing to run without a scope. This account has ~100 agents across\n"
            "unrelated products. Use --from-db (recommended), --agents FILE, or --only ID [ID...]."
        )
    if only_ids and (args.from_db or args.agents):
        missing = [aid for aid in only_ids if aid not in allowed]
        for aid in missing:
            print(f"  !! --only {aid} is not in this scope — skipped")
        allowed = {aid for aid in only_ids if aid in allowed}
        scope = f"{len(allowed)} agent(s) filtered by --only"
    if not allowed:
        sys.exit("Scope resolved to zero agents — nothing to do.")
    if not labels:
        labels = {aid: seq for aid, seq in try_sequence_types().items() if aid in allowed}

    dry_run = not args.apply
    markers = tuple(m for m in NEW_TEXT_MARKERS if m in template) or NEW_TEXT_MARKERS
    print(f"Mode: {'DRY RUN — nothing will be written or published' if dry_run else 'APPLY — write AND publish'}")
    print(f"Scope: {scope}")
    print(f"Shared block: {block_path.name}, {len(template.splitlines())} lines")
    if dry_run:
        print("Omitting --apply is the same as --dry-run: GET only. No PATCH, no create-agent-version, no publish.\n")
    else:
        print("After writing, this script publishes, then re-reads the published version.\n")

    from datetime import datetime
    import time
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    changed = skipped = unchanged = 0
    written_llms: set[str] = set()
    verify_rows: list[tuple[str, str, int | None, str, object | None, str]] = []
    written: list[tuple[str, str, int, str, object | None, str]] = []
    found: set[str] = set()
    proposed_leftover_failed = False

    for agent_id in sorted(allowed):
        latest = try_load_agent(agent_id)
        if latest is None:
            print(f"\n{'=' * 72}")
            print(f"agent_id: {agent_id}")
            print("  !! not found in Retell")
            skipped += 1
            continue
        found.add(agent_id)
        published_agent = try_load_agent(agent_id, "latest_published")
        if published_agent is None and latest.get("is_published"):
            published_agent = latest
        name = str(latest.get("agent_name") or agent_id)
        label = labels.get(agent_id)
        if label:
            name = f"{name}  [{label}]"
        lane = lane_for_sequence(label, args.lane)
        try:
            block = compose_shared_block(template, lane)
        except ValueError as exc:
            sys.exit(str(exc))
        latest_version = latest.get("version")
        published_version = published_agent.get("version") if published_agent else latest.get("base_version")
        latest_is_published = bool(latest.get("is_published"))
        state = edit_state_label(latest_is_published, latest_version, published_version)

        print(f"\n{'=' * 72}")
        print(f"agent_id: {agent_id}")
        print(f"name:     {name}")
        print(f"published version: {published_version}")
        print(f"edit state: {state}")
        if label:
            print(f"holding copy: {lane}  ({label})")
        elif args.lane:
            print(f"holding copy: {lane}  (--lane)")
        else:
            print(
                f"holding copy: {lane}  (sequence_type unknown — "
                "pass --lane campaign if this is the invoice-request agent)"
            )

        eng = engine_of(latest)
        llm_id = str(eng.get("llm_id") or "")
        if str(eng.get("type") or "") != RETELL_LLM_TYPE or not llm_id:
            print(f"  SKIP (engine {eng.get('type')!r}, prompt not editable)")
            skipped += 1
            continue

        published_prompt = prompt_from_agent(published_agent) if published_agent else ""
        write_prompt = prompt_from_agent(latest)
        # --apply writes the latest version (draft if one exists, else published).
        current = write_prompt
        updated = apply_block(current, block, lane)
        pub_eng = engine_of(published_agent) if published_agent else {}
        verify_rows.append(
            (
                agent_id,
                name,
                published_version if isinstance(published_version, int) else None,
                str(pub_eng.get("llm_id") or llm_id),
                pub_eng.get("version"),
                lane,
            )
        )

        if not latest_is_published:
            print("  callers currently hear the published version, not this draft")
            if managed_section(published_prompt).strip() != managed_section(write_prompt).strip():
                print("  draft managed block already differs from published")

        for line in block_diff_lines(
            published_prompt,
            block,
            f"{agent_id} published v{published_version}",
            f"{agent_id} new shared block",
        ):
            print(line)

        _, replaced = rewrite_unmanaged_body(unmanaged_body(current), lane)
        if replaced:
            print(f"  replaced {len(replaced)} passage(s) in the agent-owned body  (lane={lane})")
            for name in replaced:
                print(f"    - {name}")
        proposed_left = leftover_superseded(updated)
        if proposed_left:
            proposed_leftover_failed = True
            print("  !! proposed prompt still has superseded wording")
            print("     add the surrounding paragraph to OLD_ESCALATION_SCRIPT")
            for item in proposed_left:
                print(f"    - {' '.join(item.split())}")
        for line in body_diff_lines(
            current,
            updated,
            f"{agent_id} current body",
            f"{agent_id} body after rewrite",
        ):
            print(line)

        if args.diff:
            for line in difflib.unified_diff(
                current.splitlines(), updated.splitlines(),
                fromfile=f"{name} (current latest)", tofile=f"{name} (after apply)",
                lineterm="", n=1,
            ):
                print("      " + line)

        if updated == current:
            unchanged += 1
        else:
            changed += 1

        if dry_run or updated == current:
            continue

        if llm_id in written_llms:
            print("           skipped write — same LLM already updated this run")
            continue
        written_llms.add(llm_id)

        draft = editable_agent(agent_id, latest)
        draft_eng = engine_of(draft)
        draft_llm_id = str(draft_eng.get("llm_id") or llm_id)
        draft_llm_version = draft_eng.get("version")
        draft_agent_version = draft.get("version")
        saved = backup_prompt(Path(args.backup_dir), stamp, name, draft_llm_id, current)
        print(f"           backed up -> {saved}")
        written_prompt = apply_block(prompt_from_agent(draft), block, lane)
        request(
            "PATCH",
            f"/update-retell-llm/{draft_llm_id}",
            json_body={"general_prompt": written_prompt},
            params={"version": int(draft_llm_version)} if draft_llm_version is not None else None,
        )
        print(f"           written to llm {draft_llm_id} (draft)")
        if draft_agent_version is None:
            print(f"           NOT PUBLISHED — no agent version on {agent_id}")
        else:
            publish_agent(agent_id, int(draft_agent_version))
            print(f"           published {agent_id} v{draft_agent_version}")
            time.sleep(0.25)
            written.append(
                (agent_id, name, int(draft_agent_version), draft_llm_id, draft_llm_version, lane)
            )

    for missing_id in sorted(allowed - found):
        print(f"  !! agent id not found in Retell: {missing_id}")

    print(f"\n{changed} to change, {unchanged} already current, {skipped} skipped")
    if dry_run:
        print("Dry run — nothing written, nothing published.")
        if only_ids:
            print(f"Scoped to --only: {', '.join(only_ids)}")
        print("Re-run with --apply to write AND publish these agents only.")
    elif changed:
        print(f"\nOriginals saved in {Path(args.backup_dir) / stamp}")
        print(f"To undo:  python {Path(__file__).name} --restore \"{Path(args.backup_dir) / stamp}\" --apply")

    print("\nPUBLISHED VERSION CHECK")
    print("agent id / version / whether the published prompt contains the new shared-block text")
    verified_ok = True
    leftover_failed = False
    check_ids = written if (args.apply and written) else verify_rows
    for agent_id, name, version, llm_id, llm_version, lane in check_ids:
        try:
            if args.apply and written:
                prompt = published_prompt_for(agent_id, version, llm_id, llm_version)
                agent_now = load_agent(agent_id, version)
            else:
                agent_now = try_load_agent(agent_id, "latest_published") or try_load_agent(agent_id)
                prompt = prompt_from_agent(agent_now) if agent_now else ""
            is_published = bool(agent_now.get("is_published")) if agent_now and "is_published" in agent_now else None
            line = format_verify_line(
                agent_id,
                agent_now.get("version") if agent_now else version,
                is_published,
                prompt,
                markers,
                lane,
            )
            print(f"{line}  ({name})")
            if leftover_superseded(prompt):
                leftover_failed = True
                verified_ok = False
            if not published_prompt_ok(prompt, markers, lane) or is_published is False:
                verified_ok = False
        except SystemExit as exc:
            print(f"  agent_id={agent_id}  VERIFY FAILED: {exc}")
            verified_ok = False

    if dry_run and proposed_leftover_failed:
        print(
            "\nRESULT: this sync would still leave superseded wording in the prompt. "
            "Add the missing passage to OLD_ESCALATION_SCRIPT before --apply."
        )
        return 1
    if args.apply and changed and leftover_failed:
        print(
            "\nRESULT: published version still contains superseded wording. "
            "The agent has contradictory instructions. Do not treat this sync as live."
        )
        return 1
    if args.apply and changed and not verified_ok:
        print(
            "\nRESULT: published version does NOT contain the new text. "
            "Callers will still hear the old prompt. Do not treat this sync as live."
        )
        return 1
    if args.apply and changed and verified_ok:
        print("\nRESULT: published version contains the new text and no superseded wording.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
