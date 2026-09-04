"""Retell AI call-history proxy for the autonomous sequence dashboard.

Why this exists
---------------
Nothing in any of the three repos ever asked Retell about a call. The steps
table has no column linking a step to the call it placed, so the only way to
find the call behind a step was to open the Retell dashboard and match on
timestamp by eye — which is how every voice defect this month was investigated.

Every call the worker places carries `metadata={"run_id": ...}`
(autonomous_agent_backend/src/services/phone.py), so a run's calls can be
recovered from Retell today, with no migration and retroactively over calls that
have already happened.

Filtering happens here rather than in `filter_criteria` deliberately: Retell's
list filter is documented for first-class fields, not for arbitrary metadata, so
asking the API to filter on run_id would either be ignored or rejected depending
on the version. Paging newest-first and matching locally works either way, and
at current volume (tens of calls per day) one page covers a run comfortably.

Retell stays the source of truth for call content, exactly as retell_agents.py
keeps it the source of truth for prompts. Nothing here is copied into Postgres.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from services.retell_agents import RetellAgentsError, _request

logger = logging.getLogger(__name__)

_MAX_LIST_PAGES = 10
_PAGE_SIZE = 100


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _combined_cost_cents(row: dict[str, Any]) -> Optional[float]:
    cost = row.get("call_cost")
    if not isinstance(cost, dict):
        return None
    value = cost.get("combined_cost")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _summarise(row: dict[str, Any]) -> dict[str, Any]:
    """The fields the dashboard shows, flattened out of Retell's nested payload."""
    analysis = row.get("call_analysis") if isinstance(row.get("call_analysis"), dict) else {}
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    started = _as_int(row.get("start_timestamp"))
    ended = _as_int(row.get("end_timestamp"))
    return {
        "call_id": str(row.get("call_id") or ""),
        "agent_id": str(row.get("agent_id") or ""),
        "agent_name": str(row.get("agent_name") or "").strip() or None,
        "agent_version": _as_int(row.get("version")),
        "run_id": _as_int(metadata.get("run_id")),
        "direction": str(row.get("direction") or ""),
        "from_number": str(row.get("from_number") or ""),
        "to_number": str(row.get("to_number") or ""),
        "call_status": str(row.get("call_status") or ""),
        "disconnection_reason": str(row.get("disconnection_reason") or "") or None,
        "start_timestamp": started,
        "end_timestamp": ended,
        "duration_ms": (ended - started) if started and ended else None,
        "combined_cost": _combined_cost_cents(row),
        "in_voicemail": bool(analysis.get("in_voicemail")),
        "user_sentiment": str(analysis.get("user_sentiment") or "") or None,
        "call_summary": str(analysis.get("call_summary") or "") or None,
        "recording_url": str(row.get("recording_url") or "") or None,
        "transcript": str(row.get("transcript") or "") or None,
    }


def _page(pagination_key: Optional[str]) -> tuple[list[dict[str, Any]], Optional[str]]:
    params: dict[str, Any] = {"limit": _PAGE_SIZE}
    if pagination_key:
        params["pagination_key"] = pagination_key
    body = _request("POST", "/v2/list-calls", json_body={}, params=params)

    if isinstance(body, list):
        return [r for r in body if isinstance(r, dict)], None
    if isinstance(body, dict):
        raw = body.get("items")
        rows = [r for r in raw if isinstance(r, dict)] if isinstance(raw, list) else []
        next_key = body.get("pagination_key")
        return rows, (str(next_key) if next_key and body.get("has_more") else None)
    return [], None


def list_calls_for_run(run_id: int, limit: int = 50) -> list[dict[str, Any]]:
    """Every Retell call carrying this run_id in its metadata, newest first."""
    found: list[dict[str, Any]] = []
    pagination_key: Optional[str] = None
    for _ in range(_MAX_LIST_PAGES):
        rows, pagination_key = _page(pagination_key)
        for row in rows:
            summary = _summarise(row)
            if summary["run_id"] == run_id:
                found.append(summary)
                if len(found) >= limit:
                    return found
        if not pagination_key:
            break
    logger.info("Retell call history: run_id=%d matched %d call(s)", run_id, len(found))
    return found


def get_call(call_id: str) -> dict[str, Any]:
    """One call in full, for the transcript view."""
    cid = (call_id or "").strip()
    if not cid:
        raise RetellAgentsError(400, "call_id is required.")
    body = _request("GET", f"/v2/get-call/{cid}")
    if not isinstance(body, dict):
        raise RetellAgentsError(502, "Retell returned an unexpected shape for get-call.")
    return _summarise(body)
