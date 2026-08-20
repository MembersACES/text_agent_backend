"""Retell AI agent/LLM proxy for autonomous sequence voice prompts.

The CRM dashboard lists Retell agents and edits `general_prompt` / `begin_message`
on the linked Retell LLM. Retell remains the source of truth — prompts are not
copied into Postgres.
"""
from __future__ import annotations

import os
from typing import Any, Optional

import httpx

RETELL_BASE = os.getenv("RETELL_API_BASE_URL", "https://api.retellai.com").rstrip("/")
RETELL_LLM_TYPE = "retell-llm"
_TIMEOUT = 30.0
_MAX_LIST_PAGES = 20


class RetellAgentsError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _api_key() -> str:
    key = os.getenv("RETELL_API_KEY", "").strip()
    if not key:
        raise RetellAgentsError(
            503,
            "RETELL_API_KEY is not configured on this service.",
        )
    return key


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_api_key()}",
        "Content-Type": "application/json",
    }


def _message_from_body(body: Any, fallback: str) -> str:
    if isinstance(body, dict):
        for key in ("message", "detail", "error"):
            val = body.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    if isinstance(body, str) and body.strip():
        return body.strip()[:500]
    return fallback


def _request(method: str, path: str, json_body: Optional[dict] = None, params: Optional[dict] = None) -> Any:
    url = f"{RETELL_BASE}{path}"
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            r = client.request(method, url, headers=_headers(), json=json_body, params=params)
    except httpx.RequestError as e:
        raise RetellAgentsError(502, f"Retell request failed: {e}") from e

    body: Any
    try:
        body = r.json()
    except Exception:
        body = r.text

    if r.status_code == 401:
        raise RetellAgentsError(502, "Retell rejected the API key (401). Check RETELL_API_KEY.")
    if r.status_code == 404:
        raise RetellAgentsError(404, _message_from_body(body, "Retell agent or LLM not found."))
    if r.status_code >= 400:
        raise RetellAgentsError(
            502,
            _message_from_body(body, f"Retell returned {r.status_code}."),
        )
    return body


def list_voice_agents() -> list[dict[str, Any]]:
    """List unique voice agents (paginated Retell v2 API)."""
    items: list[dict[str, Any]] = []
    pagination_key: Optional[str] = None
    for _ in range(_MAX_LIST_PAGES):
        params: dict[str, Any] = {"limit": 100}
        if pagination_key:
            params["pagination_key"] = pagination_key
        body = _request(
            "POST",
            "/v2/list-agents",
            json_body={
                "filter_criteria": {
                    "channel": {"type": "string", "op": "eq", "value": "voice"},
                }
            },
            params=params,
        )
        page: list[Any]
        if isinstance(body, list):
            page = body
            has_more = False
            pagination_key = None
        elif isinstance(body, dict):
            raw = body.get("items")
            page = raw if isinstance(raw, list) else []
            has_more = bool(body.get("has_more"))
            next_key = body.get("pagination_key")
            pagination_key = str(next_key) if next_key else None
        else:
            page = []
            has_more = False
            pagination_key = None

        for row in page:
            if not isinstance(row, dict):
                continue
            agent_id = str(row.get("agent_id") or "").strip()
            if not agent_id:
                continue
            items.append(
                {
                    "agent_id": agent_id,
                    "agent_name": str(row.get("agent_name") or "").strip() or agent_id,
                    "channel": str(row.get("channel") or "voice"),
                }
            )
        if not has_more or not pagination_key:
            break
    return items


def _engine(agent: dict[str, Any]) -> dict[str, Any]:
    engine = agent.get("response_engine")
    return engine if isinstance(engine, dict) else {}


def get_agent_prompt(agent_id: str) -> dict[str, Any]:
    agent_id = (agent_id or "").strip()
    if not agent_id:
        raise RetellAgentsError(400, "agent_id is required")

    agent = _request("GET", f"/get-agent/{agent_id}")
    if not isinstance(agent, dict):
        raise RetellAgentsError(502, "Unexpected Retell get-agent response.")

    engine = _engine(agent)
    engine_type = str(engine.get("type") or "").strip()
    llm_id = str(engine.get("llm_id") or "").strip()
    llm_version = engine.get("version")

    general_prompt: Optional[str] = None
    begin_message: Optional[str] = None
    llm_is_published: Optional[bool] = None

    if engine_type == RETELL_LLM_TYPE and llm_id:
        llm = _request("GET", f"/get-retell-llm/{llm_id}")
        if isinstance(llm, dict):
            gp = llm.get("general_prompt")
            bm = llm.get("begin_message")
            general_prompt = gp if isinstance(gp, str) else None
            begin_message = bm if isinstance(bm, str) else None
            if "is_published" in llm:
                llm_is_published = bool(llm.get("is_published"))

    return {
        "agent_id": str(agent.get("agent_id") or agent_id),
        "agent_name": str(agent.get("agent_name") or "").strip() or agent_id,
        "response_engine_type": engine_type or None,
        "llm_id": llm_id or None,
        "llm_version": llm_version,
        "is_published": bool(agent.get("is_published")) if "is_published" in agent else None,
        "llm_is_published": llm_is_published,
        "prompt_editable": engine_type == RETELL_LLM_TYPE and bool(llm_id),
        "general_prompt": general_prompt,
        "begin_message": begin_message,
    }


def update_agent_prompt(
    agent_id: str,
    general_prompt: Optional[str] = None,
    begin_message: Optional[str] = None,
) -> dict[str, Any]:
    if general_prompt is None and begin_message is None:
        raise RetellAgentsError(400, "Provide general_prompt and/or begin_message.")

    current = get_agent_prompt(agent_id)
    if not current.get("prompt_editable"):
        engine_type = current.get("response_engine_type") or "unknown"
        raise RetellAgentsError(
            400,
            f"This Retell agent uses response engine {engine_type!r}, which is not a "
            "Retell LLM. Prompt text cannot be edited here.",
        )
    llm_id = str(current.get("llm_id") or "").strip()
    patch: dict[str, Any] = {}
    if general_prompt is not None:
        patch["general_prompt"] = general_prompt
    if begin_message is not None:
        patch["begin_message"] = begin_message
    _request("PATCH", f"/update-retell-llm/{llm_id}", json_body=patch)
    return get_agent_prompt(agent_id)
