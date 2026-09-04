"""Retell AI agent/LLM proxy for autonomous sequence voice prompts.

The CRM dashboard lists Retell agents and edits `general_prompt` / `begin_message`
on the linked Retell LLM. Retell remains the source of truth — prompts are not
copied into Postgres.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

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
        for key in ("message", "detail", "error", "error_message", "msg"):
            val = body.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        # Retell sometimes nests the reason, or returns a validation list. Without
        # this the caller got a bare "Retell returned 400." with no way to tell
        # what it objected to — which cost a full afternoon on 3 Sep.
        for key in ("error", "detail", "errors"):
            val = body.get(key)
            if isinstance(val, (dict, list)) and val:
                return f"{fallback} Retell said: {json.dumps(val)[:400]}"
        if body:
            return f"{fallback} Retell body: {json.dumps(body)[:400]}"
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

    if r.status_code >= 400:
        # Retell says exactly which field it objected to, and until now that
        # sentence only ever reached the browser as a 281-byte 502 body. The log
        # showed "400 Bad Request" and nothing else, which is how the 3 Sep
        # voicemail_action fault cost an afternoon. Log the body, and the keys we
        # sent, every time.
        logger.error(
            "Retell %s %s -> %s. Sent keys: %s. Retell said: %s",
            method,
            path,
            r.status_code,
            sorted((json_body or {}).keys()),
            (r.text or "")[:600],
        )

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


def get_agent_prompt(agent_id: str, version: Optional[int] = None) -> dict[str, Any]:
    """Read an agent, optionally at a specific version.

    Retell versions the agent AND the LLM behind it, and every request its own
    dashboard makes carries ?version=N. Reading and writing without a version
    lands on the published version, which is frozen — that is the whole reason
    "Cannot update published LLM" happened.
    """
    agent_id = (agent_id or "").strip()
    if not agent_id:
        raise RetellAgentsError(400, "agent_id is required")

    agent = _request(
        "GET",
        f"/get-agent/{agent_id}",
        params={"version": int(version)} if version is not None else None,
    )
    if not isinstance(agent, dict):
        raise RetellAgentsError(502, "Unexpected Retell get-agent response.")

    engine = _engine(agent)
    engine_type = str(engine.get("type") or "").strip()
    llm_id = str(engine.get("llm_id") or "").strip()
    llm_version = engine.get("version")

    general_prompt: Optional[str] = None
    begin_message: Optional[str] = None
    llm_model: Optional[str] = None
    llm_is_published: Optional[bool] = None

    if engine_type == RETELL_LLM_TYPE and llm_id:
        llm = _request(
            "GET",
            f"/get-retell-llm/{llm_id}",
            params={"version": int(llm_version)} if llm_version is not None else None,
        )
        if isinstance(llm, dict):
            gp = llm.get("general_prompt")
            bm = llm.get("begin_message")
            general_prompt = gp if isinstance(gp, str) else None
            begin_message = bm if isinstance(bm, str) else None
            model = llm.get("model")
            llm_model = str(model).strip() if model else None
            if "is_published" in llm:
                llm_is_published = bool(llm.get("is_published"))

    # Retell stores this as {"type": "prompt"|"static_sentence"|"hang_up", "text": ...}.
    # This used to read voicemail["action"], which does not exist, so it always came
    # back None and the dashboard rendered its default ("Hang up") for agents that
    # were in fact leaving a message. Read the real key, fall back to "action" only
    # for whatever older shape that line was written for.
    voicemail = agent.get("voicemail_option")
    voicemail_action: Optional[str] = None
    if isinstance(voicemail, dict):
        vtype = str(voicemail.get("type") or voicemail.get("action") or "").strip().lower()
        if vtype in ("hang_up", "hangup", "hang up"):
            voicemail_action = "hang up"
        elif vtype:
            voicemail_action = "leave a message"
    elif isinstance(voicemail, str) and voicemail.strip():
        voicemail_action = voicemail.strip()

    return {
        "agent_id": str(agent.get("agent_id") or agent_id),
        "agent_name": str(agent.get("agent_name") or "").strip() or agent_id,
        "response_engine_type": engine_type or None,
        "llm_id": llm_id or None,
        "llm_version": llm_version,
        # The agent's OWN version, which is what /publish-agent-version needs.
        # This was missing, so nothing could publish even if it wanted to.
        "version": agent.get("version"),
        "is_published": bool(agent.get("is_published")) if "is_published" in agent else None,
        "llm_is_published": llm_is_published,
        "prompt_editable": engine_type == RETELL_LLM_TYPE and bool(llm_id),
        "general_prompt": general_prompt,
        "begin_message": begin_message,
        "voice_id": str(agent.get("voice_id") or "").strip() or None,
        "language": str(agent.get("language") or "").strip() or None,
        "voice_speed": agent.get("voice_speed"),
        "voice_temperature": agent.get("voice_temperature"),
        "responsiveness": agent.get("responsiveness"),
        "interruption_sensitivity": agent.get("interruption_sensitivity"),
        "enable_backchannel": agent.get("enable_backchannel"),
        "max_call_duration_ms": agent.get("max_call_duration_ms"),
        "end_call_after_silence_ms": agent.get("end_call_after_silence_ms"),
        "voicemail_action": voicemail_action,
        "llm_model": llm_model,
    }


def list_voices() -> list[dict[str, Any]]:
    body = _request("GET", "/list-voices")
    rows = body if isinstance(body, list) else (body.get("voices") if isinstance(body, dict) else [])
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        voice_id = str(row.get("voice_id") or "").strip()
        if not voice_id:
            continue
        out.append(
            {
                "voice_id": voice_id,
                "voice_name": str(row.get("voice_name") or row.get("voice_id") or voice_id).strip(),
                "gender": str(row.get("gender") or "").strip() or None,
                "accent": str(row.get("accent") or "").strip() or None,
                "provider": str(row.get("provider") or "").strip() or None,
            }
        )
    return out


def _editable_draft(agent_id: str) -> dict[str, Any]:
    """The agent as a DRAFT we are allowed to write to.

    Publishing freezes a version. Retell's own UI never trips over this because
    on publish it ticks "Auto Create a New Draft" — the 201 you see in its
    network log is POST /create-agent-version, which forks the published version
    into an editable one and moves both the agent and its LLM to version N+1.

    Doing it here, at the START of every save rather than after publishing, makes
    it self-healing: if a draft already exists we use it, and if the last publish
    left none we make one now. A failed draft creation costs one save, not every
    future save.
    """
    current = get_agent_prompt(agent_id)
    if not current.get("is_published"):
        return current

    base = current.get("version")
    if base is None:
        logger.warning(
            "Agent %s reports published with no version — writing without a draft, "
            "which Retell may refuse.",
            agent_id,
        )
        return current

    created = _request(
        "POST",
        f"/create-agent-version/{agent_id}",
        json_body={"base_version": int(base)},
    )
    new_version = created.get("version") if isinstance(created, dict) else None
    if new_version is None:
        logger.error(
            "Created a draft for agent %s from v%s but Retell returned no version; "
            "falling back to the unversioned read.",
            agent_id,
            base,
        )
        return current
    logger.info("Agent %s v%s is published; created draft v%s", agent_id, base, new_version)
    return get_agent_prompt(agent_id, int(new_version))


def update_agent_prompt(
    agent_id: str,
    general_prompt: Optional[str] = None,
    begin_message: Optional[str] = None,
    voice_id: Optional[str] = None,
    language: Optional[str] = None,
    voice_speed: Optional[float] = None,
    voice_temperature: Optional[float] = None,
    responsiveness: Optional[float] = None,
    interruption_sensitivity: Optional[float] = None,
    enable_backchannel: Optional[bool] = None,
    max_call_duration_ms: Optional[int] = None,
    end_call_after_silence_ms: Optional[int] = None,
    voicemail_action: Optional[str] = None,
    llm_model: Optional[str] = None,
) -> dict[str, Any]:
    current = _editable_draft(agent_id)
    agent_version = current.get("version")
    llm_version = current.get("llm_version")
    llm_id = str(current.get("llm_id") or "").strip()
    llm_patch: dict[str, Any] = {}
    if general_prompt is not None:
        llm_patch["general_prompt"] = general_prompt
    if begin_message is not None:
        llm_patch["begin_message"] = begin_message
    if llm_model is not None:
        llm_patch["model"] = llm_model
    if llm_patch:
        if not current.get("prompt_editable") or not llm_id:
            engine_type = current.get("response_engine_type") or "unknown"
            raise RetellAgentsError(
                400,
                f"This Retell agent uses response engine {engine_type!r}, which is not a "
                "Retell LLM. Prompt text cannot be edited here.",
            )
        _request(
            "PATCH",
            f"/update-retell-llm/{llm_id}",
            json_body=llm_patch,
            params={"version": int(llm_version)} if llm_version is not None else None,
        )

    agent_patch: dict[str, Any] = {}
    if voice_id is not None:
        agent_patch["voice_id"] = voice_id
    if language is not None:
        agent_patch["language"] = language
    if voice_speed is not None:
        agent_patch["voice_speed"] = voice_speed
    if voice_temperature is not None:
        agent_patch["voice_temperature"] = voice_temperature
    if responsiveness is not None:
        agent_patch["responsiveness"] = responsiveness
    if interruption_sensitivity is not None:
        agent_patch["interruption_sensitivity"] = interruption_sensitivity
    if enable_backchannel is not None:
        agent_patch["enable_backchannel"] = enable_backchannel
    if max_call_duration_ms is not None:
        agent_patch["max_call_duration_ms"] = max_call_duration_ms
    if end_call_after_silence_ms is not None:
        agent_patch["end_call_after_silence_ms"] = end_call_after_silence_ms
    # Deliberately NOT written. This sent {"action": "<string>"} and Retell answers
    # 400, which surfaced to the dashboard as a bare 502 Bad Gateway and blocked
    # EVERY save on this screen, prompt edits included. Retell wants
    # {"type": ..., "text": ...}; the dashboard only has a single free-text field,
    # so it cannot express that yet. Until it can, the voicemail setting is edited
    # in Retell directly and a save here leaves it untouched rather than breaking.
    if voicemail_action is not None:
        logger.warning(
            "Ignoring voicemail_action=%r for agent %s: Retell expects "
            "{type, text} and this endpoint cannot build it yet. Edit voicemail "
            "in Retell directly. Everything else in this request still saved.",
            voicemail_action,
            agent_id,
        )

    if agent_patch:
        _request(
            "PATCH",
            f"/update-agent/{agent_id}",
            json_body=agent_patch,
            params={"version": int(agent_version)} if agent_version is not None else None,
        )

    if not llm_patch and not agent_patch:
        raise RetellAgentsError(400, "No Retell fields to update.")

    # Safe to publish now, which it was not this morning: _editable_draft() forks a
    # new draft on the next save, so freezing this version no longer blocks editing.
    if agent_version is not None:
        publish_agent(agent_id, int(agent_version))
    return get_agent_prompt(agent_id)


def publish_agent(agent_id: str, version: Optional[int] = None) -> bool:
    """Make the agent's current draft the live version.

    Retell keeps every write as a DRAFT. A PATCH to /update-agent or
    /update-retell-llm changes nothing a caller will ever hear until the draft
    version is published, and until 4 Sep 2026 nothing in any of our repos did
    that. The consequence was a fortnight of fixes that looked applied, were
    verified against the draft the API returns, and were never served: John
    spent a morning testing last week's agent because of it.

    Failure here is deliberately NOT fatal. The save has already happened by the
    time this runs, so raising would tell the dashboard the edit failed when it
    did not. Instead it logs loudly and reports is_published on the way out, so
    the caller can see the difference between "saved and live" and "saved only".
    """
    try:
        if version is None:
            version = get_agent_prompt(agent_id).get("version")
        if version is None:
            logger.error(
                "Cannot publish agent %s: Retell returned no version. The change is "
                "SAVED AS A DRAFT and will not be heard on any call until it is "
                "published in Retell.",
                agent_id,
            )
            return False
        _request(
            "POST",
            f"/publish-agent-version/{agent_id}",
            json_body={"version": int(version)},
        )
        logger.info("Published agent %s version %s", agent_id, version)
        return True
    except Exception as e:  # noqa: BLE001
        # EVERYTHING here is best-effort. The save has already gone through by the
        # time this runs, so any exception escaping would tell the dashboard the
        # edit failed when it did not — which is exactly what happened on the
        # first deploy of this function: get_agent_prompt sat outside the guard,
        # so one bad response from Retell turned a successful save into an error
        # on screen. Log it, report the failure as False, never raise.
        logger.error(
            "Agent %s saved but NOT published: %s. It will not be heard on any "
            "call until it is published in Retell.",
            agent_id,
            e,
        )
        return False


_LLM_OMIT = {
    "llm_id",
    "version",
    "is_published",
    "last_modification_timestamp",
    "is_transfer_llm",
}
_AGENT_OMIT = {
    "agent_id",
    "version",
    "base_version",
    "assigned_tags",
    "is_published",
    "last_modification_timestamp",
}


def _without_none(payload: dict[str, Any], omit: set[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in payload.items():
        if k in omit or v is None:
            continue
        out[k] = v
    return out


def duplicate_agent(source_agent_id: str, new_name: str) -> dict[str, Any]:
    """Clone a Retell LLM + voice agent so a new sequence has its own prompt."""
    source_id = (source_agent_id or "").strip()
    name = (new_name or "").strip()
    if not source_id:
        raise RetellAgentsError(400, "source_agent_id is required")
    if not name:
        raise RetellAgentsError(400, "new agent name is required")

    agent = _request("GET", f"/get-agent/{source_id}")
    if not isinstance(agent, dict):
        raise RetellAgentsError(502, "Unexpected Retell get-agent response.")
    engine = _engine(agent)
    engine_type = str(engine.get("type") or "").strip()
    llm_id = str(engine.get("llm_id") or "").strip()
    if engine_type != RETELL_LLM_TYPE or not llm_id:
        raise RetellAgentsError(
            400,
            f"Source agent {source_id} is not a Retell LLM agent, so it cannot be duplicated here.",
        )

    llm = _request("GET", f"/get-retell-llm/{llm_id}")
    if not isinstance(llm, dict):
        raise RetellAgentsError(502, "Unexpected Retell get-retell-llm response.")

    llm_payload = _without_none(llm, _LLM_OMIT)
    new_llm = _request("POST", "/create-retell-llm", json_body=llm_payload or {"general_prompt": llm.get("general_prompt") or ""})
    if not isinstance(new_llm, dict) or not new_llm.get("llm_id"):
        raise RetellAgentsError(502, "Retell created an LLM without an llm_id.")
    new_llm_id = str(new_llm["llm_id"])

    agent_payload = _without_none(agent, _AGENT_OMIT)
    agent_payload["agent_name"] = name
    agent_payload["response_engine"] = {"type": RETELL_LLM_TYPE, "llm_id": new_llm_id}
    if not agent_payload.get("voice_id"):
        raise RetellAgentsError(400, f"Source agent {source_id} has no voice_id; cannot duplicate.")

    try:
        new_agent = _request("POST", "/create-agent", json_body=agent_payload)
    except RetellAgentsError:
        new_agent = _request(
            "POST",
            "/create-agent",
            json_body={
                "agent_name": name,
                "voice_id": agent_payload["voice_id"],
                "response_engine": {"type": RETELL_LLM_TYPE, "llm_id": new_llm_id},
            },
        )
    if not isinstance(new_agent, dict) or not new_agent.get("agent_id"):
        raise RetellAgentsError(502, "Retell created an agent without an agent_id.")

    return {
        "agent_id": str(new_agent["agent_id"]),
        "agent_name": str(new_agent.get("agent_name") or name),
        "llm_id": new_llm_id,
        "source_agent_id": source_id,
    }


def _delete_ok(path: str) -> None:
    """DELETE that treats 204 / 404 / 422 as success (already gone)."""
    url = f"{RETELL_BASE}{path}"
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            r = client.request("DELETE", url, headers=_headers())
    except httpx.RequestError as e:
        raise RetellAgentsError(502, f"Retell request failed: {e}") from e
    if r.status_code in (204, 404, 422):
        return
    body: Any
    try:
        body = r.json()
    except Exception:
        body = r.text
    if r.status_code == 401:
        raise RetellAgentsError(502, "Retell rejected the API key (401). Check RETELL_API_KEY.")
    if r.status_code >= 400:
        raise RetellAgentsError(502, _message_from_body(body, f"Retell returned {r.status_code}."))


def delete_agent(agent_id: str) -> dict[str, Any]:
    """Delete a Retell voice agent and its Retell LLM, if the LLM is dedicated."""
    source_id = (agent_id or "").strip()
    if not source_id:
        raise RetellAgentsError(400, "agent_id is required")

    llm_id = ""
    agent_name = source_id
    try:
        agent = _request("GET", f"/get-agent/{source_id}")
        if isinstance(agent, dict):
            agent_name = str(agent.get("agent_name") or "").strip() or source_id
            llm_id = str(_engine(agent).get("llm_id") or "").strip()
    except RetellAgentsError as e:
        if e.status_code != 404:
            raise

    _delete_ok(f"/delete-agent/{source_id}")
    llm_deleted = False
    if llm_id:
        _delete_ok(f"/delete-retell-llm/{llm_id}")
        llm_deleted = True
    return {
        "agent_id": source_id,
        "agent_name": agent_name,
        "llm_id": llm_id or None,
        "llm_deleted": llm_deleted,
    }
