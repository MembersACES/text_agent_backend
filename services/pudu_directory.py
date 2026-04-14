"""Fetch ACES robot/site directory from n8n (weekly map webhook)."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Union

import httpx

WEEKLY_MAP_URL = os.environ.get(
    "PUDU_WEEKLY_MAP_URL",
    "https://membersaces.app.n8n.cloud/webhook/pudu_weekly_map",
)


def fetch_pudu_weekly_directory() -> List[Dict[str, Any]]:
    """
    Returns a list of row dicts (best-effort). Raises on HTTP / network errors.
    """
    r = httpx.get(WEEKLY_MAP_URL, timeout=45.0)
    r.raise_for_status()
    data: Union[List[Any], Dict[str, Any], None] = r.json()

    if isinstance(data, list):
        out: List[Dict[str, Any]] = []
        for row in data:
            if isinstance(row, dict):
                out.append(row)
            else:
                out.append({"value": row})
        return out

    if isinstance(data, dict):
        for k in ("data", "rows", "items", "robots", "list", "records"):
            inner = data.get(k)
            if isinstance(inner, list):
                return [x for x in inner if isinstance(x, dict)]
        return [data]

    logging.warning("pudu weekly map: unexpected JSON root type %s", type(data))
    return []
