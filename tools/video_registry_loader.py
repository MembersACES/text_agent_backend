"""Load compiled video_registry.json for API responses."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_REGISTRY_PATH = Path(__file__).resolve().parent / "video_registry.json"
_cache: Optional[Dict[str, Any]] = None


def load_video_registry(refresh: bool = False) -> Dict[str, Any]:
    global _cache
    if _cache is not None and not refresh:
        return _cache
    if not _REGISTRY_PATH.is_file():
        logger.warning("video_registry.json not found at %s", _REGISTRY_PATH)
        _cache = {"version": "0", "entries": []}
        return _cache
    _cache = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
    return _cache


def lookup_slug(slug: str, kind: Optional[str] = None) -> Optional[Dict[str, Any]]:
    reg = load_video_registry()
    slug = slug.strip().lower()
    for entry in reg.get("entries") or []:
        if entry.get("slug") != slug:
            continue
        if kind and entry.get("kind") != kind:
            continue
        return entry
    return None


def solution_type_for_slug(slug: str) -> Optional[str]:
    entry = lookup_slug(slug)
    if not entry:
        return None
    return entry.get("crm_solution_type_id")
