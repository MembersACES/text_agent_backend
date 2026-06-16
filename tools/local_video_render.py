"""Resolve locally rendered MP4 paths under claude-videos (dev preview before Drive upload)."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional, Tuple


def _default_claude_videos_root() -> str:
    sibling = Path(__file__).resolve().parent.parent.parent / "claude-videos"
    return str(sibling) if sibling.is_dir() else ""


def get_claude_videos_root() -> str:
    return (os.getenv("CLAUDE_VIDEOS_ROOT") or _default_claude_videos_root()).strip()


def find_local_mp4(
    slug: str,
    variant: str = "long",
    kind: str = "testimonial",
) -> Tuple[Optional[Path], Optional[str]]:
    """
    Return (path, error) for a rendered MP4 on disk.
    variant: long | 30s
    """
    root = get_claude_videos_root()
    if not root:
        return None, "CLAUDE_VIDEOS_ROOT is not configured"
    slug_val = slug.strip().lower()
    variant_val = "30s" if variant.strip().lower() in ("30s", "short") else "long"
    cut = "30s" if variant_val == "30s" else "long"

    if kind.strip().lower() == "marketing":
        base = Path(root) / "remotion" / "rendered-videos" / "Overall Marketing"
        candidates = [
            base / f"{slug_val}-{cut}.mp4",
            base / f"testimonial-{slug_val}-{cut}.mp4",
        ]
    else:
        comp = slug_val if slug_val.startswith("testimonial-") else f"testimonial-{slug_val}"
        base = Path(root) / "remotion" / "rendered-videos" / "testimonials"
        candidates = []
        if base.is_dir():
            for folder in base.iterdir():
                if not folder.is_dir():
                    continue
                candidates.append(folder / f"{comp}-{cut}.mp4")
        candidates.insert(0, base / slug_val / f"{comp}-{cut}.mp4")

    for path in candidates:
        if path.is_file() and path.stat().st_size > 0:
            return path, None
    return None, f"No local MP4 for slug={slug_val} variant={variant_val}"
