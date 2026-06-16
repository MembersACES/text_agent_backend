"""Drive filename conventions for CZA marketing / testimonial videos."""

from __future__ import annotations

import re
from typing import Literal, Optional, Tuple

VideoKind = Literal["marketing", "testimonial"]
VideoVariant = Literal["long", "30s"]

_FILENAME_RE = re.compile(
    r"^(?:(testimonial)-)?([a-z0-9-]+)-(long|30s)\.mp4$",
    re.IGNORECASE,
)


def build_drive_filename(slug: str, variant: VideoVariant, kind: VideoKind = "marketing") -> str:
    slug = slug.strip().lower()
    if kind == "testimonial":
        return f"testimonial-{slug}-{variant}.mp4"
    return f"{slug}-{variant}.mp4"


def parse_drive_filename(name: str) -> Optional[Tuple[VideoKind, str, VideoVariant]]:
    base = name.strip()
    if not base.lower().endswith(".mp4"):
        return None
    m = _FILENAME_RE.match(base)
    if not m:
        return None
    prefix, slug, variant = m.group(1), m.group(2), m.group(3).lower()
    kind: VideoKind = "testimonial" if prefix else "marketing"
    var: VideoVariant = "30s" if variant == "30s" else "long"
    return kind, slug, var


def validate_drive_filename(name: str) -> bool:
    return parse_drive_filename(name) is not None
