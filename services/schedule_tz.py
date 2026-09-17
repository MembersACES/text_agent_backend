"""Single configured timezone for campaign send windows, daily caps, and sequence fallbacks."""

from __future__ import annotations

import os
from zoneinfo import ZoneInfo

DEFAULT_SCHEDULE_TZ_NAME = "Australia/Brisbane"


def schedule_tz_name() -> str:
    name = (os.getenv("AUTONOMOUS_SCHEDULE_TZ") or DEFAULT_SCHEDULE_TZ_NAME).strip()
    return name or DEFAULT_SCHEDULE_TZ_NAME


def schedule_tz() -> ZoneInfo:
    return ZoneInfo(schedule_tz_name())
