"""
Timezone utilities for converting UTC to Australia/Melbourne
"""
from datetime import datetime
import pytz

# Timezone constants
UTC = pytz.UTC
MELBOURNE_TZ = pytz.timezone('Australia/Melbourne')


def to_melbourne_time(utc_datetime: datetime) -> datetime:
    """Convert UTC datetime to Australia/Melbourne timezone"""
    if utc_datetime is None:
        return None
    
    # If datetime is naive, assume it's UTC
    if utc_datetime.tzinfo is None:
        utc_datetime = UTC.localize(utc_datetime)
    
    # Convert to Melbourne timezone
    melbourne_time = utc_datetime.astimezone(MELBOURNE_TZ)
    return melbourne_time


def to_melbourne_iso(utc_datetime: datetime) -> str:
    """Convert UTC datetime to Australia/Melbourne ISO format string"""
    if utc_datetime is None:
        return None
    
    melbourne_time = to_melbourne_time(utc_datetime)
    return melbourne_time.isoformat()

