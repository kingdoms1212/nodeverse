"""One definition of a late event for every stage (7th round, 2026-11-21).

An event is late when its play time (occurred_at, device clock) and its receive time (received_at, collector
clock) are more than LATE_SECONDS apart. Silver already uses the same rule to file such events under the receive
time (event_time); live metrics count them but show them separately, failure-spike alerts leave them out, and
the play-date daily table counts them on the day they were played.

Play time comes from the device clock, so it is clamped to [received_at - MAX_AGE_DAYS, received_at]: the collector
refuses events older than 6 days, and a clock in the future must not move an event forward.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import uuid

LATE_SECONDS = 300
MAX_AGE_DAYS = 6


def is_late(occurred: datetime, received: datetime) -> bool:
    return abs((received - occurred).total_seconds()) > LATE_SECONDS


def play_time(occurred: datetime, received: datetime) -> datetime:
    return min(max(occurred, received - timedelta(days=MAX_AGE_DAYS)), received)


def v7_day(event_id: str) -> str | None:
    """UTC day an UUIDv7 id was created (its first 48 bits are Unix ms); None for other ids."""
    try:
        u = uuid.UUID(event_id)
    except (ValueError, TypeError, AttributeError):
        return None
    if u.version != 7:
        return None
    ms = int(u.hex[:12], 16)
    return datetime.fromtimestamp(ms / 1000, timezone.utc).date().isoformat()
