"""One set of UTC time helpers (RF-7). Event timestamps follow the contract: ISO-8601, milliseconds, 'Z'."""
from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_ms(value: datetime | None = None) -> str:
    """2026-11-20T08:00:00.123Z"""
    value = value or utc_now()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def window_key(value: datetime) -> str:
    """Second-precision key used in rt_* document ids, e.g. ST03:2026-11-20T08:00:00Z. Do not change."""
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")
