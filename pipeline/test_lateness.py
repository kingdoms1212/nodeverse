"""common/lateness.py: one rule for late events, clamped play time, UUIDv7 creation day (7th round)."""
import uuid
from datetime import datetime, timedelta, timezone

from common.lateness import LATE_SECONDS, is_late, play_time, v7_day

NOW = datetime(2026, 11, 21, 8, 0, tzinfo=timezone.utc)


def uuid7_at(ts: datetime) -> str:
    ms = int(ts.timestamp() * 1000)
    b = bytearray(ms.to_bytes(6, "big") + bytes(10))
    b[6] = (b[6] & 0x0F) | 0x70
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def test_late_threshold_matches_silver_fallback():
    assert not is_late(NOW - timedelta(seconds=LATE_SECONDS), NOW)
    assert is_late(NOW - timedelta(seconds=LATE_SECONDS + 1), NOW)
    assert is_late(NOW + timedelta(minutes=10), NOW)  # device clock ahead counts too


def test_play_time_is_clamped_to_receive_window():
    assert play_time(NOW - timedelta(days=3), NOW) == NOW - timedelta(days=3)
    assert play_time(NOW - timedelta(days=30), NOW) == NOW - timedelta(days=6)
    assert play_time(NOW + timedelta(days=2), NOW) == NOW


def test_v7_day_from_id_and_other_ids():
    assert v7_day(uuid7_at(NOW - timedelta(days=3))) == "2026-11-18"
    assert v7_day(str(uuid.uuid4())) is None
    assert v7_day("not-a-uuid") is None
