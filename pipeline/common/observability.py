"""Small driver-side timing registry; no Spark actions and no event payloads."""
from datetime import datetime, timezone
from threading import Lock

_lock = Lock()
_stages = {}


def record_stage(name, **metrics):
    with _lock:
        _stages[name] = dict(metrics, completed_at=datetime.now(timezone.utc).isoformat())


def stages():
    with _lock:
        return {name: dict(value) for name, value in _stages.items()}
