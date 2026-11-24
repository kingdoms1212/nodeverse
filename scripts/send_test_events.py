"""Send one sample learning run (v2) to the collector, for an end-to-end check.

Default is a dry run: builds the events, validates them against contracts/contract.py and prints them.
Nothing is sent unless --send is given.

    python scripts/send_test_events.py                       # dry run
    python scripts/send_test_events.py --send                # POST to http://192.168.0.81:8000/v2/events
    python scripts/send_test_events.py --send --url http://localhost:8000/v2/events

The X-API-Key is read from INGEST_KEY (env var, else pipeline/.env). It is never printed.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_URL = "http://192.168.0.81:8000/v2/events"
PLAYER_ID = "test-player-e2e"
MINIGAME_ID, CATEGORY = "ST01", "DS"
BUILD_VERSION, CONTENT_VERSION = "0.1.0", "base"


GAME_CONFIG = Path(os.environ.get("APPDATA", "")) / "Godot" / "app_userdata" / "NODEVERSE 3D Assets" / "network_config.json"


def load_key() -> str:
    """Same key the game sends: INGEST_KEY env var, else the game's network_config.json, else pipeline/.env."""
    key = os.environ.get("INGEST_KEY", "")
    if not key and GAME_CONFIG.exists():
        cfg = json.loads(GAME_CONFIG.read_text(encoding="utf-8-sig"))
        key = str(cfg.get("auth_token", "")).strip()
        if key.startswith("replace"):
            key = ""
        if key:
            print("key source: game network_config.json")
    env = ROOT / "pipeline" / ".env"
    if not key and env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("INGEST_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not key:
        sys.exit("INGEST_KEY not found (env var or pipeline/.env)")
    return key


def ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def build_events() -> list[dict]:
    session_id, run_id, attempt_id = (str(uuid.uuid4()) for _ in range(3))
    t0 = datetime.now(timezone.utc) - timedelta(seconds=30)
    steps = [  # (event_type, has_attempt, details)
        ("problem_started", False, {"level": 1}),
        ("attempt_started", True, {"level": 1}),
        ("operation_failed", True, {"operation": "insert_node", "failure_reason": "wrong_position",
                                    "state_before": "A->B->C", "state_after": "A->B->C"}),
        ("hint_used", True, {"hint_id": "ST01_H1"}),
        ("operation_succeeded", True, {"operation": "insert_node",
                                       "state_before": "A->B->C", "state_after": "A->X->B->C"}),
        ("attempt_succeeded", True, {"moves": 2, "mistakes": 1}),
        ("problem_completed", False, {"level": 1, "moves": 2, "mistakes": 1, "stars": 2}),
    ]
    events = []
    for i, (etype, has_attempt, details) in enumerate(steps, start=1):
        events.append({
            "schema_version": 2, "event_id": str(uuid.uuid4()), "player_id": PLAYER_ID,
            "session_id": session_id, "run_id": run_id, "attempt_id": attempt_id if has_attempt else None,
            "minigame_id": MINIGAME_ID, "category": CATEGORY,
            "build_version": BUILD_VERSION, "content_version": CONTENT_VERSION,
            "event_type": etype, "occurred_at": ts(t0 + timedelta(seconds=4 * i)),
            "sequence": i, "details": details,
        })
    return events


def validate(events: list[dict]) -> None:
    sys.path.insert(0, str(ROOT / "contracts"))
    try:
        from contract import LearningEventV2
    except ImportError as exc:
        print(f"(local validation skipped: {exc})")
        return
    for e in events:
        LearningEventV2.model_validate(e)
    print(f"local contract validation OK ({len(events)} events)")


def send(events: list[dict], url: str, key: str) -> None:
    for e in events:
        req = urllib.request.Request(url, data=json.dumps(e).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "X-API-Key": key})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                status, body = r.status, r.read().decode()[:300]
        except urllib.error.HTTPError as err:
            status, body = err.code, err.read().decode()[:300]
        print(f"{e['sequence']} {e['event_type']:<20} {status} {body}")
        time.sleep(0.2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--send", action="store_true", help="actually POST (default: dry run)")
    ap.add_argument("--url", default=DEFAULT_URL)
    args = ap.parse_args()
    events = build_events()
    validate(events)
    print(f"run_id={events[0]['run_id']}  player_id={PLAYER_ID}")
    if not args.send:
        for e in events:
            print(json.dumps(e, ensure_ascii=False))
        print("dry run: nothing sent (use --send)")
        return
    send(events, args.url, load_key())


if __name__ == "__main__":
    main()
