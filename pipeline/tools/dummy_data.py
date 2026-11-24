"""Realistic dummy data for tens-of-thousands-event tests (standard library only).

Every session follows the game's real rules (simulator.Session): problem_started -> attempts (each ends with
exactly one attempt_failed or attempt_succeeded) -> problem_completed, sequence 1..n without gaps.
One session is always sent by one thread in order, so the remote writer never sees artificial sequence gaps.

  python tools/dummy_data.py --events 30000 --rate 200 --players 1200 --write-context .env.remote-context.json
  python tools/dummy_data.py --events 5000 --dry-run --out runs/dummy.jsonl     # only generate

--write-context registers the dummy players (dummy-0001...) as verified members so the remote writer
creates them instead of waiting for a member mapping. Use it only against a local/test database.
Environment: INGEST_KEY (or --env-file .env), COLLECTOR_URL (default http://127.0.0.1:8000).
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import random
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from simulator import GAMES, Session  # noqa: E402


def uuid7() -> str:
    """RFC 9562 UUIDv7: 48-bit Unix ms + random, as game/scripts/network_manager.gd generate_uuid_v7()."""
    ms = int(time.time() * 1000)
    b = bytearray(ms.to_bytes(6, "big") + os.urandom(10))
    b[6] = (b[6] & 0x0F) | 0x70
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def player_name(i: int) -> str:
    return f"dummy-{i:04d}"


def generate(events: int, players: int, seed: int, fail_bias: float, run_id: str):
    """Yield complete sessions (lists of events) until at least `events` events were produced."""
    rng = random.Random(seed)
    games = list(GAMES)
    total, n = 0, 0
    while total < events:
        s = Session(run_id, player_name(rng.randint(1, players)), rng.choice(games), n, rng)
        # one problem run per session: the dashboard keys problem_runs by run_id, and the game starts a new
        # run for every problem (simulator.Session reuses one run per player, which the projection rejects)
        s.base["run_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{s.base['session_id']}/run"))
        session = list(s.script(fail_bias=fail_bias))
        for e in session:  # same id shape as the game (UUIDv7, time-ordered)
            e["event_id"] = uuid7()
        n += 1
        total += len(session)
        yield session


def write_context(path: Path, players: int) -> None:
    data = {"members": {}}
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8") or "{}") or {"members": {}}
    members = data.setdefault("members", {})
    for i in range(1, players + 1):
        name = player_name(i)
        members[name] = {"external_member_key": "game:member:" + name, "verified": True,
                         "create_if_missing": True, "nickname": f"더미{i:04d}"}
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def read_key(env_file: Path | None) -> str:
    if os.getenv("INGEST_KEY"):
        return os.environ["INGEST_KEY"]
    if env_file and env_file.exists():
        for line in env_file.read_text(encoding="utf-8-sig").splitlines():
            if line.startswith("INGEST_KEY="):
                return line.split("=", 1)[1].strip()
    sys.exit("INGEST_KEY is not set (use --env-file .env)")


def send(sessions, url: str, key: str, rate: float, threads: int) -> dict:
    q: queue.Queue = queue.Queue(maxsize=threads * 4)
    lock = threading.Lock()
    stats = {"ok": 0, "rejected": 0, "failed": 0}
    latencies: list[float] = []
    interval = 1.0 / rate if rate > 0 else 0.0
    next_slot = [time.monotonic()]

    def pace():
        if not interval:
            return
        with lock:
            slot = max(next_slot[0], time.monotonic())
            next_slot[0] = slot + interval
        delay = slot - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def post(event) -> str:
        body = json.dumps(event).encode()
        for attempt in range(3):  # same event_id + same body on retry, as the game does
            req = urllib.request.Request(url, body, {"Content-Type": "application/json", "X-API-Key": key})
            try:
                with urllib.request.urlopen(req, timeout=20) as r:
                    return "ok" if r.status == 202 else "failed"
            except urllib.error.HTTPError as e:
                if e.code == 422:
                    return "rejected"
                if e.code != 503:
                    return "failed"
            except OSError:
                pass
            time.sleep(1 + attempt)
        return "failed"

    def worker():
        while True:
            session = q.get()
            if session is None:
                return
            for event in session:  # in order: the next event waits for the previous answer
                pace()
                t0 = time.perf_counter()
                outcome = post(event)
                with lock:
                    stats[outcome] += 1
                    latencies.append(time.perf_counter() - t0)

    pool = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for t in pool:
        t.start()
    start = time.monotonic()
    for session in sessions:
        q.put(session)
    for _ in pool:
        q.put(None)
    for t in pool:
        t.join()
    elapsed = max(time.monotonic() - start, 1e-6)
    lat = sorted(latencies) or [0.0]
    sent = sum(stats.values())
    return {**stats, "sent": sent, "seconds": round(elapsed, 1), "events_per_s": round(sent / elapsed, 1),
            "p50_ms": round(lat[len(lat) // 2] * 1000), "p95_ms": round(lat[int(len(lat) * 0.95)] * 1000),
            "max_ms": round(lat[-1] * 1000)}


def main(argv=None) -> dict:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", type=int, default=30000)
    p.add_argument("--players", type=int, default=1200)
    p.add_argument("--rate", type=float, default=200, help="events per second (0 = as fast as possible)")
    p.add_argument("--threads", type=int, default=32)
    p.add_argument("--seed", type=int, default=20261120)
    p.add_argument("--fail-bias", type=float, default=0.3)
    p.add_argument("--run-id", default=None)
    p.add_argument("--url", default=os.getenv("COLLECTOR_URL", "http://127.0.0.1:8000") + "/v2/events")
    p.add_argument("--env-file", type=Path, default=Path(".env"))
    p.add_argument("--write-context", type=Path, help="register dummy players as verified members in this file")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--out", type=Path, help="also save the generated events as JSON lines")
    p.add_argument("--reverse-sessions", type=int, default=0,
                   help="send this many sessions in reverse sequence order (out-of-order arrival test)")
    p.add_argument("--sessions-out", type=Path, help="write {reversed: [...], normal: [...]} session ids here")
    a = p.parse_args(argv)
    run_id = a.run_id or f"dummy-{int(time.time())}"
    sessions = list(generate(a.events, a.players, a.seed, a.fail_bias, run_id))
    reversed_ids = []
    if a.reverse_sessions:
        picked = random.Random(a.seed + 1).sample(range(len(sessions)), min(a.reverse_sessions, len(sessions)))
        for i in picked:
            sessions[i] = list(reversed(sessions[i]))
            reversed_ids.append(sessions[i][0]["session_id"])
    if a.sessions_out:
        a.sessions_out.write_text(json.dumps({"reversed": reversed_ids, "normal": [
            s[0]["session_id"] for s in sessions if s[0]["session_id"] not in set(reversed_ids)]}), encoding="utf-8")
    count = sum(len(s) for s in sessions)
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        with a.out.open("w", encoding="utf-8") as f:
            for s in sessions:
                for e in s:
                    f.write(json.dumps(e) + "\n")
    if a.write_context:
        write_context(a.write_context, a.players)
    summary = {"run_id": run_id, "sessions": len(sessions), "events": count}
    if not a.dry_run:
        summary.update(send(sessions, a.url, read_key(a.env_file), a.rate, a.threads))
    print(json.dumps(summary, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    main()
