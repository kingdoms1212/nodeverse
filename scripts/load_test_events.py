"""Load test for the collector/gateway: N valid learning v2 events, sent concurrently.

Each run is the same 7-event play as send_test_events.py (contiguous sequence per session), so the events
pass the contract. player_id is "loadtest-<tag>" so the data is easy to find and clean up later.

    $env:INGEST_KEY="<gateway key>"; python scripts/load_test_events.py --count 10000 --workers 32

Prints throughput, latency percentiles and status counts. The key is never printed.
"""
from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import send_test_events as base


def make_events(count: int, tag: str) -> list[dict]:
    events: list[dict] = []
    while len(events) < count:
        run = base.build_events()
        for e in run:
            e["player_id"] = f"loadtest-{tag}"
        events.extend(run)
    return events[:count]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=10000)
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--url", default=base.DEFAULT_URL)
    args = ap.parse_args()
    key = base.load_key()
    tag = time.strftime("%Y%m%d%H%M%S")
    events = make_events(args.count, tag)
    print(f"player_id=loadtest-{tag}  events={len(events)}  workers={args.workers}")

    statuses: Counter = Counter()
    errors: Counter = Counter()
    lat: list[float] = []
    lock = threading.Lock()
    done = [0]

    def post(e: dict) -> None:
        req = urllib.request.Request(args.url, data=json.dumps(e).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "X-API-Key": key})
        t = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                code, body = r.status, ""
                r.read()
        except urllib.error.HTTPError as err:
            code, body = err.code, err.read().decode(errors="replace")[:120]
        except Exception as exc:  # timeouts, resets
            code, body = "conn_error", type(exc).__name__
        dt = time.perf_counter() - t
        with lock:
            statuses[code] += 1
            lat.append(dt)
            if code != 202:
                errors[f"{code} {body}"] += 1
            done[0] += 1
            if done[0] % 1000 == 0:
                print(f"  {done[0]} sent, {time.perf_counter() - t0:.1f}s")

    # sessions must arrive in sequence order: send each 7-event run in order, runs in parallel
    runs = [events[i:i + 7] for i in range(0, len(events), 7)]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(args.workers) as pool:
        list(pool.map(lambda run: [post(e) for e in run], runs))
    total = time.perf_counter() - t0

    lat.sort()
    pct = lambda p: lat[min(len(lat) - 1, int(p / 100 * len(lat)))] * 1000
    print(f"\ntotal {total:.1f}s  throughput {len(events) / total:.0f} req/s")
    print(f"latency ms  p50 {pct(50):.0f}  p95 {pct(95):.0f}  p99 {pct(99):.0f}  max {lat[-1] * 1000:.0f}"
          f"  mean {statistics.mean(lat) * 1000:.0f}")
    print("status", dict(statuses))
    for k, v in errors.most_common(5):
        print(f"  {v}x {k}")


if __name__ == "__main__":
    main()
