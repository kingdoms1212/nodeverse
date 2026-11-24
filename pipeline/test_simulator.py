"""Prevent false-success test runs and timestamp-changing update replays."""
from types import SimpleNamespace
import pytest
import simulator


def test_update_replay_keeps_exact_payload(tmp_path, monkeypatch):
    monkeypatch.setattr(simulator, "RUNS", tmp_path)
    sent = []
    def send(url, key, events):
        sent.append(events)
        return {202: len(events)}
    monkeypatch.setattr(simulator, "send_all", send)
    args = SimpleNamespace(run_id="replay", installs=2, hash_failures=1, target="v1")
    simulator.cmd_updates(args, "unused", "unused")
    # Replaying must not regenerate timestamps or payloads even if generator changes.
    monkeypatch.setattr(simulator, "update_flow", lambda *a, **k: pytest.fail("regenerated replay"))
    simulator.cmd_updates(args, "unused", "unused")
    assert sent[0] == sent[1] and sent[0]


def test_smoke_transport_failure_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setattr(simulator, "RUNS", tmp_path)
    monkeypatch.setattr(simulator, "send_all", lambda *a: {503: 1})
    with pytest.raises(SystemExit) as result:
        simulator.cmd_smoke(SimpleNamespace(run_id="failure", count=1), "unused", "unused")
    assert result.value.code == 1


def test_contract_check_failure_exits_nonzero(monkeypatch):
    monkeypatch.setattr(simulator, "post", lambda *a: (503, {}, 0.0))
    with pytest.raises(SystemExit) as result:
        simulator.cmd_checks(SimpleNamespace(run_id="failure"), "unused", "unused")
    assert result.value.code == 1


def test_dummy_data_sessions_follow_contract_and_rules():
    import contract
    from tools.dummy_data import generate
    sessions = list(generate(2000, 50, 7, 0.3, "unit"))
    assert sum(len(s) for s in sessions) >= 2000
    assert len({s[0]["run_id"] for s in sessions}) == len(sessions), "one problem run per session"
    for s in sessions:
        assert [e["sequence"] for e in s] == list(range(1, len(s) + 1))
        assert s[0]["event_type"] == "problem_started" and s[-1]["event_type"] == "problem_completed"
        ends = set()
        for e in s:
            contract.LearningEventV2.model_validate(e)
            if e["event_type"] in ("attempt_succeeded", "attempt_failed"):
                assert e["attempt_id"] not in ends, "one outcome per attempt"
                ends.add(e["attempt_id"])


def test_dummy_event_ids_are_time_ordered_uuid7():
    import uuid
    from tools.dummy_data import uuid7
    a = uuid7()
    import time
    time.sleep(0.002)
    b = uuid7()
    assert uuid.UUID(a).version == 7 and a < b


def test_silver_merge_condition_bounds_dates_and_v7_ids():
    import importlib.util, sys, types
    from datetime import date
    # silver.py needs pyspark; only the pure condition builder is tested here
    if importlib.util.find_spec("pyspark") is None:
        import pytest
        pytest.skip("pyspark not installed")
    sys.path.insert(0, "spark")
    import silver
    c = silver.merge_condition(date(2026, 11, 20), date(2026, 11, 20),
                               "019aa000-0000-7000-8000-000000000000", "019aa0ff-0000-7000-8000-000000000000")
    assert "BETWEEN DATE '2026-11-13' AND DATE '2026-11-21'" in c and "t.event_id BETWEEN" in c
    assert "t.event_id BETWEEN" not in silver.merge_condition(date(2026, 11, 20), date(2026, 11, 20), "x'; drop", "y")

