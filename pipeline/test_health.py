"""common/health.py decisions without a cluster: fake confluent-kafka metadata objects."""
from types import SimpleNamespace as NS

import pytest

from common import health
from topics import TOPICS


def _md(isr=3, leader=1, missing=(), broker="kafka-1:9092/1", brokers=(1, 2, 3)):
    def topic(n):
        return NS(error=None, partitions={i: NS(leader=leader, isrs=list(range(isr))) for i in range(n)})
    return NS(orig_broker_name=broker, orig_broker_id=1,
              brokers={b: NS(id=b) for b in brokers},
              topics={name: topic(n) for name, n in TOPICS.items() if name not in missing})


class _Admin:
    def __init__(self, md):
        self.md = md

    def list_topics(self, timeout):
        if isinstance(self.md, Exception):
            raise self.md
        return self.md


def _use(monkeypatch, md):
    monkeypatch.setattr(health, "_admin", lambda bootstrap, timeout_s: _Admin(md))


def test_ingest_ready_with_full_isr(monkeypatch):
    _use(monkeypatch, _md())
    r = health.kafka_ingest("x:9092")
    assert r["ready"] and r["problems"] == [] and r["topics"]["nv.learning.v2"]["partitions"] == 6


def test_one_broker_down_is_still_ingest_ready(monkeypatch):
    _use(monkeypatch, _md(isr=2, brokers=(1, 3)))  # ISR 2 = min.insync.replicas
    r = health.kafka_ingest("x:9092")
    assert r["ready"] and r["brokers"] == [1, 3]


@pytest.mark.parametrize("md,problem", [(dict(isr=1), "isr_1"), (dict(leader=-1), "no_leader"),
                                        (dict(missing=("nv.dlq.v1",)), "nv.dlq.v1:missing")])
def test_ingest_not_ready(monkeypatch, md, problem):
    _use(monkeypatch, _md(**md))
    r = health.kafka_ingest("x:9092")
    assert not r["ready"] and any(problem in p for p in r["problems"])


def test_broker_must_answer_for_itself(monkeypatch):
    _use(monkeypatch, _md(broker="kafka-2:9092/2"))
    assert not health.kafka_broker("kafka-1:9092")["ok"]
    assert health.kafka_broker("kafka-2:9092")["ok"]


def test_unreachable_broker_reports_error(monkeypatch):
    _use(monkeypatch, RuntimeError("timeout"))
    r = health.kafka_broker("kafka-3:9092")
    assert not r["ok"] and r["error"] == "RuntimeError"
    assert not health.kafka_ingest("x:9092")["ready"]


def test_wait_retries_until_ready(monkeypatch):
    calls = iter([False, False, True])
    monkeypatch.setitem(health.CHECKS, "flaky", lambda: next(calls))
    monkeypatch.setattr(health.time, "sleep", lambda s: None)
    assert health.wait(["flaky"], attempts=5, delay_s=0)
    calls = iter([False] * 3)
    monkeypatch.setitem(health.CHECKS, "never", lambda: next(calls))
    assert not health.wait(["never"], attempts=3, delay_s=0)
