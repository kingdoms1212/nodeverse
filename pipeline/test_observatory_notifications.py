from datetime import timedelta
import mongomock
from observatory.sampler import Sampler, AlertBook
from test_observatory import NOW, config


ISSUE = {'id': 'blocked', 'level': 'warning', 'title': 'test', 'detail': 'test'}


def test_restart_restores_incident_without_reopening(tmp_path):
    db = mongomock.MongoClient(tz_aware=True).local
    original = AlertBook()
    active, events = original.update([ISSUE], NOW)
    db.observatory_alert_state.insert_one({'_id': 'current', 'active': active, 'events': events})
    sampler = Sampler(db=db, cfg=config(tmp_path), clock=lambda: NOW)
    sampler.load_history()
    a, e = sampler.alerts.update([ISSUE], NOW+timedelta(seconds=10))
    assert len(a) == 1 and len(e) == 1
    a, e = sampler.alerts.update([], NOW+timedelta(seconds=20))
    assert not a and e[0]['phase'] == 'resolved' and len(e) == 2


def test_unknown_source_cannot_claim_recovery():
    book = AlertBook()
    book.update([ISSUE], NOW)
    active, events = book.update([], NOW+timedelta(seconds=10), unknown={'blocked'})
    assert len(active) == len(events) == 1
    active, events = book.update([], NOW+timedelta(seconds=20))
    assert not active and events[0]['phase'] == 'resolved'


def test_level_change_notified_once_and_history_bounded():
    book = AlertBook()
    book.update([ISSUE], NOW)
    active, events = book.update([dict(ISSUE, level='error')], NOW+timedelta(seconds=10))
    assert len(events) == 2 and events[0]['phase'] == 'changed'
    assert active[0]['first_seen'] == NOW.isoformat()
    assert len(book.update([dict(ISSUE, level='error')], NOW+timedelta(seconds=20))[1]) == 2
    for i in range(110):
        book.update([], NOW+timedelta(minutes=i+1))
        book.update([ISSUE], NOW+timedelta(minutes=i+1, seconds=1))
    assert len(book.events) == 100


def test_sampling_persists_only_operational_alerts(tmp_path):
    db = mongomock.MongoClient(tz_aware=True).local
    sampler = Sampler(db=db, cfg=config(tmp_path), clock=lambda: NOW)
    out = sampler.sample()
    assert out['alerts_persistent'] is True
    saved = db.observatory_alert_state.find_one({'_id': 'current'})
    assert saved['events'] and set(saved) == {'_id', 'active', 'events', 'updated_at'}
    assert not list(db.gameplay_events.find())
