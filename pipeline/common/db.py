"""MongoDB clients shared by a process (RF-5). pymongo clients are thread-safe and pool connections."""
from __future__ import annotations

import threading

from common import settings

_lock = threading.Lock()
_clients: dict = {}


def _client(uri: str, pool: int):
    from pymongo import MongoClient
    key = (uri, pool)
    with _lock:
        if key not in _clients:
            s = settings.get()
            _clients[key] = MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=s.mongo_timeout_ms,
                                        socketTimeoutMS=s.mongo_timeout_ms + 2000, maxPoolSize=pool)
        return _clients[key]


def local_mongo(pool: int = 50):
    """Database holding rt_* aggregates and the remote outbox."""
    s = settings.get()
    return _client(s.mongo_uri, pool)[s.mongo_db]


def remote_mongo(pool: int = 50):
    """Dashboard database written by the remote writer."""
    s = settings.get()
    return _client(s.direct_mongo_uri, pool)[s.direct_mongo_db]
