"""MySQL access for the remote writer: DBCP-style pool + the cross-tool writer lock (BN-5)."""
from __future__ import annotations

import pymysql

from common import settings

WRITER_LOCK = "nodeverse_direct_loader"


def _params():
    s = settings.get()
    return dict(host=s.direct_mysql_host, port=s.direct_mysql_port, user=s.direct_mysql_user,
                password=s.direct_mysql_password, database=s.direct_mysql_db, charset="utf8mb4",
                connect_timeout=5, read_timeout=6, write_timeout=6, autocommit=False)


def connect():
    return pymysql.connect(**_params())


def pool(size: int):
    """Reuse up to `size` connections, block when all are busy, ping before reuse (like Java DBCP)."""
    from dbutils.pooled_db import PooledDB
    return PooledDB(creator=pymysql, mincached=1, maxcached=size, maxconnections=size, blocking=True,
                    ping=1, reset=True, **_params())


class WriterLock:
    """Holds MySQL GET_LOCK on one dedicated connection so other direct loaders stay excluded.

    The lock used to be taken per event on a fresh connection, which also serialised every event."""

    def __init__(self, connect_fn=connect):
        self._connect = connect_fn
        self._conn = None

    def ensure(self) -> bool:
        try:
            if self._conn is not None:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT IS_USED_LOCK(%s) = CONNECTION_ID()", (WRITER_LOCK,))
                    if cur.fetchone()[0] == 1:
                        return True
            self.release()
            self._conn = self._connect()
            with self._conn.cursor() as cur:
                cur.execute("SELECT GET_LOCK(%s, 0)", (WRITER_LOCK,))
                if cur.fetchone()[0] == 1:
                    return True
        except Exception:
            pass
        self.release()
        return False

    def release(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()  # closing the session releases GET_LOCK
            except Exception:
                pass
        self._conn = None
