"""All environment configuration in one place (RF-4).

Every process reads its settings through `get()`. Defaults match compose.json, so the containers need no
extra variables. Values are read once per process; tests can call `reload()` after changing os.environ.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


def _flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "")
    return raw.strip() == "1" if raw.strip() else default


@dataclass(frozen=True)
class Settings:
    # collector
    ingest_key: str = field(default_factory=lambda: os.getenv("INGEST_KEY", ""))
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092"))
    kafka_delivery_timeout_s: float = field(default_factory=lambda: float(os.getenv("KAFKA_DELIVERY_TIMEOUT_S", "5")))
    # local MongoDB (rt_* aggregates, outbox)
    mongo_uri: str = field(default_factory=lambda: os.getenv("MONGO_URI", "mongodb://mongo:27017"))
    mongo_db: str = field(default_factory=lambda: os.getenv("MONGO_DB", "nodeverse_logs"))
    mongo_timeout_ms: int = field(default_factory=lambda: _int("MONGO_TIMEOUT_MS", 5000))
    # Spark
    lake_root: str = field(default_factory=lambda: os.getenv("LAKE_ROOT", "/data/delta"))
    checkpoint_root: str = field(default_factory=lambda: os.getenv("CHECKPOINT_ROOT", "/data/checkpoints"))
    bronze_trigger: str = field(default_factory=lambda: os.getenv("BRONZE_TRIGGER", "5 seconds"))
    stage_trigger: str = field(default_factory=lambda: os.getenv("STAGE_TRIGGER", "1 second"))
    audit_trigger: str = field(default_factory=lambda: os.getenv("AUDIT_TRIGGER", "30 seconds"))
    max_offsets_per_trigger: int = field(default_factory=lambda: _int("MAX_OFFSETS_PER_TRIGGER", 10000))
    dedup_ttl_hours: int = field(default_factory=lambda: _int("DEDUP_TTL_HOURS", 7 * 24))
    silver_optimization_enabled: bool = field(default_factory=lambda: os.getenv("SILVER_OPTIMIZATION_ENABLED", "0") == "1")
    silver_max_merge_groups: int = field(default_factory=lambda: _int("SILVER_MAX_MERGE_GROUPS", 16))
    outbox_stage_chunk: int = field(default_factory=lambda: _int("OUTBOX_STAGE_CHUNK", 500))
    # small-file compaction and VACUUM (spark/compaction.py); 0 disables the step
    compact_every_batches: int = field(default_factory=lambda: _int("COMPACT_EVERY_BATCHES", 300))
    compact_bronze_minutes: int = field(default_factory=lambda: _int("COMPACT_BRONZE_MINUTES", 10))
    vacuum_interval_hours: int = field(default_factory=lambda: _int("VACUUM_INTERVAL_HOURS", 6))
    vacuum_retain_hours: int = field(default_factory=lambda: _int("VACUUM_RETAIN_HOURS", 168))
    progress_path: str = field(default_factory=lambda: os.getenv("PROGRESS_PATH", "/data/metrics/streaming-progress.json"))
    observatory_interval_s: int = field(default_factory=lambda: _int("OBSERVATORY_INTERVAL_S", 10))
    observatory_collector_url: str = field(default_factory=lambda: os.getenv("OBSERVATORY_COLLECTOR_URL", "http://collector:8000"))
    observatory_key: str = field(default_factory=lambda: os.getenv("OBSERVATORY_KEY", ""), repr=False)
    observatory_name: str = field(default_factory=lambda: os.getenv("OBSERVATORY_NAME", "NODEVERSE"))
    observatory_bind: str = field(default_factory=lambda: os.getenv("OBSERVATORY_BIND", "127.0.0.1"))
    observatory_spark_ui_url: str = field(default_factory=lambda: os.getenv("OBSERVATORY_SPARK_UI_URL", "http://spark:4040"))
    observatory_deployment_path: str = field(default_factory=lambda: os.getenv("OBSERVATORY_DEPLOYMENT_PATH", "/run/observatory/runtime.json"))
    remote_export_enabled: bool = field(default_factory=lambda: _flag("REMOTE_EXPORT_ENABLED"))
    sink_retry_attempts: int = field(default_factory=lambda: _int("SINK_RETRY_ATTEMPTS", 5))
    query_restart_limit: int = field(default_factory=lambda: _int("QUERY_RESTART_LIMIT", 5))
    # remote writer
    direct_mongo_uri: str = field(default_factory=lambda: os.getenv("DIRECT_MONGO_URI", ""))
    direct_mongo_db: str = field(default_factory=lambda: os.getenv("DIRECT_MONGO_DATABASE", "nodeverse_logs"))
    direct_mysql_host: str = field(default_factory=lambda: os.getenv("DIRECT_MYSQL_HOST", ""))
    direct_mysql_port: int = field(default_factory=lambda: _int("DIRECT_MYSQL_PORT", 3306))
    direct_mysql_db: str = field(default_factory=lambda: os.getenv("DIRECT_MYSQL_DATABASE", "nodeverse"))
    direct_mysql_user: str = field(default_factory=lambda: os.getenv("DIRECT_MYSQL_USER", ""))
    direct_mysql_password: str = field(default_factory=lambda: os.getenv("DIRECT_MYSQL_PASSWORD", ""), repr=False)
    remote_context_path: str = field(default_factory=lambda: os.getenv("REMOTE_CONTEXT_PATH", "/run/private/remote-context.json"))
    remote_workers: int = field(default_factory=lambda: _int("REMOTE_WORKERS", 8))
    remote_batch: int = field(default_factory=lambda: _int("REMOTE_BATCH", 200))
    # 8th round: how long a resolved live member is reused before MySQL is asked again (0 = no cache)
    remote_member_cache_seconds: int = field(default_factory=lambda: _int("REMOTE_MEMBER_CACHE_SECONDS", 300))
    remote_sql_pool_size: int = field(default_factory=lambda: _int("REMOTE_SQL_POOL_SIZE", 0))  # 0 = remote_workers
    # PL-9: when waiting rows stop retrying and become 'parked' (kept, revivable)
    remote_wait_member_hours: float = field(default_factory=lambda: float(os.getenv("REMOTE_WAIT_MEMBER_HOURS", "24")))
    remote_wait_gap_minutes: float = field(default_factory=lambda: float(os.getenv("REMOTE_WAIT_GAP_MINUTES", "10")))
    remote_max_retries: int = field(default_factory=lambda: _int("REMOTE_MAX_RETRIES", 20))
    remote_done_ttl_days: int = field(default_factory=lambda: _int("REMOTE_DONE_TTL_DAYS", 30))  # 0 = keep forever

    @property
    def dedup_ttl_ms(self) -> int:
        if self.dedup_ttl_hours <= 0:
            raise ValueError('DEDUP_TTL_HOURS must be positive')
        return self.dedup_ttl_hours * 3600 * 1000

    @property
    def sql_pool_size(self) -> int:
        return self.remote_sql_pool_size or self.remote_workers

    def require_remote(self) -> None:
        missing = [n for n, v in (("DIRECT_MONGO_URI", self.direct_mongo_uri), ("DIRECT_MYSQL_HOST", self.direct_mysql_host),
                                  ("DIRECT_MYSQL_USER", self.direct_mysql_user), ("DIRECT_MYSQL_PASSWORD", self.direct_mysql_password)) if not v]
        if missing:
            raise RuntimeError("remote writer settings missing: " + ", ".join(missing))


@lru_cache(maxsize=1)
def get() -> Settings:
    return Settings()


def reload() -> Settings:
    get.cache_clear()
    return get()
