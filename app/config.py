import os
from dataclasses import dataclass

from dotenv import load_dotenv


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Config:
    project_id: str
    topic: str
    instance_connection_name: str
    db_name: str
    db_user: str
    db_password: str
    start_after: int
    excluded_staff: tuple
    poll_interval: float
    batch_size: int
    lookback_rows: int
    log_level: str


def _required(env, name):
    value = (env.get(name) or "").strip()
    if not value:
        raise ConfigError(f"missing required env var {name}")
    return value


def _int(env, name, default=None):
    raw = (env.get(name) or "").strip()
    if not raw:
        if default is None:
            raise ConfigError(f"missing required env var {name}")
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"env var {name} must be an integer, got {raw!r}")


def load_config(env=None) -> Config:
    if env is None:
        # Local dev: pick up .env if present. Real env vars (Cloud Run) take precedence;
        # .env is excluded from the image via .dockerignore.
        load_dotenv(override=False)
        env = os.environ
    excluded = tuple(
        s.strip().lower()
        for s in (env.get("EXCLUDED_STAFF") or "clinic-tablet").split(",")
        if s.strip()
    )
    try:
        poll = float((env.get("POLL_INTERVAL_SECONDS") or "1").strip())
    except ValueError:
        raise ConfigError("POLL_INTERVAL_SECONDS must be a number")
    cfg = Config(
        project_id=_required(env, "GCP_PROJECT_ID"),
        topic=_required(env, "PUBSUB_TOPIC"),
        instance_connection_name=_required(env, "INSTANCE_CONNECTION_NAME"),
        db_name=_required(env, "DB_NAME"),
        db_user=_required(env, "DB_USER"),
        db_password=_required(env, "DB_PASSWORD"),
        start_after=_int(env, "START_AFTER_CALL_LOG_ID"),
        excluded_staff=excluded,
        poll_interval=poll,
        batch_size=_int(env, "BATCH_SIZE", 500),
        lookback_rows=_int(env, "LOOKBACK_ROWS", 1000),
        log_level=(env.get("LOG_LEVEL") or "INFO").strip().upper(),
    )
    if cfg.poll_interval <= 0 or cfg.batch_size <= 0 or cfg.lookback_rows < 0:
        raise ConfigError("POLL_INTERVAL_SECONDS/BATCH_SIZE must be > 0, LOOKBACK_ROWS >= 0")
    return cfg
