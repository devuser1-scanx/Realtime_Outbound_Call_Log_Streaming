import logging

import sqlalchemy
from google.cloud.sql.connector import Connector
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import String

log = logging.getLogger(__name__)

ADVISORY_LOCK_KEY = 7_426_190_331

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dialer_stream_sent (
    call_log_id   INTEGER PRIMARY KEY,
    event_id      UUID NOT NULL,
    pubsub_msg_id VARCHAR(100),
    sent_at       TIMESTAMP NOT NULL DEFAULT NOW()
)
"""

CANDIDATES_SQL = """
SELECT cl.id, cl.staff_phone, cl.patient_number, cl.direction, cl.created_at
FROM call_logs cl
WHERE cl.id > GREATEST(
        :start_after,
        COALESCE((SELECT MAX(call_log_id) FROM dialer_stream_sent), 0) - :lookback)
  AND LOWER(cl.direction) = 'outbound'
  AND LOWER(TRIM(COALESCE(cl.staff_phone, ''))) <> ALL(:excluded)
  AND NOT EXISTS (SELECT 1 FROM dialer_stream_sent s WHERE s.call_log_id = cl.id)
ORDER BY cl.id
LIMIT :batch_size
"""

MARK_SENT_SQL = """
INSERT INTO dialer_stream_sent (call_log_id, event_id, pubsub_msg_id)
VALUES (:call_log_id, CAST(:event_id AS uuid), :pubsub_msg_id)
ON CONFLICT DO NOTHING
"""


class Database:
    def __init__(self, cfg):
        self._cfg = cfg
        self._connector = Connector()
        self.engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=self._connect,
            pool_size=2,
            max_overflow=2,
            pool_pre_ping=True,
        )
        self._lock_conn = None

    def _connect(self):
        return self._connector.connect(
            self._cfg.instance_connection_name,
            "pg8000",
            user=self._cfg.db_user,
            password=self._cfg.db_password,
            db=self._cfg.db_name,
        )

    def ensure_table(self):
        try:
            with self.engine.begin() as conn:
                conn.execute(text(CREATE_TABLE_SQL))
        except Exception:
            # An admin may have created the table; the service user then lacks CREATE.
            log.warning("CREATE TABLE IF NOT EXISTS failed; checking the table exists", exc_info=True)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM dialer_stream_sent LIMIT 1"))

    def try_lock(self) -> bool:
        """Hold a session-level advisory lock on a dedicated connection for the process lifetime."""
        if self._lock_conn is not None:
            try:
                cur = self._lock_conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                return True
            except Exception:
                log.warning("advisory lock connection lost; reconnecting")
                self._lock_conn = None
        conn = self._connect()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        got = cur.fetchone()[0]
        cur.close()
        if got:
            self._lock_conn = conn
        else:
            conn.close()
        return bool(got)

    def fetch_candidates(self, start_after, lookback, excluded, batch_size):
        stmt = text(CANDIDATES_SQL).bindparams(bindparam("excluded", type_=ARRAY(String)))
        with self.engine.connect() as conn:
            rows = conn.execute(stmt, {
                "start_after": start_after,
                "lookback": lookback,
                "excluded": list(excluded),
                "batch_size": batch_size,
            }).mappings().all()
        return [dict(r) for r in rows]

    def mark_sent(self, items):
        """items: iterable of (call_log_id, event_id, pubsub_msg_id)."""
        params = [
            {"call_log_id": i, "event_id": e, "pubsub_msg_id": m} for i, e, m in items
        ]
        if not params:
            return
        with self.engine.begin() as conn:
            conn.execute(text(MARK_SENT_SQL), params)

    def close(self):
        try:
            if self._lock_conn is not None:
                self._lock_conn.close()
        finally:
            self.engine.dispose()
            self._connector.close()
