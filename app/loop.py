import logging
import threading
from datetime import datetime, timezone

from .events import attributes_for, build_event, encode_event

log = logging.getLogger(__name__)

PUBLISH_TIMEOUT = 30
MAX_BACKOFF = 30


class StreamLoop:
    """Polls for unsent call_logs rows and publishes them.

    `store` needs: try_lock(), fetch_candidates(...), mark_sent(items).
    `publisher` needs: publish(data, attributes) -> future with .result(timeout).
    """

    def __init__(self, cfg, store, publisher, sleep=None):
        self.cfg = cfg
        self.store = store
        self.publisher = publisher
        self._stop = threading.Event()
        self._sleep = sleep or self._stop.wait
        self.last_success = None
        self.pending_estimate = 0

    def stop(self):
        self._stop.set()

    def run_once(self) -> bool:
        """One iteration. Returns True if the batch was full (more work likely waiting)."""
        if not self.store.try_lock():
            log.info("advisory lock held by another instance; standing by")
            self._mark_success()
            return False

        cfg = self.cfg
        rows = self.store.fetch_candidates(
            cfg.start_after, cfg.lookback_rows, cfg.excluded_staff, cfg.batch_size
        )
        self.pending_estimate = len(rows)

        in_flight = []
        for row in rows:
            event = build_event(row)
            try:
                fut = self.publisher.publish(encode_event(event), attributes_for(event))
            except Exception:
                log.exception("publish failed; will retry", extra={
                    "call_log_id": row["id"], "event_id": event["event_id"]})
                continue
            in_flight.append((row["id"], event["event_id"], fut))

        sent = []
        for call_log_id, event_id, fut in in_flight:
            try:
                msg_id = fut.result(timeout=PUBLISH_TIMEOUT)
            except Exception:
                log.exception("publish failed; will retry", extra={
                    "call_log_id": call_log_id, "event_id": event_id})
                continue
            sent.append((call_log_id, event_id, msg_id))
            log.info("published", extra={"call_log_id": call_log_id, "event_id": event_id})

        self.store.mark_sent(sent)
        self._mark_success()
        return len(rows) >= cfg.batch_size

    def _mark_success(self):
        self.last_success = datetime.now(timezone.utc)

    def run_forever(self):
        backoff = 1
        while not self._stop.is_set():
            try:
                full = self.run_once()
                backoff = 1
                if not full:
                    self._sleep(self.cfg.poll_interval)
            except Exception:
                log.exception("loop iteration failed")
                self._sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
