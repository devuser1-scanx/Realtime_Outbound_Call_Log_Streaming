from concurrent.futures import Future
from datetime import datetime

from app.config import Config
from app.events import should_stream
from app.loop import StreamLoop


def make_cfg(batch_size=500):
    return Config(
        project_id="p", topic="t", instance_connection_name="p:r:i", db_name="d",
        db_user="u", db_password="x", start_after=100, excluded_staff=("clinic-tablet",),
        poll_interval=1, batch_size=batch_size, lookback_rows=1000, log_level="INFO",
    )


def make_row(i, staff="Dr Smith", direction="outbound"):
    return {"id": i, "staff_phone": staff, "patient_number": "+14696400395",
            "direction": direction, "created_at": datetime(2026, 10, 26, 11, 31, 0)}


class FakeStore:
    def __init__(self, rows, locked=True):
        self.rows = rows
        self.sent = {}
        self.locked = locked

    def try_lock(self):
        return self.locked

    def fetch_candidates(self, start_after, lookback, excluded, batch_size):
        out = [r for r in self.rows
               if should_stream(r, excluded, start_after) and r["id"] not in self.sent]
        return sorted(out, key=lambda r: r["id"])[:batch_size]

    def mark_sent(self, items):
        for call_log_id, event_id, msg_id in items:
            self.sent.setdefault(call_log_id, (event_id, msg_id))


class FakePublisher:
    def __init__(self, fail_ids=()):
        self.fail = set(fail_ids)
        self.published = []

    def publish(self, data, attributes):
        import json
        call_log_id = json.loads(data)["call_log_id"]
        fut = Future()
        if call_log_id in self.fail:
            fut.set_exception(RuntimeError("boom"))
        else:
            self.published.append(call_log_id)
            fut.set_result(f"msg-{call_log_id}")
        return fut


def test_failed_publish_not_marked_and_retried():
    store = FakeStore([make_row(101), make_row(102)])
    pub = FakePublisher(fail_ids={102})
    loop = StreamLoop(make_cfg(), store, pub)

    loop.run_once()
    assert set(store.sent) == {101}

    pub.fail.clear()
    loop.run_once()
    assert set(store.sent) == {101, 102}
    assert pub.published == [101, 102]


def test_no_double_send():
    store = FakeStore([make_row(101)])
    pub = FakePublisher()
    loop = StreamLoop(make_cfg(), store, pub)
    loop.run_once()
    loop.run_once()
    assert pub.published == [101]


def test_filtered_rows_never_published():
    rows = [make_row(101, staff="clinic-tablet"), make_row(102, direction="inbound"),
            make_row(50), make_row(103, staff=None)]
    store = FakeStore(rows)
    pub = FakePublisher()
    StreamLoop(make_cfg(), store, pub).run_once()
    assert pub.published == [103]


def test_full_batch_returns_true():
    store = FakeStore([make_row(101), make_row(102), make_row(103)])
    loop = StreamLoop(make_cfg(batch_size=2), store, FakePublisher())
    assert loop.run_once() is True
    assert loop.run_once() is False


def test_lock_not_acquired_publishes_nothing():
    store = FakeStore([make_row(101)], locked=False)
    pub = FakePublisher()
    loop = StreamLoop(make_cfg(), store, pub)
    loop.run_once()
    assert pub.published == []
    assert loop.last_success is not None


def test_run_forever_survives_errors_and_backs_off():
    class Boom(FakeStore):
        calls = 0

        def fetch_candidates(self, *a):
            Boom.calls += 1
            if Boom.calls < 3:
                raise RuntimeError("db down")
            loop.stop()
            return []

    sleeps = []
    store = Boom([])
    loop = StreamLoop(make_cfg(), store, FakePublisher(), sleep=sleeps.append)
    loop.run_forever()
    assert sleeps[:2] == [1, 2]
