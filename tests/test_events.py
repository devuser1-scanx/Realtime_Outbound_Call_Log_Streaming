import json
import uuid
from datetime import datetime

from app.events import attributes_for, build_event, encode_event, event_id_for

AVRO_FIELDS = {"event_id", "event_type", "occurred_at", "call_log_id", "direction", "to_number"}

ROW = {
    "id": 1389,
    "staff_phone": "Dr Smith",
    "patient_number": "+14696400395",
    "direction": "Outbound",
    "created_at": datetime(2026, 10, 26, 11, 31, 0, 123456),
    "call_sid": "CA123",
    "appointment_id": "apt-1",
}


def test_payload_fields_match_schema_exactly():
    event = build_event(ROW)
    assert set(event) == AVRO_FIELDS
    assert event["call_log_id"] == 1389
    assert event["event_type"] == "call.initiated"
    assert event["direction"] == "outbound"


def test_full_number_and_no_leaked_fields():
    raw = encode_event(build_event(ROW)).decode()
    assert "+14696400395" in raw
    for leaked in ("CA123", "Dr Smith", "apt-1", "call_sid", "staff_phone", "appointment_id"):
        assert leaked not in raw


def test_null_number_becomes_empty_string():
    assert build_event({**ROW, "patient_number": None})["to_number"] == ""


def test_event_id_is_deterministic_uuid5():
    expected = str(uuid.uuid5(uuid.NAMESPACE_URL, "call_logs/1389/initiated"))
    assert event_id_for(1389) == expected
    assert build_event(ROW)["event_id"] == expected
    assert event_id_for(1390) != expected


def test_occurred_at_is_utc_seconds_z():
    assert build_event(ROW)["occurred_at"] == "2026-10-26T11:31:00Z"


def test_encoded_is_json_utf8_and_attributes():
    event = build_event(ROW)
    assert json.loads(encode_event(event)) == event
    assert attributes_for(event) == {
        "event_id": event["event_id"],
        "event_type": "call.initiated",
        "schema_version": "1",
    }
