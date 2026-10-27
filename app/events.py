import json
import uuid

EVENT_TYPE = "call.initiated"
SCHEMA_VERSION = "1"


def event_id_for(call_log_id: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"call_logs/{call_log_id}/initiated"))


def should_stream(row, excluded, start_after) -> bool:
    """Python mirror of the SQL filters (id floor, direction, excluded staff)."""
    if row["id"] <= start_after:
        return False
    if (row.get("direction") or "").lower() != "outbound":
        return False
    staff = (row.get("staff_phone") or "").strip().lower()
    return staff not in {e.strip().lower() for e in excluded}


def build_event(row) -> dict:
    """Build the message payload from a call_logs row. Only whitelisted fields are used."""
    return {
        "event_id": event_id_for(row["id"]),
        "event_type": EVENT_TYPE,
        "occurred_at": row["created_at"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "call_log_id": row["id"],
        "direction": "outbound",
        "to_number": row.get("patient_number") or "",
    }


def encode_event(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def attributes_for(event: dict) -> dict:
    return {
        "event_id": event["event_id"],
        "event_type": EVENT_TYPE,
        "schema_version": SCHEMA_VERSION,
    }
