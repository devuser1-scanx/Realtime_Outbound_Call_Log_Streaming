import pytest

from app.config import ConfigError, load_config
from app.db import CANDIDATES_SQL
from app.events import should_stream

EXCLUDED = ("clinic-tablet",)


def row(**kw):
    base = {"id": 2000, "staff_phone": "Dr Smith", "direction": "outbound"}
    base.update(kw)
    return base


def test_normal_outbound_streams():
    assert should_stream(row(), EXCLUDED, 1389)


@pytest.mark.parametrize("staff", ["clinic-tablet", "Clinic-Tablet", "  clinic-tablet  ", "CLINIC-TABLET\t"])
def test_clinic_tablet_excluded_case_and_whitespace(staff):
    assert not should_stream(row(staff_phone=staff), EXCLUDED, 1389)


@pytest.mark.parametrize("staff", [None, "", "   "])
def test_empty_staff_is_streamed(staff):
    assert should_stream(row(staff_phone=staff), EXCLUDED, 1389)


@pytest.mark.parametrize("direction", ["inbound", "Inbound", None])
def test_non_outbound_skipped(direction):
    assert not should_stream(row(direction=direction), EXCLUDED, 1389)


def test_direction_case_insensitive():
    assert should_stream(row(direction="OUTBOUND"), EXCLUDED, 1389)


def test_start_after_is_exclusive():
    assert not should_stream(row(id=1389), EXCLUDED, 1389)
    assert should_stream(row(id=1390), EXCLUDED, 1389)


def test_sql_matches_spec():
    assert "LOWER(cl.direction) = 'outbound'" in CANDIDATES_SQL
    assert "LOWER(TRIM(COALESCE(cl.staff_phone, ''))) <> ALL(:excluded)" in CANDIDATES_SQL
    assert "NOT EXISTS" in CANDIDATES_SQL


def test_config_fails_fast_and_lowercases_excluded():
    with pytest.raises(ConfigError):
        load_config({})
    env = {
        "GCP_PROJECT_ID": "p", "PUBSUB_TOPIC": "t", "INSTANCE_CONNECTION_NAME": "p:r:i",
        "DB_NAME": "d", "DB_USER": "u", "DB_PASSWORD": "x", "START_AFTER_CALL_LOG_ID": "5",
        "EXCLUDED_STAFF": " Clinic-Tablet , Front-Desk ",
    }
    cfg = load_config(env)
    assert cfg.excluded_staff == ("clinic-tablet", "front-desk")
    assert cfg.start_after == 5 and cfg.batch_size == 500 and cfg.lookback_rows == 1000
