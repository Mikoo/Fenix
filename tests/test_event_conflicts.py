import pytest

from fenix.services.events import register_event
from fenix.utils.errors import PermanentError
from fenix.utils.state import load, save


@pytest.fixture
def event():
    return {
        "schema_version": 1,
        "event_id": "evt-001",
        "event_type": "instance.stopped",
        "resource_id": "node-01",
        "occurred_at": "2026-10-22T18:00:00Z",
        "simulate_failures": 0
    }


def test_new_event(event):
    ledger = {}
    record = register_event(event, ledger)

    assert ledger["evt-001"] == record
    assert record["event"] == event
    assert record["attempts"] == 0
    assert record["repair_done"] is False
    assert record["notified"] is False


def test_identical_event(event):
    ledger = {}
    record = register_event(event, ledger)
    record["attempts"] = 2

    repeated = register_event(event.copy(), ledger)

    assert repeated is record
    assert repeated["attempts"] == 2
    assert len(ledger) == 1


def test_conflicting_event(event):
    ledger = {}
    register_event(event, ledger)

    modified = event.copy()
    modified["simulate_failures"] = 3

    with pytest.raises(PermanentError):
        register_event(modified, ledger)

    assert ledger["evt-001"]["event"] == event


def test_event_persistence(event, tmp_path):
    path = str(tmp_path / "events.json")
    ledger = {}

    record = register_event(event, ledger)
    record["attempts"] = 3
    save(path, ledger)

    recovered = load(path)
    record = register_event(event.copy(), recovered)

    assert record["attempts"] == 3
    assert record["event"] == event


def test_conflict_after_reload(event, tmp_path):
    path = str(tmp_path / "events.json")
    ledger = {}

    register_event(event, ledger)
    save(path, ledger)

    recovered = load(path)
    modified = event.copy()
    modified["simulate_failures"] = 5

    with pytest.raises(PermanentError):
        register_event(modified, recovered)


def test_different_event_ids(event):
    ledger = {}
    register_event(event, ledger)

    second = event.copy()
    second["event_id"] = "evt-002"

    register_event(second, ledger)

    assert len(ledger) == 2
    assert ledger["evt-001"]["event"] == event
    assert ledger["evt-002"]["event"] == second