"""Pruebas de validación de eventos - Integrante 1."""

import pytest
from fenix.services.events import validate
from fenix.utils.errors import ValidationError


@pytest.fixture
def inventory():
    return {
        "node-01": {
            "type": "ec2",
            "id": "i-123456"
        },
        "bucket-01": {
            "type": "s3",
            "id": "fenix-demo"
        }
    }


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


def test_valid_instance(event, inventory):
    result = validate(event, inventory)
    assert result == event
    assert result is not event


def test_valid_bucket(event, inventory):
    event["event_type"] = "bucket.tags_missing"
    event["resource_id"] = "bucket-01"
    assert validate(event, inventory) == event


@pytest.mark.parametrize("field,value", [
    ("schema_version", True),
    ("schema_version", 2),
    ("schema_version", "1"),
    ("event_id", "../AJENO"),
    ("event_type", "unknown.event"),
    ("resource_id", "node-99"),
    ("resource_id", "i-123456"),
    ("resource_id", "bucket-01"),
    ("occurred_at", "2026-10-22T18:00:00"),
    ("occurred_at", "2026-10-22T18:00:00+03:00"),
    ("simulate_failures", True),
    ("simulate_failures", -1),
    ("simulate_failures", 6),
    ("simulate_failures", 1.5),
])
def test_invalid_event(event, inventory, field, value):
    event[field] = value
    with pytest.raises(ValidationError):
        validate(event, inventory)


def test_extra_field(event, inventory):
    event["extra"] = "no permitido"
    with pytest.raises(ValidationError):
        validate(event, inventory)


def test_missing_field(event, inventory):
    del event["event_id"]
    with pytest.raises(ValidationError):
        validate(event, inventory)


def test_invalid_object(inventory):
    with pytest.raises(ValidationError):
        validate([], inventory)


@pytest.mark.parametrize("failures", [0, 5])
def test_failure_limits(event, inventory, failures):
    event["simulate_failures"] = failures
    assert validate(event, inventory) == event