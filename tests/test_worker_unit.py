"""Pruebas unitarias del Rol 5 (I5): Worker secuencial y ledger por etapas."""
import json
import logging
from botocore.exceptions import BotoCoreError, ClientError
import pytest

from fenix.services import remediation, worker
from fenix.utils.errors import PermanentError, RetryableError, ValidationError
from fenix.utils.state import load, save


def _sample_event(
    event_id: str = "evt-node-001",
    simulate_failures: int = 0,
) -> dict:
    """Construye un evento válido de prueba."""
    return {
        "schema_version": 1,
        "event_id": event_id,
        "event_type": "instance.stopped",
        "resource_id": "node-01",
        "occurred_at": "2026-10-22T18:00:00Z",
        "simulate_failures": simulate_failures,
    }


def _setup_inventory(tmp_path, monkeypatch) -> None:
    """Aísla el directorio de trabajo en tmp_path con un inventario válido."""
    monkeypatch.chdir(tmp_path)
    save(
        "data/inventory.json",
        {
            "node-01": {
                "type": "ec2",
                "id": "i-mock01",
                "expected_tags": {"GroupId": "grupo01"},
            }
        },
    )


def _mock_handle_deps(
    monkeypatch, calls: dict, status: str = "repaired"
) -> None:
    """Inyecta doubles de validate, repair y notify contabilizando llamadas."""
    monkeypatch.setattr(remediation, "validate", lambda evt, _inv: dict(evt))

    def _fake_repair(_res, _etype):
        calls["repair"] += 1
        return status

    def _fake_notify(_msg):
        calls["notify"] += 1

    monkeypatch.setattr(remediation, "repair", _fake_repair)
    monkeypatch.setattr(remediation, "notify", _fake_notify)


def test_handle_nominal_ledger_and_f11_idempotency(tmp_path, monkeypatch):
    """AUD-08/AUD-11: persiste repair_result, status y evita duplicados."""
    _setup_inventory(tmp_path, monkeypatch)
    event = _sample_event()
    calls = {"repair": 0, "notify": 0}
    _mock_handle_deps(monkeypatch, calls, status="repaired")

    remediation.handle(event)
    ledger = load("data/events.json")["evt-node-001"]
    assert ledger["attempts"] == 1
    assert ledger["status"] == "completed"
    assert ledger["repair_done"] is True
    assert ledger["repair_result"] == "repaired"
    assert ledger["repair_status"] == "repaired"
    assert ledger["notified"] is True
    assert calls == {"repair": 1, "notify": 1}

    remediation.handle(event)
    assert calls == {"repair": 1, "notify": 1}
    assert load("data/events.json")["evt-node-001"]["attempts"] == 1


def test_handle_f09_simulate_failures_and_attempts(tmp_path, monkeypatch):
    """F09: respeta simulate_failures=2 y completa en el intento 3."""
    _setup_inventory(tmp_path, monkeypatch)
    event = _sample_event("evt-f09", simulate_failures=2)
    calls = {"repair": 0, "notify": 0}
    _mock_handle_deps(monkeypatch, calls)

    with pytest.raises(RetryableError):
        remediation.handle(event)
    assert load("data/events.json")["evt-f09"]["status"] == "processing"

    with pytest.raises(RetryableError):
        remediation.handle(event)
    assert calls == {"repair": 0, "notify": 0}

    remediation.handle(event)
    ledger = load("data/events.json")["evt-f09"]
    assert ledger["attempts"] == 3
    assert ledger["status"] == "completed"
    assert ledger["repair_done"] is True
    assert ledger["notified"] is True
    assert calls == {"repair": 1, "notify": 1}


def test_handle_f12_recovery_between_repair_and_notify(tmp_path, monkeypatch):
    """AUD-07/F12: si falla notify tras reparar, reintento solo notifica."""
    _setup_inventory(tmp_path, monkeypatch)
    event = _sample_event("evt-f12")
    calls = {"repair": 0, "notify": 0}
    _mock_handle_deps(monkeypatch, calls, status="already_ok")

    def _flaky_notify(_msg):
        calls["notify"] += 1
        if calls["notify"] == 1:
            raise RetryableError("Timeout temporal de Telegram")

    monkeypatch.setattr(remediation, "notify", _flaky_notify)

    with pytest.raises(RetryableError):
        remediation.handle(event)

    ledger_mid = load("data/events.json")["evt-f12"]
    assert ledger_mid["status"] == "repaired"
    assert ledger_mid["repair_done"] is True
    assert ledger_mid["repair_result"] == "already_ok"
    assert ledger_mid["notified"] is False

    remediation.handle(event)
    ledger_end = load("data/events.json")["evt-f12"]
    assert ledger_end["attempts"] == 2
    assert ledger_end["status"] == "completed"
    assert ledger_end["repair_done"] is True
    assert ledger_end["notified"] is True
    assert calls == {"repair": 1, "notify": 2}


def test_handle_f15_conflict_preserves_original_ledger(tmp_path, monkeypatch):
    """F15: mismo event_id con distinto payload lanza PermanentError."""
    _setup_inventory(tmp_path, monkeypatch)
    original = _sample_event("evt-f15")
    altered = _sample_event("evt-f15", simulate_failures=1)
    calls = {"repair": 0, "notify": 0}
    _mock_handle_deps(monkeypatch, calls)

    remediation.handle(original)
    before = load("data/events.json")

    with pytest.raises(PermanentError):
        remediation.handle(altered)

    assert load("data/events.json") == before


def test_handle_repair_error_keeps_processing_state(tmp_path, monkeypatch):
    """AUD-06: un fallo de reparación no marca repair_done ni notifica."""
    _setup_inventory(tmp_path, monkeypatch)
    event = _sample_event("evt-rep-err")
    notified = []

    monkeypatch.setattr(remediation, "validate", lambda evt, _inv: dict(evt))

    def _fail_repair(_res, _etype):
        raise PermanentError("Instancia terminada")

    monkeypatch.setattr(remediation, "repair", _fail_repair)
    monkeypatch.setattr(remediation, "notify", notified.append)

    with pytest.raises(PermanentError):
        remediation.handle(event)

    ledger = load("data/events.json")["evt-rep-err"]
    assert ledger["attempts"] == 1
    assert ledger["status"] == "processing"
    assert ledger["repair_done"] is False
    assert ledger["repair_result"] is None
    assert ledger["notified"] is False
    assert not notified


def test_worker_run_ack_order_and_log_context(monkeypatch, caplog):
    """AUD-01/AUD-09: handler antes de ACK, attempt y status=acknowledged."""
    order = []
    msg = {
        "MessageId": "msg-1",
        "ReceiptHandle": "rh-1",
        "Attributes": {"ApproximateReceiveCount": "2"},
        "Body": json.dumps(_sample_event()),
    }
    monkeypatch.setattr(worker, "receive", lambda _url: [msg])
    monkeypatch.setattr(
        worker, "acknowledge", lambda _u, rh: order.append(("ack", rh))
    )
    monkeypatch.setattr(
        worker, "retry_later", lambda _u, _m: order.append(("retry", None))
    )

    with caplog.at_level(logging.INFO, logger="test-worker"):
        worker.run(
            "http://q",
            lambda p: order.append(("handler", p["event_id"])),
            logging.getLogger("test-worker"),
            once=True,
        )

    assert order == [("handler", "evt-node-001"), ("ack", "rh-1")]
    record = caplog.records[-1]
    assert record.attempt == 2
    assert record.status == "acknowledged"
    assert record.event_id == "evt-node-001"
    assert record.message_id == "msg-1"


@pytest.mark.parametrize(
    "attrs,expected_attempt",
    [
        (None, 1),
        ({}, 1),
        ({"ApproximateReceiveCount": "abc"}, 1),
        ({"ApproximateReceiveCount": True}, 1),
        ({"ApproximateReceiveCount": "0"}, 1),
        ({"ApproximateReceiveCount": "3"}, 3),
    ],
)
def test_worker_attempt_extraction_fallbacks(
    monkeypatch, caplog, attrs, expected_attempt
):
    """AUD-02/AUD-03/AUD-04: maneja Attributes ausente o contador inválido."""
    msg = {
        "MessageId": "msg-attr",
        "ReceiptHandle": "rh-attr",
        "Body": json.dumps(_sample_event()),
    }
    if attrs is not None:
        msg["Attributes"] = attrs

    monkeypatch.setattr(worker, "receive", lambda _url: [msg])
    monkeypatch.setattr(worker, "acknowledge", lambda _u, _rh: None)

    with caplog.at_level(logging.INFO, logger="test-worker"):
        worker.run(
            "http://q",
            lambda _p: None,
            logging.getLogger("test-worker"),
            once=True,
        )

    assert caplog.records[-1].attempt == expected_attempt


@pytest.mark.parametrize(
    "body,exc",
    [
        ("{bad-json", None),
        ("[1, 2, 3]", None),
        (json.dumps(_sample_event()), ValidationError("Esquema inválido")),
        (json.dumps(_sample_event()), RetryableError("Fallo transitorio")),
        (json.dumps(_sample_event()), PermanentError("Conflicto")),
    ],
)
def test_worker_run_errors_trigger_retry_without_ack(monkeypatch, body, exc):
    """JSON inválido, payload no dict y errores llaman retry_later sin ACK."""
    acks = []
    retries = []
    msg = {"MessageId": "msg-err", "ReceiptHandle": "rh-err", "Body": body}

    monkeypatch.setattr(worker, "receive", lambda _url: [msg])
    monkeypatch.setattr(worker, "acknowledge", lambda _u, rh: acks.append(rh))
    monkeypatch.setattr(
        worker, "retry_later", lambda _u, m: retries.append(m["MessageId"])
    )

    def _handler(_payload):
        if exc is not None:
            raise exc

    worker.run(
        "http://q", _handler, logging.getLogger("test-worker"), once=True
    )
    assert not acks
    assert retries == ["msg-err"]


@pytest.mark.parametrize(
    "retry_exc",
    [
        KeyError("Attributes"),
        ValueError("invalid literal"),
        TypeError("NoneType"),
        ValidationError("Endpoint inválido"),
        ClientError({"Error": {"Code": "InternalError"}}, "ChangeVis"),
    ],
)
def test_worker_run_retry_later_failure_is_isolated(
    monkeypatch, caplog, retry_exc
):
    """AUD-02..AUD-05: fallo en retry_later no detiene el worker."""
    sleeps = []
    msg = {
        "MessageId": "msg-retry-fail",
        "ReceiptHandle": "rh-rf",
        "Body": "{broken",
    }
    monkeypatch.setattr(worker, "receive", lambda _url: [msg])
    monkeypatch.setattr(worker, "acknowledge", lambda _u, _rh: None)
    monkeypatch.setattr(worker.time, "sleep", sleeps.append)

    def _broken_retry(_url, _msg):
        raise retry_exc

    monkeypatch.setattr(worker, "retry_later", _broken_retry)

    with caplog.at_level(logging.ERROR, logger="test-worker"):
        worker.run(
            "http://q",
            lambda _p: None,
            logging.getLogger("test-worker"),
            once=True,
        )

    assert sleeps == [2]
    assert any(r.message == "sqs_retry_error" for r in caplog.records)


def test_worker_run_continues_after_bad_message(monkeypatch):
    """Un mensaje corrupto no detiene el procesamiento del siguiente."""
    acks = []
    retries = []
    messages = [
        {"MessageId": "msg-bad", "ReceiptHandle": "rh-bad", "Body": "{broken"},
        {
            "MessageId": "msg-ok",
            "ReceiptHandle": "rh-ok",
            "Body": json.dumps(_sample_event("evt-ok")),
        },
    ]

    monkeypatch.setattr(worker, "receive", lambda _url: messages)
    monkeypatch.setattr(worker, "acknowledge", lambda _u, rh: acks.append(rh))
    monkeypatch.setattr(
        worker, "retry_later", lambda _u, m: retries.append(m["MessageId"])
    )

    processed = []
    worker.run(
        "http://q",
        lambda p: processed.append(p["event_id"]),
        logging.getLogger("test-worker"),
        once=True,
    )
    assert retries == ["msg-bad"]
    assert processed == ["evt-ok"]
    assert acks == ["rh-ok"]


def test_worker_run_transport_error_sleeps_and_retries(monkeypatch):
    """Ante error de transporte SQS espera 2 segundos antes de reintentar."""
    sleeps = []
    calls = {"receive": 0}

    def _receive(_url):
        calls["receive"] += 1
        if calls["receive"] == 1:
            raise ClientError(
                {"Error": {"Code": "ServiceUnavailable"}}, "ReceiveMessage"
            )
        raise KeyboardInterrupt()

    monkeypatch.setattr(worker, "receive", _receive)
    monkeypatch.setattr(worker.time, "sleep", sleeps.append)

    with pytest.raises(KeyboardInterrupt):
        worker.run(
            "http://q",
            lambda _p: None,
            logging.getLogger("test-worker"),
            once=False,
        )

    assert sleeps == [2]


def test_worker_run_propagates_not_implemented(monkeypatch):
    """NotImplementedError no debe ser capturado por el worker."""
    msg = {
        "MessageId": "msg-ni",
        "ReceiptHandle": "rh-ni",
        "Body": json.dumps(_sample_event()),
    }
    monkeypatch.setattr(worker, "receive", lambda _url: [msg])
    monkeypatch.setattr(worker, "acknowledge", lambda _u, _rh: None)
    monkeypatch.setattr(worker, "retry_later", lambda _u, _m: None)

    def _not_ready(_payload):
        raise NotImplementedError("Falta completar I1")

    with pytest.raises(NotImplementedError):
        worker.run(
            "http://q",
            _not_ready,
            logging.getLogger("test-worker"),
            once=True,
        )


def test_worker_run_ack_failure_does_not_log_acknowledged(monkeypatch, caplog):
    """AUD-10: fallo en acknowledge se registra sin marcar acknowledged."""
    msg = {
        "MessageId": "msg-ack-err",
        "ReceiptHandle": "rh-ack-err",
        "Body": json.dumps(_sample_event()),
    }
    monkeypatch.setattr(worker, "receive", lambda _url: [msg])

    def _fail_ack(_url, _rh):
        raise BotoCoreError()

    monkeypatch.setattr(worker, "acknowledge", _fail_ack)
    monkeypatch.setattr(worker, "retry_later", lambda _u, _m: None)

    with caplog.at_level(logging.INFO, logger="test-worker"):
        worker.run(
            "http://q",
            lambda _p: None,
            logging.getLogger("test-worker"),
            once=True,
        )

    statuses = [getattr(r, "status", None) for r in caplog.records]
    assert "acknowledged" not in statuses
    assert "BotoCoreError" in statuses
