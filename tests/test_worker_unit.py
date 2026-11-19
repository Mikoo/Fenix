"""Pruebas unitarias del Rol 5 (I5): Worker secuencial y ledger por etapas."""
import json
import logging
from botocore.exceptions import BotoCoreError, ClientError
import pytest

from fenix.services import remediation, worker
from fenix.utils.errors import PermanentError, RetryableError, ValidationError
from fenix.utils.state import load, save


def _sample_event(event_id: str = "evt-node-001", simulate_failures: int = 0) -> dict:
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


def test_handle_nominal_and_f11_idempotency(tmp_path, monkeypatch):
    """Completa reparación y aviso, persiste ledger y evita repetir al reenviar (F11)."""
    _setup_inventory(tmp_path, monkeypatch)
    event = _sample_event()
    calls = {"repair": 0, "notify": 0}

    monkeypatch.setattr(remediation, "validate", lambda evt, _inv: dict(evt))
    monkeypatch.setattr(
        remediation,
        "repair",
        lambda _res, _etype: calls.__setitem__("repair", calls["repair"] + 1) or "repaired",
    )
    monkeypatch.setattr(
        remediation,
        "notify",
        lambda _msg: calls.__setitem__("notify", calls["notify"] + 1),
    )

    remediation.handle(event)
    ledger = load("data/events.json")
    assert ledger["evt-node-001"]["attempts"] == 1
    assert ledger["evt-node-001"]["repair_done"] is True
    assert ledger["evt-node-001"]["repair_status"] == "repaired"
    assert ledger["evt-node-001"]["notified"] is True
    assert calls == {"repair": 1, "notify": 1}

    # F11: repetir el mismo evento completo no repite reparación ni aviso.
    remediation.handle(event)
    assert calls == {"repair": 1, "notify": 1}
    assert load("data/events.json")["evt-node-001"]["attempts"] == 1


def test_handle_f09_simulate_failures_and_attempts(tmp_path, monkeypatch):
    """Respeta simulate_failures=2 y completa en el intento 3 (F09)."""
    _setup_inventory(tmp_path, monkeypatch)
    event = _sample_event("evt-f09", simulate_failures=2)
    calls = {"repair": 0, "notify": 0}

    monkeypatch.setattr(remediation, "validate", lambda evt, _inv: dict(evt))
    monkeypatch.setattr(
        remediation,
        "repair",
        lambda _res, _etype: calls.__setitem__("repair", calls["repair"] + 1) or "repaired",
    )
    monkeypatch.setattr(
        remediation,
        "notify",
        lambda _msg: calls.__setitem__("notify", calls["notify"] + 1),
    )

    with pytest.raises(RetryableError):
        remediation.handle(event)
    with pytest.raises(RetryableError):
        remediation.handle(event)
    assert calls == {"repair": 0, "notify": 0}

    remediation.handle(event)
    ledger = load("data/events.json")["evt-f09"]
    assert ledger["attempts"] == 3
    assert ledger["repair_done"] is True
    assert ledger["notified"] is True
    assert calls == {"repair": 1, "notify": 1}


def test_handle_f12_recovery_between_repair_and_notify(tmp_path, monkeypatch):
    """Si falla notify tras reparar, el reintento solo notifica sin repetir repair (F12)."""
    _setup_inventory(tmp_path, monkeypatch)
    event = _sample_event("evt-f12")
    calls = {"repair": 0, "notify": 0}

    monkeypatch.setattr(remediation, "validate", lambda evt, _inv: dict(evt))
    monkeypatch.setattr(
        remediation,
        "repair",
        lambda _res, _etype: calls.__setitem__("repair", calls["repair"] + 1) or "repaired",
    )

    def _flaky_notify(_msg):
        calls["notify"] += 1
        if calls["notify"] == 1:
            raise RetryableError("Timeout temporal de Telegram")

    monkeypatch.setattr(remediation, "notify", _flaky_notify)

    with pytest.raises(RetryableError):
        remediation.handle(event)

    ledger_mid = load("data/events.json")["evt-f12"]
    assert ledger_mid["repair_done"] is True
    assert ledger_mid["notified"] is False

    remediation.handle(event)
    ledger_end = load("data/events.json")["evt-f12"]
    assert ledger_end["attempts"] == 2
    assert ledger_end["repair_done"] is True
    assert ledger_end["notified"] is True
    assert calls == {"repair": 1, "notify": 2}


def test_handle_f15_conflict_preserves_original_ledger(tmp_path, monkeypatch):
    """Mismo event_id con distinto payload lanza PermanentError sin alterar ledger (F15)."""
    _setup_inventory(tmp_path, monkeypatch)
    original = _sample_event("evt-f15")
    altered = _sample_event("evt-f15", simulate_failures=1)

    monkeypatch.setattr(remediation, "validate", lambda evt, _inv: dict(evt))
    monkeypatch.setattr(remediation, "repair", lambda _res, _etype: "repaired")
    monkeypatch.setattr(remediation, "notify", lambda _msg: None)

    remediation.handle(original)
    before = load("data/events.json")

    with pytest.raises(PermanentError):
        remediation.handle(altered)

    after = load("data/events.json")
    assert after == before


def test_handle_repair_error_propagates_without_marking_done(tmp_path, monkeypatch):
    """Un error de reparación no marca repair_done ni llama a notify."""
    _setup_inventory(tmp_path, monkeypatch)
    event = _sample_event("evt-rep-err")
    notified = []

    monkeypatch.setattr(remediation, "validate", lambda evt, _inv: dict(evt))
    monkeypatch.setattr(
        remediation,
        "repair",
        lambda _res, _etype: (_ for _ in ()).throw(PermanentError("Instancia terminada")),
    )
    monkeypatch.setattr(remediation, "notify", lambda msg: notified.append(msg))

    with pytest.raises(PermanentError):
        remediation.handle(event)

    ledger = load("data/events.json")["evt-rep-err"]
    assert ledger["attempts"] == 1
    assert ledger["repair_done"] is False
    assert ledger["notified"] is False
    assert notified == []


def test_worker_run_ack_order_and_once(monkeypatch):
    """Ejecuta handler antes de acknowledge y respeta once=True."""
    order = []
    msg = {
        "MessageId": "msg-1",
        "ReceiptHandle": "rh-1",
        "Body": json.dumps(_sample_event()),
    }
    monkeypatch.setattr(worker, "receive", lambda _url: [msg])
    monkeypatch.setattr(worker, "acknowledge", lambda _u, rh: order.append(("ack", rh)))
    monkeypatch.setattr(worker, "retry_later", lambda _u, _m: order.append(("retry", None)))

    def _handler(payload):
        order.append(("handler", payload["event_id"]))

    worker.run("http://q", _handler, logging.getLogger("test-worker"), once=True)
    assert order == [("handler", "evt-node-001"), ("ack", "rh-1")]


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
    """JSON inválido, payload no dict y errores del handler llaman retry_later sin ACK."""
    acks = []
    retries = []
    msg = {"MessageId": "msg-err", "ReceiptHandle": "rh-err", "Body": body}

    monkeypatch.setattr(worker, "receive", lambda _url: [msg])
    monkeypatch.setattr(worker, "acknowledge", lambda _u, rh: acks.append(rh))
    monkeypatch.setattr(worker, "retry_later", lambda _u, m: retries.append(m["MessageId"]))

    def _handler(_payload):
        if exc is not None:
            raise exc

    worker.run("http://q", _handler, logging.getLogger("test-worker"), once=True)
    assert acks == []
    assert retries == ["msg-err"]


def test_worker_run_continues_after_bad_message(monkeypatch):
    """Un mensaje corrupto no detiene el procesamiento del siguiente mensaje."""
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
    monkeypatch.setattr(worker, "retry_later", lambda _u, m: retries.append(m["MessageId"]))

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
            raise ClientError({"Error": {"Code": "ServiceUnavailable"}}, "ReceiveMessage")
        raise KeyboardInterrupt()

    monkeypatch.setattr(worker, "receive", _receive)
    monkeypatch.setattr(worker.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(KeyboardInterrupt):
        worker.run("http://q", lambda _p: None, logging.getLogger("test-worker"), once=False)

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
        worker.run("http://q", _not_ready, logging.getLogger("test-worker"), once=True)


def test_worker_run_ack_failure_does_not_crash(monkeypatch):
    """Un fallo de red en acknowledge se registra sin derribar el worker."""
    msg = {
        "MessageId": "msg-ack-err",
        "ReceiptHandle": "rh-ack-err",
        "Body": json.dumps(_sample_event()),
    }
    monkeypatch.setattr(worker, "receive", lambda _url: [msg])
    monkeypatch.setattr(
        worker,
        "acknowledge",
        lambda _u, _rh: (_ for _ in ()).throw(BotoCoreError()),
    )
    monkeypatch.setattr(worker, "retry_later", lambda _u, _m: None)

    worker.run("http://q", lambda _p: None, logging.getLogger("test-worker"), once=True)
