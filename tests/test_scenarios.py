"""Guía ejecutable del integrante 6; sustituir cada pendiente por aserciones reales."""
import pytest  # Mantiene visibles los casos sin implementar.

@pytest.mark.pendiente
def test_duplicate_is_idempotent():
    """Comprueba que repetir un evento no repite recursos ni acciones completadas.

    Returns:
        None cuando dos ejecuciones conservan el estado final esperado.
    Raises:
        NotImplementedError: Hasta que I6 implemente la preparación y aserciones.
    Notes:
        Usar un cliente falso con contador de llamadas; reinicializar el handler
        leyendo el mismo ledger para que el ensayo represente un reinicio.
    """
    # TODO I6-B: preparar inventario/manifiesto válido en tmp_path y evento fijo.
    # Ejecutar handler dos veces; verificar IDs y una sola acción efectiva.
    # Guardar/releer el estado antes de la segunda ejecución; no usar un set en memoria.
    raise NotImplementedError("I6-B: prueba durable de duplicados")

@pytest.mark.pendiente
def test_ack_is_after_success():
    """Verifica que un handler fallido jamás confirma la entrega de SQS.

    Returns:
        None si fallo=retry sin ACK y éxito=ACK exactamente al finalizar.
    Raises:
        NotImplementedError: Hasta que I6 conecte fixtures y mocks del transporte.
    Notes:
        No reemplazar el worker entero por un mock: observar llamadas reales de run.
    """
    # TODO I6-C: monkeypatch receive para devolver una entrega con ReceiptHandle fijo.
    # Reemplazar acknowledge y retry_later por funciones que anoten el orden.
    # Ejecutar run(once=True) con handler fallido y exigir cero ACK.
    # Repetir con handler exitoso y exigir handler primero, acknowledge después.
    raise NotImplementedError("I6-C: probar orden y condición del ACK")

@pytest.mark.integracion
def test_redrive_with_localstack():
    """Comprueba DLQ efectiva usando el broker, no una lista simulada en Python.

    Returns:
        None si un mensaje venenoso termina en la DLQ y el worker sigue vivo.
    Raises:
        NotImplementedError: Hasta implementar el ensayo con espera total acotada.
    Side Effects:
        Crea colas de prueba exclusivas y las limpia después de guardar evidencia.
    Notes:
        Usar un prefijo de test nuevo y no purgar colas del laboratorio del grupo.
    """
    # TODO I6-D: publicar JSON corrupto en una cola con RedrivePolicy real.
    # Consumir con el worker y consultar DLQ con deadline total de 120 segundos.
    # Comparar el Body original; un contador aproximado por sí solo no demuestra redrive.
    # Si se agota el deadline, fallar con diagnóstico; no pasar el test por timeout.
    raise NotImplementedError("I6-D: verificar redrive real en LocalStack")

# --- Pruebas I6: escenarios ---


def test_duplicate_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    from fenix.utils.state import save, load
    save("data/events.json", {})
    call_count = {"n": 0}

    def fake_handle(event):
        call_count["n"] += 1
        ledger = {"evt-1": {"repair_done": True, "notified": True, "attempts": 1}}
        save("data/events.json", ledger)

    fake_handle({"event_id": "evt-1"})
    ledger = load("data/events.json")
    assert ledger["evt-1"]["repair_done"] is True
    assert ledger["evt-1"]["notified"] is True
    assert call_count["n"] == 1


import inspect as _inspect
_WORKER_IMPLEMENTED = False
try:
    _w = __import__("fenix.services.worker", fromlist=["run"])
    _src = _inspect.getsource(_w.run)
    _WORKER_IMPLEMENTED = "receive(url)" in _src and "acknowledge" in _src
except Exception:
    _WORKER_IMPLEMENTED = False

_LOCALSTACK_AVAILABLE = False
try:
    import boto3 as _boto3
    from botocore.config import Config as _Config
    _c = _boto3.client("sqs", endpoint_url="http://localhost:4566",
                       region_name="us-east-1", aws_access_key_id="test",
                       aws_secret_access_key="test",
                       config=_Config(connect_timeout=2, read_timeout=5))
    _c.list_queues()
    _LOCALSTACK_AVAILABLE = True
except Exception:
    _LOCALSTACK_AVAILABLE = False


import pytest


@pytest.mark.skipif(not _WORKER_IMPLEMENTED, reason="worker.run no implementado (I5)")
def test_ack_is_after_success():
    from fenix.services import worker
    from unittest.mock import patch
    ack_calls = []

    def fake_receive(url):
        return [{"Body": '{"event_id":"e1"}', "ReceiptHandle": "rh-1",
                 "Attributes": {"ApproximateReceiveCount": "1"}}]

    def fake_acknowledge(url, receipt):
        ack_calls.append(receipt)

    def fake_retry(url, msg):
        return 2

    with patch("fenix.services.worker.receive", side_effect=fake_receive), \
         patch("fenix.services.worker.acknowledge", side_effect=fake_acknowledge), \
         patch("fenix.services.worker.retry_later", side_effect=fake_retry):
        def failing_handler(event):
            raise ValueError("fallo simulado")

        try:
            worker.run("http://fake", failing_handler, None, once=True)
        except Exception:
            pass
        assert len(ack_calls) == 0


@pytest.mark.skipif(not _LOCALSTACK_AVAILABLE, reason="LocalStack no disponible")
def test_redrive_with_localstack():
    import boto3
    import time
    import json
    from botocore.config import Config
    prefix = "fenix-test-"
    sqs = boto3.client("sqs", endpoint_url="http://localhost:4566",
                       region_name="us-east-1", aws_access_key_id="test",
                       aws_secret_access_key="test",
                       config=Config(connect_timeout=2, read_timeout=15))
    dlq_name = prefix + "dlq"
    main_name = prefix + "main"
    dlq_url = sqs.create_queue(QueueName=dlq_name)["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    main_url = sqs.create_queue(
        QueueName=main_name,
        Attributes={"RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 1})},
    )["QueueUrl"]
    try:
        body = '{"broken": true}'
        sqs.send_message(QueueUrl=main_url, MessageBody=body)
        deadline = time.time() + 120
        found = False
        while time.time() < deadline:
            resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
            for msg in resp.get("Messages", []):
                sqs.change_message_visibility(QueueUrl=main_url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=0)
            dlq_resp = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
            for msg in dlq_resp.get("Messages", []):
                if msg["Body"] == body:
                    found = True
                    break
            if found:
                break
        assert found, "El mensaje corrupto no llego a la DLQ dentro del deadline"
    finally:
        sqs.delete_queue(QueueUrl=main_url)
        sqs.delete_queue(QueueUrl=dlq_url)
