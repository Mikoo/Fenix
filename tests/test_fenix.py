"""Pruebas del Rol 3 (HMAC, Receptor HTTP y Reparación Convergente EC2/S3)."""
import asyncio  # Ejecuta peticiones ASGI hacia FastAPI sin abrir puertos.
import time  # Genera timestamps dentro y fuera de la ventana de 300 s.
from botocore.exceptions import ClientError  # Simula caída o error de AWS.
import httpx  # Cliente ASGI oficial listado en requirements.txt.
import pytest  # Ejecuta aserciones y verificación de excepciones.
from fenix.services.receiver import app  # Aplicación HTTP del receptor Fénix.
from fenix.services.remediation import repair  # Motor convergente EC2 y S3.
from fenix.utils.crypto import signature, verify  # Primitivas HMAC-SHA256.
from fenix.utils.errors import (  # Excepciones públicas del dominio.
    PermanentError,
    RetryableError,
    ValidationError,
)


def _post_webhook(
    body: bytes, headers: dict[str, str] | None = None
) -> httpx.Response:
    """Envía un POST en memoria a /webhook mediante transporte ASGI de httpx.

    Args:
        body: Bytes crudos a transmitir en el cuerpo HTTP.
        headers: Cabeceras opcionales (como X-Webhook-Signature).
    Returns:
        Objeto Response de httpx con código de estado y JSON de respuesta.
    """
    async def _send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)  # Conecta directo a FastAPI.
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.post(
                "/webhook", content=body, headers=headers or {}
            )

    return asyncio.run(_send())  # Ejecuta corrutina limpia por cada caso.


@pytest.mark.pendiente
def test_signature_authenticates_raw_bytes():
    """Acepta el original y rechaza la misma información con otros bytes.

    Returns:
        None si el verificador respeta el contrato HMAC.
    Raises:
        AssertionError: Si no se rechaza el cuerpo alterado.
    Notes:
        La clave que sigue es solo una fixture pública sin uso fuera del test.
    """
    secret = "clave-ficticia-solo-test"  # No es un secreto de ningún servicio.
    body = b'{"x":1}'  # Espacios distintos producen firmas distintas.
    digest = signature(secret, 1000, body)  # El reloj de la prueba es fijo.
    header = f"t=1000,v1={digest}"  # Mismo formato que el emisor real.
    verify(secret, header, body, now=1000)  # Original debe aceptarse.
    with pytest.raises(ValidationError):  # JSON equivalente cambia los bytes.
        verify(secret, header, b'{"x": 1}', now=1000)  # Cuerpo modificado.


@pytest.mark.pendiente
def test_f04_f05_webhook_signature_and_window(monkeypatch):
    """Comprueba F04 y F05: firma ausente, alterada, vieja o >16 KiB sin SQS.

    Args:
        monkeypatch: Fixture de pytest para aislar variables y publicación.
    """
    secret = "clave-ficticia-solo-test"  # Secreto de prueba de >= 16 chars.
    monkeypatch.setenv("WEBHOOK_SECRET", secret)  # Configura entorno local.
    published = []  # Registra si hubo intento indebido de publicar en SQS.
    monkeypatch.setattr(
        "fenix.services.receiver.publish",
        lambda url, evt: published.append((url, evt)) or "msg-1",
    )
    body = b'{"schema_version":1}'  # Payload binario de ensayo.
    now = int(time.time())  # Timestamp actual dentro de ventana.
    valid_sig = signature(secret, now, body)  # Firma válida para body.
    assert _post_webhook(body).status_code == 401  # Sin firma -> 401.
    old_sig = signature(secret, now - 3600, body)  # Timestamp hace una hora.
    res_old = _post_webhook(
        body, {"X-Webhook-Signature": f"t={now - 3600},v1={old_sig}"}
    )
    assert res_old.status_code == 401  # F04: fuera de ventana de 300 s -> 401.
    res_mod = _post_webhook(
        b'{"schema_version": 1}',
        {"X-Webhook-Signature": f"t={now},v1={valid_sig}"},
    )
    assert res_mod.status_code == 401  # F05: mismo JSON con otros bytes.
    assert _post_webhook(b"a" * 16385).status_code == 413  # Supera 16 KiB.
    assert not published  # Ningún rechazo pudo publicar en la cola SQS.


@pytest.mark.pendiente
def test_f06_f13_webhook_json_list_and_sqs_status(monkeypatch):
    """Comprueba F06 (400 JSON roto, 422 lista), F13 (503 SQS caído) y 202.

    Args:
        monkeypatch: Fixture de pytest para simular inventario, esquema y SQS.
    """
    secret = "clave-ficticia-solo-test"  # Secreto de laboratorio para tests.
    monkeypatch.setenv("WEBHOOK_SECRET", secret)  # Inyecta secreto en entorno.
    monkeypatch.setenv("QUEUE_PREFIX", "fenix-test")  # Prefijo de cola mock.
    now = int(time.time())  # Epoch actual para firmar cada petición.
    for payload, code in ((b"{bad_json", 400), (b'[{"id":1}]', 422)):  # F06.
        sig = signature(secret, now, payload)  # Firma válida sobre bytes.
        res = _post_webhook(
            payload, {"X-Webhook-Signature": f"t={now},v1={sig}"}
        )
        assert res.status_code == code  # Verifica 400 y 422 respectivamente.
    valid_body = b'{"event_id":"evt-node-001"}'  # Objeto JSON para F13 y 202.
    headers = {
        "X-Webhook-Signature": (
            f"t={now},v1={signature(secret, now, valid_body)}"
        )
    }
    monkeypatch.setattr("fenix.services.receiver.load", lambda _: {})
    monkeypatch.setattr(
        "fenix.services.receiver.validate", lambda payload, _: payload
    )

    def _broken_sqs(_):
        raise ClientError({"Error": {"Code": "InternalError"}}, "GetQueueUrl")

    monkeypatch.setattr("fenix.services.receiver.queue_url", _broken_sqs)
    assert _post_webhook(valid_body, headers).status_code == 503  # F13: 503.
    monkeypatch.setattr(
        "fenix.services.receiver.queue_url", lambda _: "http://q"
    )
    monkeypatch.setattr(
        "fenix.services.receiver.publish", lambda _u, _e: "msg-99"
    )
    res_202 = _post_webhook(valid_body, headers)  # Envío con SQS sano.
    assert res_202.status_code == 202  # Confirmado por SQS -> HTTP 202.
    assert res_202.json()["message_id"] == "msg-99"  # Devuelve MessageId real.


@pytest.mark.pendiente
def test_f02_repair_ec2_convergent(monkeypatch):
    """Comprueba convergencia EC2 en repair(): stopped, running y estados.

    Args:
        monkeypatch: Fixture para simular respuestas del cliente Boto3 EC2.
    """
    state = {"name": "stopped", "starts": 0}  # Estado inicial detenido.

    class FakeEC2:  # Doble de prueba determinista para EC2 mock.
        """Cliente falso EC2 para describe_instances y start_instances."""

        def describe_instances(self, **kwargs):
            """Devuelve la instancia en su estado simulado actual."""
            _ = kwargs
            return {
                "Reservations": [
                    {"Instances": [{"State": {"Name": state["name"]}}]}
                ]
            }

        def start_instances(self, **kwargs):
            """Transiciona de stopped a running e incrementa el contador."""
            _ = kwargs
            state["starts"] += 1
            state["name"] = "running"

    monkeypatch.setattr(
        "fenix.services.remediation.client", lambda _: FakeEC2()
    )
    node = {"type": "ec2", "id": "i-mock01"}  # Descriptor de inventario EC2.
    assert repair(node, "instance.stopped") == "repaired"  # Arranca el nodo.
    assert state["starts"] == 1  # Se invocó start_instances una sola vez.
    assert repair(node, "instance.stopped") == "already_ok"  # Ya está running.
    assert state["starts"] == 1  # No volvió a invocar start_instances.
    state["name"] = "pending"  # Simula estado intermedio de transición.
    with pytest.raises(RetryableError):  # Debe pedir reintento posterior.
        repair(node, "instance.stopped")
    state["name"] = "terminated"  # Simula instancia destruida.
    with pytest.raises(PermanentError):  # Fallo definitivo hacia DLQ.
        repair(node, "instance.stopped")


@pytest.mark.pendiente
def test_f03_repair_s3_preserves_foreign_tags(monkeypatch):
    """Comprueba F03: restaura tags obligatorias y preserva tags ajenas en S3.

    Args:
        monkeypatch: Fixture para simular get/put_bucket_tagging en S3.
    """
    store = {  # El bucket perdió Owner pero tiene una etiqueta ajena extra.
        "tags": [
            {"Key": "ManagedBy", "Value": "ppa2"},
            {"Key": "Project", "Value": "fenix"},
            {"Key": "GroupId", "Value": "grupo01"},
            {"Key": "CostCenter", "Value": "finanzas"},
        ],
        "writes": 0,
    }

    class FakeS3:  # Doble de prueba determinista para S3 tagging.
        """Cliente falso S3 que conserva las etiquetas en memoria."""

        def get_bucket_tagging(self, **kwargs):
            """Retorna el TagSet actual del bucket simulado."""
            _ = kwargs
            return {"TagSet": list(store["tags"])}

        def put_bucket_tagging(self, **kwargs):
            """Actualiza el TagSet y cuenta la escritura."""
            store["writes"] += 1
            store["tags"] = list(kwargs["Tagging"]["TagSet"])

    monkeypatch.setattr(
        "fenix.services.remediation.client", lambda _: FakeS3()
    )
    bucket = {
        "type": "s3",
        "id": "fenix-grupo01-demo",
        "expected_tags": {
            "ManagedBy": "ppa2",
            "Project": "fenix",
            "GroupId": "grupo01",
            "Owner": "grupo01",
        },
    }
    assert repair(bucket, "bucket.tags_missing") == "repaired"  # Restaura.
    final_map = {item["Key"]: item["Value"] for item in store["tags"]}
    assert final_map["Owner"] == "grupo01"  # Recuperó la etiqueta obligatoria.
    assert final_map["CostCenter"] == "finanzas"  # F03: preservó tag ajena.
    assert repair(bucket, "bucket.tags_missing") == "already_ok"  # Convergió.
    assert store["writes"] == 1  # No reescribió tags cuando ya estaban bien.
