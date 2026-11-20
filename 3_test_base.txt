"""Pruebas operativas del apoyo entregado y del Rol 4 (F14 y F16)."""
import gzip  # Abre archivos históricos rotados y comprimidos (.gz).
import json  # Verifica que cada línea del log sea JSON válido.
import pytest  # Define marcas y casos de prueba repetibles.
import requests  # Simula errores de transporte HTTP en notify.
from fenix.services.notifications import notify  # Adaptador externo Rol 4.
from fenix.services.sqs import publish  # Validación de límites de delay.
from fenix.utils.errors import PermanentError, RetryableError  # Excepciones.
from fenix.utils.logging_json import setup  # Pipeline QueueHandler + Gzip.
from fenix.utils.state import load, save  # Persistencia atómica local.


@pytest.mark.base
def test_state_survives_restart(tmp_path):
    """Comprueba dos escrituras y una nueva lectura del estado.

    Args:
        tmp_path: Carpeta temporal aislada provista por pytest.
    """
    path = str(tmp_path / "state.json")  # No usa datos reales del grupo.
    save(path, {"repair_done": True})  # Simula la finalización de un paso.
    assert load(path)["repair_done"] is True  # Comprueba lectura posterior.
    save(path, {"repair_done": True, "notified": True})  # Añade una etapa.
    assert load(path)["notified"] is True  # La nueva versión reemplaza.


@pytest.mark.base
def test_corrupt_state_is_not_empty(tmp_path):
    """Evita que un JSON roto se trate como estado inicial.

    Args:
        tmp_path: Carpeta temporal aislada de pytest.
    """
    path = tmp_path / "broken.json"  # Construye una ruta de ensayo.
    path.write_text("{bad", encoding="utf-8")  # Introduce fallo deliberado.
    with pytest.raises(json.JSONDecodeError):  # El helper no lo silencia.
        load(str(path))  # Intenta leer el archivo dañado.


@pytest.mark.base
def test_delay_is_bounded():
    """Rechaza un delay de más de quince minutos antes de conectar con SQS."""
    with pytest.raises(ValueError):  # Esta prueba no necesita Docker.
        publish("unused", {"event_id": "evt-001"}, delay=901)  # Caso borde.


@pytest.mark.base
def test_json_log_context(tmp_path, monkeypatch):
    """Verifica el contexto de correlación y el cierre de QueueListener.

    Args:
        tmp_path: Carpeta donde guardar el log de prueba.
        monkeypatch: Fixture que restaura el directorio después del ensayo.
    """
    monkeypatch.chdir(tmp_path)  # Aísla los logs del ensayo.
    logger, listener = setup("test")  # Inicia un único pipeline.
    try:  # Asegura vaciar la cola aunque falle la aserción posterior.
        logger.info("recibido", extra={"event_id": "evt-001"})  # Contexto.
    finally:  # La limpieza no decide éxito del negocio.
        listener.stop()  # Drena los registros pendientes.
    row = json.loads(next((tmp_path / "logs").glob("*.jsonl")).read_text())
    assert row["event_id"] == "evt-001"  # Conserva correlación.
    assert row["timestamp"].endswith("+00:00")  # Fecha UTC explícita.


@pytest.mark.base
def test_f16_gzip_rotation_and_json_lines(tmp_path, monkeypatch):
    """Comprueba F16: rotación con tamaño reducido, lectura .gz y JSON UTC.

    Args:
        tmp_path: Carpeta temporal donde generar y rotar logs.
        monkeypatch: Fixture para aislar el directorio de trabajo.
    """
    monkeypatch.chdir(tmp_path)  # Ejecuta dentro del directorio temporal.
    logger, listener = setup("rotacion", max_bytes=250)  # Fuerza rotación.
    try:  # Emite los cinco niveles con contexto correlacionado sin secretos.
        for idx, level in enumerate(
            ("debug", "info", "warning", "error", "critical"), start=1
        ):
            getattr(logger, level)(
                f"evento_prueba_{idx}",
                extra={
                    "event_id": "evt-rot-01",
                    "message_id": f"msg-{idx}",
                    "resource_id": "node-01",
                    "attempt": 1,
                    "action": "test_rotate",
                    "status": "ok",
                },
            )
    finally:  # Drena QueueListener para asegurar cierre de archivos .gz.
        listener.stop()
    gz_files = list((tmp_path / "logs").glob("*.jsonl.*.gz"))  # Históricos.
    assert gz_files  # Debe haberse generado al menos un archivo .gz rotado.
    with gzip.open(gz_files[0], "rt", encoding="utf-8") as handle:  # Lee .gz.
        lines = [line.strip() for line in handle if line.strip()]
    assert lines  # El archivo comprimido contiene líneas legibles.
    for line in lines:  # Cada línea del histórico debe ser JSON UTC válido.
        parsed = json.loads(line)
        assert parsed["timestamp"].endswith("+00:00")
        assert parsed["event_id"] == "evt-rot-01"
        assert parsed["resource_id"] == "node-01"


@pytest.mark.base
def test_f14_notify_telegram_and_token_privacy(monkeypatch):
    """Comprueba F14 y seguridad: 429/timeout reintentables sin filtrar token.

    Args:
        monkeypatch: Fixture para configurar variables y simular requests.post.
    """
    secret_token = "123456:TOKEN_SECRETO_NO_FILTRAR"  # Token ficticio test.
    monkeypatch.setenv("NOTIFY_PROVIDER", "telegram")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", secret_token)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100999")

    class FakeResp:  # Respuesta HTTP simulada para el adaptador requests.
        """Modela código HTTP, cabeceras y cuerpo JSON de Telegram."""

        def __init__(self, status: int, body: dict, headers: dict = None):
            self.status_code = status
            self._body = body
            self.headers = headers or {}
            self.text = json.dumps(body)

        def json(self):
            """Devuelve el diccionario simulado de respuesta."""
            return self._body

        def ok_status(self) -> bool:
            """Indica si el código HTTP simulado es 200."""
            return self.status_code == 200

    def _timeout_post(url, **kwargs):
        _ = kwargs
        raise requests.Timeout(f"Timeout conectando a {url}")

    monkeypatch.setattr(
        "fenix.services.notifications.requests.post", _timeout_post
    )
    with pytest.raises(RetryableError) as exc_info:  # Timeout es transitorio.
        notify("Nodo reparado")
    assert secret_token not in str(exc_info.value)  # Jamás filtra el token.
    monkeypatch.setattr(
        "fenix.services.notifications.requests.post",
        lambda *_a, **_k: FakeResp(429, {"ok": False}, {"Retry-After": "4"}),
    )
    with pytest.raises(RetryableError):  # HTTP 429 -> RetryableError.
        notify("Nodo reparado")
    monkeypatch.setattr(
        "fenix.services.notifications.requests.post",
        lambda *_a, **_k: FakeResp(401, {"ok": False}),
    )
    with pytest.raises(PermanentError):  # HTTP 401 -> PermanentError.
        notify("Nodo reparado")
    monkeypatch.setattr(
        "fenix.services.notifications.requests.post",
        lambda *_a, **_k: FakeResp(200, {"ok": True}),
    )
    assert notify("Nodo reparado") is None  # Éxito confirmado por Telegram.
