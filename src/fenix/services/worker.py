"""Plantilla central de integración: un worker, un mensaje por vez."""
import json  # Convierte Body solo dentro del control por mensaje.
import time  # Evita un bucle intensivo si el emulador no responde.
from botocore.exceptions import BotoCoreError, ClientError
from fenix.services.sqs import receive, acknowledge, retry_later
from fenix.utils.errors import ProjectError


def _extract_attempt(message: dict) -> int:
    """Obtiene el número de intento actual de forma segura (default 1)."""
    attrs = message.get("Attributes")
    if not isinstance(attrs, dict):
        return 1
    raw_count = attrs.get("ApproximateReceiveCount", 1)
    if isinstance(raw_count, bool):
        return 1
    try:
        attempt = int(raw_count)
    except (TypeError, ValueError):
        return 1
    return attempt if attempt >= 1 else 1


def _handle_retry(
    url: str,
    message: dict,
    logger,
    context: dict,
) -> None:
    """Programa el reintento en SQS aislando fallos del broker o atributos."""
    try:
        retry_later(url, message)
    except (
        ClientError,
        BotoCoreError,
        ProjectError,
        KeyError,
        ValueError,
        TypeError,
        OSError,
    ) as retry_error:
        logger.error(
            "sqs_retry_error",
            extra={
                **context,
                "action": "retry_later",
                "status": type(retry_error).__name__,
            },
        )
        time.sleep(2)


def _process_message(url: str, message: dict, handler, logger) -> None:
    """Procesa un mensaje SQS individual preservando correlación y ACK."""
    context = {
        "event_id": None,
        "message_id": message.get("MessageId"),
        "resource_id": None,
        "attempt": _extract_attempt(message),
    }
    try:
        raw_body = message.get("Body")
        if not isinstance(raw_body, str):
            raise ValueError("Body ausente o no textual en mensaje SQS")
        payload = json.loads(raw_body)
        if not isinstance(payload, dict):
            raise ValueError("El payload debe ser un objeto JSON")
        if isinstance(payload.get("event_id"), str):
            context["event_id"] = payload["event_id"]
        if isinstance(payload.get("resource_id"), str):
            context["resource_id"] = payload["resource_id"]
        handler(payload)
    except (
        json.JSONDecodeError,
        ProjectError,
        ClientError,
        BotoCoreError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        logger.error(
            "mensaje_fallido",
            extra={
                **context,
                "action": "retry_later",
                "status": type(error).__name__,
            },
        )
        _handle_retry(url, message, logger, context)
        return

    try:
        acknowledge(url, message.get("ReceiptHandle"))
        logger.info(
            "mensaje_procesado",
            extra={
                **context,
                "action": "acknowledge",
                "status": "acknowledged",
            },
        )
    except (ClientError, BotoCoreError) as ack_error:
        logger.error(
            "sqs_ack_error",
            extra={
                **context,
                "action": "acknowledge",
                "status": type(ack_error).__name__,
            },
        )


def run(url: str, handler, logger, once: bool = False) -> None:
    """Consume mensajes y confirma únicamente resultados durables.

    Args:
        url: URL de la cola de trabajo, obtenida por get_queue_url.
        handler: Callable handler(event) que revalida y ejecuta negocio.
        logger: Logger JSON del proceso worker.
        once: Si es True, realiza una sola consulta y termina.
    Returns:
        None al terminar la consulta única o al detenerse mediante Ctrl+C.
    Raises:
        NotImplementedError: Si falta completar una plantilla dependiente.
    Side Effects:
        Recibe mensajes, ejecuta el handler y cambia o confirma recepción.
    Notes:
        El parseo ocurre por mensaje para que un JSON corrupto no termine
        el worker ni oculte los otros mensajes.
    """
    while True:
        try:
            messages = receive(url)
        except (ClientError, BotoCoreError) as error:
            logger.error(
                "sqs_receive_error",
                extra={"action": "receive", "status": type(error).__name__},
            )
            if once:
                return
            time.sleep(2)
            continue

        for message in messages:
            _process_message(url, message, handler, logger)

        if once:
            return
