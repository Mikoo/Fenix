"""Plantilla central de integración: un worker, un mensaje por vez."""
import json  # Convierte Body solo dentro del control por mensaje.
import time  # Evita un bucle intensivo si el emulador no responde.
from botocore.exceptions import BotoCoreError, ClientError
from fenix.services.sqs import receive, acknowledge, retry_later
from fenix.utils.errors import ProjectError


def run(url: str, handler, logger, once: bool = False) -> None:
    """Consume mensajes y confirma únicamente resultados durables.

    Args:
        url: URL de la cola de trabajo, obtenida por get_queue_url.
        handler: Callable handler(event) que revalida y ejecuta el caso de negocio.
        logger: Logger JSON del proceso worker.
        once: Si es True, realiza una sola consulta y termina; útil para pruebas.
    Returns:
        None al terminar la consulta única o al detenerse mediante Ctrl+C.
    Raises:
        NotImplementedError: Si todavía no se completaron las plantillas del alumno.
    Side Effects:
        Recibe mensajes, ejecuta el handler y cambia o confirma su recepción.
    Notes:
        El parseo ocurre por mensaje para que un JSON corrupto no termine el
        worker ni oculte los otros mensajes.
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
            message_id = message.get("MessageId")
            receipt = message.get("ReceiptHandle")
            event_id = None
            resource_id = None
            try:
                raw_body = message.get("Body")
                if not isinstance(raw_body, str):
                    raise ValueError("Body ausente o no textual en mensaje SQS")
                payload = json.loads(raw_body)
                if not isinstance(payload, dict):
                    raise ValueError("El payload del mensaje debe ser un objeto JSON")
                raw_event_id = payload.get("event_id")
                if isinstance(raw_event_id, str):
                    event_id = raw_event_id
                raw_resource_id = payload.get("resource_id")
                if isinstance(raw_resource_id, str):
                    resource_id = raw_resource_id
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
                        "event_id": event_id,
                        "message_id": message_id,
                        "resource_id": resource_id,
                        "action": "retry_later",
                        "status": type(error).__name__,
                    },
                )
                try:
                    retry_later(url, message)
                except (ClientError, BotoCoreError) as retry_error:
                    logger.error(
                        "sqs_retry_error",
                        extra={
                            "event_id": event_id,
                            "message_id": message_id,
                            "resource_id": resource_id,
                            "action": "retry_later",
                            "status": type(retry_error).__name__,
                        },
                    )
                    time.sleep(2)
                continue

            try:
                acknowledge(url, receipt)
                logger.info(
                    "mensaje_procesado",
                    extra={
                        "event_id": event_id,
                        "message_id": message_id,
                        "resource_id": resource_id,
                        "action": "acknowledge",
                        "status": "ok",
                    },
                )
            except (ClientError, BotoCoreError) as ack_error:
                logger.error(
                    "sqs_ack_error",
                    extra={
                        "event_id": event_id,
                        "message_id": message_id,
                        "resource_id": resource_id,
                        "action": "acknowledge",
                        "status": type(ack_error).__name__,
                    },
                )

        if once:
            return
