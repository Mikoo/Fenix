"""Mensajeria del integrante 2; colas estandar SQS y backoff acotado."""
import json  # Convierte eventos y politicas a texto JSON.
from fenix.config.settings import client  # Garantiza el endpoint local.


def ensure_queues(prefix: str) -> tuple[str, str]:
    """Crea primero la DLQ y despues configura la cola de trabajo.

    Args:
        prefix: Prefijo aprobado como fenix-grupo01 o efimero-grupo01.
    Returns:
        Tupla (work_url, dlq_url), tomada de las respuestas de Boto3.
    Raises:
        ClientError: Si SQS rechaza una operacion; la CLI lo registra.
    Side Effects:
        Crea dos colas y actualiza RedrivePolicy en la cola principal.
    Notes:
        Repetir con la misma configuracion conserva ambas colas.
    """
    sqs = client("sqs")
    dead = sqs.create_queue(
        QueueName=f"{prefix}-dlq",
        Attributes={"MessageRetentionPeriod": "1209600"},
    )["QueueUrl"]
    arn = sqs.get_queue_attributes(
        QueueUrl=dead, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    work = sqs.create_queue(
        QueueName=f"{prefix}-events",
        Attributes={
            "VisibilityTimeout": "45",
            "MessageRetentionPeriod": "345600",
        },
    )["QueueUrl"]
    redrive_policy = {"deadLetterTargetArn": arn, "maxReceiveCount": "3"}
    sqs.set_queue_attributes(
        QueueUrl=work,
        Attributes={"RedrivePolicy": json.dumps(redrive_policy)},
    )
    return work, dead


def queue_url(prefix: str) -> str:
    """Busca la URL actual de la cola de trabajo ya creada.

    Args:
        prefix: Prefijo configurado por el grupo.
    Returns:
        URL retornada por SQS.
    Raises:
        ClientError: Si la cola todavia no existe.
    """
    return client("sqs").get_queue_url(
        QueueName=f"{prefix}-events"
    )["QueueUrl"]


def publish(url: str, event: dict, delay: int = 0) -> str:
    """Publica el evento sin confirmarlo ni procesarlo.

    Args:
        url: QueueUrl obtenida del SDK.
        event: Diccionario validado, serializable a JSON, con event_id.
        delay: Retardo inicial entre 0 y 900 segundos, inclusive.
    Returns:
        MessageId asignado por SQS.
    Raises:
        ValueError: Si el retardo esta fuera de rango.
        TypeError: Si event no se puede serializar.
        ClientError: Si falla el envio a SQS.
    """
    if not 0 <= delay <= 900:
        raise ValueError("DelaySeconds debe estar entre 0 y 900")
    response = client("sqs").send_message(
        QueueUrl=url,
        MessageBody=json.dumps(event),
        DelaySeconds=delay,
        MessageAttributes={
            "EventId": {"DataType": "String", "StringValue": event["event_id"]}
        },
    )
    return response["MessageId"]


def receive(url: str) -> list[dict]:
    """Obtiene como maximo un mensaje para simplificar la defensa inicial.

    Args:
        url: URL de la cola principal.
    Returns:
        Lista vacia o un mensaje crudo con Body, ReceiptHandle y Attributes.
    Raises:
        ClientError: Si SQS no responde; el bucle externo decide continuar.
    """
    response = client("sqs").receive_message(
        QueueUrl=url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
        VisibilityTimeout=45,
        MessageSystemAttributeNames=["ApproximateReceiveCount"],
        MessageAttributeNames=["All"],
    )
    return response.get("Messages", [])


def acknowledge(url: str, receipt: str) -> None:
    """Elimina exclusivamente la entrega actual despues del exito durable.

    Args:
        url: URL de la cola fuente.
        receipt: ReceiptHandle de la recepcion mas reciente, no MessageId.
    Raises:
        ClientError: Si SQS no pudo confirmar la eliminacion.
    """
    client("sqs").delete_message(QueueUrl=url, ReceiptHandle=receipt)


def retry_later(url: str, message: dict) -> int:
    """Programa el proximo intento sin copiar ni borrar el mensaje original.

    Args:
        url: URL de la cola fuente.
        message: Mensaje con ReceiptHandle y ApproximateReceiveCount.
    Returns:
        Segundos asignados: min(30, 2 ** numero_de_recepcion).
    Raises:
        ClientError: Si no se pudo cambiar la visibilidad.
    """
    count = int(message["Attributes"]["ApproximateReceiveCount"])
    segundos = min(30, 2 ** count)
    client("sqs").change_message_visibility(
        QueueUrl=url,
        ReceiptHandle=message["ReceiptHandle"],
        VisibilityTimeout=segundos,
    )
    return segundos
