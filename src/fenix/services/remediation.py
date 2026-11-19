"""Acciones convergentes a completar por integrante 3 y coordinadas por 5."""
from botocore.exceptions import BotoCoreError, ClientError  # Errores del SDK.
from fenix.config.settings import client  # Cliente Boto3 restringido a local.
from fenix.utils.state import load, save  # Persistencia atómica del ledger.
from fenix.services.cloud_support import aws_tags  # Formato TagSet de AWS.
from fenix.services.events import validate  # Revalidación del contrato en I5.
from fenix.services.notifications import notify  # Envío de aviso externo I4.
from fenix.utils.errors import (  # Excepciones semánticas del dominio Fénix.
    ProjectError,
    RetryableError,
    PermanentError,
)

TRANSIENT_CODES = {  # Códigos de error AWS que ameritan reintento por SQS.
    "RequestLimitExceeded",
    "Throttling",
    "ThrottlingException",
    "SlowDown",
    "ServiceUnavailable",
    "InternalError",
    "RequestTimeout",
}


def _classify_aws_error(error: Exception) -> ProjectError:
    """Clasifica un fallo de Boto3 en transitorio o permanente sin secretos.

    Args:
        error: Excepción ClientError o BotoCoreError capturada del SDK.
    Returns:
        RetryableError ante throttling/5xx/red; PermanentError en otros casos.
    """
    if isinstance(error, BotoCoreError):  # Fallo de red o transporte local.
        return RetryableError("Fallo de transporte al consultar LocalStack")
    if isinstance(error, ClientError):  # Inspecciona código y status HTTP.
        resp = error.response  # Diccionario estructurado devuelto por Boto3.
        code = resp.get("Error", {}).get("Code", "")  # Código simbólico AWS.
        status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if code in TRANSIENT_CODES or status == 429 or status >= 500:  # 5xx.
            return RetryableError(f"Fallo transitorio de AWS: {code}")
        return PermanentError(f"Rechazo permanente de AWS: {code}")
    return PermanentError("Error inesperado al operar recurso cloud")


def _extract_instance(response: dict) -> dict:
    """Obtiene el primer descriptor de instancia desde describe_instances.

    Args:
        response: Respuesta cruda de Boto3 EC2 describe_instances.
    Returns:
        Diccionario de la instancia encontrada.
    Raises:
        PermanentError: Si la respuesta no contiene la instancia solicitada.
    """
    reservations = response.get("Reservations", [])  # Lista de reservas EC2.
    if not reservations or not reservations[0].get("Instances"):  # Vacío.
        raise PermanentError("La instancia EC2 del inventario no existe")
    return reservations[0]["Instances"][0]  # Única instancia del recurso.


def _repair_ec2(resource: dict) -> str:
    """Verifica y arranca de forma convergente una instancia EC2 mock.

    Args:
        resource: Descriptor con id (InstanceId) y expected_tags opcionales.
    Returns:
        "already_ok" si ya estaba running; "repaired" si pasó a running.
    Raises:
        RetryableError: Si está en transición (pending/stopping) o falla red.
        PermanentError: Si está terminada, no existe o contradice tags.
    """
    instance_id = resource.get("id")  # Lee InstanceId autorizado del grupo.
    if not isinstance(instance_id, str) or not instance_id:  # Valida ID.
        raise PermanentError("InstanceId inválido en el inventario")
    ec2 = client("ec2")  # Construye cliente apuntando a localhost:4566.
    try:  # Consulta estado real antes de decidir cualquier acción.
        current = _extract_instance(
            ec2.describe_instances(InstanceIds=[instance_id])
        )
    except (ClientError, BotoCoreError) as error:  # Traduce fallo del SDK.
        raise _classify_aws_error(error) from error
    expected = resource.get("expected_tags")  # Etiquetas esperadas del grupo.
    raw_tags = current.get("Tags")  # Etiquetas actuales si el mock las envía.
    if isinstance(expected, dict) and isinstance(raw_tags, list) and raw_tags:
        actual = {t["Key"]: t["Value"] for t in raw_tags if "Key" in t}
        for key in ("ManagedBy", "Project", "GroupId"):  # Identidad de grupo.
            if key in expected and actual.get(key) != expected[key]:  # Ajeno.
                raise PermanentError("La instancia no pertenece al grupo")
    state = current.get("State", {}).get("Name", "")  # Estado actual EC2.
    if state == "running":  # Ya converge al estado deseado sin tocar AWS.
        return "already_ok"
    if state in ("pending", "stopping"):  # Transición: esperar otra entrega.
        raise RetryableError(f"Instancia EC2 en transición: {state}")
    if state in ("terminated", "shutting-down"):  # No puede arrancarse más.
        raise PermanentError(f"Instancia EC2 irrecuperable: {state}")
    if state != "stopped":  # Cualquier otro estado no previsto se rechaza.
        raise PermanentError(f"Estado EC2 no soportado: {state}")
    try:  # Solicita arranque convergente (jamás reboot_instances).
        ec2.start_instances(InstanceIds=[instance_id])  # Inicia el nodo mock.
        after = _extract_instance(
            ec2.describe_instances(InstanceIds=[instance_id])
        )
    except (ClientError, BotoCoreError) as error:  # Clasifica error de API.
        raise _classify_aws_error(error) from error
    new_state = after.get("State", {}).get("Name", "")  # Estado post-start.
    if new_state == "running":  # Confirmado por API que volvió a running.
        return "repaired"
    if new_state in ("pending", "stopped", "stopping"):  # Aún no convergió.
        raise RetryableError("La instancia EC2 aún no observa estado running")
    raise PermanentError(f"Estado EC2 inválido tras arranque: {new_state}")


def _repair_s3(resource: dict) -> str:
    """Restaura etiquetas obligatorias en S3 preservando etiquetas ajenas.

    Args:
        resource: Descriptor con id (BucketName) y expected_tags obligatorias.
    Returns:
        "already_ok" si ya tenía las tags; "repaired" si fueron restauradas.
    Raises:
        RetryableError: Si falla temporalmente S3 o la verificación posterior.
        PermanentError: Si el bucket no existe o faltan expected_tags.
    """
    bucket = resource.get("id")  # Nombre del bucket autorizado en inventario.
    expected = resource.get("expected_tags")  # Mapa de etiquetas requeridas.
    if not isinstance(bucket, str) or not bucket:  # Valida nombre de bucket.
        raise PermanentError("Nombre de bucket inválido en el inventario")
    if not isinstance(expected, dict) or not expected:  # Exige política tags.
        raise PermanentError("Faltan etiquetas esperadas para el bucket S3")
    s3 = client("s3")  # Construye cliente S3 path-style hacia LocalStack.
    try:  # Lee las etiquetas actuales antes de modificar el recurso.
        tag_set = s3.get_bucket_tagging(Bucket=bucket).get("TagSet", [])
    except ClientError as error:  # Solo NoSuchTagSet equivale a tags vacías.
        code = error.response.get("Error", {}).get("Code", "")
        if code != "NoSuchTagSet":  # NoSuchBucket u otro error se clasifica.
            raise _classify_aws_error(error) from error
        tag_set = []  # El bucket existe pero no tiene conjunto de etiquetas.
    except BotoCoreError as error:  # Error de conexión con el emulador.
        raise _classify_aws_error(error) from error
    current = {item["Key"]: item["Value"] for item in tag_set}  # Mapa actual.
    if (
        "GroupId" in current
        and "GroupId" in expected
        and current["GroupId"] != expected["GroupId"]
    ):  # Bloquea adopción si el bucket tiene explícitamente otro GroupId.
        raise PermanentError("El bucket pertenece a otro grupo")
    merged = {**current, **expected}  # Preserva tags ajenas y pisa requeridas.
    if current == merged:  # No hay desvío respecto a las tags obligatorias.
        return "already_ok"
    try:  # Aplica la mezcla y vuelve a consultar para confirmar convergencia.
        s3.put_bucket_tagging(
            Bucket=bucket, Tagging={"TagSet": aws_tags(merged)}
        )
        check_set = s3.get_bucket_tagging(Bucket=bucket).get("TagSet", [])
    except (ClientError, BotoCoreError) as error:  # Clasifica fallo de S3.
        raise _classify_aws_error(error) from error
    after = {item["Key"]: item["Value"] for item in check_set}  # Post-lectura.
    if any(after.get(k) != v for k, v in merged.items()):  # Verifica mezcla.
        raise RetryableError("Las etiquetas S3 aún no reflejan la reparación")
    return "repaired"


def repair(resource: dict, event_type: str) -> str:
    """Acerca un recurso autorizado a su estado deseado y verifica el cambio.

    Args:
        resource: Registro de inventario con type/id y etiquetas esperadas.
        event_type: instance.stopped o bucket.tags_missing ya validado.
    Returns:
        "repaired" si cambió el estado; "already_ok" si ya cumplía la política.
    Raises:
        RetryableError: La API falla temporalmente o aún no converge.
        PermanentError: Recurso inexistente o configuración no autorizada.
    Side Effects:
        Arranca una instancia EC2 mock detenida o restaura tags de un bucket.
    Notes:
        No usar reboot_instances: repetir un reinicio no es convergencia.
        Un estado pending requiere otra recepción, sin esperas largas.
    """
    if not isinstance(resource, dict):  # Exige descriptor válido.
        raise PermanentError("Descriptor de recurso inválido")
    res_type = resource.get("type")  # Tipo de recurso en inventory.json.
    if res_type == "ec2" and event_type == "instance.stopped":  # Caso nodo.
        return _repair_ec2(resource)
    if res_type == "s3" and event_type == "bucket.tags_missing":  # Caso S3.
        return _repair_s3(resource)
    raise PermanentError("Combinación de recurso y evento no soportada")


def handle(event: dict) -> None:
    """Completa reparación y notificación usando un registro durable.

    Args:
        event: Objeto recibido de SQS, que se vuelve a validar aquí.
    Returns:
        None solo cuando reparación y aviso quedaron registrados con éxito.
    Raises:
        ValidationError: Si el evento fue inyectado a SQS y no es válido.
        RetryableError: Si la simulación o una dependencia temporal falla.
        PermanentError: Conflicto de event_id o recurso no autorizado.
        NotImplementedError: Mientras falte completar el bloque del Rol 5.
    Side Effects:
        Actualiza recursos, data/events.json y el canal de comunicación.
    Notes:
        Un solo proceso modifica el ledger. Un timeout al notificar puede
        repetir un aviso: no hay transacción común entre JSON, AWS y red.
    """
    _ = (event, load, save, validate, notify)  # Reservado para Rol 5 (I5-B).
    raise NotImplementedError("I5-B: integrar etapas y ledger durable")
