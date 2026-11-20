"""Contrato de eventos de autorreparación; integrante 1."""
from fenix.utils.errors import ValidationError, PermanentError
from fenix.utils.validation import identifier, utc_text


def validate(event: dict, inventory: dict) -> dict:
    """Valida esquema cerrado y resuelve exclusivamente aliases autorizados.

    Args:
        event: JSON objeto con schema_version, event_id, event_type, resource_id,
            occurred_at y simulate_failures.
        inventory: Mapa alias -> recurso, preparado por setup-demo y revisado por el grupo.
    Returns:
        Copia validada del evento; no modifica inventory.
    Raises:
        ValidationError: Tipo, campo, timestamp o recurso no permitido.
    Notes:
        event_type solo permite instance.stopped y bucket.tags_missing.
        simulate_failures solo es de laboratorio: entero de 0 a 5, excluyendo bool.
    """
    if not isinstance(event, dict):
        raise ValidationError("El evento debe ser un diccionario.")

    required_fields = {
        "schema_version",
        "event_id",
        "event_type",
        "resource_id",
        "occurred_at",
        "simulate_failures"
    }

    if set(event) != required_fields:
        raise ValidationError("Campos del evento incorrectos.")

    if type(event["schema_version"]) is not int or event["schema_version"] != 1:
        raise ValidationError("Versión de esquema inválida.")

    identifier(event["event_id"])
    identifier(event["resource_id"])
    utc_text(event["occurred_at"])

    event_type = event["event_type"]

    allowed_types = {
       "instance.stopped": "ec2",
       "bucket.tags_missing": "s3"
    }

    if not isinstance(event_type, str) or event_type not in allowed_types:
        raise ValidationError("Tipo de evento no permitido.")

    resource_id = event["resource_id"]

    if resource_id not in inventory:
        raise ValidationError("Recurso no autorizado.")

    resource = inventory[resource_id]

    if not isinstance(resource, dict) or resource.get("type") != allowed_types[event_type]:
        raise ValidationError("El evento no coincide con el recurso.")

    failures = event["simulate_failures"]

    if type(failures) is not int or not 0 <= failures <= 5:
        raise ValidationError("Cantidad de fallos inválida.")

    return event.copy()


def register_event(event: dict, ledger: dict) -> dict:
    """Vincula un evento validado con su identificador.

    Permite reintentos con contenido idéntico y rechaza
    la reutilización del identificador con otro contenido.
    """
    event_id = event["event_id"]

    if event_id in ledger:
        record = ledger[event_id]

        if record["event"] != event:
            raise PermanentError(
                "Conflicto de event_id: contenido diferente."
            )

        return record

    record = {
        "event": event.copy(),
        "attempts": 0,
        "repair_done": False,
        "notified": False
    }

    ledger[event_id] = record

    return record