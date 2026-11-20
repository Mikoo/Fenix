"""Contratos de validación del integrante 1."""
import re  # Comprueba el formato de los identificadores.
from datetime import datetime, timezone  # Trabaja con fechas conscientes de UTC.
from fenix.utils.errors import ValidationError  # Error público del dominio.


def identifier(value: str) -> str:
    """Valida un identificador corto y devuelve su texto sin modificar.

    Args:
        value: Texto entre 3 y 32 caracteres; letras minúsculas, dígitos o guion.
    Returns:
        El identificador original válido.
    Raises:
        ValidationError: Si el tipo o el patrón no cumplen el contrato.
    Examples:
        identifier("grupo-01") devuelve "grupo-01"; "../ajeno" debe fallar.
    """
    if not isinstance(value, str):
        raise ValidationError("El identificador debe ser texto.")

    if not re.fullmatch(r"[a-z0-9-]{3,32}", value):
        raise ValidationError("Identificador inválido.")

    return value


def ttl_seconds(value: str) -> int:
    """Valida el TTL didáctico empleado en la opción de entornos efímeros.

    Args:
        value: Texto con un entero entre 60 y 600 segundos inclusive.
    Returns:
        El número entero de segundos.
    Raises:
        ValidationError: Si no es entero o está fuera de los límites.
    Notes:
        La CLI traduce ValidationError a parser.error para salir con código 2.
    """
    if not isinstance(value, str):
        raise ValidationError("El TTL debe ser texto.")

    try:
        ttl = int(value)
    except ValueError as exc:
        raise ValidationError("El TTL debe ser un entero.") from exc

    if not 60 <= ttl <= 600:
        raise ValidationError("El TTL debe estar entre 60 y 600 segundos.")

    return ttl


def utc_text(value: str) -> datetime:
    """Interpreta una fecha UTC explícita sin aceptar fechas ingenuas.

    Args:
        value: ISO 8601 terminado en Z o con offset +00:00.
    Returns:
        Fecha con zona UTC; por ejemplo 2026-10-22T18:00:00+00:00.
    Raises:
        ValidationError: Si falta zona, hay offset no nulo o formato inválido.
    """
    if not isinstance(value, str):
        raise ValidationError("La fecha debe ser texto.")
    if not value.endswith(("Z", "+00:00")):
        raise ValidationError("La fecha debe indicar UTC explícitamente.")
    try:
        fecha = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError("Formato de fecha inválido.") from exc

    if fecha.tzinfo is None or fecha.utcoffset() != timezone.utc.utcoffset(fecha):
        raise ValidationError("La fecha debe estar en UTC.")

    return fecha
