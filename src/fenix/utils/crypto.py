"""Firma de apoyo y verificación a completar por el integrante 3."""
import hashlib  # Selecciona SHA-256 para autenticar el contenido.
import hmac  # Usa una comparación segura para las firmas.
import time  # Comprueba la ventana temporal.
from fenix.utils.errors import ValidationError  # Rechazo público de firma.


def signature(secret: str, timestamp: int, body: bytes) -> str:
    """Firma exactamente timestamp, punto y cuerpo binario original.

    Args:
        secret: Secreto compartido de 16 a 256 caracteres, del entorno.
        timestamp: Unix epoch en segundos, igual al enviado en la cabecera.
        body: Bytes exactos enviados por HTTP; no reserializar el JSON.
    Returns:
        HMAC-SHA256 hexadecimal de 64 caracteres.
    Raises:
        ValidationError: Si el secreto o los parámetros están fuera de rango.
    """
    if not isinstance(secret, str) or not 16 <= len(secret) <= 256:  # Rango.
        raise ValidationError(
            "Configurar WEBHOOK_SECRET entre 16 y 256 caracteres"
        )
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, int)
        or not isinstance(body, bytes)
    ):  # Rechaza bool disfrazado de int o cuerpo no binario.
        raise ValidationError("Parámetros de firma inválidos")
    signed = str(timestamp).encode("ascii") + b"." + body  # Prefijo firmado.
    return hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()


def verify(
    secret: str, header: str, body: bytes, now: int | None = None
) -> None:
    """Rechaza cabeceras ambiguas y firmas o fechas inválidas.

    Args:
        secret: Secreto compartido del entorno, nunca del payload.
        header: Cabecera exacta t=<epoch>,v1=<hex> sin claves duplicadas.
        body: Bytes del cuerpo antes de llamar a json.loads.
        now: Reloj inyectable para pruebas; None usa time.time.
    Returns:
        None si las comprobaciones son satisfactorias.
    Raises:
        ValidationError: Formato inválido, firma o ventana mayor a 300 s.
    Notes:
        La ventana de tiempo mitiga replay antiguo. No impide repetir un evento
        válido dentro de la ventana; event_id e idempotencia cubren ese caso.
    """
    if (
        not isinstance(header, str)
        or not 68 <= len(header) <= 128
        or not isinstance(body, bytes)
    ):  # Acota tipos y longitud máxima de cabecera contra DoS en memoria.
        raise ValidationError("Cabecera o cuerpo de firma inválidos")
    parts = header.split(",")  # Separa exactamente los dos campos del header.
    if len(parts) != 2:  # Exige t y v1 una sola vez, sin comas sobrantes.
        raise ValidationError("Formato de cabecera de firma inválido")
    fields: dict[str, str] = {}  # Acumula claves para detectar duplicados.
    for item in parts:  # Analiza cada par clave=valor de la cabecera.
        if "=" not in item:  # Rechaza segmentos malformados sin signo igual.
            raise ValidationError("Segmento de firma malformado")
        key, value = item.split("=", 1)  # Divide solo en el primer igual.
        if key in fields or not key or not value:  # Bloquea claves repetidas.
            raise ValidationError("Clave de firma duplicada o vacía")
        fields[key] = value  # Registra el valor asociado a la clave.
    if set(fields) != {"t", "v1"}:  # Exige únicamente las claves t y v1.
        raise ValidationError("Claves de firma no reconocidas")
    raw_ts, digest = fields["t"], fields["v1"]  # Extrae timestamp y hash hex.
    if not raw_ts.isascii() or not raw_ts.isdigit():  # Solo dígitos enteros.
        raise ValidationError("Timestamp de firma no entero")
    timestamp = int(raw_ts)  # Convierte epoch validado a entero.
    if timestamp <= 0 or str(timestamp) != raw_ts:  # Bloquea ceros a la izq.
        raise ValidationError("Timestamp de firma ambiguo o no positivo")
    hex_chars = "0123456789abcdef"  # Conjunto hexadecimal en minúsculas.
    if len(digest) != 64 or any(c not in hex_chars for c in digest):  # 64 hex.
        raise ValidationError("Formato hexadecimal v1 inválido")
    current = int(time.time()) if now is None else now  # Reloj actual o test.
    if (
        isinstance(current, bool)
        or not isinstance(current, int)
        or abs(current - timestamp) > 300
    ):  # Verifica ventana temporal de ±300 segundos contra replay/drift.
        raise ValidationError("Firma fuera de la ventana permitida")
    expected = signature(secret, timestamp, body)  # Recalcula sobre raw bytes.
    if not hmac.compare_digest(expected, digest):  # Compara en tiempo fijo.
        raise ValidationError("Firma HMAC inválida")
