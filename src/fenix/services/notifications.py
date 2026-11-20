"""Adaptador HTTP guiado de notificaciones externas (Rol 4 - Integrante 4)."""
import os  # Recupera credenciales desde el entorno sin hardcodear secretos.
import requests  # Cliente HTTP con soporte de timeouts de conexión y lectura.
from fenix.utils.errors import PermanentError, RetryableError  # Excepciones.


def _parse_retry_after(header_val: str | None) -> int:
    """Interpreta Retry-After y acota la espera sugerida a 30 segundos.

    Args:
        header_val: Valor opcional de la cabecera HTTP Retry-After.
    Returns:
        Entero entre 1 y 30 segundos (por defecto 2 si no es numérico).
    """
    if isinstance(header_val, str) and header_val.strip().isdigit():  # Entero.
        return max(1, min(30, int(header_val.strip())))  # Limita hasta 30 s.
    return 2  # Demora base conservadora cuando el proveedor no informa.


def _post_external(url: str, payload: dict, is_telegram: bool) -> None:
    """Envía petición POST con timeout=(2, 5) sin filtrar nunca la URL/token.

    Args:
        url: Endpoint privado de Telegram o webhook de Slack.
        payload: Cuerpo JSON público de hasta 1000 caracteres.
        is_telegram: True para validar JSON ok=true; False para texto ok.
    Raises:
        RetryableError: Ante timeout, caída de red, HTTP 429 o HTTP 5xx.
        PermanentError: Ante rechazo 4xx definitivo o contrato incumplido.
    """
    try:  # Aplica timeout estricto de 2 s de conexión y 5 s de lectura.
        response = requests.post(url, json=payload, timeout=(2, 5))
    except (requests.Timeout, requests.ConnectionError):  # Oculta URL privada.
        raise RetryableError(
            "Fallo transitorio de red o timeout al notificar"
        ) from None
    except requests.RequestException:  # Cualquier otro error de transporte.
        raise PermanentError(
            "Error de configuración HTTP al enviar notificación"
        ) from None
    status = response.status_code  # Código HTTP devuelto por el proveedor.
    if status == 429 or status >= 500:  # Limitación de tasa o fallo servidor.
        wait_s = _parse_retry_after(response.headers.get("Retry-After"))
        raise RetryableError(
            f"Proveedor externo saturado o no disponible (retry={wait_s}s)"
        )
    if 400 <= status < 500:  # Credenciales inválidas o chat no autorizado.
        raise PermanentError(
            f"Rechazo permanente del proveedor de avisos (HTTP {status})"
        )
    if status != 200:  # Cualquier otro código fuera del contrato esperado.
        raise RetryableError(f"Estado HTTP inesperado del proveedor: {status}")
    if is_telegram:  # Telegram exige HTTP 200 y JSON con campo ok is True.
        try:  # Protege contra respuestas 200 que no sean JSON válido.
            body = response.json()
        except ValueError as error:  # Proxy o pasarela devolvió texto plano.
            raise RetryableError(
                "Respuesta no JSON recibida desde Telegram"
            ) from error
        if not isinstance(body, dict) or body.get("ok") is not True:  # Falla.
            raise PermanentError("Telegram rechazó el mensaje (ok != true)")
    elif response.text.strip() != "ok":  # Slack exige HTTP 200 y texto "ok".
        raise PermanentError("Slack no confirmó la entrega con texto ok")


def notify(text: str) -> None:
    """Envía un mensaje breve y valida la respuesta del proveedor elegido.

    Args:
        text: Resumen público de hasta 1000 caracteres, sin secretos.
    Returns:
        None únicamente después de confirmar éxito del proveedor.
    Raises:
        RetryableError: Timeout, conexión, 429 o respuesta 5xx.
        PermanentError: Configuración inválida, 4xx definitivo o rechazo.
    Side Effects:
        Publica en el chat/canal del grupo. Un timeout puede causar duplicados.
    Notes:
        El modo console sirve para desarrollo; no reemplaza evidencia real.
        No registra la excepción requests original para no exponer el token.
    """
    if not isinstance(text, str) or not text.strip():  # Valida antes de modo.
        raise PermanentError("El resumen de notificación no puede ser vacío")
    summary = text[:1000]  # Acota longitud máxima según contrato de sección 5.
    mode = os.getenv("NOTIFY_PROVIDER", "console").strip().lower()  # Canal.
    if mode == "console":  # Modo de ensayo local sin cuentas externas.
        print(summary)  # Imprime solo el resumen público acotado.
        return
    if mode == "telegram":  # Integración mediante Telegram Bot API.
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()  # Lee token bot.
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()  # Lee chat dest.
        if not token or not chat_id:  # Falla rápido si falta configuración.
            raise PermanentError(
                "Configurar TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID en .env"
            )
        url = f"https://api.telegram.org/bot{token}/sendMessage"  # Endpoint.
        _post_external(url, {"chat_id": chat_id, "text": summary}, True)
        return
    if mode == "slack":  # Integración alternativa mediante Incoming Webhook.
        webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()  # URL Slack.
        if not webhook_url.startswith("https://"):  # Exige esquema seguro.
            raise PermanentError("Configurar SLACK_WEBHOOK_URL válida en .env")
        _post_external(webhook_url, {"text": summary}, False)
        return
    raise PermanentError("Proveedor NOTIFY_PROVIDER desconocido")
