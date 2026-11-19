"""Receptor FastAPI: autenticación, validación y aceptación durable por SQS."""
import os  # Recupera el secreto sin incluirlo en el repositorio.
import json  # Decodifica después de verificar los bytes.
from fastapi import FastAPI, Request, HTTPException  # Interfaz HTTP del curso.
from starlette.concurrency import run_in_threadpool  # No bloquea event loop.
from botocore.exceptions import BotoCoreError, ClientError  # Fallos del SDK.
from fenix.utils.crypto import verify  # Verificación HMAC y ventana temporal.
from fenix.utils.errors import ValidationError  # Error semántico de dominio.
from fenix.utils.state import load  # Lectura del inventario persistido.
from fenix.services.events import validate  # Validación del esquema cerrado.
from fenix.services.sqs import queue_url, publish  # Publicación en cola SQS.

app = FastAPI(title="Fénix receptor de alertas")  # Un servidor, un worker.


@app.post("/webhook", status_code=202)
async def webhook(request: Request) -> dict:
    """Recibe un evento firmado y contesta 202 solo después de encolarlo.

    Args:
        request: Petición con X-Webhook-Signature y cuerpo de hasta 16 KiB.
    Returns:
        JSON con status=accepted, event_id y message_id de SQS.
    Raises:
        HTTPException: 401 firma; 400 JSON; 413 tamaño; 422 contrato; 503 SQS.
    Side Effects:
        Publica en SQS; no repara recursos ni llama a Telegram/Slack aquí.
    Notes:
        Leer bytes completos basta para el laboratorio; un proxy limitaría
        antes el tamaño de la transferencia en un despliegue real.
    """
    raw = await request.body()  # Se conservan los bytes originales.
    if len(raw) > 16384:  # Mantiene acotados los ejemplos didácticos.
        raise HTTPException(413, "Cuerpo demasiado grande")  # No encola.
    sig_headers = request.headers.getlist("x-webhook-signature")  # Cabeceras.
    header = sig_headers[0] if len(sig_headers) == 1 else ""  # Rechaza >1.
    secret = os.environ.get("WEBHOOK_SECRET", "")  # Lee secreto del entorno.
    try:  # Evalúa autenticidad e integridad sobre los bytes crudos.
        verify(secret, header, raw)  # Verifica HMAC-SHA256 y ventana ±300 s.
    except ValidationError as error:  # Traduce rechazo criptográfico a 401.
        raise HTTPException(401, "Firma inválida o expirada") from error
    try:  # Recién después de autenticar se decodifica el texto y el JSON.
        payload = json.loads(raw.decode("utf-8"))  # Exige UTF-8 y JSON válido.
    except (UnicodeDecodeError, json.JSONDecodeError) as error:  # Sintaxis.
        raise HTTPException(400, "JSON o codificación inválidos") from error
    try:  # Carga el inventario local desde disco sin bloquear el event loop.
        inventory = await run_in_threadpool(load, "data/inventory.json")
    except (OSError, json.JSONDecodeError) as error:  # Estado local dañado.
        raise HTTPException(503, "Inventario local no disponible") from error
    try:  # Valida contrato cerrado y pertenencia del recurso al inventario.
        if not isinstance(payload, dict):  # Rechaza listas o primitivos JSON.
            raise ValidationError("El evento debe ser un objeto JSON")
        event = validate(payload, inventory)  # Devuelve copia validada.
    except ValidationError as error:  # Esquema o recurso no autorizado -> 422.
        raise HTTPException(422, "Contrato de evento inválido") from error
    try:  # Ejecuta Boto3 síncrono en threadpool sin bloquear FastAPI.
        prefix = os.environ["QUEUE_PREFIX"]  # Prefijo de colas del grupo.
        url = await run_in_threadpool(queue_url, prefix)  # Resuelve QueueUrl.
        message_id = await run_in_threadpool(publish, url, event)  # Encola.
    except (
        ClientError,
        BotoCoreError,
        ValidationError,
        KeyError,
        OSError,
        ValueError,
        TypeError,
    ) as error:  # Si SQS no está disponible, jamás promete aceptación 202.
        raise HTTPException(503, "Servicio SQS no disponible") from error
    return {  # Responde 202 únicamente tras confirmar send_message en SQS.
        "status": "accepted",
        "event_id": event["event_id"],
        "message_id": message_id,
    }
