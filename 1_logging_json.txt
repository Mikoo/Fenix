"""Apoyo reutilizable de Tritón y Vulcano; verificado por el integrante 4."""
from datetime import datetime, timezone  # Emite marcas temporales en UTC.
import gzip  # Comprime los archivos históricos rotados.
import json  # Serializa cada registro como una línea JSON válida.
import logging  # Biblioteca estándar de observabilidad de Python.
from logging.handlers import (  # Manejadores asíncronos y de rotación.
    QueueHandler,
    QueueListener,
    RotatingFileHandler,
)
import os  # Obtiene PID y elimina el archivo plano tras comprimirlo.
from pathlib import Path  # Crea el directorio local de logs.
import queue  # Cola en memoria para desacoplar la escritura en disco.
import shutil  # Copia flujos binarios hacia el archivo Gzip sin saturar RAM.

CONTEXT_FIELDS = (  # Lista cerrada de campos de correlación autorizados.
    "event_id",
    "message_id",
    "resource_id",
    "attempt",
    "action",
    "status",
)


class JSONFormatter(logging.Formatter):
    """Serializa campos permitidos; no copia indiscriminadamente __dict__.

    Notes:
        Cada proceso usa su propio archivo para evitar rotaciones concurrentes.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Convierte un registro en JSON compatible con json.loads.

        Args:
            record: Registro del logger con extras públicos opcionales.
        Returns:
            Una línea JSON con fecha UTC, nivel, evento y contexto autorizado.
        Raises:
            TypeError: Si un extra no es serializable; usar escalares públicos.
        """
        created_utc = datetime.fromtimestamp(
            record.created, timezone.utc
        ).isoformat()  # Convierte epoch del registro a ISO 8601 UTC (+00:00).
        data = {
            "timestamp": created_utc,
            "level": record.levelname,
            "message": record.getMessage(),
            "process": record.process,
            "thread": record.threadName,
        }  # Campos base obligatorios de cada línea de telemetría.
        for name in CONTEXT_FIELDS:  # Agrega contexto o null si no aplica.
            data[name] = getattr(record, name, None)
        return json.dumps(data, ensure_ascii=False)  # Nunca incluye secretos.


def gzip_name(name: str) -> str:
    """Calcula el nombre comprimido de un histórico rotado.

    Args:
        name: Nombre propuesto por RotatingFileHandler.
    Returns:
        El mismo nombre con sufijo .gz.
    """
    return name + ".gz"  # Permite identificar backups comprimidos.


def gzip_rotate(source: str, dest: str) -> None:
    """Comprime el archivo cerrado y luego elimina el histórico original.

    Args:
        source: Archivo plano cerrado por el manejador rotativo.
        dest: Ruta de destino del archivo comprimido .gz.
    Returns:
        None.
    Raises:
        OSError: Si falla la lectura, compresión o eliminación.
    Notes:
        Esta versión didáctica no promete publicación atómica del gzip.
    """
    with open(source, "rb") as origin, gzip.open(dest, "wb") as target:
        shutil.copyfileobj(origin, target)  # Comprime por bloques en stream.
    os.remove(source)  # Borra el plano recién tras cerrar el archivo .gz.


def setup(
    name: str, max_bytes: int = 2 * 1024 * 1024
) -> tuple[logging.Logger, QueueListener]:
    """Inicia el pipeline JSON en un archivo exclusivo de este proceso.

    Args:
        name: Nombre público del componente, por ejemplo fenix-worker.
        max_bytes: Tamaño máximo antes de rotar (2 MiB por defecto).
    Returns:
        Tupla (logger, listener); el llamador ejecuta listener.stop en finally.
    Raises:
        OSError: Si no se puede crear logs/ o abrir el archivo de salida.
    Side Effects:
        Inicia un hilo secundario y abre un manejador rotativo por proceso.
    """
    Path("logs").mkdir(exist_ok=True)  # Carpeta runtime ignorada por Git.
    log_path = f"logs/{name}-{os.getpid()}.jsonl"  # Archivo único por PID.
    sink = RotatingFileHandler(
        log_path, maxBytes=max_bytes, backupCount=3, encoding="utf-8"
    )  # Rota al alcanzar max_bytes conservando hasta 3 históricos .gz.
    sink.setFormatter(JSONFormatter())  # Aplica esquema JSON UTC cerrado.
    sink.namer = gzip_name  # Define extensión .gz para cada rotación.
    sink.rotator = gzip_rotate  # Comprime y limpia el archivo plano rotado.
    records: queue.Queue = queue.Queue()  # Cola en RAM (no reemplaza a SQS).
    logger = logging.getLogger(name)  # Instancia asociada al componente.
    logger.setLevel(logging.DEBUG)  # Habilita los 5 niveles desde DEBUG.
    logger.handlers.clear()  # Evita handlers duplicados al reinicializar.
    logger.propagate = False  # No duplica salidas en el logger raíz.
    logger.addHandler(QueueHandler(records))  # Productor no bloquea en disco.
    listener = QueueListener(records, sink)  # Hilo dedicado a escritura I/O.
    listener.start()  # Inicia consumo asíncrono de registros en segundo plano.
    return logger, listener  # El cierre limpio pertenece al bloque finally.
