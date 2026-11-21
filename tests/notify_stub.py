"""Stub de notify para pruebas de I6 sin enviar mensajes reales."""
from fenix.utils.errors import PermanentError, RetryableError


class NotifyStub:
    """Reemplazo de fenix.services.notifications.notify para pruebas."""

    def __init__(self) -> None:
        """Inicializa el registro de llamadas en modo ok."""
        self.calls: list[str] = []
        self.mode: str = "ok"
        self._remaining_rate_limits: int = 0

    def set_mode(self, mode: str, count: int = 1) -> None:
        """Configura el comportamiento simulado del canal de avisos."""
        self.mode = mode
        self._remaining_rate_limits = count

    def __call__(self, text: str) -> None:
        """Registra el aviso y simula 429 transitorio o 4xx permanente."""
        self.calls.append(text)
        if self.mode == "rate_limit" and self._remaining_rate_limits > 0:
            self._remaining_rate_limits -= 1
            raise RetryableError("429 Too Many Requests (simulado)")
        if self.mode == "permanent":
            raise PermanentError("4xx definitivo (simulado)")

    def reset(self) -> None:
        """Reinicia llamadas y estado del stub entre pruebas."""
        self.calls.clear()
        self.mode = "ok"
        self._remaining_rate_limits = 0


TESTED_FUNCTIONS: list[str] = []


def register(function_name: str) -> None:
    """Registra una funcion cubierta dentro de la lista de trazabilidad."""
    if function_name not in TESTED_FUNCTIONS:
        TESTED_FUNCTIONS.append(function_name)


def tested_report() -> str:
    """Devuelve el listado legible de funciones cubiertas."""
    return "\n".join(TESTED_FUNCTIONS)
