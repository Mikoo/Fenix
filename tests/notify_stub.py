"""Stub de notify para pruebas de I6 sin enviar mensajes reales."""
from fenix.utils.errors import RetryableError, PermanentError


class NotifyStub:
    """Reemplazo de fenix.services.notifications.notify para pruebas."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.mode: str = "ok"
        self._remaining_rate_limits: int = 0

    def set_mode(self, mode: str, count: int = 1) -> None:
        self.mode = mode
        self._remaining_rate_limits = count

    def __call__(self, text: str) -> None:
        self.calls.append(text)
        if self.mode == "rate_limit" and self._remaining_rate_limits > 0:
            self._remaining_rate_limits -= 1
            raise RetryableError("429 Too Many Requests (simulado)")
        if self.mode == "permanent":
            raise PermanentError("4xx definitivo (simulado)")

    def reset(self) -> None:
        self.calls.clear()
        self.mode = "ok"
        self._remaining_rate_limits = 0


TESTED_FUNCTIONS: list[str] = []


def register(function_name: str) -> None:
    if function_name not in TESTED_FUNCTIONS:
        TESTED_FUNCTIONS.append(function_name)


def tested_report() -> str:
    return "\n".join(TESTED_FUNCTIONS)