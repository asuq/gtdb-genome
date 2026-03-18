"""Logging helpers for gtdb-genomes."""

from __future__ import annotations

from collections.abc import Iterable
import logging
import shlex


LOGGER_NAME = "gtdb_genomes"


def normalise_secrets(secrets: Iterable[str | None]) -> tuple[str, ...]:
    """Return the non-empty secrets that should be redacted from logs."""

    return tuple(secret for secret in secrets if secret)


def redact_text(text: str, secrets: Iterable[str | None]) -> str:
    """Redact all known secrets from one text value."""

    redacted_text = text
    for secret in normalise_secrets(secrets):
        redacted_text = redacted_text.replace(secret, "[REDACTED]")
    return redacted_text


def format_command(command: list[str]) -> str:
    """Format a subprocess argv list for human-readable logging."""

    return shlex.join(command)


def redact_command(
    command: list[str],
    secrets: Iterable[str | None],
) -> str:
    """Render a shell-safe command string with secrets redacted."""

    return redact_text(format_command(command), secrets)


def get_logger() -> logging.Logger:
    """Return the package logger."""

    return logging.getLogger(LOGGER_NAME)


def configure_console_logging(debug: bool = False) -> logging.Logger:
    """Configure console logging for the current process."""

    logger = get_logger()
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG if debug else logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(handler)
    return logger
