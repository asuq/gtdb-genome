"""Logging helpers for gtdb-genomes."""

from __future__ import annotations

from collections.abc import Iterable
import logging
from pathlib import Path
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


def configure_logging(
    debug: bool = False,
    dry_run: bool = False,
    output_root: Path | None = None,
) -> tuple[logging.Logger, Path | None]:
    """Configure console logging and, when allowed, the debug log file."""

    logger = configure_console_logging(debug=debug)
    debug_log_path: Path | None = None
    if debug and not dry_run and output_root is not None:
        debug_log_path = output_root / "debug.log"
        debug_log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(debug_log_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s"),
        )
        logger.addHandler(file_handler)
    return logger, debug_log_path


def close_logger(logger: logging.Logger) -> None:
    """Close and detach all handlers from the package logger."""

    for handler in tuple(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
