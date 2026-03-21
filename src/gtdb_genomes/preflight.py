"""Preflight checks for external tool availability."""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(slots=True)
class PreflightError(Exception):
    """Raised when a required external tool is missing."""

    message: str

    def __str__(self) -> str:
        """Return the error message."""
        return self.message


def get_early_required_tools(
    dry_run: bool,
) -> tuple[str, ...]:
    """Return tools that must be checked before dry-run planning exits."""

    if not dry_run:
        return ()
    return ("unzip",)


def get_supported_preflight_tools(
    dry_run: bool,
) -> tuple[str, ...]:
    """Return tools required for supported planning and execution paths."""

    if dry_run:
        return ("datasets",)
    return ("datasets", "unzip")


def check_required_tools(required_tools: Sequence[str]) -> None:
    """Ensure that the required external tools are available."""

    missing_tools = [
        tool_name
        for tool_name in required_tools
        if shutil.which(tool_name) is None
    ]
    if missing_tools:
        tools = ", ".join(missing_tools)
        raise PreflightError(
            f"Missing required external tools: {tools}",
        )
