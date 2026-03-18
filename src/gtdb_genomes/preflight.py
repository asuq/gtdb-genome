"""Preflight checks for external tool availability."""

from __future__ import annotations

import shutil
from dataclasses import dataclass


@dataclass(slots=True)
class PreflightError(Exception):
    """Raised when a required external tool is missing."""

    message: str

    def __str__(self) -> str:
        """Return the error message."""
        return self.message


def check_required_tools() -> None:
    """Ensure that the required external tools are available."""

    missing_tools = [
        tool_name
        for tool_name in ("datasets", "unzip")
        if shutil.which(tool_name) is None
    ]
    if missing_tools:
        tools = ", ".join(missing_tools)
        raise PreflightError(
            f"Missing required external tools: {tools}",
        )
