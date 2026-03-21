"""Focused tests for external-tool preflight helpers."""

from __future__ import annotations

import shutil

import pytest

from gtdb_genomes.preflight import (
    PreflightError,
    check_required_tools,
    get_early_required_tools,
    get_supported_preflight_tools,
)


def test_get_early_required_tools_only_requires_unzip_for_dry_runs() -> None:
    """Dry-runs should preflight `unzip` before planning exits."""

    assert get_early_required_tools(dry_run=True) == ("unzip",)
    assert get_early_required_tools(dry_run=False) == ()


def test_get_supported_preflight_tools_preserves_runtime_requirements() -> None:
    """Supported planning should keep datasets-only dry-runs and full real runs."""

    assert get_supported_preflight_tools(dry_run=True) == ("datasets",)
    assert get_supported_preflight_tools(dry_run=False) == (
        "datasets",
        "unzip",
    )


def test_check_required_tools_raises_for_missing_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing external tools should raise one combined preflight error."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: None)

    with pytest.raises(
        PreflightError,
        match="Missing required external tools: datasets, unzip",
    ):
        check_required_tools(("datasets", "unzip"))
