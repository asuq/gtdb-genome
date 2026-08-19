"""Focused tests for external-tool preflight helpers."""

from __future__ import annotations

import shutil
import subprocess

import pytest

from gtdb_genomes.preflight import (
    PreflightError,
    check_required_tools,
    get_supported_preflight_tools,
)


def test_get_supported_preflight_tools_preserves_runtime_requirements() -> None:
    """Supported planning and execution should require only datasets."""

    assert get_supported_preflight_tools() == ("datasets",)


def test_check_required_tools_accepts_supported_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supported external-tool versions should pass preflight unchanged."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return supported datasets version output."""

        del capture_output, text, check, timeout
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="datasets version: 18.4.0\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    check_required_tools(("datasets",))


def test_check_required_tools_accepts_late_18_x_datasets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The datasets policy should accept later minor releases within major 18."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return a high-end supported datasets version."""

        del capture_output, text, check, timeout
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="datasets version: 18.99.0\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    check_required_tools(("datasets",))


def test_check_required_tools_raises_for_missing_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing external tools should raise one combined preflight error."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: None)

    with pytest.raises(
        PreflightError,
        match="Missing required external tools: datasets",
    ):
        check_required_tools(("datasets",))


def test_check_required_tools_raises_for_unsupported_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Out-of-range tool versions should fail preflight with the supported window."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return an unsupported datasets version."""

        del capture_output, text, check, timeout
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="datasets version: 19.0.0\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        PreflightError,
        match="Supported range: >=18.4.0,<19.0.0",
    ):
        check_required_tools(("datasets",))


def test_check_required_tools_rejects_non_zero_version_commands_with_parseable_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-zero version commands should not satisfy the preflight gate."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return parseable version text even though the command failed."""

        del capture_output, text, check, timeout
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="datasets version: 18.4.0\n",
            stderr="wrapper failed\n",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        PreflightError,
        match="Could not determine the installed version",
    ):
        check_required_tools(("datasets",))


def test_check_required_tools_rejects_datasets_versions_below_supported_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Datasets versions older than 18.4.0 should fail preflight."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one pre-floor datasets version."""

        del capture_output, text, check, timeout
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="datasets version: 18.3.1\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        PreflightError,
        match="Supported range: >=18.4.0,<19.0.0",
    ):
        check_required_tools(("datasets",))


def test_check_required_tools_raises_for_unparseable_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unparseable version output should fail preflight conservatively."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return unparsable version output for the required command."""

        del command, capture_output, text, check, timeout
        return subprocess.CompletedProcess(
            ["datasets", "version"],
            0,
            stdout="datasets version unavailable\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        PreflightError,
        match="Could not parse the installed version",
    ):
        check_required_tools(("datasets",))
