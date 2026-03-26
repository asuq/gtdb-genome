"""Tests for datasets download planning and retry handling."""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
import sys

import pytest

from gtdb_genomes.download import (
    DEHYDRATE_ACCESSION_THRESHOLD,
    build_batch_dehydrate_command,
    build_direct_batch_download_command,
    build_rehydrate_command,
    get_rehydrate_workers,
    run_streamed_command,
    run_retryable_command,
    select_download_method,
    validate_include_value,
    write_accession_input_file,
)
from gtdb_genomes.subprocess_utils import (
    NCBI_API_KEY_ENV_VAR,
    ProgressMilestoneTracker,
    build_datasets_subprocess_environment,
)

COMMAND_TEST_ACCESSION_FILE = Path("tmp") / "accessions.txt"
COMMAND_TEST_ARCHIVE_FILE = Path("tmp") / "out.zip"
COMMAND_TEST_BAG_DIRECTORY = Path("tmp") / "bag"


def test_validate_include_value_requires_genome() -> None:
    """The include value should stay genome-centric."""

    assert validate_include_value(" genome , gff3 ") == "genome,gff3"
    assert validate_include_value("genome,gff3,protein") == "genome,gff3,protein"

    with pytest.raises(ValueError, match="must contain 'genome'"):
        validate_include_value("protein,gff3")


def test_validate_include_value_rejects_unknown_values() -> None:
    """Unsupported include values should fail locally."""

    with pytest.raises(ValueError, match="unsupported include value"):
        validate_include_value("genome,mrna")


def test_command_builders_match_datasets_cli_shape() -> None:
    """Command builders should emit the expected datasets argv layout."""

    direct_batch_command = build_direct_batch_download_command(
        COMMAND_TEST_ACCESSION_FILE,
        COMMAND_TEST_ARCHIVE_FILE,
        "genome",
        debug=True,
    )
    rehydrate_command = build_rehydrate_command(
        COMMAND_TEST_BAG_DIRECTORY,
        7,
        debug=True,
    )
    batch_dehydrate_command = build_batch_dehydrate_command(
        COMMAND_TEST_ACCESSION_FILE,
        COMMAND_TEST_ARCHIVE_FILE,
        "genome",
        debug=True,
    )

    assert direct_batch_command == [
        "datasets",
        "download",
        "genome",
        "accession",
        "--inputfile",
        str(COMMAND_TEST_ACCESSION_FILE),
        "--filename",
        str(COMMAND_TEST_ARCHIVE_FILE),
        "--include",
        "genome",
        "--debug",
    ]
    assert rehydrate_command == [
        "datasets",
        "rehydrate",
        "--directory",
        str(COMMAND_TEST_BAG_DIRECTORY),
        "--max-workers",
        "7",
        "--debug",
    ]
    assert batch_dehydrate_command == [
        "datasets",
        "download",
        "genome",
        "accession",
        "--inputfile",
        str(COMMAND_TEST_ACCESSION_FILE),
        "--filename",
        str(COMMAND_TEST_ARCHIVE_FILE),
        "--include",
        "genome",
        "--dehydrated",
        "--debug",
    ]


def test_select_download_method_uses_count_only_threshold() -> None:
    """Auto mode should switch to dehydrate at the documented count threshold."""

    assert select_download_method(5).method_used == "direct"
    assert select_download_method(5).accession_count == 5
    assert (
        select_download_method(DEHYDRATE_ACCESSION_THRESHOLD).method_used
        == "dehydrate"
    )
    assert (
        select_download_method(DEHYDRATE_ACCESSION_THRESHOLD + 1).method_used
        == "dehydrate"
    )


def test_build_datasets_subprocess_environment_overrides_child_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Child environments should honour the explicit CLI API key."""

    monkeypatch.setenv(NCBI_API_KEY_ENV_VAR, "ambient-secret")

    environment = build_datasets_subprocess_environment("cli-secret")

    assert environment[NCBI_API_KEY_ENV_VAR] == "cli-secret"
    assert os.environ[NCBI_API_KEY_ENV_VAR] == "ambient-secret"


def test_build_datasets_subprocess_environment_clears_ambient_child_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Child environments should not inherit ambient API keys implicitly."""

    monkeypatch.setenv(NCBI_API_KEY_ENV_VAR, "ambient-secret")

    environment = build_datasets_subprocess_environment(None)

    assert NCBI_API_KEY_ENV_VAR not in environment
    assert os.environ[NCBI_API_KEY_ENV_VAR] == "ambient-secret"


def test_worker_caps_and_accession_input_file_follow_documented_limits(
    tmp_path: Path,
) -> None:
    """Rehydrate caps and accession input files should stay deterministic."""

    accession_file = write_accession_input_file(
        tmp_path / "accessions.txt",
        ["GCA_1", "GCA_1", "GCF_2"],
    )

    assert get_rehydrate_workers(64) == 30
    assert accession_file.read_text(encoding="ascii") == "GCA_1\nGCF_2\n"


def test_run_retryable_command_records_retries_before_success() -> None:
    """Retryable commands should keep retry history with fixed delays."""

    attempts = iter([1, 1, 0])
    sleep_calls: list[float] = []

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return a sequence of fake command outcomes."""

        assert env == {NCBI_API_KEY_ENV_VAR: "secret"}
        return subprocess.CompletedProcess(
            command,
            next(attempts),
            stdout="",
            stderr="temporary failure",
        )

    result = run_retryable_command(
        ["datasets", "download"],
        stage="download",
        environment={NCBI_API_KEY_ENV_VAR: "secret"},
        sleep_func=sleep_calls.append,
        runner=runner,
    )

    assert result.succeeded is True
    assert sleep_calls == [5, 15]
    assert [failure.final_status for failure in result.failures] == [
        "retry_scheduled",
        "retry_scheduled",
    ]


def test_progress_milestone_tracker_handles_newline_output() -> None:
    """Progress milestones should advance deterministically from newline output."""

    tracker = ProgressMilestoneTracker(step=10)

    assert tracker.consume("stdout", "5%\n10%\n35%\n") == (10, 20, 30)
    assert tracker.consume("stdout", "39%\n40%\n") == (40,)


def test_progress_milestone_tracker_handles_carriage_return_and_mixed_streams() -> None:
    """Progress milestones should survive carriage returns and mixed streams."""

    tracker = ProgressMilestoneTracker(step=10)

    assert tracker.consume("stdout", "\r5%\r15%\r20%") == (10, 20)
    assert tracker.consume("stderr", "45%\n") == (30, 40)
    assert tracker.consume("stdout", "92%\n") == (50, 60, 70, 80, 90)


def test_run_streamed_command_logs_progress_milestones(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The streamed runner should log 10% milestones from stdout and stderr."""

    logger = logging.getLogger("test-streamed-progress")
    caplog.set_level(logging.INFO, logger=logger.name)

    result = run_streamed_command(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "sys.stdout.write('5%\\r15%\\r20%\\n'); "
                "sys.stdout.flush(); "
                "sys.stderr.write('45%\\n'); "
                "sys.stderr.flush()"
            ),
        ],
        environment=None,
        timeout_seconds=10,
        logger=logger,
        progress_label="direct_batch_3: download",
        progress_step=10,
    )

    assert result.returncode == 0
    assert result.stdout == "5%\n15%\n20%\n"
    assert result.stderr == "45%\n"
    assert "direct_batch_3: download progress 10%" in caplog.text
    assert "direct_batch_3: download progress 20%" in caplog.text
    assert "direct_batch_3: download progress 30%" in caplog.text
    assert "direct_batch_3: download progress 40%" in caplog.text
    assert "direct_batch_3: download progress 50%" not in caplog.text


def test_run_streamed_command_skips_logs_without_parseable_percentages(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The streamed runner should stay quiet when no percentages are emitted."""

    logger = logging.getLogger("test-streamed-no-progress")
    caplog.set_level(logging.INFO, logger=logger.name)

    result = run_streamed_command(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "sys.stdout.write('Downloading data\\nDone\\n'); "
                "sys.stdout.flush(); "
                "sys.stderr.write('still working\\n'); "
                "sys.stderr.flush()"
            ),
        ],
        environment=None,
        timeout_seconds=10,
        logger=logger,
        progress_label="dehydrated_batch: rehydrate",
        progress_step=10,
    )

    assert result.returncode == 0
    assert "progress" not in caplog.text


def test_run_streamed_command_tracks_progress_across_chunk_boundaries(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Chunked stream reads should still emit progress milestones."""

    logger = logging.getLogger("test-streamed-progress-chunks")
    caplog.set_level(logging.INFO, logger=logger.name)
    monkeypatch.setattr("gtdb_genomes.download.STREAM_READ_CHUNK_SIZE", 2)

    result = run_streamed_command(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "sys.stdout.write('9%\\r10%\\r19%\\r20%\\n'); "
                "sys.stdout.flush()"
            ),
        ],
        environment=None,
        timeout_seconds=10,
        logger=logger,
        progress_label="direct_batch_9: download",
        progress_step=10,
    )

    assert result.returncode == 0
    assert "direct_batch_9: download progress 10%" in caplog.text
    assert "direct_batch_9: download progress 20%" in caplog.text


def test_run_retryable_command_uses_stage_message_for_silent_failures() -> None:
    """Silent subprocess failures should still leave a useful error message."""

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one failed subprocess result without any output."""

        assert env is None
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr="",
        )

    result = run_retryable_command(
        ["datasets", "download"],
        stage="download",
        sleep_func=lambda delay: None,
        runner=runner,
    )

    assert result.succeeded is False
    assert result.failures[-1].error_message == "download command failed"


def test_run_retryable_command_streamed_mode_keeps_retry_history_before_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Streamed commands should preserve retries and log progress milestones."""

    attempts = iter(["fail", "success"])
    sleep_calls: list[float] = []
    logger = logging.getLogger("test-streamed-retry-success")
    caplog.set_level(logging.INFO, logger=logger.name)

    def stream_runner(
        command: list[str],
        environment: dict[str, str] | None,
        timeout_seconds: int,
        progress_logger: logging.Logger,
        progress_label: str,
        progress_step: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one failure before a successful streamed retry."""

        assert command == ["datasets", "download"]
        assert environment == {NCBI_API_KEY_ENV_VAR: "secret"}
        assert timeout_seconds > 0
        assert progress_logger is logger
        assert progress_label == "direct_batch_1: download"
        assert progress_step == 10
        attempt = next(attempts)
        if attempt == "fail":
            return subprocess.CompletedProcess(
                command,
                1,
                stdout="",
                stderr="temporary failure",
            )
        progress_logger.info("%s progress 10%%", progress_label)
        progress_logger.info("%s progress 20%%", progress_label)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="10%\n20%\ncomplete\n",
            stderr="",
        )

    result = run_retryable_command(
        ["datasets", "download"],
        stage="download",
        environment={NCBI_API_KEY_ENV_VAR: "secret"},
        sleep_func=sleep_calls.append,
        logger=logger,
        progress_label="direct_batch_1: download",
        stream_runner=stream_runner,
    )

    assert result.succeeded is True
    assert result.stdout == "10%\n20%\ncomplete\n"
    assert sleep_calls == [5]
    assert [failure.final_status for failure in result.failures] == [
        "retry_scheduled",
    ]
    assert "direct_batch_1: download progress 10%" in caplog.text
    assert "direct_batch_1: download progress 20%" in caplog.text


def test_run_retryable_command_retries_timeouts_before_success() -> None:
    """Timeouts should consume the retry budget like other transient failures."""

    attempts = iter(["timeout", "success"])
    sleep_calls: list[float] = []

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise one timeout before returning a success."""

        assert env is None
        attempt = next(attempts)
        if attempt == "timeout":
            raise subprocess.TimeoutExpired(command, timeout)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    result = run_retryable_command(
        ["datasets", "download"],
        stage="download",
        sleep_func=sleep_calls.append,
        runner=runner,
    )

    assert result.succeeded is True
    assert sleep_calls == [5]
    assert result.failures[0].error_type == "timeout"


def test_run_retryable_command_streamed_mode_keeps_partial_timeout_output() -> None:
    """Streamed timeouts should preserve partial stdout and stderr."""

    sleep_calls: list[float] = []

    def stream_runner(
        command: list[str],
        environment: dict[str, str] | None,
        timeout_seconds: int,
        progress_logger: logging.Logger,
        progress_label: str,
        progress_step: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise one timeout with partial streamed output."""

        del environment, progress_logger, progress_label, progress_step
        raise subprocess.TimeoutExpired(
            command,
            timeout_seconds,
            output="partial stdout",
            stderr="partial stderr",
        )

    result = run_retryable_command(
        ["datasets", "download"],
        stage="download",
        sleep_func=sleep_calls.append,
        logger=logging.getLogger("test-streamed-timeout"),
        progress_label="direct_batch_1: download",
        stream_runner=stream_runner,
    )

    assert result.succeeded is False
    assert result.stdout == "partial stdout"
    assert result.stderr == "partial stderr"
    assert sleep_calls == [5, 15, 45]
    assert "stdout: partial stdout" in result.failures[-1].error_message
    assert "stderr: partial stderr" in result.failures[-1].error_message


def test_run_retryable_command_keeps_partial_timeout_output() -> None:
    """Timeout failures should preserve truncated partial stdout and stderr."""

    sleep_calls: list[float] = []

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise a timeout that carries partial command output."""

        del capture_output, text, check, env
        raise subprocess.TimeoutExpired(
            command,
            timeout,
            output="partial stdout",
            stderr="partial stderr",
        )

    result = run_retryable_command(
        ["datasets", "download"],
        stage="download",
        sleep_func=sleep_calls.append,
        runner=runner,
    )

    assert result.succeeded is False
    assert result.stdout == "partial stdout"
    assert result.stderr == "partial stderr"
    assert sleep_calls == [5, 15, 45]
    assert "stdout: partial stdout" in result.failures[-1].error_message
    assert "stderr: partial stderr" in result.failures[-1].error_message


def test_run_retryable_command_returns_spawn_failure_without_retry() -> None:
    """Spawn failures should fail fast instead of consuming the retry budget."""

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise a missing-executable error before a child process starts."""

        assert env is None
        raise FileNotFoundError("datasets")

    result = run_retryable_command(
        ["datasets", "download"],
        stage="download",
        sleep_func=lambda delay: None,
        runner=runner,
    )

    assert result.succeeded is False
    assert len(result.failures) == 1
    assert result.failures[0].error_type == "spawn_error"
    assert result.failures[0].error_message.startswith(
        "download command could not start",
    )


def test_run_retryable_command_streamed_mode_returns_spawn_failure_without_retry() -> None:
    """Streamed spawn failures should still fail fast without retries."""

    def stream_runner(
        command: list[str],
        environment: dict[str, str] | None,
        timeout_seconds: int,
        progress_logger: logging.Logger,
        progress_label: str,
        progress_step: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise a missing-executable error before the child starts."""

        del command, environment, timeout_seconds, progress_logger, progress_label
        del progress_step
        raise FileNotFoundError("datasets")

    result = run_retryable_command(
        ["datasets", "download"],
        stage="download",
        sleep_func=lambda delay: None,
        logger=logging.getLogger("test-streamed-spawn-error"),
        progress_label="direct_batch_1: download",
        stream_runner=stream_runner,
    )

    assert result.succeeded is False
    assert len(result.failures) == 1
    assert result.failures[0].error_type == "spawn_error"
