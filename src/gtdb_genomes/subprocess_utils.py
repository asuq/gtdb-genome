"""Shared subprocess timeout, progress, and error-message helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import os
import re
import subprocess


DEFAULT_SUBPROCESS_TIMEOUT_SECONDS = 4 * 60 * 60
NCBI_API_KEY_ENV_VAR = "NCBI_API_KEY"
TIMEOUT_OUTPUT_EXCERPT_LIMIT = 200
PROGRESS_TAIL_LIMIT = 32
PROGRESS_PERCENT_PATTERN = re.compile(r"(?<!\d)(100|[1-9]?\d)\s*%(?!\d)")


def get_stage_display_name(stage: str) -> str:
    """Return one user-facing subprocess stage label."""

    return stage.replace("_", " ")


def build_subprocess_error_message(
    stage: str,
    result: subprocess.CompletedProcess[str],
) -> str:
    """Build a non-empty error message for one failed subprocess result."""

    error_message = result.stderr.strip() or result.stdout.strip()
    if error_message:
        return error_message
    return f"{get_stage_display_name(stage)} command failed"


def normalise_subprocess_stream_output(output: str | bytes | None) -> str:
    """Return one normalised subprocess stream value as text."""

    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace")
    return output


def normalise_incremental_subprocess_output(output: str) -> str:
    """Normalise one incremental subprocess chunk for progress parsing."""

    return output.replace("\r", "\n")


@dataclass(slots=True)
class ProgressMilestoneTracker:
    """Track percentage milestones across multiple streamed subprocess outputs."""

    step: int = 10
    next_milestone: int = 10
    stream_tails: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate one tracker configuration."""

        if self.step <= 0:
            raise ValueError("progress step must be positive")

    def consume(self, stream_name: str, output: str) -> tuple[int, ...]:
        """Return newly crossed percentage milestones for one stream chunk."""

        if not output:
            return ()
        prior_tail = self.stream_tails.get(stream_name, "")
        combined = (
            f"{prior_tail}{normalise_incremental_subprocess_output(output)}"
        )
        milestones: list[int] = []
        for match in PROGRESS_PERCENT_PATTERN.finditer(combined):
            percentage = int(match.group(1))
            while self.next_milestone <= percentage and self.next_milestone <= 100:
                milestones.append(self.next_milestone)
                self.next_milestone += self.step
        self.stream_tails[stream_name] = combined[-PROGRESS_TAIL_LIMIT:]
        return tuple(milestones)


def build_timeout_output_excerpt(timeout_error: subprocess.TimeoutExpired) -> str:
    """Build one truncated timeout excerpt from partial stdout and stderr."""

    output_text = normalise_subprocess_stream_output(timeout_error.output).strip()
    stderr_text = normalise_subprocess_stream_output(timeout_error.stderr).strip()
    parts: list[str] = []
    if output_text:
        truncated_output = output_text[:TIMEOUT_OUTPUT_EXCERPT_LIMIT]
        if len(output_text) > TIMEOUT_OUTPUT_EXCERPT_LIMIT:
            truncated_output = f"{truncated_output}..."
        parts.append(f"stdout: {truncated_output}")
    if stderr_text:
        truncated_stderr = stderr_text[:TIMEOUT_OUTPUT_EXCERPT_LIMIT]
        if len(stderr_text) > TIMEOUT_OUTPUT_EXCERPT_LIMIT:
            truncated_stderr = f"{truncated_stderr}..."
        parts.append(f"stderr: {truncated_stderr}")
    return "; ".join(parts)


def build_timeout_error_message(
    stage: str,
    timeout_seconds: int,
    timeout_error: subprocess.TimeoutExpired | None = None,
) -> str:
    """Build a timeout error message for one subprocess stage."""

    message = (
        f"{get_stage_display_name(stage)} command timed out after "
        f"{timeout_seconds} seconds"
    )
    if timeout_error is None:
        return message
    excerpt = build_timeout_output_excerpt(timeout_error)
    if not excerpt:
        return message
    return f"{message} ({excerpt})"


def build_spawn_error_message(stage: str, error: OSError) -> str:
    """Build a process-spawn error message for one subprocess stage."""

    return f"{get_stage_display_name(stage)} command could not start: {error}"


def build_datasets_subprocess_environment(
    ncbi_api_key: str | None,
    inherited_environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return one child environment that follows the explicit CLI API-key contract."""

    environment = dict(
        os.environ if inherited_environment is None else inherited_environment,
    )
    if ncbi_api_key:
        environment[NCBI_API_KEY_ENV_VAR] = ncbi_api_key
    else:
        environment.pop(NCBI_API_KEY_ENV_VAR, None)
    return environment
