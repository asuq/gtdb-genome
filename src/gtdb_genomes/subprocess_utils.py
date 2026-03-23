"""Shared subprocess timeout and error-message helpers."""

from __future__ import annotations

from collections.abc import Mapping
import os
import subprocess


DEFAULT_SUBPROCESS_TIMEOUT_SECONDS = 4 * 60 * 60
NCBI_API_KEY_ENV_VAR = "NCBI_API_KEY"
TIMEOUT_OUTPUT_EXCERPT_LIMIT = 200


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
