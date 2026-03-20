"""Download execution helpers for the GTDB workflow."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from gtdb_genomes.download import (
    CommandFailureRecord,
    build_batch_dehydrate_command,
    build_direct_batch_download_command,
    build_rehydrate_command,
    get_ordered_unique_accessions,
    get_rehydrate_workers,
    run_retryable_command,
    write_accession_input_file,
)
from gtdb_genomes.layout import LayoutError, RunDirectories, extract_archive
from gtdb_genomes.logging_utils import redact_command
from gtdb_genomes.metadata import (
    get_assembly_accession_stem,
    parse_assembly_accession,
    parse_assembly_accession_stem,
)

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


MAX_DIRECT_BATCH_PASSES = 4


@dataclass(slots=True)
class AccessionPlan:
    """One unique accession to resolve and download for the run."""

    original_accession: str
    selected_accession: str
    download_request_accession: str
    conversion_status: str


@dataclass(slots=True)
class AccessionExecution:
    """The materialised download outcome for one accession plan."""

    original_accession: str
    final_accession: str | None
    conversion_status: str
    download_status: str
    download_batch: str
    payload_directory: Path | None
    failures: tuple[CommandFailureRecord, ...]


@dataclass(slots=True)
class DownloadExecutionResult:
    """The realised download execution details for one run."""

    executions: dict[str, AccessionExecution]
    method_used: str
    download_concurrency_used: int
    rehydrate_workers_used: int
    shared_failures: tuple["SharedFailureContext", ...] = ()


@dataclass(frozen=True, slots=True)
class ResolvedPayloadDirectory:
    """The extracted payload directory and its realised accession."""

    final_accession: str
    directory: Path


@dataclass(slots=True)
class SharedFailureContext:
    """Shared failure history scoped to one affected accession subset."""

    affected_original_accessions: tuple[str, ...]
    failures: tuple[CommandFailureRecord, ...]


@dataclass(slots=True)
class PartialBatchPayloadResolution:
    """Resolved and unresolved payloads for one extracted batch archive."""

    resolved_payloads: dict[str, ResolvedPayloadDirectory]
    unresolved_messages: dict[str, str]


@dataclass(slots=True)
class DirectBatchPhaseResult:
    """Accumulated results from one direct batch phase."""

    executions: dict[str, AccessionExecution]
    unresolved_groups: tuple[tuple[str, tuple[AccessionPlan, ...]], ...]
    shared_failures: tuple[SharedFailureContext, ...]


# Payload discovery and layout helpers.


def group_plans_by_download_request_accession(
    plans: tuple[AccessionPlan, ...],
) -> tuple[tuple[str, tuple[AccessionPlan, ...]], ...]:
    """Group accession plans by request accession in first-seen order."""

    grouped_plans: dict[str, list[AccessionPlan]] = {}
    for plan in plans:
        grouped_plans.setdefault(plan.download_request_accession, []).append(plan)
    return tuple(
        (download_request_accession, tuple(group))
        for download_request_accession, group in grouped_plans.items()
    )


def attach_attempted_accession(
    failures: tuple[CommandFailureRecord, ...],
    attempted_accession: str,
) -> tuple[CommandFailureRecord, ...]:
    """Fill missing attempted-accession values on shared failure records."""

    return tuple(
        replace(
            failure,
            attempted_accession=(
                failure.attempted_accession
                if failure.attempted_accession is not None
                else attempted_accession
            ),
        )
        for failure in failures
    )


def build_resolved_payload_directory(
    candidate: Path,
) -> ResolvedPayloadDirectory | None:
    """Return one resolved payload directory when the path name is an accession."""

    if not candidate.is_dir():
        return None
    parsed_accession = parse_assembly_accession(candidate.name)
    if parsed_accession is None:
        return None
    return ResolvedPayloadDirectory(
        final_accession=parsed_accession.accession,
        directory=candidate,
    )


def collect_root_payload_directories(
    root: Path,
) -> tuple[ResolvedPayloadDirectory, ...]:
    """Collect accession-named directories directly under one root."""

    return tuple(
        resolved_payload
        for candidate in sorted(root.iterdir(), key=lambda path: path.name)
        if (resolved_payload := build_resolved_payload_directory(candidate)) is not None
    )


def has_accession_named_parent(candidate: Path, root: Path) -> bool:
    """Return whether a candidate is nested below another accession directory."""

    for parent in candidate.parents:
        if parent == root:
            return False
        if parse_assembly_accession(parent.name) is not None:
            return True
    return False


def collect_payload_directories(
    extraction_root: Path,
) -> tuple[ResolvedPayloadDirectory, ...]:
    """Collect realised payload directories from one extracted archive."""

    data_root = extraction_root / "ncbi_dataset" / "data"
    if data_root.is_dir():
        payload_directories = collect_root_payload_directories(data_root)
        if payload_directories:
            return payload_directories

    payload_directories = tuple(
        resolved_payload
        for candidate in sorted(
            extraction_root.rglob("*"),
            key=lambda path: str(path.relative_to(extraction_root)),
        )
        if (resolved_payload := build_resolved_payload_directory(candidate)) is not None
        and not has_accession_named_parent(candidate, extraction_root)
    )
    if payload_directories:
        return payload_directories
    raise LayoutError("Could not locate extracted payload directories")


def locate_accession_payload_directory(
    extraction_root: Path,
    requested_accession: str,
) -> ResolvedPayloadDirectory:
    """Locate the extracted payload directory for one requested accession."""

    payload_directories = locate_batch_payload_directories(
        extraction_root,
        (requested_accession,),
    )
    return payload_directories[requested_accession]


def locate_batch_payload_directories(
    extraction_root: Path,
    requested_accessions: tuple[str, ...],
) -> dict[str, ResolvedPayloadDirectory]:
    """Locate extracted payload directories for one request batch."""

    resolution = locate_partial_batch_payload_directories(
        extraction_root,
        requested_accessions,
    )
    if resolution.unresolved_messages:
        unresolved_text = "; ".join(
            resolution.unresolved_messages[requested_accession]
            for requested_accession in requested_accessions
            if requested_accession in resolution.unresolved_messages
        )
        raise LayoutError(unresolved_text)
    return resolution.resolved_payloads


def locate_partial_batch_payload_directories(
    extraction_root: Path,
    requested_accessions: tuple[str, ...],
) -> PartialBatchPayloadResolution:
    """Locate payloads for one request batch without failing atomically."""

    try:
        payload_records = collect_payload_directories(extraction_root)
    except LayoutError:
        payload_records = ()
    payloads_by_accession = {
        payload.final_accession: payload for payload in payload_records
    }
    payloads_by_stem: dict[str, list[ResolvedPayloadDirectory]] = defaultdict(list)
    for payload in payload_records:
        payloads_by_stem[get_assembly_accession_stem(payload.final_accession)].append(
            payload,
        )

    located_payloads: dict[str, ResolvedPayloadDirectory] = {}
    unresolved_messages: dict[str, str] = {}
    for requested_accession in requested_accessions:
        exact_match = payloads_by_accession.get(requested_accession)
        if exact_match is not None:
            located_payloads[requested_accession] = exact_match
            continue

        request_stem = parse_assembly_accession_stem(requested_accession)
        if request_stem is None:
            unresolved_messages[requested_accession] = (
                "Could not locate extracted payload directory for requested "
                f"accession {requested_accession}"
            )
            continue

        stem_matches = tuple(payloads_by_stem.get(request_stem.accession, ()))
        if len(stem_matches) == 1:
            located_payloads[requested_accession] = stem_matches[0]
            continue
        if len(stem_matches) > 1:
            unresolved_messages[requested_accession] = (
                "Resolved multiple extracted payload directories for requested "
                f"accession {requested_accession}: "
                f"{', '.join(payload.final_accession for payload in stem_matches)}"
            )
            continue
        unresolved_messages[requested_accession] = (
            "Could not locate extracted payload directory for requested "
            f"accession {requested_accession}"
        )
    return PartialBatchPayloadResolution(
        resolved_payloads=located_payloads,
        unresolved_messages=unresolved_messages,
    )


# Failure and execution record helpers.


def build_layout_failure(
    error: Exception,
    final_status: str = "retry_exhausted",
) -> CommandFailureRecord:
    """Build a synthetic failure record for a local layout error."""

    return CommandFailureRecord(
        stage="layout",
        attempt_index=1,
        max_attempts=1,
        error_type=type(error).__name__,
        error_message=str(error),
        final_status=final_status,
    )


def build_direct_layout_failure(
    error_message: str,
    attempted_accession: str,
    attempt_index: int,
    max_attempts: int,
    final_status: str,
) -> CommandFailureRecord:
    """Build one direct-batch layout failure for a single accession token."""

    return CommandFailureRecord(
        stage="layout",
        attempt_index=attempt_index,
        max_attempts=max_attempts,
        error_type="LayoutError",
        error_message=error_message,
        final_status=final_status,
        attempted_accession=attempted_accession,
    )


def build_shared_failure_context(
    original_accessions: tuple[str, ...],
    failures: tuple[CommandFailureRecord, ...],
    attempted_accession: str,
) -> SharedFailureContext:
    """Scope shared failures to the affected original accessions."""

    return SharedFailureContext(
        affected_original_accessions=get_ordered_unique_accessions(
            original_accessions,
        ),
        failures=attach_attempted_accession(failures, attempted_accession),
    )


def extract_download_payload(
    requested_accession: str,
    archive_path: Path,
    run_directories: RunDirectories,
    *,
    extraction_key: str | None = None,
) -> tuple[ResolvedPayloadDirectory | None, tuple[CommandFailureRecord, ...]]:
    """Extract one downloaded archive and locate its payload directory."""

    extraction_root = run_directories.extracted_root / (
        requested_accession if extraction_key is None else extraction_key
    )
    try:
        extract_archive(archive_path, extraction_root)
    except LayoutError as error:
        return None, (build_layout_failure(error),)

    try:
        payload_directory = locate_accession_payload_directory(
            extraction_root,
            requested_accession,
        )
    except LayoutError as error:
        return None, (build_layout_failure(error),)
    return payload_directory, ()


def build_failed_execution(
    original_accession: str,
    failures: tuple[CommandFailureRecord, ...],
    download_batch: str,
) -> AccessionExecution:
    """Build a failed execution for one original accession."""

    return AccessionExecution(
        original_accession=original_accession,
        final_accession=None,
        conversion_status="failed_no_usable_accession",
        download_status="failed",
        download_batch=download_batch,
        payload_directory=None,
        failures=failures,
    )


def build_successful_execution(
    plan: AccessionPlan,
    final_accession: str,
    download_status: str,
    download_batch: str,
    payload_directory: Path,
    failures: tuple[CommandFailureRecord, ...],
) -> AccessionExecution:
    """Build a successful execution for one accession plan."""

    conversion_status = plan.conversion_status
    if (
        download_status == "downloaded_after_fallback"
        and plan.conversion_status == "paired_to_gca"
    ):
        conversion_status = "paired_to_gca_fallback_original_on_download_failure"
    return AccessionExecution(
        original_accession=plan.original_accession,
        final_accession=final_accession,
        conversion_status=conversion_status,
        download_status=download_status,
        download_batch=download_batch,
        payload_directory=payload_directory,
        failures=failures,
    )


def build_direct_batch_archive_path(
    run_directories: RunDirectories,
    batch_label: str,
) -> Path:
    """Return the archive path for one direct batch pass."""

    return run_directories.downloads_root / f"{batch_label}.zip"


def build_phase_failed_executions(
    plans: tuple[AccessionPlan, ...],
    failure_history: dict[str, list[CommandFailureRecord]],
    last_download_batches: dict[str, str],
) -> dict[str, AccessionExecution]:
    """Build failed executions for one set of unresolved direct plans."""

    return {
        plan.original_accession: build_failed_execution(
            plan.original_accession,
            tuple(failure_history[plan.original_accession]),
            last_download_batches[plan.original_accession],
        )
        for plan in plans
    }


def build_batch_layout_failures(
    failures: tuple[CommandFailureRecord, ...],
    error: Exception,
) -> tuple[CommandFailureRecord, ...]:
    """Append one synthetic local layout failure to a batch failure list."""

    return failures + (build_layout_failure(error),)


def build_batch_archive_path(run_directories: RunDirectories) -> Path:
    """Return the shared archive path for a dehydrated batch download."""

    return run_directories.downloads_root / "dehydrated_batch.zip"


# Direct batch execution.


def execute_direct_batch_phase(
    plan_groups: tuple[tuple[str, tuple[AccessionPlan, ...]], ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    *,
    batch_stage: str,
    batch_prefix: str,
    success_status: str,
    failure_history: dict[str, list[CommandFailureRecord]],
    last_download_batches: dict[str, str],
) -> DirectBatchPhaseResult:
    """Execute one batch-based direct phase with shrinking retry inputs."""

    secrets = tuple(secret for secret in (args.ncbi_api_key,) if secret)
    pending_groups = plan_groups
    executions: dict[str, AccessionExecution] = {}
    shared_failures: list[SharedFailureContext] = []

    for attempt_index in range(1, MAX_DIRECT_BATCH_PASSES + 1):
        if not pending_groups:
            break
        batch_label = f"{batch_prefix}_{attempt_index}"
        pending_request_accessions = tuple(
            request_accession for request_accession, _ in pending_groups
        )
        logger.info(
            "%s: starting %s for %d request accession(s)",
            batch_label,
            batch_stage,
            len(pending_request_accessions),
        )
        affected_original_accessions = tuple(
            plan.original_accession
            for _, grouped_plans in pending_groups
            for plan in grouped_plans
        )
        for original_accession in affected_original_accessions:
            last_download_batches[original_accession] = batch_label
        accession_file = write_accession_input_file(
            run_directories.working_root / f"{batch_label}.txt",
            pending_request_accessions,
        )
        archive_path = build_direct_batch_archive_path(
            run_directories,
            batch_label,
        )
        download_command = build_direct_batch_download_command(
            accession_file,
            archive_path,
            args.include,
            ncbi_api_key=args.ncbi_api_key,
            debug=args.debug,
        )
        logger.debug(
            "Running %s",
            redact_command(download_command, secrets),
        )
        batch_attempted_accessions = ";".join(pending_request_accessions)
        batch_result = run_retryable_command(
            download_command,
            stage=batch_stage,
            attempted_accession=batch_attempted_accessions,
        )
        if not batch_result.succeeded:
            logger.warning(
                "%s: %s failed before payload extraction",
                batch_label,
                batch_stage,
            )
            shared_failures.append(
                build_shared_failure_context(
                    affected_original_accessions,
                    batch_result.failures,
                    batch_attempted_accessions,
                ),
            )
            return DirectBatchPhaseResult(
                executions=executions,
                unresolved_groups=pending_groups,
                shared_failures=tuple(shared_failures),
            )

        extraction_root = run_directories.extracted_root / batch_label
        try:
            extract_archive(archive_path, extraction_root)
        except LayoutError as error:
            logger.warning(
                "%s: extraction failed after %s",
                batch_label,
                batch_stage,
            )
            shared_failures.append(
                build_shared_failure_context(
                    affected_original_accessions,
                    (build_layout_failure(error),),
                    batch_attempted_accessions,
                ),
            )
            return DirectBatchPhaseResult(
                executions=executions,
                unresolved_groups=pending_groups,
                shared_failures=tuple(shared_failures),
            )

        resolution = locate_partial_batch_payload_directories(
            extraction_root,
            pending_request_accessions,
        )
        made_progress = bool(resolution.resolved_payloads)

        # Stop retrying after the first no-progress pass. Re-running the same
        # unresolved set would only repeat the same failing batch.
        can_retry = attempt_index < MAX_DIRECT_BATCH_PASSES and made_progress
        unresolved_groups: list[tuple[str, tuple[AccessionPlan, ...]]] = []
        final_status = "retry_scheduled" if can_retry else "retry_exhausted"

        for request_accession, grouped_plans in pending_groups:
            payload = resolution.resolved_payloads.get(request_accession)
            if payload is not None:
                for plan in grouped_plans:
                    plan_failures = tuple(failure_history[plan.original_accession])
                    executions[plan.original_accession] = build_successful_execution(
                        plan,
                        payload.final_accession,
                        success_status,
                        batch_label,
                        payload.directory,
                        plan_failures,
                    )
                continue

            failure_record = build_direct_layout_failure(
                resolution.unresolved_messages[request_accession],
                request_accession,
                attempt_index,
                MAX_DIRECT_BATCH_PASSES,
                final_status,
            )
            for plan in grouped_plans:
                failure_history[plan.original_accession].append(failure_record)
            unresolved_groups.append((request_accession, grouped_plans))

        logger.info(
            "%s: completed with %d resolved and %d pending request accession(s)",
            batch_label,
            len(resolution.resolved_payloads),
            len(unresolved_groups),
        )

        if not unresolved_groups:
            return DirectBatchPhaseResult(
                executions=executions,
                unresolved_groups=(),
                shared_failures=tuple(shared_failures),
            )
        if can_retry:
            pending_groups = tuple(unresolved_groups)
            continue
        return DirectBatchPhaseResult(
            executions=executions,
            unresolved_groups=tuple(unresolved_groups),
            shared_failures=tuple(shared_failures),
        )

    return DirectBatchPhaseResult(
        executions=executions,
        unresolved_groups=pending_groups,
        shared_failures=tuple(shared_failures),
    )


def execute_direct_accession_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
) -> DownloadExecutionResult:
    """Execute direct downloads with batch retries and original fallback."""

    if not plans:
        return DownloadExecutionResult(
            executions={},
            method_used="direct",
            download_concurrency_used=0,
            rehydrate_workers_used=0,
            shared_failures=(),
        )
    plan_groups = group_plans_by_download_request_accession(plans)
    executions: dict[str, AccessionExecution] = {}
    shared_failures: list[SharedFailureContext] = []
    failure_history: dict[str, list[CommandFailureRecord]] = {
        plan.original_accession: [] for plan in plans
    }
    last_download_batches: dict[str, str] = {
        plan.original_accession: plan.original_accession for plan in plans
    }

    preferred_phase = execute_direct_batch_phase(
        plan_groups,
        args,
        run_directories,
        logger,
        batch_stage="preferred_download",
        batch_prefix="direct_batch",
        success_status="downloaded",
        failure_history=failure_history,
        last_download_batches=last_download_batches,
    )
    executions.update(preferred_phase.executions)
    shared_failures.extend(preferred_phase.shared_failures)

    preferred_unresolved_plans: list[AccessionPlan] = []
    fallback_groups: list[tuple[str, tuple[AccessionPlan, ...]]] = []

    # Only rows that switched to a preferred request accession can retry
    # against their original accession during the fallback phase.
    for _, grouped_plans in preferred_phase.unresolved_groups:
        for plan in grouped_plans:
            preferred_unresolved_plans.append(plan)
            if plan.conversion_status == "paired_to_gca":
                fallback_groups.append((plan.original_accession, (plan,)))
    failed_after_preferred = tuple(
        plan
        for plan in preferred_unresolved_plans
        if plan.conversion_status != "paired_to_gca"
    )
    executions.update(
        build_phase_failed_executions(
            failed_after_preferred,
            failure_history,
            last_download_batches,
        ),
    )

    if fallback_groups:
        fallback_phase = execute_direct_batch_phase(
            tuple(fallback_groups),
            args,
            run_directories,
            logger,
            batch_stage="fallback_download",
            batch_prefix="direct_fallback_batch",
            success_status="downloaded_after_fallback",
            failure_history=failure_history,
            last_download_batches=last_download_batches,
        )
        executions.update(fallback_phase.executions)
        shared_failures.extend(fallback_phase.shared_failures)
        unresolved_fallback_plans = tuple(
            plan
            for _, grouped_plans in fallback_phase.unresolved_groups
            for plan in grouped_plans
        )
        executions.update(
            build_phase_failed_executions(
                unresolved_fallback_plans,
                failure_history,
                last_download_batches,
            ),
        )

    return DownloadExecutionResult(
        executions=executions,
        method_used="direct",
        download_concurrency_used=1,
        rehydrate_workers_used=0,
        shared_failures=tuple(shared_failures),
    )


# Dehydrated execution and fallback.


def execute_batch_dehydrate_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> DownloadExecutionResult:
    """Execute one dehydrated batch download with fallback to direct mode."""

    if not plans:
        return DownloadExecutionResult(
            executions={},
            method_used="dehydrate",
            download_concurrency_used=0,
            rehydrate_workers_used=0,
            shared_failures=(),
        )

    batch_attempted_accessions = ";".join(
        get_ordered_unique_accessions(
            plan.download_request_accession for plan in plans
        ),
    )
    logger.info(
        "dehydrated_batch: starting preferred_download for %d request accession(s)",
        len(plans),
    )
    affected_original_accessions = tuple(
        plan.original_accession for plan in plans
    )
    accession_file = write_accession_input_file(
        run_directories.working_root / "dehydrate_accessions.txt",
        (plan.download_request_accession for plan in plans),
    )
    archive_path = build_batch_archive_path(run_directories)
    download_command = build_batch_dehydrate_command(
        accession_file,
        archive_path,
        args.include,
        ncbi_api_key=args.ncbi_api_key,
        debug=args.debug,
    )
    logger.debug("Running %s", redact_command(download_command, secrets))
    batch_download = run_retryable_command(
        download_command,
        stage="preferred_download",
        attempted_accession=batch_attempted_accessions,
    )
    if not batch_download.succeeded:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_shared_failure_context(
                affected_original_accessions,
                batch_download.failures,
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=0,
        )
    logger.info("dehydrated_batch: download archive completed")

    extraction_root = run_directories.extracted_root / "dehydrated_batch"
    try:
        extract_archive(archive_path, extraction_root)
    except LayoutError as error:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_shared_failure_context(
                affected_original_accessions,
                build_batch_layout_failures(batch_download.failures, error),
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=0,
        )

    rehydrate_workers = get_rehydrate_workers(args.threads)
    logger.info(
        "dehydrated_batch: starting rehydrate with %d worker(s)",
        rehydrate_workers,
    )
    rehydrate_command = build_rehydrate_command(
        extraction_root,
        rehydrate_workers,
        ncbi_api_key=args.ncbi_api_key,
        debug=args.debug,
    )
    logger.debug("Running %s", redact_command(rehydrate_command, secrets))
    rehydrate_result = run_retryable_command(
        rehydrate_command,
        stage="rehydrate",
        attempted_accession=batch_attempted_accessions,
    )
    if not rehydrate_result.succeeded:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_shared_failure_context(
                affected_original_accessions,
                batch_download.failures + rehydrate_result.failures,
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=rehydrate_workers,
        )
    logger.info("dehydrated_batch: rehydrate completed")

    shared_failures = build_shared_failure_context(
        affected_original_accessions,
        batch_download.failures + rehydrate_result.failures,
        batch_attempted_accessions,
    )
    executions: dict[str, AccessionExecution] = {}
    try:
        payload_directories = locate_batch_payload_directories(
            extraction_root,
            tuple(plan.download_request_accession for plan in plans),
        )
        for plan in plans:
            payload = payload_directories[plan.download_request_accession]
            executions[plan.original_accession] = AccessionExecution(
                original_accession=plan.original_accession,
                final_accession=payload.final_accession,
                conversion_status=plan.conversion_status,
                download_status="downloaded",
                download_batch="dehydrated_batch",
                payload_directory=payload.directory,
                failures=(),
            )
    except LayoutError as error:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_shared_failure_context(
                affected_original_accessions,
                build_batch_layout_failures(shared_failures.failures, error),
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=rehydrate_workers,
        )
    logger.info(
        "dehydrated_batch: completed with %d resolved accession(s)",
        len(executions),
    )

    return DownloadExecutionResult(
        executions=executions,
        method_used="dehydrate",
        download_concurrency_used=1,
        rehydrate_workers_used=rehydrate_workers,
        shared_failures=(shared_failures,) if shared_failures.failures else (),
    )


def fallback_batch_to_direct(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    batch_failures: SharedFailureContext,
    rehydrate_workers_used: int,
) -> DownloadExecutionResult:
    """Fall back from a failed dehydrated batch workflow to direct downloads."""

    logger.warning(
        "Batch dehydrated download failed; falling back to batch direct downloads",
    )
    logger.info(
        "Starting direct fallback for %d accession plan(s)",
        len(plans),
    )
    direct_result = execute_direct_accession_plans(
        plans,
        args,
        run_directories,
        logger,
    )
    return DownloadExecutionResult(
        executions=direct_result.executions,
        method_used="dehydrate_fallback_direct",
        download_concurrency_used=direct_result.download_concurrency_used,
        rehydrate_workers_used=rehydrate_workers_used,
        shared_failures=(batch_failures, *direct_result.shared_failures),
    )


def execute_accession_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    decision_method: str,
    run_directories: RunDirectories,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> DownloadExecutionResult:
    """Execute accession plans for the selected download method."""

    if decision_method == "dehydrate":
        return execute_batch_dehydrate_plans(
            plans,
            args,
            run_directories,
            logger,
            secrets,
        )
    return execute_direct_accession_plans(
        plans,
        args,
        run_directories,
        logger,
    )
