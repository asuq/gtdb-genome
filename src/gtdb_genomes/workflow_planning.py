"""Planning helpers for the GTDB workflow."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import polars as pl

from gtdb_genomes.download import (
    CommandFailureRecord,
    build_preview_command,
    get_ordered_unique_accessions,
    run_preview_command,
    select_download_method,
    write_accession_input_file,
)
from gtdb_genomes.logging_utils import redact_command, redact_text
from gtdb_genomes.metadata import (
    MetadataLookupError,
    apply_accession_preferences,
    build_download_request_accession,
    build_summary_command,
    run_summary_lookup_with_retries,
)
from gtdb_genomes.workflow_execution import AccessionPlan
from gtdb_genomes.workflow_selection import build_unsupported_accession_frame

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


# Temporary planning workspace helpers.


def get_staging_directory_root() -> Path | None:
    """Return the configured temporary root for workflow staging files."""

    temp_root = os.environ.get("TMPDIR")
    if not temp_root:
        return None
    path = Path(temp_root)
    if path.exists() and not path.is_dir():
        return None
    path.mkdir(parents=True, exist_ok=True)
    return path


def create_staging_directory(prefix: str) -> TemporaryDirectory[str]:
    """Create one temporary workflow staging directory."""

    temp_root = get_staging_directory_root()
    if temp_root is None:
        return TemporaryDirectory(prefix=prefix)
    return TemporaryDirectory(prefix=prefix, dir=temp_root)


# Metadata preference resolution.


def build_accession_plans(
    mapped_frame: pl.DataFrame,
    *,
    prefer_genbank: bool,
    version_fixed: bool,
) -> tuple[AccessionPlan, ...]:
    """Build one unique download plan per original NCBI accession."""

    if mapped_frame.is_empty():
        return ()
    unique_rows = mapped_frame.unique(
        subset=["ncbi_accession"],
        keep="first",
        maintain_order=True,
    ).rows(named=True)
    return tuple(
        AccessionPlan(
            original_accession=row["ncbi_accession"],
            selected_accession=row["final_accession"],
            download_request_accession=build_download_request_accession(
                row["final_accession"],
                prefer_genbank=prefer_genbank,
                version_fixed=version_fixed,
            ),
            conversion_status=row["conversion_status"],
        )
        for row in unique_rows
    )


def resolve_supported_accession_preferences(
    supported_selected_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[pl.DataFrame, tuple[CommandFailureRecord, ...]]:
    """Resolve preferred accessions for supported selected rows."""

    summary_map: dict[str, set[str]] = {}
    metadata_failures: tuple[CommandFailureRecord, ...] = ()
    supported_accessions = get_ordered_unique_accessions(
        supported_selected_frame.get_column("ncbi_accession").to_list(),
    )
    if not supported_selected_frame.is_empty() and args.prefer_genbank:
        logger.info(
            "Running metadata lookup for %d supported accession(s)",
            len(supported_accessions),
        )
        with create_staging_directory("gtdb_genomes_metadata_") as metadata_directory:
            metadata_accession_file = write_accession_input_file(
                Path(metadata_directory) / "accessions.txt",
                supported_accessions,
            )
            metadata_command = build_summary_command(
                metadata_accession_file,
                ncbi_api_key=args.ncbi_api_key,
            )
            logger.debug("Running %s", redact_command(metadata_command, secrets))
            try:
                summary_lookup = run_summary_lookup_with_retries(
                    supported_accessions,
                    metadata_accession_file,
                    ncbi_api_key=args.ncbi_api_key,
                )
                summary_map = summary_lookup.summary_map
                metadata_failures = summary_lookup.failures
                logger.info(
                    "Metadata lookup finished with %d preferred mapping(s)",
                    len(summary_map),
                )
            except MetadataLookupError as error:
                metadata_failures = error.failures
                logger.warning(
                    "Metadata lookup failed; falling back to original accessions: %s",
                    redact_text(str(error), secrets),
                )
                summary_map = {}
    return (
        apply_accession_preferences(
            supported_selected_frame,
            summary_map,
            prefer_genbank=args.prefer_genbank,
        ),
        metadata_failures,
    )


# Automatic method planning.


def plan_supported_downloads(
    supported_mapped_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[tuple[AccessionPlan, ...], str]:
    """Build supported-accession plans and resolve the effective method."""

    accession_plans = build_accession_plans(
        supported_mapped_frame,
        prefer_genbank=args.prefer_genbank,
        version_fixed=args.version_fixed,
    )
    if not accession_plans:
        return (), args.download_method

    preview_accessions = get_ordered_unique_accessions(
        plan.download_request_accession for plan in accession_plans
    )
    with create_staging_directory("gtdb_genomes_preview_") as preview_directory:
        preview_accession_file = write_accession_input_file(
            Path(preview_directory) / "accessions.txt",
            preview_accessions,
        )
        preview_command = build_preview_command(
            preview_accession_file,
            args.include,
            ncbi_api_key=args.ncbi_api_key,
            debug=args.debug,
        )
        logger.debug("Running %s", redact_command(preview_command, secrets))
        preview_text = run_preview_command(
            preview_accession_file,
            args.include,
            ncbi_api_key=args.ncbi_api_key,
            debug=args.debug,
        )

    decision = select_download_method(
        args.download_method,
        len(preview_accessions),
        preview_text=preview_text,
    )
    return accession_plans, decision.method_used


def prepare_planning_inputs(
    supported_selected_frame: pl.DataFrame,
    unsupported_selected_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[
    pl.DataFrame,
    tuple[CommandFailureRecord, ...],
    tuple[AccessionPlan, ...],
    str,
]:
    """Resolve accession preferences and plan the supported download strategy."""

    supported_mapped_frame, metadata_failures = resolve_supported_accession_preferences(
        supported_selected_frame,
        args,
        logger,
        secrets,
    )
    unsupported_mapped_frame = build_unsupported_accession_frame(
        unsupported_selected_frame,
    )
    mapped_frame = pl.concat(
        [
            frame
            for frame in (supported_mapped_frame, unsupported_mapped_frame)
            if not frame.is_empty()
        ],
        how="vertical",
    )
    accession_plans, decision_method = plan_supported_downloads(
        supported_mapped_frame,
        args,
        logger,
        secrets,
    )
    logger.info(
        "Automatic planning selected %s for %d supported accession(s)",
        decision_method,
        len(accession_plans),
    )
    return mapped_frame, metadata_failures, accession_plans, decision_method
