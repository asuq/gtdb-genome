"""GTDB taxonomy table loading and normalisation."""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import polars as pl

from gtdb_genomes.bundled_data_validation import load_validated_taxonomy_text
from gtdb_genomes.release_resolver import BundledDataError, ReleaseResolution


TAXONOMY_COLUMNS = ["gtdb_accession", "lineage"]


def get_logical_taxonomy_filename(path: Path) -> str:
    """Return a stable taxonomy filename for manifests and output tables."""

    if path.name.endswith(".gz"):
        return path.name[:-3]
    return path.name


def load_taxonomy_table(
    path: Path,
    *,
    expected_sha256: str,
    expected_row_count: int,
) -> pl.DataFrame:
    """Load one bundled GTDB taxonomy table."""

    try:
        taxonomy_text = load_validated_taxonomy_text(
            path,
            expected_sha256=expected_sha256,
            expected_row_count=expected_row_count,
        )
    except (OSError, ValueError) as error:
        raise BundledDataError(str(error)) from error
    try:
        frame = pl.read_csv(
            StringIO(taxonomy_text),
            separator="\t",
            has_header=False,
            new_columns=TAXONOMY_COLUMNS,
        )
    except pl.exceptions.PolarsError as error:
        raise BundledDataError(
            f"Bundled taxonomy table could not be parsed: {path}",
        ) from error
    accession_column = pl.col("gtdb_accession")
    return frame.with_columns(
        pl.when(
            accession_column.str.starts_with("RS_")
            | accession_column.str.starts_with("GB_"),
        ).then(
            accession_column.str.slice(3),
        ).otherwise(
            accession_column,
        ).alias("ncbi_accession"),
        pl.lit(get_logical_taxonomy_filename(path)).alias("taxonomy_file"),
    )


def load_release_taxonomy(resolution: ReleaseResolution) -> pl.DataFrame:
    """Load and combine the bundled taxonomy tables for a resolved release."""

    frames: list[pl.DataFrame] = []
    if resolution.bacterial_taxonomy is not None:
        if (
            resolution.bacterial_taxonomy_sha256 is None
            or resolution.bacterial_taxonomy_rows is None
        ):
            raise BundledDataError(
                "Bundled taxonomy integrity metadata is missing for "
                f"{resolution.bacterial_taxonomy}",
            )
        frames.append(
            load_taxonomy_table(
                resolution.bacterial_taxonomy,
                expected_sha256=resolution.bacterial_taxonomy_sha256,
                expected_row_count=resolution.bacterial_taxonomy_rows,
            ),
        )
    if resolution.archaeal_taxonomy is not None:
        if (
            resolution.archaeal_taxonomy_sha256 is None
            or resolution.archaeal_taxonomy_rows is None
        ):
            raise BundledDataError(
                "Bundled taxonomy integrity metadata is missing for "
                f"{resolution.archaeal_taxonomy}",
            )
        frames.append(
            load_taxonomy_table(
                resolution.archaeal_taxonomy,
                expected_sha256=resolution.archaeal_taxonomy_sha256,
                expected_row_count=resolution.archaeal_taxonomy_rows,
            ),
        )
    if not frames:
        return pl.DataFrame(
            schema={
                "gtdb_accession": pl.String,
                "lineage": pl.String,
                "ncbi_accession": pl.String,
                "taxonomy_file": pl.String,
            },
        )
    return pl.concat(frames, how="vertical")
