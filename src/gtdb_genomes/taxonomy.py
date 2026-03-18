"""GTDB taxonomy table loading and normalisation."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from gtdb_genomes.release_resolver import ReleaseResolution


TAXONOMY_COLUMNS = ["gtdb_accession", "lineage"]


def normalise_gtdb_accession(gtdb_accession: str) -> str:
    """Convert a GTDB accession token into the underlying NCBI accession."""

    if gtdb_accession.startswith("RS_") or gtdb_accession.startswith("GB_"):
        return gtdb_accession[3:]
    return gtdb_accession


def load_taxonomy_table(path: Path) -> pl.DataFrame:
    """Load one bundled GTDB taxonomy table."""

    frame = pl.read_csv(
        path,
        separator="\t",
        has_header=False,
        new_columns=TAXONOMY_COLUMNS,
    )
    return frame.with_columns(
        pl.col("gtdb_accession").map_elements(
            normalise_gtdb_accession,
            return_dtype=pl.String,
        ).alias("ncbi_accession"),
        pl.lit(path.name).alias("taxonomy_file"),
    )


def load_release_taxonomy(resolution: ReleaseResolution) -> pl.DataFrame:
    """Load and combine the bundled taxonomy tables for a resolved release."""

    frames: list[pl.DataFrame] = []
    if resolution.bacterial_taxonomy is not None:
        frames.append(load_taxonomy_table(resolution.bacterial_taxonomy))
    if resolution.archaeal_taxonomy is not None:
        frames.append(load_taxonomy_table(resolution.archaeal_taxonomy))
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
