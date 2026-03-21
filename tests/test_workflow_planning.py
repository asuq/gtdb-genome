"""Tests for workflow planning behaviour."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.metadata import AssemblyStatusInfo
from gtdb_genomes.workflow_planning import (
    build_suppressed_accession_notes,
    resolve_supported_accession_preferences,
)
from tests.workflow_contract_helpers import build_cli_args


def test_resolve_supported_accession_preferences_skips_metadata_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Metadata lookup should stay out of the default non-GenBank path."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metadata lookup should not run"),
        ),
    )
    supported_selected_frame = pl.DataFrame(
        {
            "requested_taxon": ["g__Escherichia"],
            "taxon_slug": ["g__Escherichia"],
            "gtdb_accession": ["RS_GCF_000001.1"],
            "ncbi_accession": ["GCF_000001.1"],
            "lineage": ["d__Bacteria;p__Proteobacteria;g__Escherichia"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )

    mapped_frame, metadata_failures, suppressed_notes = (
        resolve_supported_accession_preferences(
            supported_selected_frame,
            build_cli_args(tmp_path / "output", prefer_genbank=False),
            logging.getLogger("test-planning-skip-metadata"),
            (),
        )
    )

    assert metadata_failures == ()
    assert suppressed_notes == {}
    assert mapped_frame.select(
        "final_accession",
        "conversion_status",
    ).rows(named=True) == [
        {
            "final_accession": "GCF_000001.1",
            "conversion_status": "unchanged_original",
        },
    ]


def test_build_suppressed_accession_notes_prefers_selected_accession_status() -> None:
    """Suppression warnings should use the selected accession status when known."""

    mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": ["GCF_000001.1"],
            "final_accession": ["GCA_000001.3"],
            "conversion_status": ["paired_to_gca"],
        },
    )

    notes = build_suppressed_accession_notes(
        mapped_frame,
        {
            "GCF_000001.1": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession="GCA_000001.3",
                paired_assembly_status="current",
            ),
            "GCA_000001.3": AssemblyStatusInfo(
                assembly_status="suppressed",
                suppression_reason="removed by submitter",
                paired_accession=None,
                paired_assembly_status=None,
            ),
        },
    )

    assert notes["GCF_000001.1"].selected_accession == "GCA_000001.3"
    assert notes["GCF_000001.1"].suppression_reason == "removed by submitter"
