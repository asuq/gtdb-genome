"""NCBI metadata lookup and accession preference handling."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable


ACCESSION_PATTERN = re.compile(r"GC[AF]_\d+\.\d+")


def build_summary_command(
    accessions: Iterable[str],
    api_key: str | None = None,
    datasets_bin: str = "datasets",
) -> list[str]:
    """Build the datasets summary command for assembly accessions."""

    command = [
        datasets_bin,
        "summary",
        "genome",
        "accession",
        *accessions,
        "--as-json-lines",
    ]
    if api_key:
        command.extend(["--api-key", api_key])
    return command


def extract_accessions(payload: object) -> set[str]:
    """Recursively extract assembly accessions from a JSON-like payload."""

    found: set[str] = set()
    if isinstance(payload, dict):
        for value in payload.values():
            found.update(extract_accessions(value))
        return found
    if isinstance(payload, list):
        for value in payload:
            found.update(extract_accessions(value))
        return found
    if isinstance(payload, str):
        found.update(ACCESSION_PATTERN.findall(payload))
    return found


def parse_summary_json_lines(
    raw_text: str,
    requested_accessions: Iterable[str],
) -> dict[str, set[str]]:
    """Map requested accessions to the accessions discovered in summary output."""

    requested = set(requested_accessions)
    summaries: dict[str, set[str]] = {}
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        discovered = extract_accessions(payload)
        matching_requested = requested.intersection(discovered)
        if len(matching_requested) == 1:
            summaries[next(iter(matching_requested))] = discovered
    return summaries


def choose_preferred_accession(
    requested_accession: str,
    discovered_accessions: set[str] | None,
    prefer_gca: bool = True,
) -> tuple[str, str]:
    """Choose the final accession and conversion status for one request."""

    if not prefer_gca:
        return requested_accession, "unchanged_original"
    if discovered_accessions is None:
        return requested_accession, "metadata_lookup_failed_fallback_original"
    if requested_accession.startswith("GCA_"):
        return requested_accession, "unchanged_original"
    paired_gca = sorted(
        accession
        for accession in discovered_accessions
        if accession.startswith("GCA_")
    )
    if paired_gca:
        return paired_gca[0], "paired_to_gca"
    return requested_accession, "unchanged_original"
