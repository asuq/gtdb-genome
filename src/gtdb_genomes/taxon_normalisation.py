"""Requested GTDB taxon normalisation helpers."""

from __future__ import annotations

from collections.abc import Sequence
import re


GTDB_TAXON_PATTERN = re.compile(r"^(?P<rank>[dpcofgs])__(?P<body>\S(?:.*\S)?)$")


def normalise_requested_taxon(requested_taxon: str) -> str:
    """Trim only surrounding whitespace from one requested GTDB taxon."""

    return requested_taxon.strip()


def is_complete_requested_taxon(requested_taxon: str) -> bool:
    """Return whether one parsed CLI value is a complete GTDB taxon token."""

    taxon = normalise_requested_taxon(requested_taxon)
    match = GTDB_TAXON_PATTERN.fullmatch(taxon)
    if match is None:
        return False
    rank = match.group("rank")
    body = match.group("body")
    has_internal_whitespace = any(character.isspace() for character in body)
    if rank == "s":
        return has_internal_whitespace
    return not has_internal_whitespace


def normalise_requested_taxa(requested_taxa: Sequence[str]) -> tuple[str, ...]:
    """Normalise requested GTDB taxa while preserving order and duplicates."""

    return tuple(
        normalise_requested_taxon(requested_taxon)
        for requested_taxon in requested_taxa
    )
