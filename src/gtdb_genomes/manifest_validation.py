"""Shared validation helpers for GTDB manifest parsing."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar


ParsedValueT = TypeVar("ParsedValueT")


@dataclass(frozen=True, slots=True)
class ManifestHeaderValidationError(ValueError):
    """Structured failure for manifest-header validation."""

    kind: str
    missing_fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ManifestRequiredFieldError(ValueError):
    """Structured failure for one required manifest field."""

    kind: str
    field_name: str


@dataclass(frozen=True, slots=True)
class ManifestInvalidFieldError(ValueError):
    """Structured failure for one invalid optional manifest field."""

    field_name: str
    detail: str


@dataclass(frozen=True, slots=True)
class ManifestIntegrityPairingError(ValueError):
    """Structured failure for taxonomy and integrity field pairings."""

    kind: str
    taxonomy_field_name: str
    related_field_name: str


def normalise_manifest_headers(
    fieldnames: Sequence[str | None] | None,
) -> tuple[str, ...]:
    """Return normalised manifest headers or raise a structured error."""

    if fieldnames is None:
        raise ManifestHeaderValidationError(kind="missing_header")
    normalised_fieldnames = tuple(
        "" if fieldname is None else fieldname.strip()
        for fieldname in fieldnames
    )
    if any(not fieldname for fieldname in normalised_fieldnames):
        raise ManifestHeaderValidationError(kind="malformed_header")
    return normalised_fieldnames


def validate_required_manifest_headers(
    fieldnames: Sequence[str],
    required_fields: Sequence[str],
) -> None:
    """Ensure a manifest header contains every required field."""

    missing_fields = tuple(
        field_name for field_name in required_fields if field_name not in fieldnames
    )
    if missing_fields:
        raise ManifestHeaderValidationError(
            kind="missing_required_fields",
            missing_fields=missing_fields,
        )


def get_required_manifest_field_value(
    row: dict[str, str | None],
    field_name: str,
) -> str:
    """Return one required manifest field or raise a structured error."""

    raw_value = row.get(field_name)
    if raw_value is None:
        raise ManifestRequiredFieldError(
            kind="missing_field",
            field_name=field_name,
        )
    value = raw_value.strip()
    if not value:
        raise ManifestRequiredFieldError(
            kind="blank_field",
            field_name=field_name,
        )
    return value


def parse_optional_manifest_field(
    raw_value: str | None,
    *,
    field_name: str,
    parser: Callable[[str | None], ParsedValueT],
) -> ParsedValueT:
    """Parse one optional manifest field with structured error wrapping."""

    try:
        return parser(raw_value)
    except ValueError as error:
        raise ManifestInvalidFieldError(
            field_name=field_name,
            detail=str(error),
        ) from error


def validate_manifest_integrity_pairing(
    *,
    taxonomy_field_name: str,
    taxonomy_path: str | None,
    sha256_field_name: str,
    sha256_value: str | None,
    row_count_field_name: str,
    row_count_value: int | None,
) -> None:
    """Validate one taxonomy field against its integrity metadata fields."""

    if taxonomy_path is None:
        if sha256_value is not None or row_count_value is not None:
            raise ManifestIntegrityPairingError(
                kind="orphan_integrity",
                taxonomy_field_name=taxonomy_field_name,
                related_field_name=(
                    sha256_field_name
                    if sha256_value is not None
                    else row_count_field_name
                ),
            )
        return
    if sha256_value is None:
        raise ManifestIntegrityPairingError(
            kind="missing_integrity",
            taxonomy_field_name=taxonomy_field_name,
            related_field_name=sha256_field_name,
        )
    if row_count_value is None:
        raise ManifestIntegrityPairingError(
            kind="missing_integrity",
            taxonomy_field_name=taxonomy_field_name,
            related_field_name=row_count_field_name,
        )
