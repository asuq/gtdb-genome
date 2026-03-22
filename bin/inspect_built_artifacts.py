"""Inspect built wheel and sdist archives for required packaged content."""

from __future__ import annotations

import argparse
import sys
import tarfile
import zipfile
from pathlib import Path


SDIST_REQUIRED_SUFFIXES = (
    "README.md",
    "LICENSE",
    "NOTICE",
    "licenses/CC-BY-SA-4.0.txt",
    "data/gtdb_taxonomy/releases.tsv",
)
SDIST_REQUIRED_FRAGMENTS = (
    "src/gtdb_genomes/",
    "data/gtdb_taxonomy/",
)
WHEEL_REQUIRED_SUFFIXES = (
    "gtdb_genomes/__init__.py",
    "gtdb_genomes/_build_info.json",
    "gtdb_genomes/data/gtdb_taxonomy/releases.tsv",
)
WHEEL_REQUIRED_FRAGMENTS = (
    "gtdb_genomes/data/gtdb_taxonomy/",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command-line arguments for the archive inspector."""

    parser = argparse.ArgumentParser(
        description="Inspect built gtdb-genomes distribution artifacts.",
    )
    parser.add_argument(
        "dist_dir",
        nargs="?",
        default="dist",
        help="Distribution directory that contains one wheel and one sdist.",
    )
    return parser.parse_args(argv)


def read_archive_members(archive_path: Path) -> tuple[str, ...]:
    """Return the ordered member names from one archive."""

    if archive_path.suffix == ".whl":
        with zipfile.ZipFile(archive_path) as handle:
            return tuple(handle.namelist())
    with tarfile.open(archive_path, "r:gz") as handle:
        return tuple(handle.getnames())


def require_single_artifact(dist_dir: Path, pattern: str) -> Path:
    """Return the single artifact that matches one glob pattern."""

    matches = sorted(dist_dir.glob(pattern))
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one {pattern!r} artifact under {dist_dir}, "
            f"found {len(matches)}",
        )
    return matches[0]


def require_suffixes(
    members: tuple[str, ...],
    suffixes: tuple[str, ...],
    archive_label: str,
) -> None:
    """Require archive members whose names end with the selected suffixes."""

    missing = [
        suffix
        for suffix in suffixes
        if not any(member.endswith(suffix) for member in members)
    ]
    if missing:
        raise ValueError(
            f"{archive_label} is missing required members: {', '.join(missing)}",
        )


def require_fragments(
    members: tuple[str, ...],
    fragments: tuple[str, ...],
    archive_label: str,
) -> None:
    """Require archive members whose names contain the selected fragments."""

    missing = [
        fragment
        for fragment in fragments
        if not any(fragment in member for member in members)
    ]
    if missing:
        raise ValueError(
            f"{archive_label} is missing required content paths: "
            f"{', '.join(missing)}",
        )


def inspect_artifacts(dist_dir: Path) -> None:
    """Validate the built sdist and wheel contents in one directory."""

    sdist_path = require_single_artifact(dist_dir, "*.tar.gz")
    wheel_path = require_single_artifact(dist_dir, "*.whl")
    sdist_members = read_archive_members(sdist_path)
    wheel_members = read_archive_members(wheel_path)

    require_suffixes(sdist_members, SDIST_REQUIRED_SUFFIXES, sdist_path.name)
    require_fragments(sdist_members, SDIST_REQUIRED_FRAGMENTS, sdist_path.name)
    require_suffixes(wheel_members, WHEEL_REQUIRED_SUFFIXES, wheel_path.name)
    require_fragments(wheel_members, WHEEL_REQUIRED_FRAGMENTS, wheel_path.name)


def main(argv: list[str] | None = None) -> int:
    """Run the built-artifact inspection command."""

    args = parse_args(argv)
    dist_dir = Path(args.dist_dir)
    try:
        inspect_artifacts(dist_dir)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    print(f"Validated built artifacts under {dist_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
