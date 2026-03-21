"""Custom Hatch build hook for bundled GTDB payload verification."""

from __future__ import annotations

from pathlib import Path
import sys

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from gtdb_genomes.provenance import (
    get_git_revision,
    read_pyproject_version,
    write_build_info,
)
from gtdb_genomes.release_resolver import (
    BundledDataError,
    get_release_manifest_path,
    load_release_manifest,
    resolve_release,
    validate_release_resolution,
)


class CustomBuildHook(BuildHookInterface):
    """Validate bundled taxonomy data before building any artefact."""

    def initialise_build_info(
        self,
        *,
        build_data: dict[str, object],
    ) -> None:
        """Generate packaged build metadata and stage it for the artefact."""

        build_directory = Path(self.directory)
        package_version = read_pyproject_version(PROJECT_ROOT)
        build_info_path = build_directory / "generated" / "gtdb_genomes" / "_build_info.json"
        write_build_info(
            build_info_path,
            package_version_value=package_version,
            git_revision=get_git_revision(),
        )
        force_include = build_data.setdefault("force_include", {})
        assert isinstance(force_include, dict)
        force_include[str(build_info_path)] = "gtdb_genomes/_build_info.json"

    def validate_bundled_taxonomy(self) -> None:
        """Validate every manifest-configured bundled release before build."""

        manifest_path = get_release_manifest_path()
        entries = load_release_manifest(manifest_path)
        if not entries:
            raise RuntimeError(
                f"Bundled release manifest is empty: {manifest_path}",
            )
        for entry in entries:
            validate_release_resolution(resolve_release(entry.resolved_release))

    def initialize(
        self,
        version: str,
        build_data: dict[str, object],
    ) -> None:
        """Reject builds that do not contain the validated bundled payload."""

        if version == "editable":
            return
        try:
            self.validate_bundled_taxonomy()
        except BundledDataError as error:
            raise RuntimeError(
                "Bundled GTDB taxonomy payload is not ready for packaging. "
                f"{error}",
            ) from error
        self.initialise_build_info(build_data=build_data)
