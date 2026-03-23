"""Tests for the custom Hatch build hook."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("hatchling.builders.hooks.plugin.interface")

from hatch_build import CustomBuildHook, append_requires_external_metadata
from hatch_metadata import get_external_runtime_requirements


def test_initialise_build_info_requires_force_include_dict(
    tmp_path: Path,
) -> None:
    """The build hook should reject non-dict force-include state explicitly."""

    hook = CustomBuildHook.__new__(CustomBuildHook)
    hook.directory = str(tmp_path)

    with pytest.raises(RuntimeError, match="force_include"):
        hook.initialise_build_info(build_data={"force_include": []})


def test_append_requires_external_metadata_appends_known_runtime_requirements() -> None:
    """Built metadata should advertise the documented external runtime tools once."""

    metadata_text = append_requires_external_metadata(
        "Metadata-Version: 2.4\nName: gtdb-genomes\nVersion: 0.1.0\n",
    )

    for requirement in get_external_runtime_requirements():
        assert f"Requires-External: {requirement}" in metadata_text
        assert metadata_text.count(f"Requires-External: {requirement}") == 1
