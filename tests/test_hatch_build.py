"""Tests for the custom Hatch build hook."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("hatchling.builders.hooks.plugin.interface")

from hatch_build import CustomBuildHook


def test_initialise_build_info_requires_force_include_dict(
    tmp_path: Path,
) -> None:
    """The build hook should reject non-dict force-include state explicitly."""

    hook = CustomBuildHook.__new__(CustomBuildHook)
    hook.directory = str(tmp_path)

    with pytest.raises(RuntimeError, match="force_include"):
        hook.initialise_build_info(build_data={"force_include": []})
