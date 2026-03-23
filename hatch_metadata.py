"""Custom Hatch metadata helpers for external runtime requirements."""

from __future__ import annotations

from hatchling.metadata.plugin.interface import MetadataHookInterface


EXTERNAL_RUNTIME_REQUIREMENTS = (
    "ncbi-datasets-cli (>=18.4.0,<18.22.0)",
    "unzip (>=6.0,<7.0)",
)


def get_external_runtime_requirements() -> tuple[str, ...]:
    """Return the external runtime requirements advertised in built metadata."""

    return EXTERNAL_RUNTIME_REQUIREMENTS


class CustomMetadataHook(MetadataHookInterface):
    """Keep custom metadata policy loadable for the project build."""

    PLUGIN_NAME = "custom"

    def update(self, metadata: dict[str, object]) -> None:
        """Validate that the project metadata mapping is available during builds."""

        if not isinstance(metadata, dict):
            raise TypeError(
                "Custom metadata hook expected a project metadata mapping",
            )
