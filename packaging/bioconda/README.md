# Bioconda Packaging Notes

`meta.yaml.template` is a draft recipe template for the first public Bioconda
submission.

It is intentionally quarantined from merge-ready packaging because the source
archive URL and final `sha256` must be filled from a tagged GitHub release.
The template also mirrors the current runtime floors from `pyproject.toml`,
including `polars >=1.31.0,<2.0.0`.

Do not submit or publish this template unchanged. Copy it to `meta.yaml` only
when a tagged release archive exists and the final `sha256` has been verified.
