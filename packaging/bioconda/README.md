# Bioconda Packaging Notes

`meta.yaml` is the upstream copy of the merged Bioconda recipe for
`gtdb-genomes` version `0.2.0`, updated here for the next dependency-window release.

Keep this file synced from the Bioconda recipe when Bioconda-specific changes
are merged. The current recipe uses the tagged GitHub release `sdist`, the
verified `sha256`, `noarch: python`, and the same lightweight smoke tests used
for the submitted package.

The smoke tests cover bundled taxonomy loading plus one offline zero-match dry-run path
so the packaged CLI contract is exercised without a live download.
The current runtime requirements include `polars >=1.31.0,<2.0.0`,
`tqdm >=4.60.0,<5.0.0`, `ncbi-datasets-cli >=18.4.0,<18.27.0`, and
`unzip >=6.0,<7.0`.

For future releases:

1. Bump the recipe `version`.
2. Update the source archive URL and `sha256`.
3. Keep runtime requirements aligned with `pyproject.toml` and the documented
   external-tool support window.
4. Run Bioconda lint and build checks.
5. Submit a small update PR to `bioconda/bioconda-recipes`.
