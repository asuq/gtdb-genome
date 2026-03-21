## Release hardening notes

Date: 2026-03-21

This note records the release-hardening pass requested after mentor review.
The work was split into three commits so the packaging, workflow, and CI
changes remain reviewable.

### Commit `8d5072d` `fix(bundle): validate bundled taxonomy payloads`

Why:

- The release artefact path previously depended on whatever happened to be
  present under `data/gtdb_taxonomy` at build time.
- Runtime validation only proved that bundled taxonomy files could be opened,
  which was too weak for packaged scientific data.
- The generated payload bootstrap needed to preserve a good local release if a
  refresh failed part-way through.

What changed:

- Added `src/gtdb_genomes/bundled_data_validation.py` to centralise bundled
  payload checksum, gzip, UTF-8, row-count, and row-structure validation.
- Extended `data/gtdb_taxonomy/releases.tsv` and the release resolver runtime
  contract with required SHA256 and row-count fields for each configured
  taxonomy payload.
- Strengthened `src/gtdb_genomes/release_resolver.py` so runtime release
  resolution validates the installed payload against the manifest rather than
  merely opening the files.
- Updated `src/gtdb_genomes/taxonomy_bundle.py` so bootstrap refreshes those
  runtime integrity fields and keeps the previously materialised release when
  staging fails.
- Added `hatch_build.py` and wired it through `pyproject.toml` so source builds
  fail clearly when the bundled payload has not been bootstrapped.

Debugging notes:

- A packaging check exposed that Hatch passes the build target label
  (`standard`) into the hook `initialize()` method. The hook now reads the
  package version from `pyproject.toml` instead of recording that label in
  `_build_info.json`.
- A manual `uv build` from the source checkout now fails with the expected
  bootstrap guidance, which is the intended release gate.

### Commit `524fb07` `fix(workflow): capture deterministic run provenance`

Why:

- Successful runs did not record enough provenance to reconstruct exact
  accession-selection behaviour later.
- `--prefer-genbank` still ranked newer suppressed `GCA_*` candidates ahead of
  usable unsuppressed ones.
- Total metadata lookup failure still degraded the run too coarsely when the
  user explicitly requested GenBank preference.

What changed:

- Added `src/gtdb_genomes/provenance.py` to capture package version, git
  revision, external tool versions, bundled-manifest hash, taxonomy hashes, and
  a deterministic SHA256-based `run_id`.
- Expanded the output schema in `src/gtdb_genomes/layout.py` and
  `src/gtdb_genomes/workflow_outputs.py` so success-path manifests record
  `version_latest`, build/runtime provenance, `selected_accession`, and the
  exact `download_request_accession`.
- Updated `src/gtdb_genomes/metadata.py` and
  `src/gtdb_genomes/workflow_planning.py` so GenBank preference ranking uses
  suppression status first, then version, and falls back to the original
  accession when every matching `GCA_*` candidate is suppressed.
- Changed `src/gtdb_genomes/workflow.py` so total metadata lookup failure under
  `--prefer-genbank` exits with code `5` instead of warning and silently
  degrading.

Debugging notes:

- The planning and output contract helpers needed synthetic
  `ReleaseResolution` hashes and row counts after the resolver contract grew.
- The output tests now assert both fixed-version and `--version-latest`
  `download_request_accession` behaviour so later regressions are visible.

### Commit `50dbdbb` `chore(ci): enforce packaged runtime validation`

Why:

- The packaged-runtime path needed to be enforced continuously, not only by
  convention.
- The release workflow needed to validate a clean installed wheel without `uv`
  on `PATH`.
- The real-data documentation and bash contract had to match the new CI/runtime
  behaviour.

What changed:

- Updated `.github/workflows/ci.yml` to keep the pytest matrix on Python
  `3.12`, `3.13`, and `3.14`, and to split Validation C into build and clean
  runtime jobs.
- Added `.github/workflows/release.yml` to enforce bootstrap, build, clean
  runtime wheel installation, bundled-data validation, and packaged-runtime case
  execution for release candidates.
- Updated `bin/run-real-data-tests-remote.sh` and the related tests so `C5`
  runs in CI without requiring `NCBI_API_KEY`, while `C7` remains optional and
  key-gated.
- Refreshed `docs/usage-details.md`, `docs/real-data-validation.md`, and the
  packaging tests in `tests/test_entrypoints.py` so the public contract matches
  the code and the build gate is covered explicitly.

Debugging notes:

- A synthetic packaging fixture now covers both sides of the build contract:
  one manifest-only source checkout that must fail, and one minimal valid
  bundled release that must build and resolve `latest` inside a clean runtime
  venv without `uv`.

### Verification

Commands run in the requested environment:

- `mamba run -n gtdb-genome uv run --group dev pytest -q`
- `mamba run -n gtdb-genome uv build --out-dir /tmp/gtdb-build-check`

Results:

- `pytest`: `202 passed in 6.49s`
- `uv build` from the source checkout fails intentionally until
  `bootstrap_taxonomy` has materialised the bundled payload.

### Scope notes

- The unrelated `README.md` edit and untracked `CITATION.cff` were left
  untouched.
