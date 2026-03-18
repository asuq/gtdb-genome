# Development Plan

This document is the engineering handoff for implementing `gtdb-genomes` after the documentation phase is complete.

The current phase is documentation-only. No Python package, wrapper script, or implementation modules should be created until this plan is accepted.

## Phase 1: Project scaffold and tooling

### Goal

Create the minimal project skeleton for a `uv`-managed Python application and establish the base CLI shape without implementing the full download workflow.

### Concrete tasks

- create `pyproject.toml` for a Python 3.12+ project
- choose and declare the build backend compatible with `uv`
- create the package layout under `src/gtdb_genomes/`
- add the repo-local wrapper command that executes through `uv`
- add startup checks for required external tools: `datasets` and `unzip`
- define argument parsing for the planned CLI options

### Acceptance criteria

- running the future wrapper with `--help` shows the documented options
- the package can be executed with `uv run python -m gtdb_genomes.cli --help`
- missing external tools produce clear, non-secret-bearing error messages

### Notable risks or assumptions

- the eventual build backend must work both with repo-local `uv` usage and with Conda packaging
- the Bioconda package must install a normal entrypoint rather than the repo-local `uv` wrapper

## Phase 2: GTDB release discovery and taxonomy cache

### Goal

Implement reliable GTDB release resolution across historical naming variants and cache the taxonomy tables in the repository.

### Concrete tasks

- query the GTDB releases index
- normalise supported `--release` inputs to a concrete release path
- inspect the target release directory for available bacterial and archaeal taxonomy TSV files
- support historical filename variants rather than one fixed pattern
- download the required TSV files into `data/gtdb_taxonomy/<resolved_release>/`
- reuse cached files when present

### Acceptance criteria

- representative older and newer GTDB releases resolve correctly
- the selected taxonomy TSV files are cached in the repo and reused on subsequent runs
- `latest` resolves to a concrete release before caching

### Notable risks or assumptions

- GTDB directory HTML may change and should therefore be parsed defensively
- some releases may not present both bacterial and archaeal files in the same way

## Phase 3: Taxon filtering and accession selection

### Goal

Parse GTDB taxonomy tables, match requested taxa by descendant membership, and produce the initial accession set.

### Concrete tasks

- load taxonomy TSV files with Polars
- parse accession and lineage columns safely
- support repeatable `--taxon`
- implement descendant membership matching
- merge matches across taxa while keeping per-taxon membership information
- deduplicate the accession set for download planning

### Acceptance criteria

- repeated taxa produce a combined accession set without losing taxon membership mapping
- descendant matching behaves as documented for rank tokens such as `d__`, `g__`, and `s__`
- the accession list and taxon mapping can be exported into summary tables

### Notable risks or assumptions

- taxonomy rows may contain unusual spacing or formatting and should be normalised carefully
- overlapping taxon requests are expected and must be handled intentionally

## Phase 4: NCBI metadata lookup and `GCA` preference

### Goal

Refine the GTDB accession set by preferring paired `GCA` accessions when NCBI metadata makes that possible.

### Concrete tasks

- query NCBI assembly metadata for selected accessions
- detect paired GenBank and RefSeq relationships
- replace `GCF_*` accessions with paired `GCA_*` accessions when available
- preserve the original accession when no paired `GCA` accession exists
- record conversion status for later summaries

### Acceptance criteria

- paired `GCA` accessions are preferred when present
- original accessions are retained when pairing is unavailable
- summary output records original accession, final accession, and conversion status

### Notable risks or assumptions

- NCBI metadata fields may vary slightly across records and should be read robustly
- the design prioritises completeness over strict `GCA` conversion

## Phase 5: Download orchestration and concurrency control

### Goal

Select the correct `datasets` workflow and control concurrency safely.

### Concrete tasks

- implement `direct`, `dehydrate`, and `auto` modes
- run `datasets --preview` in `auto` mode
- switch to dehydrate/rehydrate for requests with at least 1,000 genomes or more than 15 GB
- implement direct-mode accession sharding
- cap direct-mode download concurrency at 5 jobs
- map `--threads` to local worker limits and rehydrate worker count
- support `--include` passthrough and `--api-key` forwarding with redaction

### Acceptance criteria

- auto mode chooses the documented path for small and large requests
- direct mode never exceeds 5 concurrent `datasets` download jobs
- dehydrate mode uses one package download followed by controlled rehydration
- command construction respects the documented `--include` and `--threads` behaviour

### Notable risks or assumptions

- upstream `datasets` behaviour may change between versions
- preview output parsing must be robust to minor format changes

## Phase 6: Unzip and output reorganisation

### Goal

Convert raw `datasets` output into the documented final directory structure.

### Concrete tasks

- unzip downloaded archives into working directories
- locate assembly-specific content within the `ncbi_dataset` layout
- create `OUTPUT/` summary files directly under the output root
- create per-taxon directories under `OUTPUT/taxa/<taxon_slug>/`
- copy each genome into every taxon directory where it belongs
- write per-taxon accession manifests directly inside each taxon directory
- support `--keep-temp` for preserving intermediate files

### Acceptance criteria

- final output matches the documented tree
- there is no shared `OUTPUT/genomes/` directory
- duplicate genomes appear in each relevant taxon directory
- duplicate-copy actions are recorded in logs

### Notable risks or assumptions

- the `datasets` output tree may differ slightly between direct and dehydrated workflows
- copying duplicates may increase disk usage substantially for broad taxon selections

## Phase 7: Logging, debug mode, and secret redaction

### Goal

Provide clear operational logging and strong secret hygiene.

### Concrete tasks

- define normal and debug logging formats
- add redaction helpers for API keys and command traces
- write `OUTPUT/debug.log` only when `--debug` is enabled
- log duplicate-copy events, download decisions, and failure summaries
- ensure errors do not leak secrets

### Acceptance criteria

- API keys never appear in normal logs, debug logs, manifests, or cache files
- `--debug` produces a more detailed redacted log without changing functional behaviour
- failure messages remain actionable without exposing sensitive values

### Notable risks or assumptions

- shell history and process inspection remain outside the control of the tool
- third-party command errors may need additional redaction before display

## Phase 8: Testing

### Goal

Build a test suite that validates behaviour without depending on large live downloads by default.

### Concrete tasks

- add unit tests for release resolution and filename discovery
- add fixture-based tests for taxonomy parsing and taxon matching
- add tests for accession conversion logic
- add tests for direct vs dehydrate decision rules
- add tests for concurrency capping at 5
- add tests for output reorganisation and duplicate-copy handling
- add tests for secret redaction and debug logging
- use fake or stubbed `datasets` command behaviour where practical

### Acceptance criteria

- the documented behaviours are covered by automated tests
- tests can run locally without requiring a live large-scale NCBI download
- edge cases around duplicates, missing pairs, and partial failures are covered

### Notable risks or assumptions

- a small number of integration tests may still be useful later, but they should be separate from default unit tests
- fixture maintenance will be needed if upstream output formats change

## Phase 9: Packaging and release preparation

### Goal

Prepare the project for future distribution without changing the documented behaviour.

### Concrete tasks

- align package metadata with the root README
- finalise the console entrypoint name as `gtdb-genomes`
- complete the Bioconda recipe with real version, source URL, and checksum
- verify dependency availability for Conda packaging
- ensure the Conda package installs a normal entrypoint instead of the repo-local `uv` wrapper
- review user-facing documentation for release readiness

### Acceptance criteria

- packaging metadata is consistent across the Python project and Bioconda recipe
- the Bioconda recipe can be completed with concrete release metadata
- end-user documentation matches the shipped CLI behaviour

### Notable risks or assumptions

- some dependencies, especially the NCBI `datasets` CLI package name and channel source, may need confirmation during packaging
- packaging should happen after the implementation is stable enough to justify a tagged release

## Cross-Cutting Decisions

These decisions are fixed across all phases:

- command name: `gtdb-genomes`
- documentation phase first, implementation later
- repeatable `--taxon`
- no `--taxa-file`, `--domain`, or `--api-key-env`
- default `--include genome`
- `--debug` writes `OUTPUT/debug.log`
- taxonomy TSVs are cached in the repo
- no shared `OUTPUT/genomes/`
- manifests are written directly under `OUTPUT/` and directly under each taxon directory
- duplicated genomes are copied into each taxon folder and logged
- direct-mode network concurrency never exceeds 5
