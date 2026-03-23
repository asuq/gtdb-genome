## 2026-03-23 moderator merge blockers

### Context
- Moderator blocked merge on four immediate areas:
  - count-only planner had an off-by-one threshold
  - API-key handling and debug contract were inconsistent
  - docs overstated API-key safety and underexplained `--prefer-genbank`
  - Bioconda template smoke tests were too shallow

### Decisions
- Keep the planner intentionally count-only for this project.
- Do not reintroduce preview or the generic `> 15 GB` switch.
- Change the dehydrated threshold from `> 1000` to `>= 1000`.
- Keep `--prefer-genbank` behaviour unchanged and document it as a live,
  time-dependent NCBI optimisation.
- Forbid `--debug` with `--ncbi-api-key`.
- Stop passing the API key on child argv and inject it through the child
  environment instead.

### Patches
- `69fc36b` `fix(runtime): harden planner threshold and API-key handling`
  - added `build_datasets_subprocess_environment()`
  - switched auto planning to `>= 1000`
  - removed child `--api-key` argv forwarding from metadata/download/rehydrate
  - passed the API key through child environment construction
  - rejected `--debug` with `--ncbi-api-key` in CLI parsing
  - added direct, dehydrated, metadata, CLI, and planner tests

- `0086295` `docs(packaging): clarify runtime contract and Bioconda smoke tests`
  - documented the count-only planner as intentional
  - clarified `--prefer-genbank` and `--version-latest` as current-NCBI,
    time-dependent behaviour
  - narrowed the API-key safety guarantee to the tool's own outputs/logs
  - made Linux packaged-runtime validation and output-directory limitations
    more prominent
  - strengthened the Bioconda template with an offline zero-match dry-run smoke
    test

### Verification
- `./.venv/bin/pytest -q`
- Result: `263 passed, 4 skipped`

### Remaining external work
- Public Bioconda readiness still requires a real pushed tag, a published
  GitHub release asset, and a verified release-archive SHA256.

## 2026-03-23 moderator follow-up blockers

### Context
- Moderator follow-up review accepted the count-only planner within the
  project scope, but still blocked on:
  - ambient-secret handling and brittle redaction
  - over-conservative GenBank fallback logic under `--version-latest`
  - missing controlled failure boundary around execution/runtime surprises
  - release validation and Bioconda smoke tests still stopping at path checks

### Decisions
- Honour ambient `NCBI_API_KEY` as the normal workflow path and keep
  `--ncbi-api-key` as an explicit override.
- Reject `--debug` whenever any effective API key is active, whether it came
  from the environment or the flag.
- Replace literal-only secret redaction with central pattern-based sanitising
  for common key-bearing forms while still masking exact known values.
- Make explicit RefSeq `paired_accession` and `paired_assembly_status` the
  primary GenBank chooser path when they are complete and usable.
- Preserve the existing fixed-version contract: normal `--prefer-genbank`
  still refuses cross-version promotion, and only `--version-latest` can move
  across revisions.
- Treat incomplete metadata as blocking only when it can change the winning
  candidate, not when it belongs to a lower-ranked stale candidate.
- Add a controlled unexpected-internal-failure boundary that returns exit `9`
  instead of a Python traceback.
- Make `resolve_and_validate_release()` a true full payload validator and use
  that stronger contract in the Bioconda smoke test.

### Patches
- `b62a079` `fix(secrets): honour ambient api keys`
  - resolved the effective NCBI API key from `--ncbi-api-key` or ambient
    `NCBI_API_KEY`, with the flag overriding the environment
  - rejected `--debug` whenever an effective API key is active
  - kept child `datasets` execution explicit by passing only the effective key
    through the child environment
  - replaced literal-only secret masking with regex-backed redaction for env
    assignments, CLI spellings, header-style leaks, JSON-style leaks, and raw
    known values
  - updated real-data helpers and docs so they stop teaching argv as the
    default secret ingress path

- `be72f6d` `fix(metadata): prefer explicit GenBank pairing`
  - tightened accession extraction around known `datasets` summary fields plus
    a narrow fallback path
  - made explicit paired-assembly metadata the primary mapping source when it
    is complete and usable
  - preserved the fixed-version contract outside `--version-latest`
  - removed the old "any incomplete candidate poisons the whole mapping"
    behaviour so lower-ranked stale candidates no longer force fallback
  - added deterministic behaviour when explicit pairing and heuristic
    candidates disagree

- `2c020ec` `fix(runtime): contain unexpected execution failures`
  - added exit code `9` for unexpected internal errors
  - wrapped execution and final output materialisation in a controlled failure
    boundary with best-effort cleanup
  - preserved partial timeout stdout/stderr excerpts in download and metadata
    retry diagnostics so incidents remain debuggable without raw tracebacks

- `57b5384` `fix(release): fully validate bundled payloads`
  - changed `resolve_and_validate_release()` to call
    `validate_release_payload()` rather than the lighter path-only check
  - strengthened the Bioconda template smoke test to load taxonomy after the
    explicit release validation step
  - updated README, usage details, packaging notes, and release tests so they
    all describe the ambient-secret contract, the explicit-pair GenBank logic,
    and the stronger release-validation meaning consistently

### Upstream checksum note
- Checked the GTDB/UQ mirror metadata for a stronger public checksum listing
  than `MD5SUM`/`MD5SUM.txt`.
- Did not find a stronger checksum source exposed alongside the mirrored
  taxonomy payloads.
- Kept the packaged-runtime SHA-256 guarantees unchanged and left the
  source-checkout bootstrap MD5 caveat explicit in the docs.

### Verification
- `mamba run -n gtdb-genome uv run pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_logging.py tests/test_real_data_scripts.py tests/test_download.py tests/workflow_contract_helpers.py`
- Result: `75 passed in 2.24s`

- `mamba run -n gtdb-genome uv run pytest -q tests/test_metadata.py tests/test_workflow_planning.py tests/test_edge_contract_planning.py tests/test_edge_contract_outputs.py`
- Result: `69 passed in 6.37s`

- `mamba run -n gtdb-genome uv run pytest -q tests/test_download.py tests/test_metadata.py tests/test_cli_integration.py tests/test_edge_contract_entrypoints.py tests/test_edge_contract_outputs.py`
- Result: `77 passed in 5.46s`

- `mamba run -n gtdb-genome uv run pytest -q tests/test_release_resolver.py tests/test_repo_contracts.py tests/test_real_data_scripts.py tests/test_cli.py`
- Result: `93 passed in 8.97s`

- `mamba run -n gtdb-genome uv run pytest -q`
- Result: `279 passed, 1 skipped in 15.36s`

- `mamba run -n gtdb-genome uv build`
- Result: built `dist/gtdb_genomes-0.1.0.tar.gz` and
  `dist/gtdb_genomes-0.1.0-py3-none-any.whl`

## 2026-03-23 release-sdist packaging boundary and failure-decomposition pass

### Context
- Maintainer review still blocked merge on four points:
  - the repository checkout was not a self-contained community build input
  - `run_id` was deterministic for the request envelope but not for the
    realised biological output under live NCBI accession rewriting
  - direct-mode unresolved groups could fail too coarsely after one mixed-batch
    failure
  - dehydrated-mode fallback discarded partial success and collapsed too much
    work into serial direct fallback
- Chosen project direction for this pass:
  - keep the repository checkout bootstrap-based for maintainers
  - treat the tagged release `sdist` as the only supported community packaging
    input
  - fix provenance and failure amplification without changing the count-only
    planner or the current direct-download concurrency model

### Decisions
- Added a stable accession-decision digest derived from the realised accession
  map and made `run_id` depend on that digest.
- Preserved the public CLI surface and changed only run-summary provenance:
  `run_summary.tsv` now records `accession_decision_sha256`.
- Replaced coarse whole-group direct failure handling with recursive group
  decomposition down to singleton requests.
- Preserved partial dehydrated success before sending only unresolved plans
  into direct fallback.
- Split metadata-specific parsing out of `metadata.py` so the new blocker fixes
  did not make that file even larger.
- Narrowed the community packaging boundary in docs and CI to the tagged
  release `sdist`; kept the repository bootstrap and MD5 caveat explicitly
  scoped to maintainers and source checkouts.

### Patches
- `77561ad` `fix(provenance): tie run ids to accession outcomes`
  - added `run_identity.py` with stable accession-decision record and digest
    builders plus the new run-id constructor
  - extended `run_summary.tsv` with `accession_decision_sha256`
  - changed run-summary generation so `run_id` now changes when the realised
    accession decisions change under the same top-level request
  - added regression tests for digest stability, digest sensitivity to
    biological-output fields, and fixed-versus-latest run-id divergence

- `489b1cf` `fix(execution): decompose failed download cohorts`
  - added `workflow_execution_batches.py` with shared request-grouping and
    recursive decomposition helpers
  - changed direct mode to bisect failed or layout-unresolved mixed batches
    until only irreducible bad requests fail
  - changed dehydrated execution to preserve resolved payloads after
    extraction or rehydrate failures and to fall back only unresolved plans
  - added edge-contract tests for mixed-batch failure decomposition,
    layout-only decomposition, and partial dehydrated success preservation

- `632ffe5` `refactor(metadata): split summary parsing helpers`
  - extracted accession parsing into `assembly_accessions.py`
  - extracted structured summary parsing and status extraction into
    `metadata_summary_parsing.py`
  - kept `gtdb_genomes.metadata` as the stable public import surface for the
    chooser and retry logic while shrinking the module footprint

- `f1147a4` `test(packaging): validate release-sdist build boundary`
  - strengthened `test_hatch_build.py` to exercise actual bundled-payload
    validation with both passing and checksum-mismatch synthetic releases
  - updated README, usage details, real-data validation notes, and Bioconda
    packaging notes so they consistently describe the tagged release `sdist`
    as the community packaging input and keep the MD5 bootstrap caveat outside
    that trust boundary
  - added a main CI `validation-c-sdist-runtime` job that installs the exact
    built `sdist` into a clean runtime without rerunning
    `bootstrap_taxonomy` and validates bundled taxonomy loading
  - aligned repo-contract tests with the new runtime and packaging wording

### Debugging note
- The new execution regression suite initially appeared to hang.
- Root cause: `test_batch_dehydrate_passes_api_key_via_child_environment()`
  still monkeypatched the old atomic `locate_batch_payload_directories()`
  seam after the code switched to `locate_partial_batch_payload_directories()`.
- Consequence: the test fell through into real direct fallback behaviour and
  waited instead of resolving the dehydrated batch in-memory.
- Fix: patched the test to monkeypatch the new partial-resolution seam and
  return `PartialBatchPayloadResolution`.

### Verification
- `mamba run -n gtdb-genome uv run pytest -q tests/test_metadata.py tests/test_workflow_planning.py tests/test_edge_contract_planning.py`
- Result: `56 passed in 1.39s`

- `mamba run -n gtdb-genome uv run pytest -q tests/test_hatch_build.py tests/test_repo_contracts.py`
- Result: `16 passed, 1 skipped in 6.29s`

- `mamba run -n gtdb-genome uv run pytest -q tests/test_run_identity.py tests/test_edge_contract_outputs.py tests/test_edge_contract_execution.py`
- Result: `36 passed in 4.89s`

- `mamba run -n gtdb-genome uv run pytest -q`
- Result: `292 passed, 1 skipped in 14.83s`

- `mamba run -n gtdb-genome uv build`
- Result: built `dist/gtdb_genomes-0.1.0.tar.gz` and
  `dist/gtdb_genomes-0.1.0-py3-none-any.whl`

## 2026-03-23 live identity and runtime-contract follow-up

### Context
- Moderator re-review still blocked code approval on the live assembly-identity
  paths:
  - `--version-latest` needed to anchor to explicit paired GenBank metadata
    when present
  - post-extraction payload resolution still accepted stem-only matches for
    versioned requests
- A broader contract pass was also requested to make the runtime requirements
  explicit in built package metadata and to tighten the docs around dry-run
  preflight, bootstrap scope, and live NCBI reproducibility.

### Decisions
- Keep early `unzip` preflight for dry-runs as an intentional fail-fast
  behaviour.
- Make complete explicit `paired_accession` and `paired_assembly_status`
  authoritative for `--version-latest`, and fall back conservatively when
  explicit pairing conflicts with the heuristic family view.
- Allow post-extraction stem-only payload resolution only for genuinely
  versionless request tokens emitted by `--version-latest`.
- Advertise external runtime tools in built wheel/sdist metadata with
  `Requires-External` hints, while keeping CLI preflight as the authoritative
  runtime gate.
- Treat `bootstrap_taxonomy` as a maintainer and source-checkout workflow,
  not the recommended end-user installation path.

### Patches
- `2a414b7` `fix(metadata): anchor latest mode to explicit GenBank pairs`
  - augmented candidate metadata scope with explicit paired GCA accessions
    even when omitted from the first summary payload
  - anchored `--version-latest` to the explicit paired GenBank family when
    that pairing is complete
  - introduced conservative fallback status
    `paired_gca_conflict_fallback_original` for explicit-versus-heuristic
    family disagreement
  - emitted a workflow warning when that conflict fallback occurs
  - added regression tests for omitted explicit pairs and conflict warnings

- `8623439` `fix(layout): require exact payload matches for versioned requests`
  - restricted stem-only payload matching to versionless request tokens
  - made versioned requests fail closed when only a same-family different
    version is present after extraction
  - preferred the canonical `ncbi_dataset/data` root before any recursive
    fallback scan
  - extended staging temp-root discovery to honour `TMPDIR`, then `TMP`, then
    `TEMP`
  - added edge-contract tests for versioned rejection, canonical-root
    preference, and temp-env fallback behaviour

- `96f39f3` `fix(contract): publish runtime metadata hints`
  - added `hatch_metadata.py` plus build-hook finalisation that injects
    `Requires-External` headers for `ncbi-datasets-cli` and `unzip` into built
    wheel and sdist metadata
  - updated `pyproject.toml` so the sdist includes both Hatch hook scripts
    and the custom metadata hook is declared explicitly
  - tightened CLI help, README, usage details, and real-data validation docs
    around explicit-pair latest-mode behaviour, exact payload identity rules,
    dry-run `unzip` preflight, audit-trail fields, and maintainer-only
    taxonomy bootstrap scope
  - added packaging regression tests for `Requires-External` emission in both
    wheel `METADATA` and sdist `PKG-INFO`
  - fixed an isolated-build import issue by putting the project root on
    `sys.path` before the build hook imports `hatch_metadata`
  - fixed the wheel-from-sdist path by explicitly including `hatch_metadata.py`
    in the sdist

### Verification
- `mamba run -n gtdb-genome uv run pytest -q tests/test_metadata.py tests/test_workflow_planning.py`
- Result: `39 passed in 0.21s`

- `mamba run -n gtdb-genome uv run pytest -q tests/test_edge_contract_payloads.py tests/test_edge_contract_planning.py tests/test_preflight.py`
- Result: `33 passed in 2.85s`

- `mamba run -n gtdb-genome uv run pytest -q tests/test_hatch_build.py tests/test_repo_contracts.py`
- Result: `16 passed, 1 skipped in 8.63s`

- `mamba run -n gtdb-genome uv run pytest -q`
- Result: `286 passed, 1 skipped in 18.82s`

- `mamba run -n gtdb-genome uv build`
- Result: built `dist/gtdb_genomes-0.1.0.tar.gz` and
  `dist/gtdb_genomes-0.1.0-py3-none-any.whl`

## 2026-03-24 live regression repair and stale-review triage

### Context
- A later moderator pass mixed three different categories:
  - one real new regression introduced during the release-metadata work
  - two still-live runtime bugs that had not actually been fixed yet
  - several stale findings that were already corrected by earlier commits
- The goal of this pass was to repair only the live regressions and leave the
  deliberate design choices alone.

### Live issues fixed in this pass
- `96f39f3` introduced a real wheel-compliance regression by mutating wheel
  `METADATA` after build without regenerating `RECORD`.
- Primary `--prefer-genbank` metadata lookup still aborted the whole workflow
  with exit `5` when the first summary lookup exhausted retries.
- `execute_accession_plans()` still treated any unknown internal method string
  as implicit direct mode instead of failing loudly.

### Stale moderator findings confirmed as already fixed
- `77561ad` already made `run_id` depend on
  `accession_decision_sha256`, so run identity now tracks realised accession
  outcomes rather than only the request envelope.
- `489b1cf` already decomposed direct-batch failures and preserved partial
  dehydrated success before fallback; later reviews that still described
  whole-cohort direct/dehydrate failure amplification were stale against the
  current tree.
- `57b5384` already made `resolve_and_validate_release()` a real payload
  validator rather than a path-only check.

### Intentional deferred design choices
- Keep the planner count-only for this project.
- Keep direct downloads serial in the current workflow.
- Keep duplicate-across-taxa output materialisation as explicit copied payload
  trees for now.
- Keep the repository checkout as a maintainer/source-checkout input and the
  tagged release `sdist` as the supported community packaging boundary.
- Keep the MD5-scoped source bootstrap caveat explicitly limited to the
  maintainer/source-checkout workflow.

### Patches
- `9e1da74` `fix(build): regenerate wheel record after metadata patching`
  - regenerated wheel `RECORD` after appending `Requires-External` headers to
    `METADATA`
  - made missing `RECORD` a build-time error in the post-processor
  - extended `bin/inspect_built_artifacts.py` so artifact validation now checks
    wheel `RECORD` row coverage, hash values, and file sizes
  - added a synthetic-wheel regression test that proves the patched wheel is
    internally self-consistent

- `0c66df7` `fix(planning): degrade metadata lookup failures to original accessions`
  - wrapped the primary supported-accession summary lookup in the same
    degrade-to-original spirit previously used for candidate paired-GCA lookup
  - preserved shared metadata failure provenance when retry exhaustion returns
    concrete failure rows
  - changed dry-run entrypoint behaviour so `--prefer-genbank` remains a true
    preference instead of a run-level hard failure when the initial metadata
    lookup is unavailable

- `a79204b` `fix(execution): reject unsupported download methods`
  - made execution dispatch exhaustive for `"direct"` and `"dehydrate"`
  - changed unexpected internal method values from silent direct-mode fallback
    to an explicit internal-contract error handled by the existing exit-`9`
    boundary

### Moderator response matrix
- Fixed now:
  - wheel `RECORD` integrity after metadata post-processing
  - primary metadata lookup aborting `--prefer-genbank` runs
  - silent direct-mode fallback for unknown execution methods
- Already fixed before this pass:
  - run-id provenance drift
  - direct failure decomposition
  - partial dehydrated fallback preservation
  - full bundled-payload validation in `resolve_and_validate_release()`
- Intentional and currently deferred:
  - count-only planner
  - serial direct downloads
  - duplicate copied payloads across taxa
  - maintainer-only checkout bootstrap with MD5 caveat

### Verification
- `mamba run -n gtdb-genome uv run pytest -q tests/test_hatch_build.py tests/test_repo_contracts.py`
- Result: `16 passed, 1 skipped in 6.59s`

- `mamba run -n gtdb-genome uv run pytest -q tests/test_workflow_planning.py tests/test_edge_contract_entrypoints.py tests/test_edge_contract_execution.py`
- Result: `37 passed in 0.93s`

- `mamba run -n gtdb-genome uv run pytest -q`
- Result: `294 passed, 1 skipped in 14.35s`

- `mamba run -n gtdb-genome uv build`
- Result: built `dist/gtdb_genomes-0.1.0.tar.gz` and
  `dist/gtdb_genomes-0.1.0-py3-none-any.whl`

- `mamba run -n gtdb-genome python3 bin/inspect_built_artifacts.py dist`
- Result: `Validated built artifacts under dist`
