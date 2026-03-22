# Moderator Follow-Up Notes For Fallback Provenance And Merge-Contract Fixes

Date: 2026-03-22

## Issue Summary
- `workflow_outputs.build_enriched_output_rows()` rebuilt
  `download_request_accession` from planning-time state instead of using the
  request token that actually produced the execution result.
- This broke the documented contract for fallback rows, especially
  `paired_to_gca_fallback_original_on_download_failure`, where:
  - `selected_accession` stays on the preferred paired accession,
  - `download_batch` reflects the fallback execution batch,
  - but `download_request_accession` could incorrectly report the preferred
    token instead of the terminal fallback token.
- Release workflows also lacked a direct archive-content inspection step after
  `uv build`.
- Archive extraction trusted `unzip` without member sanitisation, and taxonomy
  bootstrap accepted any `source_root_url` scheme.

## Root Cause
- Planning and execution provenance were conflated at the manifest layer.
- Execution models carried download batch and realised accession data, but not
  the exact request token used for the row.
- Packaging validation relied on build/install checks rather than explicit
  archive-content inspection.
- Zip-member trust and bootstrap source-root trust were left implicit.

## Patch Summary
1. Added `request_accession_used` to `AccessionExecution` and populated it in:
   - direct preferred success
   - direct fallback success
   - terminal failures after preferred-only or preferred-plus-fallback paths
   - dehydrated success
   - dehydrate-to-direct fallback passthrough
   - unsupported synthetic executions
2. Changed `build_enriched_output_rows()` to use execution-time
   `request_accession_used` for `download_request_accession`.
3. Added explicit fallback regressions for:
   - fixed-version direct fallback success
   - latest-mode direct fallback success
   - fallback exhaustion
   - dehydrate-to-direct fallback success
   - direct builder-level provenance divergence
4. Added `bin/inspect_built_artifacts.py` and wired it into:
   - `.github/workflows/ci.yml`
   - `.github/workflows/release.yml`
5. Hardened archive extraction:
   - reject empty member names
   - reject absolute paths
   - reject parent traversal
   - reject drive-rooted paths
   - reject symlinks and other explicit non-regular entries
6. Hardened taxonomy bootstrap:
   - require HTTPS `source_root_url` by default
   - retain a test-only `allow_file_urls=True` override for local fixture
     mirrors
   - document that source-checkout bootstrap authenticity still depends on the
     upstream MD5 listing, while packaged runtime integrity uses bundled
     SHA-256 plus row counts

## Validation
- `./.venv/bin/pytest -q tests/test_edge_contract_execution.py tests/test_edge_contract_outputs.py`
- `./.venv/bin/pytest -q tests/test_cli.py tests/test_repo_contracts.py tests/test_layout.py tests/test_taxonomy_bundle.py`
- `./.venv/bin/pytest -q`

## Result
- Full suite passed: `235 passed, 3 skipped`
