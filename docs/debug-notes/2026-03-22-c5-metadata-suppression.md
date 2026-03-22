# C5 metadata suppression and paired-GenBank fallback

## Failure

Live real-data case `C5` failed with exit code `6` while downloading the
`g__Bacteroides` selection with `--prefer-genbank`.

The failed accession was `RS_GCF_003670205.1`. The workflow switched to the
paired GenBank accession and later recorded a generic layout failure for
`GCA_003670205.1`, without the suppressed-assembly note.

## Root cause

`datasets summary` from `ncbi-datasets-cli 18.21.0` emitted snake_case JSON
fields such as `assembly_info`, `assembly_status`, `suppression_reason`, and
`paired_assembly`.

The metadata parser only read camelCase keys. That meant the runtime treated
real suppressed assemblies as having unknown status metadata.

Two follow-on behaviours made the failure merge-blocking:

- suppressed status on paired accessions was not propagated into planning and
  failure reporting
- a successful second summary lookup that silently omitted a requested paired
  `GCA_*` accession could still lead to `paired_to_gca`

## Patch summary

- accept both snake_case and camelCase status fields in
  `src/gtdb_genomes/metadata.py`
- track requested accessions that are missing or incomplete after every summary
  lookup
- require explicit usable status metadata before promoting a paired `GCA_*`
- fall back to the original `GCF_*` accession when paired metadata is
  incomplete or unknown
- enforce supported runtime versions for `datasets` and `unzip` in preflight
- quarantine the checked-in Bioconda recipe as `meta.yaml.template`

## Verification

- requested pytest slice passed:
  `tests/test_metadata.py`
  `tests/test_workflow_planning.py`
  `tests/test_preflight.py`
  `tests/test_edge_contract_entrypoints.py`
  `tests/test_edge_contract_planning.py`
  `tests/test_edge_contract_outputs.py`
  `tests/test_repo_contracts.py`
- live rerun of `bin/run-real-data-tests-remote.sh C5` completed with case
  status `PASS`
- `download_failures.tsv` now keeps the failing accession on
  `GCF_003670205.1` and appends the suppressed-assembly note to the recorded
  layout error
