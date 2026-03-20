# C1 Threaded Direct-Download Segfault Note

## Failure

- remote packaged-runtime case `C1` intermittently exited `139`
- the failing runs created the output root plus `.gtdb_genomes_work/` and
  `taxa/`, but wrote no manifests and no stderr text
- the same remote environment completed:
  - `remote-smoke-c1` with `--threads 2`
  - `c1-serial` with `--threads 1`
  - `C4` and `C6` from the remote runner

## Current reading

- the packaged wheel and bundled taxonomy data are not the issue
- the failing path is specific to the `latest / s__Thermoflexus hugenholtzii`
  direct-download case under the remote runner
- because `c1-serial` succeeds, the next investigation target is the threaded
  direct-download path rather than release resolution or the selected taxa

## Patches added

- debug instrumentation in the threaded direct-download workflow:
  - resolved worker count
  - per-group start and completion lines
  - redacted direct download commands
  - archive extraction start and completion lines
- remote investigation controls:
  - `REAL_DATA_C1_THREADS`
  - `REAL_DATA_PYTHON_FAULTHANDLER`
  - `REAL_DATA_DEBUG_SAFE`
- runner evidence copy for `debug.log` when present

## Next repro

```bash
export REMOTE_TEST_ROOT=/tmp/gtdb-realtests/remote-$(date +%Y%m%d)-debug
export REAL_DATA_PYTHON_FAULTHANDLER=1
export REAL_DATA_DEBUG_SAFE=1
bash /tmp/gtdb-genome-remote/run-real-data-tests-remote.sh C1
```

```bash
export REMOTE_TEST_ROOT=/tmp/gtdb-realtests/remote-$(date +%Y%m%d)-serial
export REAL_DATA_C1_THREADS=1
export REAL_DATA_PYTHON_FAULTHANDLER=1
export REAL_DATA_DEBUG_SAFE=1
bash /tmp/gtdb-genome-remote/run-real-data-tests-remote.sh C1
```

Compare `_evidence/C1/debug.log`, `_evidence/C1/stderr.log`, and
`run_summary.tsv`.
