# gtdb-genomes

`gtdb-genomes` is a planned command-line tool for downloading genomes from NCBI based on GTDB taxa and GTDB release taxonomy tables.

This repository is currently in a documentation-first phase. The command is not implemented yet. The documents in this repository lock the intended behaviour before coding starts.

## What The Tool Will Do

The planned workflow is:

1. Resolve a GTDB release, including historical GTDB release layouts.
2. Download the relevant GTDB taxonomy TSV files into the repository cache.
3. Select genomes whose GTDB lineage contains one or more requested taxa.
4. Use the accession recorded in the GTDB TSV as the starting accession set.
5. Prefer paired `GCA` accessions when NCBI metadata provides a GenBank counterpart.
6. Download genomes from NCBI with the `datasets` command-line tool.
7. Choose direct download or dehydrate/rehydrate based on request size.
8. Unzip the downloaded archive and reorganise the output into per-taxon folders.

Completeness has priority over strict `GCA` conversion. If a paired `GCA` accession is unavailable, the original accession is retained.

## Planned Prerequisites

The documented design assumes these tools are available:

- `uv`
- `datasets`
- `unzip`

Repo-local execution is planned through `uv`. The intended wrapper command will be `gtdb-genomes`.

## Planned Command Form

```bash
gtdb-genomes --release latest --taxon g__Escherichia --output results
```

The planned interface includes:

- `--release`
- repeatable `--taxon`
- `--output`
- `--prefer-gca` / `--no-prefer-gca`
- `--download-method {auto,direct,dehydrate}`
- `--threads`
- `--api-key`
- `--include`
- `--debug`
- `--keep-temp`
- `--dry-run`

The design explicitly does not include:

- `--taxa-file`
- `--domain`
- `--api-key-env`

## Planned Option Defaults

### `--release`

Accepts values such as `latest`, `80`, `95`, `214`, `226`, `220.0`, or `release220/220.0`. The implementation will normalise these into a concrete GTDB release path.

### `--taxon`

Repeatable. Each requested taxon is matched by descendant membership. In practice, a genome is selected when its GTDB lineage contains the requested GTDB token.

### `--prefer-gca`

Enabled by default. The tool will try to replace `GCF_*` accessions with paired `GCA_*` accessions when NCBI metadata exposes that relationship. If no paired `GCA` accession exists, the original accession will still be downloaded.

### `--download-method`

Defaults to `auto`.

The planned rules are:

- use direct download for smaller requests
- switch to dehydrate/rehydrate when the request contains at least 1,000 genomes
- also switch to dehydrate/rehydrate when `datasets --preview` reports more than 15 GB

### `--threads`

Defaults to all available CPU threads.

The planned concurrency rules are:

- direct download may shard the accession list across multiple `datasets download genome accession` jobs
- direct-mode network concurrency must never exceed 5 concurrent download jobs
- dehydrate mode uses one package download, then `datasets rehydrate --max-workers` for local file retrieval
- rehydrate worker count is planned as `min(threads, 30)`

### `--include`

Defaults to `genome`.

`--include` controls which file classes the upstream `datasets` command should fetch in addition to its standard metadata package. Planned examples include:

- `genome`
- `genome,gff3`
- `genome,gff3,protein`
- `none`

The planned implementation will validate the argument lightly, then pass it through to `datasets download genome accession --include` rather than translating it into custom internal presets.

### `--debug`

When enabled, the planned tool will:

- log at debug level
- record per-step timings
- emit redacted command traces
- write a redacted `OUTPUT/debug.log`

Debug mode is separate from `--keep-temp`. Temporary files will still be removed unless `--keep-temp` is also set.

## Output Layout

The planned output structure is:

```text
OUTPUT/
|-- accession_map.tsv
|-- download_failures.tsv
|-- run_summary.tsv
|-- taxon_summary.tsv
|-- debug.log                  # only when --debug is used
`-- taxa/
    |-- g__Escherichia/
    |   |-- taxon_accessions.tsv
    |   |-- GCA_000005845.2/
    |   `-- GCF_000026265.1/
    `-- s__Escherichia_coli/
        |-- taxon_accessions.tsv
        `-- GCA_000005845.2/
```

Important layout decisions:

- manifests are written directly under `OUTPUT/`
- per-taxon manifest files are written directly under each taxon directory
- taxon directories use a filesystem-safe taxon slug
- there is no shared `OUTPUT/genomes/` directory
- if the same genome belongs to more than one requested taxon, it is copied into each matching taxon directory
- duplicate-copy events are planned to be logged explicitly

## Taxonomy Cache

GTDB taxonomy TSV files are planned to be downloaded and kept in the repository rather than inside each output directory.

Planned cache location:

```text
data/gtdb_taxonomy/<resolved_release>/
```

This allows repeated runs against the same GTDB release without re-downloading the taxonomy tables every time.

## Representative Usage Examples

These examples describe the intended interface. They do not work yet because the tool is not implemented.

Small direct download:

```bash
gtdb-genomes \
  --release latest \
  --taxon g__Escherichia \
  --output results/escherichia
```

Large request expected to use dehydrate/rehydrate:

```bash
gtdb-genomes \
  --release 214 \
  --taxon d__Bacteria \
  --download-method auto \
  --threads 12 \
  --output results/bacteria
```

Prefer paired `GCA` accessions and request extra annotation:

```bash
gtdb-genomes \
  --release latest \
  --taxon s__Methanobrevibacter smithii \
  --prefer-gca \
  --include genome,gff3 \
  --output results/methanobrevibacter
```

Enable debug logging:

```bash
gtdb-genomes \
  --release 95 \
  --taxon g__Bacteroides \
  --debug \
  --output results/bacteroides
```

Pass an NCBI API key directly to the planned command:

```bash
gtdb-genomes \
  --release latest \
  --taxon g__Salmonella \
  --api-key "${NCBI_API_KEY}" \
  --output results/salmonella
```

## API Key Handling

The planned implementation will accept `--api-key` and pass it to the upstream `datasets` command without writing it to project files.

The tool is intended to:

- never print the API key in logs
- never save the API key in manifests, cache files, or debug output
- redact the API key from recorded command traces and error messages

Known limitation:

- if a user types the API key directly on the shell command line, the shell history or operating-system process inspection may still expose it outside the control of this tool

## Known Limitations In The Planned Design

- the repository currently contains documents only, not executable code
- GTDB release discovery must support historical naming changes across releases
- `GCA` preference depends on paired accession metadata being available from NCBI
- very large requests will still depend on upstream `datasets` performance and NCBI service availability
- direct download concurrency is intentionally capped at 5 to avoid excessive server load

## Future Packaging

The future implementation is planned to support two use cases:

- repo-local development and execution through `uv`
- a later Bioconda package that installs a normal Conda-native `gtdb-genomes` command without requiring `uv` at runtime

The current Bioconda material in this repository is only a template for future packaging work.

## Additional Documents

- [Pipeline concept](docs/pipeline-concept.md)
- [Step-wise development plan](docs/development-plan.md)
- [Bioconda recipe template](packaging/bioconda/meta.yaml)
