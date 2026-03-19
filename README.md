# gtdb-genomes

[![Python >=3.12](https://img.shields.io/badge/python-%3E%3D3.12-3776AB.svg)](https://www.python.org/downloads/)
[![GitHub release](https://img.shields.io/github/v/release/asuq/gtdb-genome)](https://github.com/asuq/gtdb-genome/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

`gtdb-genomes` downloads NCBI genomes from GTDB taxon selections using bundled
GTDB taxonomy tables for local taxon resolution and the NCBI `datasets` CLI for
all NCBI metadata and genome download operations.

## Installation

`gtdb-genomes` is available on PyPI and can be installed with conda
(`bioconda`):

```bash
conda install -c bioconda -c conda-forge gtdb-genomes
```

## Command Form

```bash
gtdb-genomes --gtdb-release latest --gtdb-taxon g__Escherichia --outdir results
```

Core options:

- `--gtdb-release`
- repeatable `--gtdb-taxon`
- `--outdir`
- `--prefer-genbank`
- `--download-method {auto,direct,dehydrate}`
- `--threads`
- `--ncbi-api-key`
- `--include`
- `--debug`
- `--keep-temp`
- `--dry-run`

## Examples

Small direct download:

```bash
gtdb-genomes \
  --gtdb-release latest \
  --gtdb-taxon g__Escherichia \
  --outdir results/escherichia
```

Prefer paired GenBank accessions and request extra annotation:

```bash
gtdb-genomes \
  --gtdb-release latest \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --prefer-genbank \
  --include genome,gff3 \
  --ncbi-api-key "${NCBI_API_KEY}" \
  --outdir results/methanobrevibacter
```

Bundled-data-only dry-run:

```bash
gtdb-genomes \
  --gtdb-release 95 \
  --gtdb-taxon "s__Thermoflexus hugenholtzii" \
  --download-method direct \
  --dry-run \
  --outdir /tmp/gtdb_dry_run
```

## Workflow

The tool:

1. Resolves a GTDB release from the bundled release manifest.
2. Loads the bundled GTDB taxonomy tables for that release.
3. Selects genomes whose lineage contains one or more requested GTDB taxa.
4. Starts from the accession recorded in the GTDB taxonomy table.
5. Optionally prefers paired GenBank assemblies when `--prefer-genbank` is set.
6. Uses the NCBI `datasets` command for metadata lookup and genome download.
7. Chooses direct download or batch dehydrate/rehydrate based on request size.
8. Unzips and reorganises the downloaded payload into per-taxon folders.

Detailed CLI behaviour, retry rules, output layout, runtime contract, and
bundled-data notes are documented in
[Usage details](docs/usage-details.md).

> [!IMPORTANT]
> `--ncbi-api-key` expects an NCBI API key. The tool passes it only to the
> upstream `datasets` command and does not use it for GTDB release resolution,
> local taxonomy loading, or any other service.

> [!CAUTION]
> Some legacy GTDB releases include genome accessions starting with `UBA`.
> These legacy accessions are not supported by NCBI and are not supported by
> `gtdb-genomes`. When selected, the tool warns and skips them. Check
> BioProject `PRJNA417962`, since most `UBA` genomes are assigned through that
> bioproject.

## Development And Packaging

Supported workflows:

- source-checkout development through `uv run gtdb-genomes ...` or
  `uv run python -m gtdb_genomes ...`
- packaged installation, including future Bioconda packaging, through the
  normal `gtdb-genomes ...` command

`uv` is a development tool only. Packaged runtime use should not depend on it.

## Additional Documents

- [Usage details](docs/usage-details.md)
- [Real-data validation guide](docs/real-data-validation.md)
- [Pipeline concept](docs/pipeline-concept.md)
- [Step-wise development plan](docs/development-plan.md)
- [Bioconda recipe template](packaging/bioconda/meta.yaml)
