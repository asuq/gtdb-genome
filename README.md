# gtdb-genomes

[![Python >=3.12](https://img.shields.io/badge/python-%3E%3D3.12-3776AB.svg)](https://www.python.org/downloads/)
[![GitHub release](https://img.shields.io/github/v/release/asuq/gtdb-genome)](https://github.com/asuq/gtdb-genome/releases)
[![Code licence: MIT](https://img.shields.io/badge/code-MIT-green.svg)](LICENSE)
[![Bundled data licence: CC BY-SA 4.0](https://img.shields.io/badge/bundled%20data-CC--BY--SA%204.0-blue.svg)](licenses/CC-BY-SA-4.0.txt)

`gtdb-genomes` downloads NCBI genomes from GTDB taxon selections using bundled
GTDB taxonomy tables for local taxon resolution and the NCBI `datasets` CLI for
all NCBI metadata and genome download operations.

The detailed runtime contract, output layout, retry rules, and bundled-data
notes live in [docs/usage-details.md](docs/usage-details.md).

## Licensing

The project code and packaging glue are released under the MIT licence.
Published source and wheel archives also bundle GTDB taxonomy tables under
`data/gtdb_taxonomy`, and those bundled data files remain under
CC BY-SA 4.0 rather than MIT.

The bundled GTDB taxonomy payload is shipped as separate `.tsv.gz` release
tables for runtime use and packaging convenience. Attribution and redistribution
details for that bundled data are recorded in [NOTICE](NOTICE) and the included
[CC BY-SA 4.0 licence text](licenses/CC-BY-SA-4.0.txt). The bundled taxonomy
payload is not relicensed by this project.

## Installation

The checked-in Bioconda recipe at
[packaging/bioconda/meta.yaml](packaging/bioconda/meta.yaml) is prepared for
the first public release, but it still awaits the published release sdist
archive and final SHA256 checksum before it can be submitted.

```bash
uv sync --group dev
uv run gtdb-genomes --help
```

## Quick Start

```bash
gtdb-genomes --gtdb-release latest --gtdb-taxon g__Escherichia --outdir results
```

## Command Contract

See [docs/usage-details.md](docs/usage-details.md) for the full CLI contract.
In short:

- `--gtdb-release` accepts bundled release aliases, including `latest`
- `--gtdb-taxon` is repeatable and matches exact GTDB lineage tokens
- `--outdir` must be empty or absent
- `--prefer-genbank` optionally prefers paired GenBank accessions
- `--threads` configures supported workflow workers, but direct downloads stay
  serial in the current workflow. See [docs/usage-details.md](docs/usage-details.md)
  for the detailed contract.
- `--dry-run` resolves and plans without creating genome payload output

## Examples

Small download where automatic planning stays on the direct path:

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

Pin the exact selected GenBank version instead of requesting the latest
revision:

```bash
gtdb-genomes \
  --gtdb-release latest \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --prefer-genbank \
  --version-fixed \
  --outdir results/methanobrevibacter-fixed
```

Supported dry-run with automatic planning:

```bash
gtdb-genomes \
  --gtdb-release 95 \
  --gtdb-taxon "s__Thermoflexus hugenholtzii" \
  --dry-run \
  --outdir /tmp/gtdb_dry_run
```

Dry-runs now check `unzip` early so real-run archive requirements fail fast.

## Output Layout

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
    |   `-- GCA_000005845.2/
    `-- s__Escherichia_coli/
        |-- taxon_accessions.tsv
        `-- GCA_000005845.2/
```

For detailed summary-file definitions, retry rules, runtime codes, and bundled
data notes, see [docs/usage-details.md](docs/usage-details.md).

> [!IMPORTANT]
> `--ncbi-api-key` expects an NCBI API key. The tool passes it only to the
> upstream `datasets` command and does not use it for GTDB release resolution,
> local taxonomy loading, or any other service.

> [!NOTE]
> Some legacy GTDB releases include genome accessions starting with `UBA`.
> These legacy accessions are not supported by NCBI and are not supported by
> `gtdb-genomes`. When selected, the tool warns and skips them. Check
> BioProject `PRJNA417962`, since most `UBA` genomes are assigned through that
> bioproject.

## Development And Packaging

Supported workflows:

- source-checkout development through `uv run gtdb-genomes ...` or
  `uv run python -m gtdb_genomes ...`
- the checked-in Bioconda recipe is prepared for the first public release, but
  it still needs the published release archive URL and SHA256 checksum before
  submission

`uv` is a development tool only. Packaged runtime use should not depend on it.

## Additional Documents

- [Usage details](docs/usage-details.md)
- [Real-data validation guide](docs/real-data-validation.md)
- [Bioconda recipe](packaging/bioconda/meta.yaml)
