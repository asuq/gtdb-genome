# Debug Note: C5 suppressed assembly warning

## Context

- Remote case `C5` failed for one genome only: `GCF_003670205.1`
- The user checked NCBI and suspected that the assembly had been suppressed
- The workflow did not explain that suppression status to the user, so the
  failure looked like a generic download problem

## Live NCBI findings on 2026-03-20

- NCBI docs do not support a blanket rule of "suppressed means fully gone"
  - assembly status docs:
    [Genome assembly versioning and status](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/data-processing/policies-annotation/genome-processing/version-status/)
  - data processing docs:
    [GenBank and SRA Data Processing](https://www.ncbi.nlm.nih.gov/sra/docs/sequence-data-processing/)
- For the specific failed accession, the live FTP record shows:
  - [assembly_status.txt](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/003/670/205/GCF_003670205.1_ASM367020v1/assembly_status.txt)
    contains `status=suppressed`
  - the FTP directory still exists:
    [GCF_003670205.1_ASM367020v1](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/003/670/205/GCF_003670205.1_ASM367020v1/)
  - [md5checksums.txt](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/003/670/205/GCF_003670205.1_ASM367020v1/md5checksums.txt)
    only lists assembly report and stats files
  - the genomic FASTA payload URL returns `404`
- Practical conclusion:
  - suppressed assemblies may still keep accession-level record artefacts
  - `GCF_003670205.1` currently does not expose a downloadable genome payload

## Code response

- Keep runtime detection inside the existing `datasets summary` path
- Do not add FTP probes or a second NCBI lookup path during normal runs
- Parse `assemblyInfo.assemblyStatus`, `assemblyInfo.suppressionReason`, and
  `assemblyInfo.pairedAssembly.status` from metadata summary JSON
- Warn the user twice when the selected download target is metadata-confirmed
  suppressed
  - once during planning
  - once again at the end if the accession still failed
- Append a persistent suppression note to `download_failures.tsv` for failed
  suppressed accessions without changing the TSV schema

## Commits

- `d1b89b3` `feat(workflow): warn on suppressed assembly targets`
- `dbb3069` `test(workflow): cover suppressed assembly warnings`
