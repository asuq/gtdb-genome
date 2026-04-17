"""Tests for taxonomy manifest refresh and bootstrap helpers."""

from __future__ import annotations

import gzip
import hashlib
from pathlib import Path

import pytest

from gtdb_genomes.release_resolver import (
    BundledDataError,
    validate_configured_taxonomy_file,
)
from gtdb_genomes.taxonomy_bundle import (
    BOOTSTRAP_COMMAND,
    TaxonomyBundleEntry,
    TaxonomyBundleError,
    bootstrap_manifest_entries,
    bootstrap_taxonomy_bundle,
    compress_tsv_bytes,
    infer_taxonomy_source_name,
    load_taxonomy_bundle_manifest,
    parse_latest_release_number,
    parse_release_directory_numbers,
    materialise_taxonomy_file,
    refresh_taxonomy_bundle_manifest,
    validate_bootstrap_entry,
)


RUNTIME_MANIFEST_HEADER = (
    "resolved_release\taliases\tbacterial_taxonomy\tarchaeal_taxonomy\t"
    "bacterial_taxonomy_sha256\tarchaeal_taxonomy_sha256\t"
    "bacterial_taxonomy_rows\tarchaeal_taxonomy_rows\tis_latest"
)
BOOTSTRAP_MANIFEST_HEADER = f"{RUNTIME_MANIFEST_HEADER}\tsource_root_url\tchecksum_filename"
DUMMY_SHA256 = "0" * 64
DUMMY_ROWS = "1"


def write_manifest_text(path: Path, text: str) -> None:
    """Write one manifest fixture to disk."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="ascii")


def build_runtime_manifest_row(
    resolved_release: str,
    aliases: str,
    bacterial_taxonomy: str,
    archaeal_taxonomy: str,
    is_latest: str,
    bacterial_sha256: str = DUMMY_SHA256,
    archaeal_sha256: str = "",
    bacterial_rows: str = DUMMY_ROWS,
    archaeal_rows: str = "",
) -> str:
    """Build one manifest row with runtime integrity columns."""

    return "\t".join(
        (
            resolved_release,
            aliases,
            bacterial_taxonomy,
            archaeal_taxonomy,
            bacterial_sha256,
            archaeal_sha256,
            bacterial_rows,
            archaeal_rows,
            is_latest,
        ),
    )


def build_bootstrap_manifest_row(
    resolved_release: str,
    aliases: str,
    bacterial_taxonomy: str,
    archaeal_taxonomy: str,
    is_latest: str,
    source_root_url: str,
    checksum_filename: str,
    bacterial_sha256: str = DUMMY_SHA256,
    archaeal_sha256: str = "",
    bacterial_rows: str = DUMMY_ROWS,
    archaeal_rows: str = "",
) -> str:
    """Build one bootstrap manifest row with build metadata."""

    return "\t".join(
        (
            resolved_release,
            aliases,
            bacterial_taxonomy,
            archaeal_taxonomy,
            bacterial_sha256,
            archaeal_sha256,
            bacterial_rows,
            archaeal_rows,
            is_latest,
            source_root_url,
            checksum_filename,
        ),
    )


def write_bytes(path: Path, content: bytes) -> None:
    """Write raw bytes to one fixture path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def build_md5_line(filename: str, content: bytes) -> str:
    """Return one checksum-file line for a fixture payload."""

    checksum = hashlib.md5(content).hexdigest()
    return f"{checksum} ./{filename}"


def write_checksum_file(
    root: Path,
    filename: str,
    payloads: dict[str, bytes],
) -> None:
    """Write one checksum listing for a fake mirror release directory."""

    content = "\n".join(
        build_md5_line(payload_name, payload)
        for payload_name, payload in payloads.items()
    )
    write_bytes(root / filename, (content + "\n").encode("ascii"))


def write_checksum_lines(
    root: Path,
    filename: str,
    lines: tuple[str, ...],
) -> None:
    """Write one raw checksum listing for duplicate-entry fixtures."""

    write_bytes(root / filename, ("\n".join(lines) + "\n").encode("ascii"))


def read_gzip_text(path: Path) -> str:
    """Read one gzipped text fixture."""

    with gzip.open(path, "rt", encoding="ascii") as handle:
        return handle.read()


def install_fake_remote_mapping(
    monkeypatch: pytest.MonkeyPatch,
    *,
    url_texts: dict[str, str],
    url_bytes: dict[str, bytes],
) -> None:
    """Patch URL readers to serve deterministic remote fixtures."""

    def fake_read_url_text(url: str) -> str:
        """Return one text fixture or raise the normal bundling error."""

        if url in url_texts:
            return url_texts[url]
        if url in url_bytes:
            return url_bytes[url].decode("utf-8")
        raise TaxonomyBundleError(f"Could not read URL: {url}")

    def fake_read_url_bytes(url: str) -> bytes:
        """Return one byte fixture or raise the normal bundling error."""

        if url in url_bytes:
            return url_bytes[url]
        if url in url_texts:
            return url_texts[url].encode("utf-8")
        raise TaxonomyBundleError(f"Could not read URL: {url}")

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.read_url_text",
        fake_read_url_text,
    )
    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.read_url_bytes",
        fake_read_url_bytes,
    )


def test_parse_release_directory_numbers_ignores_non_release_entries() -> None:
    """Release discovery should keep only numeric `releaseNNN/` directories."""

    index_html = (
        '<a href="release95/">release95/</a>\n'
        '<a href="latest/">latest/</a>\n'
        '<a href="release232/">release232/</a>\n'
        '<a href="temporary/">temporary/</a>\n'
        '<a href="release95/">release95/</a>\n'
    )

    assert parse_release_directory_numbers(index_html) == (95, 232)


def test_parse_latest_release_number_rejects_unparseable_text() -> None:
    """Latest-version parsing should fail fast on malformed VERSION payloads."""

    with pytest.raises(TaxonomyBundleError, match="Could not parse"):
        parse_latest_release_number("Released sometime recently\n")


def test_infer_taxonomy_source_name_prefers_current_release_filenames() -> None:
    """Filename inference should prefer `bac120` and `ar53` over legacy names."""

    checksum_mapping = {
        "bac120_taxonomy_r232.tsv.gz": ("a" * 32,),
        "bac_taxonomy_r232.tsv.gz": ("b" * 32,),
        "ar53_taxonomy_r232.tsv.gz": ("c" * 32,),
        "ar122_taxonomy_r232.tsv.gz": ("d" * 32,),
    }

    assert infer_taxonomy_source_name(
        232,
        checksum_mapping,
        (
            "bac120_taxonomy_r{release}.tsv.gz",
            "bac_taxonomy_r{release}.tsv.gz",
        ),
    ) == "bac120_taxonomy_r232.tsv.gz"
    assert infer_taxonomy_source_name(
        232,
        checksum_mapping,
        (
            "ar53_taxonomy_r{release}.tsv.gz",
            "ar122_taxonomy_r{release}.tsv.gz",
        ),
    ) == "ar53_taxonomy_r232.tsv.gz"


def test_refresh_manifest_builds_runtime_integrity_for_plain_tsv_release(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Refresh should gzip plain TSV sources and write runtime integrity fields."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                RUNTIME_MANIFEST_HEADER,
                build_runtime_manifest_row(
                    "80.0",
                    "80,80.0",
                    "bac_taxonomy_r80.tsv.gz",
                    "",
                    "false",
                ),
            ],
        )
        + "\n",
    )
    releases_root_url = "https://example.org/releases/"
    source_root_url = f"{releases_root_url}release80/80.0/"
    bacterial_plain = b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n"
    install_fake_remote_mapping(
        monkeypatch,
        url_texts={
            releases_root_url: '<a href="release80/">release80/</a>\n',
            f"{releases_root_url}latest/VERSION.txt": "v80 Released Apr 15, 2026\n",
            f"{source_root_url}MD5SUM": build_md5_line(
                "bac_taxonomy_r80.tsv",
                bacterial_plain,
            )
            + "\n",
        },
        url_bytes={
            f"{source_root_url}bac_taxonomy_r80.tsv": bacterial_plain,
        },
    )

    entries = refresh_taxonomy_bundle_manifest(
        manifest_path,
        releases_root_url=releases_root_url,
    )

    assert len(entries) == 1
    assert entries[0].source_root_url == source_root_url
    assert entries[0].checksum_filename == "MD5SUM"
    assert entries[0].bacterial_taxonomy == "bac_taxonomy_r80.tsv.gz"
    assert entries[0].bacterial_taxonomy_sha256 is not None
    assert entries[0].bacterial_taxonomy_rows == 1
    manifest_text = manifest_path.read_text(encoding="ascii")
    assert "source_root_url" in manifest_text


def test_load_taxonomy_bundle_manifest_rejects_missing_required_headers(
    tmp_path: Path,
) -> None:
    """Bundling manifest loading should reject missing required columns."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                "resolved_release\tbacterial_taxonomy\tarchaeal_taxonomy\tis_latest",
                "95.0\tbac.tsv.gz\t\ttrue",
            ],
        )
        + "\n",
    )

    with pytest.raises(TaxonomyBundleError, match="missing required columns"):
        load_taxonomy_bundle_manifest(manifest_path)


def test_load_taxonomy_bundle_manifest_rejects_blank_required_fields(
    tmp_path: Path,
) -> None:
    """Bundling manifest loading should reject blank required values."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    " ",
                    "bac120_taxonomy_r95.tsv.gz",
                    "",
                    "true",
                    "https://example.org/release95/95.0/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )

    with pytest.raises(TaxonomyBundleError, match="blank field aliases"):
        load_taxonomy_bundle_manifest(manifest_path)


@pytest.mark.parametrize(
    ("invalid_taxonomy_path", "expected_message"),
    [
        ("../escape.tsv.gz", "parent-directory references"),
        ("/absolute/escape.tsv.gz", "relative path"),
        ("C:/drive-rooted.tsv.gz", "drive-rooted"),
    ],
)
def test_load_taxonomy_bundle_manifest_rejects_invalid_taxonomy_paths(
    tmp_path: Path,
    invalid_taxonomy_path: str,
    expected_message: str,
) -> None:
    """Bundling manifest loading should reject taxonomy paths that escape the tree."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    "95,95.0",
                    invalid_taxonomy_path,
                    "",
                    "true",
                    "https://example.org/release95/95.0/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )

    with pytest.raises(TaxonomyBundleError, match=expected_message):
        load_taxonomy_bundle_manifest(manifest_path)


def test_refresh_manifest_discovers_newer_releases_and_rebuilds_in_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Refresh should preserve curated history and append newly discovered releases."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                RUNTIME_MANIFEST_HEADER,
                build_runtime_manifest_row(
                    "95.0",
                    "95,95.0",
                    "bac120_taxonomy_r95.tsv.gz",
                    "ar122_taxonomy_r95.tsv.gz",
                    "false",
                    archaeal_sha256=DUMMY_SHA256,
                    archaeal_rows=DUMMY_ROWS,
                ),
                build_runtime_manifest_row(
                    "226.0",
                    "226,226.0,latest",
                    "bac120_taxonomy_r226.tsv.gz",
                    "ar53_taxonomy_r226.tsv.gz",
                    "true",
                    archaeal_sha256=DUMMY_SHA256,
                    archaeal_rows=DUMMY_ROWS,
                ),
            ],
        )
        + "\n",
    )
    releases_root_url = "https://example.org/releases/"
    bacterial_plain = b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n"
    archaeal_95_plain = b"RS_GCF_000002.1\td__Archaea;g__Methanobrevibacter\n"
    bacterial_95_gzip = gzip.compress(bacterial_plain, mtime=7)
    archaeal_95_gzip = gzip.compress(archaeal_95_plain, mtime=7)
    bacterial_226_plain = b"RS_GCF_000003.1\td__Bacteria;g__Thermoflexus\n"
    archaeal_226_plain = b"RS_GCF_000004.1\td__Archaea;g__Methanobrevibacter\n"
    bacterial_226_gzip = gzip.compress(bacterial_226_plain, mtime=0)
    archaeal_226_gzip = gzip.compress(archaeal_226_plain, mtime=0)
    bacterial_232_plain = b"RS_GCF_000005.1\td__Bacteria;g__Thermoflexus\n"
    archaeal_232_plain = b"RS_GCF_000006.1\td__Archaea;g__Methanobrevibacter\n"
    bacterial_232_gzip = gzip.compress(bacterial_232_plain, mtime=0)
    archaeal_232_gzip = gzip.compress(archaeal_232_plain, mtime=0)
    install_fake_remote_mapping(
        monkeypatch,
        url_texts={
            releases_root_url: "\n".join(
                [
                    '<a href="release95/">release95/</a>',
                    '<a href="release214/">release214/</a>',
                    '<a href="release226/">release226/</a>',
                    '<a href="release232/">release232/</a>',
                    '<a href="latest/">latest/</a>',
                    "",
                ],
            ),
            f"{releases_root_url}latest/VERSION.txt": "v232 Released Apr 15, 2026\n",
            f"{releases_root_url}release95/95.0/MD5SUM": "\n".join(
                [
                    build_md5_line("bac120_taxonomy_r95.tsv.gz", bacterial_95_gzip),
                    build_md5_line("ar122_taxonomy_r95.tsv.gz", archaeal_95_gzip),
                    "",
                ],
            ),
            f"{releases_root_url}release226/226.0/MD5SUM.txt": "\n".join(
                [
                    build_md5_line("bac120_taxonomy_r226.tsv.gz", bacterial_226_gzip),
                    build_md5_line("ar53_taxonomy_r226.tsv.gz", archaeal_226_gzip),
                    "",
                ],
            ),
            f"{releases_root_url}release232/232.0/MD5SUM.txt": "\n".join(
                [
                    build_md5_line("bac120_taxonomy_r232.tsv.gz", bacterial_232_gzip),
                    build_md5_line("ar53_taxonomy_r232.tsv.gz", archaeal_232_gzip),
                    "",
                ],
            ),
        },
        url_bytes={
            f"{releases_root_url}release95/95.0/bac120_taxonomy_r95.tsv.gz": bacterial_95_gzip,
            f"{releases_root_url}release95/95.0/ar122_taxonomy_r95.tsv.gz": archaeal_95_gzip,
            f"{releases_root_url}release226/226.0/bac120_taxonomy_r226.tsv.gz": bacterial_226_gzip,
            f"{releases_root_url}release226/226.0/ar53_taxonomy_r226.tsv.gz": archaeal_226_gzip,
            f"{releases_root_url}release232/232.0/bac120_taxonomy_r232.tsv.gz": bacterial_232_gzip,
            f"{releases_root_url}release232/232.0/ar53_taxonomy_r232.tsv.gz": archaeal_232_gzip,
        },
    )

    entries = refresh_taxonomy_bundle_manifest(manifest_path, releases_root_url=releases_root_url)

    assert [entry.resolved_release for entry in entries] == ["95.0", "226.0", "232.0"]
    assert entries[-1].aliases == "232,232.0,release232,release232/232.0,latest"
    assert [entry.is_latest for entry in entries] == ["false", "false", "true"]
    assert all(entry.source_root_url is not None for entry in entries)
    assert "214.0" not in manifest_path.read_text(encoding="ascii")


def test_refresh_manifest_tolerates_unrelated_duplicate_checksum_entries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Refresh should ignore conflicting duplicate entries for unrelated files."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                RUNTIME_MANIFEST_HEADER,
                build_runtime_manifest_row(
                    "214.0",
                    "214,214.0",
                    "bac120_taxonomy_r214.tsv.gz",
                    "ar53_taxonomy_r214.tsv.gz",
                    "false",
                    archaeal_sha256=DUMMY_SHA256,
                    archaeal_rows=DUMMY_ROWS,
                ),
            ],
        )
        + "\n",
    )
    releases_root_url = "https://example.org/releases/"
    bacterial_plain = b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n"
    bacterial_gzip = gzip.compress(bacterial_plain, mtime=0)
    archaeal_plain = b"RS_GCF_000002.1\td__Archaea;g__Methanobrevibacter\n"
    archaeal_gzip = gzip.compress(archaeal_plain, mtime=0)
    install_fake_remote_mapping(
        monkeypatch,
        url_texts={
            releases_root_url: '<a href="release214/">release214/</a>\n',
            f"{releases_root_url}latest/VERSION.txt": "v214 Released Apr 15, 2026\n",
            f"{releases_root_url}release214/214.0/MD5SUM.txt": "\n".join(
                (
                    build_md5_line("bac120_taxonomy_r214.tsv.gz", bacterial_gzip),
                    build_md5_line("ar53_taxonomy_r214.tsv.gz", archaeal_gzip),
                    build_md5_line(
                        "genomic_files_all/ar53_msa_marker_genes_all_r214.tar.gz",
                        b"first",
                    ),
                    build_md5_line(
                        "genomic_files_all/ar53_msa_marker_genes_all_r214.tar.gz",
                        b"second",
                    ),
                    "",
                ),
            ),
        },
        url_bytes={
            f"{releases_root_url}release214/214.0/bac120_taxonomy_r214.tsv.gz": bacterial_gzip,
            f"{releases_root_url}release214/214.0/ar53_taxonomy_r214.tsv.gz": archaeal_gzip,
        },
    )

    entries = refresh_taxonomy_bundle_manifest(
        manifest_path,
        releases_root_url=releases_root_url,
    )

    assert entries[0].checksum_filename == "MD5SUM.txt"


def test_bootstrap_taxonomy_bundle_gzips_plain_tsv_payloads_deterministically(
    tmp_path: Path,
) -> None:
    """Bootstrap should gzip plain mirror TSV files with deterministic output."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    bacterial_plain = b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "80.0",
                    "80,80.0",
                    "bac_taxonomy_r80.tsv.gz",
                    "",
                    "false",
                    f"{(tmp_path / 'mirror' / 'release80' / '80.0').as_uri()}/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )
    release_root = tmp_path / "mirror" / "release80" / "80.0"
    payloads = {"bac_taxonomy_r80.tsv": bacterial_plain}
    write_checksum_file(release_root, "MD5SUM", payloads)
    write_bytes(release_root / "bac_taxonomy_r80.tsv", bacterial_plain)

    generated_paths = bootstrap_taxonomy_bundle(
        manifest_path,
        data_root=data_root,
        allow_file_urls=True,
    )

    output_path = data_root / "80.0" / "bac_taxonomy_r80.tsv.gz"
    assert generated_paths == (output_path,)
    assert output_path.read_bytes() == compress_tsv_bytes(bacterial_plain)
    assert read_gzip_text(output_path) == bacterial_plain.decode("ascii")


def test_bootstrap_taxonomy_bundle_preserves_upstream_gzip_payloads(
    tmp_path: Path,
) -> None:
    """Bootstrap should keep upstream gzipped taxonomy files unchanged."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    archaeal_plain = b"RS_GCF_000002.1\td__Archaea;g__Methanobrevibacter\n"
    archaeal_gzip = gzip.compress(archaeal_plain, mtime=123)
    source_root = tmp_path / "mirror" / "release226" / "226.0"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "226.0",
                    "226,latest",
                    "",
                    "ar53_taxonomy_r226.tsv.gz",
                    "true",
                    f"{source_root.as_uri()}/",
                    "MD5SUM.txt",
                    bacterial_sha256="",
                    bacterial_rows="",
                    archaeal_sha256=DUMMY_SHA256,
                    archaeal_rows=DUMMY_ROWS,
                ),
            ],
        )
        + "\n",
    )
    payloads = {"ar53_taxonomy_r226.tsv.gz": archaeal_gzip}
    write_checksum_file(source_root, "MD5SUM.txt", payloads)
    write_bytes(source_root / "ar53_taxonomy_r226.tsv.gz", archaeal_gzip)

    bootstrap_taxonomy_bundle(
        manifest_path,
        data_root=data_root,
        allow_file_urls=True,
    )

    output_path = data_root / "226.0" / "ar53_taxonomy_r226.tsv.gz"
    assert output_path.read_bytes() == archaeal_gzip


def test_bootstrap_taxonomy_bundle_ignores_unrelated_duplicate_checksum_entries(
    tmp_path: Path,
) -> None:
    """Bootstrap should tolerate conflicting duplicate entries for unrelated files."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    source_root = tmp_path / "mirror" / "release214" / "214.0"
    bacterial_plain = b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n"
    bacterial_gzip = gzip.compress(bacterial_plain, mtime=0)
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "214.0",
                    "214,214.0",
                    "bac120_taxonomy_r214.tsv.gz",
                    "",
                    "false",
                    f"{source_root.as_uri()}/",
                    "MD5SUM.txt",
                ),
            ],
        )
        + "\n",
    )
    write_checksum_lines(
        source_root,
        "MD5SUM.txt",
        (
            build_md5_line("bac120_taxonomy_r214.tsv.gz", bacterial_gzip),
            build_md5_line("genomic_files_all/dup.tar.gz", b"first"),
            build_md5_line("genomic_files_all/dup.tar.gz", b"second"),
        ),
    )
    write_bytes(source_root / "bac120_taxonomy_r214.tsv.gz", bacterial_gzip)

    bootstrap_taxonomy_bundle(
        manifest_path,
        data_root=data_root,
        allow_file_urls=True,
    )

    output_path = data_root / "214.0" / "bac120_taxonomy_r214.tsv.gz"
    assert output_path.read_bytes() == bacterial_gzip


def test_bootstrap_taxonomy_bundle_rejects_missing_checksum_file(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail when a release directory has no checksum file."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "80.0",
                    "80,80.0",
                    "bac_taxonomy_r80.tsv.gz",
                    "",
                    "false",
                    f"{(tmp_path / 'mirror' / 'release80' / '80.0').as_uri()}/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )

    with pytest.raises(TaxonomyBundleError, match="Could not read URL"):
        bootstrap_taxonomy_bundle(
            manifest_path,
            data_root=manifest_path.parent,
            allow_file_urls=True,
        )


def test_validate_bootstrap_entry_requires_https_source_root_url() -> None:
    """Bootstrap entries should reject non-HTTPS source roots by default."""

    entry = TaxonomyBundleEntry(
        resolved_release="95.0",
        aliases="95,95.0",
        bacterial_taxonomy="bac120_taxonomy_r95.tsv.gz",
        archaeal_taxonomy=None,
        bacterial_taxonomy_sha256=DUMMY_SHA256,
        archaeal_taxonomy_sha256=None,
        bacterial_taxonomy_rows=1,
        archaeal_taxonomy_rows=None,
        is_latest="true",
        source_root_url="http://example.org/release95/95.0/",
        checksum_filename="MD5SUM",
    )

    with pytest.raises(TaxonomyBundleError, match="must use an HTTPS source_root_url"):
        validate_bootstrap_entry(entry)


def test_materialise_taxonomy_file_requires_checksum_value(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Materialisation should fail explicitly when the checksum is missing."""

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.resolve_source_name",
        lambda target_name, available_filenames: "bac.tsv.gz",
    )
    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.get_checksum_for_source",
        lambda source_name, checksum_mapping, source_root_url: None,
    )

    with pytest.raises(TaxonomyBundleError, match="Checksum entry"):
        materialise_taxonomy_file(
            "https://example.org/release95/95.0/",
            "bac.tsv.gz",
            tmp_path / "bac.tsv.gz",
            {"bac.tsv.gz": ("deadbeef",)},
        )


def test_bootstrap_manifest_entries_requires_source_metadata_even_after_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Bootstrap should raise explicit errors instead of relying on asserts."""

    entry = TaxonomyBundleEntry(
        resolved_release="95.0",
        aliases=("95", "95.0"),
        bacterial_taxonomy="bac.tsv.gz",
        archaeal_taxonomy=None,
        bacterial_taxonomy_sha256=None,
        archaeal_taxonomy_sha256=None,
        bacterial_taxonomy_rows=None,
        archaeal_taxonomy_rows=None,
        is_latest=True,
        source_root_url=None,
        checksum_filename=None,
    )

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.validate_bootstrap_entry",
        lambda entry, allow_file_urls=False: None,
    )

    with pytest.raises(TaxonomyBundleError, match="missing source_root_url"):
        bootstrap_manifest_entries((entry,), tmp_path)


def test_bootstrap_taxonomy_bundle_rejects_unresolvable_source_from_checksum_map(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail when no matching source name exists in the checksum map."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    source_root = tmp_path / "mirror" / "release95" / "95.0"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    "95,95.0",
                    "bac120_taxonomy_r95.tsv.gz",
                    "",
                    "true",
                    f"{source_root.as_uri()}/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )
    write_checksum_file(source_root, "MD5SUM", {})
    write_bytes(
        source_root / "bac120_taxonomy_r95.tsv.gz",
        gzip.compress(b"row\n", mtime=0),
    )

    with pytest.raises(TaxonomyBundleError, match="mirror source matching"):
        bootstrap_taxonomy_bundle(
            manifest_path,
            data_root=data_root,
            allow_file_urls=True,
        )


def test_bootstrap_taxonomy_bundle_rejects_checksum_mismatch(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail when a downloaded file does not match the MD5 entry."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    source_root = tmp_path / "mirror" / "release95" / "95.0"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    "95,95.0",
                    "bac120_taxonomy_r95.tsv.gz",
                    "",
                    "true",
                    f"{source_root.as_uri()}/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )
    payload = gzip.compress(
        b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n",
        mtime=0,
    )
    write_checksum_file(
        source_root,
        "MD5SUM",
        {"bac120_taxonomy_r95.tsv.gz": gzip.compress(b"other\n", mtime=0)},
    )
    write_bytes(source_root / "bac120_taxonomy_r95.tsv.gz", payload)

    with pytest.raises(TaxonomyBundleError, match="Checksum mismatch"):
        bootstrap_taxonomy_bundle(
            manifest_path,
            data_root=data_root,
            allow_file_urls=True,
        )


def test_bootstrap_taxonomy_bundle_preserves_existing_release_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap should leave an existing release untouched when staging fails."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    release_root = data_root / "95.0"
    release_root.mkdir(parents=True)
    sentinel_path = release_root / "sentinel.txt"
    sentinel_path.write_text("original\n", encoding="ascii")
    source_root = tmp_path / "mirror" / "release95" / "95.0"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    "95,95.0",
                    "bac120_taxonomy_r95.tsv.gz",
                    "",
                    "true",
                    f"{source_root.as_uri()}/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.load_checksum_mapping",
        lambda source_root_url, checksum_filename: {
            "bac120_taxonomy_r95.tsv.gz": ("checksum",),
        },
    )

    def fail_materialise(*args, **kwargs) -> None:
        """Raise a deterministic bootstrap failure during staging."""

        raise TaxonomyBundleError("staging failed")

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.materialise_taxonomy_file",
        fail_materialise,
    )

    with pytest.raises(TaxonomyBundleError, match="staging failed"):
        bootstrap_taxonomy_bundle(
            manifest_path,
            data_root=data_root,
            allow_file_urls=True,
        )

    assert sentinel_path.read_text(encoding="ascii") == "original\n"
    assert release_root.is_dir()


def test_bootstrap_taxonomy_bundle_restores_release_and_manifest_on_refresh_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest refresh failures should roll back the swapped release directory."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    release_root = data_root / "95.0"
    release_root.mkdir(parents=True)
    sentinel_path = release_root / "sentinel.txt"
    sentinel_path.write_text("original\n", encoding="ascii")
    original_manifest_text = "\n".join(
        [
            BOOTSTRAP_MANIFEST_HEADER,
            build_bootstrap_manifest_row(
                "95.0",
                "95,95.0",
                "bac120_taxonomy_r95.tsv.gz",
                "",
                "true",
                "https://example.org/release95/95.0/",
                "MD5SUM",
            ),
        ],
    ) + "\n"
    write_manifest_text(manifest_path, original_manifest_text)

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.load_checksum_mapping",
        lambda source_root_url, checksum_filename: {
            "bac120_taxonomy_r95.tsv.gz": ("checksum",),
        },
    )

    def fake_materialise_taxonomy_file(
        source_root_url: str,
        target_name: str | None,
        target_path: Path | None,
        checksum_mapping: dict[str, tuple[str, ...]],
    ) -> None:
        """Create one staged payload without touching the network."""

        del source_root_url, checksum_mapping
        if target_name is None or target_path is None:
            return
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(
            gzip.compress(
                b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n",
                mtime=0,
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.materialise_taxonomy_file",
        fake_materialise_taxonomy_file,
    )

    def fail_refresh_runtime_manifest(
        manifest_path: Path,
        entries: tuple[TaxonomyBundleEntry, ...],
        data_root: Path,
    ) -> None:
        """Fail deterministically after the staged release has been swapped in."""

        del manifest_path, entries, data_root
        raise TaxonomyBundleError("refresh failed")

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.refresh_runtime_manifest",
        fail_refresh_runtime_manifest,
    )

    with pytest.raises(TaxonomyBundleError, match="refresh failed"):
        bootstrap_taxonomy_bundle(
            manifest_path,
            data_root=data_root,
            allow_file_urls=True,
        )

    assert manifest_path.read_text(encoding="ascii") == original_manifest_text
    assert sentinel_path.read_text(encoding="ascii") == "original\n"
    assert not (release_root / "bac120_taxonomy_r95.tsv.gz").exists()


def test_bootstrap_manifest_entries_refreshes_manifest_after_each_successful_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Partial bootstrap failure should keep the manifest aligned with disk."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    release95_source = "https://example.invalid/release95/95.0/"
    release96_source = "https://example.invalid/release96/96.0/"
    entry95 = TaxonomyBundleEntry(
        resolved_release="95.0",
        aliases="95,95.0",
        bacterial_taxonomy="bac120_taxonomy_r95.tsv.gz",
        archaeal_taxonomy=None,
        bacterial_taxonomy_sha256=DUMMY_SHA256,
        archaeal_taxonomy_sha256=None,
        bacterial_taxonomy_rows=int(DUMMY_ROWS),
        archaeal_taxonomy_rows=None,
        is_latest="true",
        source_root_url=release95_source,
        checksum_filename="MD5SUM",
    )
    entry96 = TaxonomyBundleEntry(
        resolved_release="96.0",
        aliases="96,96.0",
        bacterial_taxonomy="bac120_taxonomy_r96.tsv.gz",
        archaeal_taxonomy=None,
        bacterial_taxonomy_sha256=DUMMY_SHA256,
        archaeal_taxonomy_sha256=None,
        bacterial_taxonomy_rows=int(DUMMY_ROWS),
        archaeal_taxonomy_rows=None,
        is_latest="false",
        source_root_url=release96_source,
        checksum_filename="MD5SUM",
    )
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    "95,95.0",
                    "bac120_taxonomy_r95.tsv.gz",
                    "",
                    "true",
                    release95_source,
                    "MD5SUM",
                ),
                build_bootstrap_manifest_row(
                    "96.0",
                    "96,96.0",
                    "bac120_taxonomy_r96.tsv.gz",
                    "",
                    "false",
                    release96_source,
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )

    payload95 = gzip.compress(
        b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n",
        mtime=0,
    )
    expected_sha256 = hashlib.sha256(payload95).hexdigest()

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.load_checksum_mapping",
        lambda source_root_url, checksum_filename: {},
    )

    def fake_materialise_taxonomy_file(
        source_root_url: str,
        target_name: str | None,
        target_path: Path | None,
        checksum_mapping: dict[str, tuple[str, ...]],
    ) -> None:
        """Write the first release payload and fail on the second release."""

        del checksum_mapping
        if target_name is None or target_path is None:
            return
        if source_root_url == release96_source:
            raise TaxonomyBundleError("staging failed for 96.0")
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(payload95)

    monkeypatch.setattr(
        "gtdb_genomes.taxonomy_bundle.materialise_taxonomy_file",
        fake_materialise_taxonomy_file,
    )

    with pytest.raises(TaxonomyBundleError, match="staging failed for 96.0"):
        bootstrap_manifest_entries(
            (entry95, entry96),
            data_root,
            manifest_path=manifest_path,
        )

    manifest_entries = load_taxonomy_bundle_manifest(manifest_path)
    assert manifest_entries[0].resolved_release == "95.0"
    assert manifest_entries[0].bacterial_taxonomy_sha256 == expected_sha256
    assert manifest_entries[0].bacterial_taxonomy_rows == 1
    assert manifest_entries[1].resolved_release == "96.0"
    assert manifest_entries[1].bacterial_taxonomy_sha256 == DUMMY_SHA256
    assert manifest_entries[1].bacterial_taxonomy_rows == 1
    assert (data_root / "95.0" / "bac120_taxonomy_r95.tsv.gz").read_bytes() == (
        payload95
    )
    assert not (data_root / "96.0").exists()


def test_bootstrap_taxonomy_bundle_accepts_duplicate_identical_selected_checksum(
    tmp_path: Path,
) -> None:
    """Bootstrap should accept repeated identical checksum entries for one target."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    source_root = tmp_path / "mirror" / "release95" / "95.0"
    payload = gzip.compress(
        b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n",
        mtime=0,
    )
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    "95,95.0",
                    "bac120_taxonomy_r95.tsv.gz",
                    "",
                    "true",
                    f"{source_root.as_uri()}/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )
    line = build_md5_line("bac120_taxonomy_r95.tsv.gz", payload)
    write_checksum_lines(source_root, "MD5SUM", (line, line))
    write_bytes(source_root / "bac120_taxonomy_r95.tsv.gz", payload)

    bootstrap_taxonomy_bundle(
        manifest_path,
        data_root=data_root,
        allow_file_urls=True,
    )

    output_path = data_root / "95.0" / "bac120_taxonomy_r95.tsv.gz"
    assert output_path.read_bytes() == payload


def test_bootstrap_taxonomy_bundle_rejects_conflicting_selected_checksum_entries(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail when the chosen source file has conflicting hashes."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    source_root = tmp_path / "mirror" / "release95" / "95.0"
    payload = gzip.compress(
        b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n",
        mtime=0,
    )
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    "95,95.0",
                    "bac120_taxonomy_r95.tsv.gz",
                    "",
                    "true",
                    f"{source_root.as_uri()}/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )
    write_checksum_lines(
        source_root,
        "MD5SUM",
        (
            build_md5_line("bac120_taxonomy_r95.tsv.gz", payload),
            build_md5_line("bac120_taxonomy_r95.tsv.gz", gzip.compress(b"other\n", mtime=0)),
        ),
    )
    write_bytes(source_root / "bac120_taxonomy_r95.tsv.gz", payload)

    with pytest.raises(TaxonomyBundleError, match="conflicting entries for selected source file"):
        bootstrap_taxonomy_bundle(
            manifest_path,
            data_root=data_root,
            allow_file_urls=True,
        )


def test_bootstrap_taxonomy_bundle_requires_refreshed_source_metadata(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail clearly when the manifest lacks build metadata."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                RUNTIME_MANIFEST_HEADER,
                build_runtime_manifest_row(
                    "95.0",
                    "95,95.0",
                    "bac120_taxonomy_r95.tsv.gz",
                    "",
                    "true",
                ),
            ],
        )
        + "\n",
    )

    with pytest.raises(TaxonomyBundleError, match="Run the refresh command first"):
        bootstrap_taxonomy_bundle(manifest_path, data_root=manifest_path.parent)


def test_bootstrap_taxonomy_bundle_rejects_missing_inferred_source_name(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail when a taxonomy filename cannot be inferred upstream."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    source_root = tmp_path / "mirror" / "release95" / "95.0"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                BOOTSTRAP_MANIFEST_HEADER,
                build_bootstrap_manifest_row(
                    "95.0",
                    "95,95.0",
                    "missing_taxonomy.tsv.gz",
                    "",
                    "true",
                    f"{source_root.as_uri()}/",
                    "MD5SUM",
                ),
            ],
        )
        + "\n",
    )
    write_checksum_file(
        source_root,
        "MD5SUM",
        {"bac120_taxonomy_r95.tsv.gz": gzip.compress(b"row\n", mtime=0)},
    )

    with pytest.raises(TaxonomyBundleError, match="mirror source matching"):
        bootstrap_taxonomy_bundle(
            manifest_path,
            data_root=manifest_path.parent,
            allow_file_urls=True,
        )


def test_missing_taxonomy_error_recommends_bootstrap_command(
    tmp_path: Path,
) -> None:
    """Missing local taxonomy should point source checkouts at the bootstrap command."""

    missing_path = tmp_path / "data" / "gtdb_taxonomy" / "95.0" / "bac.tsv.gz"

    with pytest.raises(BundledDataError) as error_info:
        validate_configured_taxonomy_file(
            missing_path,
            expected_sha256=DUMMY_SHA256,
            expected_row_count=1,
        )

    assert BOOTSTRAP_COMMAND in str(error_info.value)
