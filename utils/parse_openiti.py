"""
utils/parse_openiti.py

Parse the OpenITI metadata YML files into a versioned JSON store.

This is Stage 1 of the pipeline — a one-off preparation step run once per
corpus version.  The output is designed to be **committed to the repo** so
downstream users skip this step entirely and load the pre-built file.

Subcommands
-----------
    build   Parse the full corpus for the first time (or from scratch).
            Writes data/openiti_<corpus_version>.json.

    update  Re-parse after a corpus version bump or YML additions.
            Identical to build — re-parses everything and overwrites the
            output file.  No HTTP requests; safe to run at any time.

Typical usage
-------------
    # First time (or after a fresh corpus download):
    python utils/parse_openiti.py build --dir /path/to/corpus_2025_1_9

    # After bumping corpus_version in openiti.yml and downloading new YMLs:
    python utils/parse_openiti.py update --dir /path/to/corpus_2025_1_9

Output
------
    data/openiti_<corpus_version>.json

    Contains two top-level dicts — books and authors — both keyed by URI.
    The file is committed to the repo so the WorldCat enrichment (Stage 2)
    and all downstream stages can load it directly without the raw corpus.

Loading in downstream stages
-----------------------------
    from utils.parse_openiti import load_openiti_corpus

    books, authors = load_openiti_corpus(
        "data/openiti_parsed_corpus_2025_1_9.json"
    )
    book = books.get("0685NasirDinBaydawi.AnwarTanzil")
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from parsers.openiti import (
    OpenITIAuthorData,
    OpenITIBookData,
    OpenITIMetaYmls,
    OpenITITSV,
)
from utils.config import load_openiti_config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _book_to_dict(book: OpenITIBookData) -> dict:
    return asdict(book)


def _author_to_dict(author: OpenITIAuthorData) -> dict:
    return asdict(author)


def _book_from_dict(d: dict) -> OpenITIBookData:
    return OpenITIBookData(**d)


def _author_from_dict(d: dict) -> OpenITIAuthorData:
    return OpenITIAuthorData(**d)


# ---------------------------------------------------------------------------
# Core parse + write
# ---------------------------------------------------------------------------

def _load_corpus(directory: str, yml_only: bool = False) -> OpenITIMetaYmls:
    """Load corpus from YMLs, optionally merged with TSV data.

    yml_only: If True, load only from YML files (legacy mode).
              If False, merge TSV data (if available) with YML data.
    """
    openiti_cfg = load_openiti_config()

    if yml_only:
        print(f"Loading OpenITI corpus from YML files in {directory} …")
        corpus = OpenITIMetaYmls(directory)
        print(corpus)
        if corpus.failed:
            print(f"  ({len(corpus.failed)} files failed to parse — see parse_errors in output)")
        return corpus

    # Try TSV-first approach with YML fallback
    tsv_path = openiti_cfg.corpus_tsv
    if not Path(tsv_path).exists():
        print(f"Warning: corpus_tsv not found at {tsv_path}")
        print("Falling back to YML-only parsing")
        return _load_corpus(directory, yml_only=True)

    print(f"Loading OpenITI from TSV: {tsv_path} …")
    tsv_corpus = OpenITITSV(tsv_path)
    print(tsv_corpus)

    print(f"Loading OpenITI from YML files in {directory} (for wikidata_id and missing fields) …")
    yml_corpus = OpenITIMetaYmls(directory)
    if yml_corpus.failed:
        print(f"  ({len(yml_corpus.failed)} files failed to parse — see parse_errors in output)")

    # Merge: TSV primary, YML supplementary
    return _merge_tsv_yml(tsv_corpus, yml_corpus)


def _merge_tsv_yml(
    tsv_corpus: OpenITITSV,
    yml_corpus: OpenITIMetaYmls,
) -> OpenITIMetaYmls:
    """Merge TSV-parsed corpus with YML-parsed corpus.

    TSV is primary (overwrites all fields); YML fills missing fields,
    especially wikidata_id which is extracted from EXTID field.
    """
    # Create a new corpus object to hold merged data
    class MergedCorpus:
        def __init__(self):
            self.authors = {}
            self.books = {}
            self.versions = {}
            self.failed = []

    merged = MergedCorpus()
    merged.versions = tsv_corpus.versions.copy()

    # Merge books: TSV primary, YML supplementary
    for book_uri, tsv_book in tqdm(tsv_corpus.books.items(), desc="Merging books", unit="book", leave=False):
        yml_book = yml_corpus.books.get(book_uri)
        merged_book = _book_merge(tsv_book, yml_book)
        merged.books[book_uri] = merged_book

    # Merge authors: TSV primary, YML supplementary (especially wikidata_id)
    for author_uri, tsv_author in tqdm(tsv_corpus.authors.items(), desc="Merging authors", unit="author", leave=False):
        yml_author = yml_corpus.authors.get(author_uri)
        merged_author = _author_merge(tsv_author, yml_author)
        merged.authors[author_uri] = merged_author

    return merged


def _book_merge(
    tsv_book: OpenITIBookData,
    yml_book: OpenITIBookData | None,
) -> OpenITIBookData:
    """Merge a TSV book with YML data (if present).

    TSV overwrites all fields; YML fills missing fields.
    """
    merged = OpenITIBookData(
        uri=tsv_book.uri,
        author_uri=tsv_book.author_uri,
        death_year_ah=tsv_book.death_year_ah or (yml_book.death_year_ah if yml_book else None),
        author_slug=tsv_book.author_slug,
        title_slug=tsv_book.title_slug,
        title_lat=tsv_book.title_lat or (yml_book.title_lat if yml_book else None),
        title_ara=tsv_book.title_ara or (yml_book.title_ara if yml_book else None),
        # For IDs, try YML if TSV doesn't have them
        wikidata_id=tsv_book.wikidata_id or (yml_book.wikidata_id if yml_book else None),
        viaf_id=tsv_book.viaf_id or (yml_book.viaf_id if yml_book else None),
    )
    return merged


def _author_merge(
    tsv_author: OpenITIAuthorData,
    yml_author: OpenITIAuthorData | None,
) -> OpenITIAuthorData:
    """Merge a TSV author with YML data (if present).

    TSV provides _lat (ArabicBetaCode) name fields.
    YML provides _lat, _ara (converted), and wikidata_id.
    Wikidata fields are preserved from YML (source of truth).
    """
    if yml_author is None:
        # No YML data, return TSV author as-is
        return tsv_author

    # Start with YML author (has wikidata_id, converted _ara fields, and wd_* fields)
    merged = OpenITIAuthorData(
        uri=yml_author.uri,
        death_year_ah=yml_author.death_year_ah,
        name_slug=yml_author.name_slug or tsv_author.name_slug,
        # Name components: prefer TSV _lat if available, otherwise YML _lat
        # For _ara: prefer YML (converted from BetaCode), fallback to TSV if YML missing
        name_shuhra_lat=tsv_author.name_shuhra_lat or yml_author.name_shuhra_lat,
        name_shuhra_ara=yml_author.name_shuhra_ara or tsv_author.name_shuhra_ara,
        name_ism_lat=tsv_author.name_ism_lat or yml_author.name_ism_lat,
        name_ism_ara=yml_author.name_ism_ara or tsv_author.name_ism_ara,
        name_kunya_lat=tsv_author.name_kunya_lat or yml_author.name_kunya_lat,
        name_kunya_ara=yml_author.name_kunya_ara or tsv_author.name_kunya_ara,
        name_laqab_lat=tsv_author.name_laqab_lat or yml_author.name_laqab_lat,
        name_laqab_ara=yml_author.name_laqab_ara or tsv_author.name_laqab_ara,
        name_nasab_lat=tsv_author.name_nasab_lat or yml_author.name_nasab_lat,
        name_nasab_ara=yml_author.name_nasab_ara or tsv_author.name_nasab_ara,
        name_nisba_lat=tsv_author.name_nisba_lat or yml_author.name_nisba_lat,
        name_nisba_ara=yml_author.name_nisba_ara or tsv_author.name_nisba_ara,
        # Wikidata ID from YML (source of truth)
        wikidata_id=yml_author.wikidata_id,
        # Preserve wikidata enrichment fields from YML
        wd_label_ar=yml_author.wd_label_ar,
        wd_label_en=yml_author.wd_label_en,
        wd_aliases_ar=yml_author.wd_aliases_ar,
        wd_aliases_en=yml_author.wd_aliases_en,
        wd_death_year=yml_author.wd_death_year,
        wd_fetched_at=yml_author.wd_fetched_at,
        wd_error=yml_author.wd_error,
    )
    return merged


def _resolve_output_path(output_path: str | None, corpus_version: str) -> Path:
    if output_path:
        return Path(output_path)
    if not corpus_version:
        raise ValueError(
            "corpus_version is not set in openiti.yml — cannot name output file.\n"
            "Set corpus_version to the name of your OpenITI corpus snapshot "
            "(e.g. corpus_2025_1_9)."
        )
    return _ROOT / "data" / f"openiti_{corpus_version}.json"


def _write_output(
    path: Path,
    corpus,
    corpus_path: str,
    corpus_version: str,
    preserve_existing: Path | None = None,
) -> None:
    """Serialise the parsed corpus to JSON.

    If preserve_existing is provided, load that file and preserve wikidata
    enrichment (wd_* fields) from existing author records.
    """
    # Load existing enrichment if requested
    existing_authors = {}
    if preserve_existing and preserve_existing.exists():
        try:
            print(f"Preserving wikidata enrichment from {preserve_existing} …")
            existing_data = json.loads(preserve_existing.read_text(encoding="utf-8"))
            existing_authors = existing_data.get("authors", {})
        except Exception as e:
            print(f"Warning: failed to load existing enrichment: {e}")

    # Build author output with preserved wikidata fields
    authors_output = {}
    for uri, author in tqdm(corpus.authors.items(), desc="Serializing authors", unit="author", leave=False):
        author_dict = _author_to_dict(author)
        # Preserve wikidata enrichment if it exists
        if uri in existing_authors:
            existing = existing_authors[uri]
            for wd_field in ["wd_label_ar", "wd_label_en", "wd_aliases_ar", "wd_aliases_en", "wd_death_year", "wd_fetched_at", "wd_error"]:
                if wd_field in existing and existing[wd_field] is not None:
                    author_dict[wd_field] = existing[wd_field]
        authors_output[uri] = author_dict

    output = {
        "_meta": {
            "schema_version": _SCHEMA_VERSION,
            "corpus_path":    corpus_path,
            "corpus_version": corpus_version,
            "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "total_books":    len(corpus.books),
            "total_authors":  len(corpus.authors),
            "total_versions": len(corpus.versions),
            "total_failed":   len(corpus.failed),
            "parse_errors":   corpus.failed,
        },
        "books":   {uri: _book_to_dict(b)   for uri, b in corpus.books.items()},
        "authors": authors_output,
    }
    path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Public pipeline functions
# ---------------------------------------------------------------------------

def build(
    data_dir:    str,
    output_path: str | None = None,
    yml_only:    bool = False,
) -> Path:
    """Parse the full corpus and write the versioned JSON store.

    data_dir
        Path to the OpenITI corpus directory (passed via --dir on the CLI).

    output_path
        Override the default output path.  Defaults to
        data/openiti_<corpus_version>.json.

    yml_only
        If True, parse only YML files (legacy mode).
        If False (default), use TSV as primary source with YML fallback.

    Returns the output Path.
    """
    openiti_cfg = load_openiti_config()
    out_path    = _resolve_output_path(output_path, openiti_cfg.corpus_version)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    preserve_wikidata = False
    if out_path.exists():
        print(f"Output already exists: {out_path}")
        print("Overwriting.  Run `update` in future to make intent explicit.")
        preserve_wikidata = True

    corpus = _load_corpus(data_dir, yml_only=yml_only)
    _write_output(out_path, corpus, data_dir, openiti_cfg.corpus_version, preserve_existing=out_path if preserve_wikidata else None)

    print(
        f"\nDone.  Books: {len(corpus.books)}  "
        f"Authors: {len(corpus.authors)}  "
        f"Failed: {len(corpus.failed)}  "
        f"Output: {out_path}"
    )
    return out_path


def update(
    data_dir:    str,
    output_path: str | None = None,
    yml_only:    bool = False,
) -> Path:
    """Re-parse after a corpus version bump or YML additions.

    Identical to build — re-parses the full corpus and overwrites the
    output file.  There is no HTTP cost, so a full re-parse is always safe.
    Preserves existing Wikidata enrichment (wd_* fields).

    yml_only
        If True, parse only YML files (legacy mode).
        If False (default), use TSV as primary source with YML fallback.

    Returns the output Path.
    """
    openiti_cfg = load_openiti_config()
    out_path    = _resolve_output_path(output_path, openiti_cfg.corpus_version)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    corpus = _load_corpus(data_dir, yml_only=yml_only)
    # Always try to preserve wikidata enrichment on update
    _write_output(out_path, corpus, data_dir, openiti_cfg.corpus_version, preserve_existing=out_path if out_path.exists() else None)

    print(
        f"\nDone.  Books: {len(corpus.books)}  "
        f"Authors: {len(corpus.authors)}  "
        f"Failed: {len(corpus.failed)}  "
        f"Output: {out_path}"
    )
    return out_path


# ---------------------------------------------------------------------------
# Public loader (used by the WorldCat enrichment and matching stages)
# ---------------------------------------------------------------------------

def load_openiti_corpus(
    path: str,
) -> tuple[dict[str, OpenITIBookData], dict[str, OpenITIAuthorData]]:
    """Load a parsed OpenITI corpus file and return typed dicts.

    Returns (books, authors) where both dicts are keyed by URI.

    Typical use::

        from utils.parse_openiti import load_openiti_corpus

        books, authors = load_openiti_corpus(
            "data/openiti_parsed_corpus_2025_1_9.json"
        )
        book = books.get("0685NasirDinBaydawi.AnwarTanzil")
        if book:
            print(book.title_lat, book.death_year_ah)
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    books   = {uri: _book_from_dict(d)   for uri, d in data.get("books",   {}).items()}
    authors = {uri: _author_from_dict(d) for uri, d in data.get("authors", {}).items()}
    return books, authors


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_cli(args: argparse.Namespace) -> None:
    build(data_dir=args.dir, output_path=args.output, yml_only=args.yml_only)


def _update_cli(args: argparse.Namespace) -> None:
    update(data_dir=args.dir, output_path=args.output, yml_only=args.yml_only)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description=(
            "Parse OpenITI metadata YML files into a versioned JSON store.\n\n"
            "Output is written to data/openiti_<corpus_version>.json\n"
            "and should be committed to the repo as a reusable artefact."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    _shared = argparse.ArgumentParser(add_help=False)
    _shared.add_argument(
        "--dir", required=True, metavar="PATH",
        help="Path to the OpenITI corpus directory (e.g. /path/to/corpus_2025_1_9).",
    )
    _shared.add_argument(
        "--output", default=None,
        help="Override output path (default: data/openiti_parsed_<version>.json).",
    )
    _shared.add_argument(
        "--yml-only", action="store_true",
        help="Parse only YML files (legacy mode). By default, uses TSV as primary source.",
    )

    p_build = sub.add_parser(
        "build", parents=[_shared],
        help="Parse the full corpus and write the JSON store.",
    )
    p_build.set_defaults(func=_build_cli)

    p_update = sub.add_parser(
        "update", parents=[_shared],
        help="Re-parse after a corpus version bump or YML additions.",
    )
    p_update.set_defaults(func=_update_cli)

    args = parser.parse_args()
    args.func(args)
