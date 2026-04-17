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

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from parsers.openiti import OpenITIAuthorData, OpenITIBookData, OpenITIMetaYmls
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

def _load_corpus(directory: str) -> OpenITIMetaYmls:
    print(f"Loading OpenITI corpus from {directory} …")
    corpus = OpenITIMetaYmls(directory)
    print(corpus)
    if corpus.failed:
        print(f"  ({len(corpus.failed)} files failed to parse — see parse_errors in output)")
    return corpus


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
    corpus: OpenITIMetaYmls,
    corpus_path: str,
    corpus_version: str,
) -> None:
    """Serialise the parsed corpus to JSON."""
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
        "authors": {uri: _author_to_dict(a) for uri, a in corpus.authors.items()},
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
) -> Path:
    """Parse the full corpus and write the versioned JSON store.

    data_dir
        Path to the OpenITI corpus directory (passed via --dir on the CLI).

    output_path
        Override the default output path.  Defaults to
        data/openiti_<corpus_version>.json.

    Returns the output Path.
    """
    openiti_cfg = load_openiti_config()
    out_path    = _resolve_output_path(output_path, openiti_cfg.corpus_version)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        print(f"Output already exists: {out_path}")
        print("Overwriting.  Run `update` in future to make intent explicit.")

    corpus = _load_corpus(data_dir)
    _write_output(out_path, corpus, data_dir, openiti_cfg.corpus_version)

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
) -> Path:
    """Re-parse after a corpus version bump or YML additions.

    Identical to build — re-parses the full corpus and overwrites the
    output file.  There is no HTTP cost, so a full re-parse is always safe.

    Returns the output Path.
    """
    openiti_cfg = load_openiti_config()
    out_path    = _resolve_output_path(output_path, openiti_cfg.corpus_version)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    corpus = _load_corpus(data_dir)
    _write_output(out_path, corpus, data_dir, openiti_cfg.corpus_version)

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
            print(book.title_a, book.death_year_ah)
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    books   = {uri: _book_from_dict(d)   for uri, d in data.get("books",   {}).items()}
    authors = {uri: _author_from_dict(d) for uri, d in data.get("authors", {}).items()}
    return books, authors


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_cli(args: argparse.Namespace) -> None:
    build(data_dir=args.dir, output_path=args.output)


def _update_cli(args: argparse.Namespace) -> None:
    update(data_dir=args.dir, output_path=args.output)


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
