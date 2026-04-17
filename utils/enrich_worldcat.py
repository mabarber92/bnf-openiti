"""
utils/enrich_worldcat.py

Fetch WorldCat metadata for OpenITI books that have OCLC links in their
version YML files and write a versioned enrichment JSON to data/.

This is Stage 4b of the pipeline — an optional, one-off enrichment step.
The output is designed to be **committed to the repo** so downstream users
do not need to repeat the HTTP requests.

Subcommands
-----------
    build   First-time fetch. Retrieves all books with OCLC links from the
            current corpus.  Takes ~25 min at 1 req/s for ~1,300 records.

    update  Incremental update.  Loads an existing enrichment file, skips
            records where the stored OCLC ID matches the corpus YML, and
            fetches only new or changed entries.  Safe to run after a corpus
            update — minimises HTTP requests.

Typical usage
-------------
    # First time (or for a new corpus version):
    python utils/enrich_worldcat.py build

    # After a corpus update or adding new version YMLs:
    python utils/enrich_worldcat.py update

Output
------
    data/openiti_worldcat_<corpus_version>.json

    Keyed by book URI; each record contains title_ar, title_lat,
    author_names_ar, author_names_lat, language, oclc_id, fetched_at.
    Records with failed fetches retain an "error" key so update can skip them
    without re-fetching unless the OCLC link has changed.

Rate limiting
-------------
Default: 1 request per second.  Do not set --delay below 1.0.
The User-Agent header identifies this as a research tool.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.config import load_config, load_openiti_config
from utils.tokens import has_arabic as _has_arabic

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = 1
_USER_AGENT = (
    "Mozilla/5.0 (OpenITI-BNF research pipeline; "
    "https://github.com/mathewbarber/bnf-openiti; "
    "mailto:mathew.barber92@googlemail.com)"
)
_OCLC_RE = re.compile(r"worldcat\.org/oclc/(\d+)")
_JSONLD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# OCLC link extraction
# ---------------------------------------------------------------------------

def _first_oclc_id(links: list[str]) -> str | None:
    """Return the first OCLC ID found in a list of version links."""
    for url in links:
        m = _OCLC_RE.search(url)
        if m:
            return m.group(1)
    return None


# ---------------------------------------------------------------------------
# HTTP fetch
# ---------------------------------------------------------------------------

def _fetch_oclc(oclc_id: str, timeout: int = 12) -> tuple[int | None, str]:
    """Fetch the WorldCat page for an OCLC ID.

    Returns (http_status, body_text).  On network error returns (None, error_msg).
    Follows redirects automatically (worldcat.org → search.worldcat.org).
    """
    url = f"http://www.worldcat.org/oclc/{oclc_id}"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, str(exc)
    except Exception as exc:
        return None, str(exc)


# ---------------------------------------------------------------------------
# JSON-LD extraction
# ---------------------------------------------------------------------------

def _extract_book_jsonld(body: str) -> dict | None:
    """Find and return the schema.org Book object from the page HTML.

    WorldCat embeds a DataFeed containing one Book element in a
    <script type="application/ld+json"> block.
    """
    for block in _JSONLD_RE.findall(body):
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue

        # DataFeed wrapper (current WorldCat format)
        if data.get("@type") == "DataFeed":
            for element in data.get("dataFeedElement", []):
                if element.get("@type") == "Book":
                    return element
        # Bare Book object (older format)
        if data.get("@type") == "Book":
            return data

    return None


def _parse_book_record(oclc_id: str, book_json: dict) -> dict:
    """Extract title, author names, and language from a schema.org Book object.

    name       — may be a string or list; Arabic-script value → title_ar,
                 Latin-script value → title_lat.
    author     — may be a single dict or list; .name may be string or list.
    workExample — array of edition records; language taken from the first.
    """
    # --- title ---
    raw_names = book_json.get("name") or []
    if isinstance(raw_names, str):
        raw_names = [raw_names]

    title_ar  = next((n for n in raw_names if _has_arabic(n)), None)
    title_lat = next((n for n in raw_names if n and not _has_arabic(n)), None)

    # Strip " | WorldCat.org" suffix that sometimes appears
    if title_lat:
        title_lat = re.sub(r"\s*\|\s*WorldCat\.org$", "", title_lat).strip() or None
    if title_ar:
        title_ar = title_ar.strip() or None

    # --- author names ---
    raw_author = book_json.get("author") or {}
    if isinstance(raw_author, list):
        raw_author = raw_author[0] if raw_author else {}
    author_name_field = raw_author.get("name") or []
    if isinstance(author_name_field, str):
        author_name_field = [author_name_field]

    author_names_ar  = [n.strip() for n in author_name_field if _has_arabic(n) and n.strip()]
    author_names_lat = [n.strip() for n in author_name_field if n and not _has_arabic(n) and n.strip()]

    # --- language --- (from first workExample)
    language = None
    for example in book_json.get("workExample", []):
        lang = example.get("inLanguage")
        if lang:
            language = lang
            break

    return {
        "oclc_id":          oclc_id,
        "title_ar":         title_ar,
        "title_lat":        title_lat,
        "author_names_ar":  author_names_ar,
        "author_names_lat": author_names_lat,
        "language":         language,
        "fetched_at":       datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Core fetch loop (shared by build and update)
# ---------------------------------------------------------------------------

def _fetch_targets(
    targets: list[tuple[str, str]],   # (book_uri, oclc_id)
    existing: dict[str, dict],
    delay: float,
    label: str,
    timeout: int = 12,
) -> tuple[dict[str, dict], int]:
    """Fetch WorldCat data for the given targets and merge with existing records.

    Returns (updated_records, n_failed).
    """
    records = dict(existing)
    n_failed = 0
    now_iso = lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")

    for uri, oclc_id in tqdm(targets, desc=label):
        status, body = _fetch_oclc(oclc_id, timeout=timeout)

        if status != 200:
            records[uri] = {
                "oclc_id":    oclc_id,
                "error":      f"HTTP {status}: {body[:120]}",
                "fetched_at": now_iso(),
            }
            n_failed += 1
        else:
            book_json = _extract_book_jsonld(body)
            if book_json is None:
                records[uri] = {
                    "oclc_id":    oclc_id,
                    "error":      "no Book JSON-LD found in page",
                    "fetched_at": now_iso(),
                }
                n_failed += 1
            else:
                records[uri] = _parse_book_record(oclc_id, book_json)

        time.sleep(delay)

    return records, n_failed


# ---------------------------------------------------------------------------
# Public pipeline functions
# ---------------------------------------------------------------------------

def _resolve_output_path(output_path: str | None, corpus_version: str) -> Path:
    if output_path:
        return Path(output_path)
    if not corpus_version:
        raise ValueError(
            "corpus_version is not set in openiti.yml — cannot name output file.\n"
            "Set corpus_version to the name of your OpenITI corpus snapshot "
            "(e.g. corpus_2025_1_9)."
        )
    return _ROOT / "data" / f"openiti_worldcat_{corpus_version}.json"


def _load_corpus(cfg):
    from parsers.openiti import OpenITIMetaYmls
    if not cfg.openiti_data_path:
        raise ValueError(
            "openiti_data_path not set in config.yml — cannot locate corpus."
        )
    print(f"Loading OpenITI corpus from {cfg.openiti_data_path} …")
    corpus = OpenITIMetaYmls(cfg.openiti_data_path)
    print(corpus)
    return corpus


def _collect_oclc_targets(corpus) -> dict[str, str]:
    """Return {book_uri: oclc_id} for every book with an OCLC link."""
    return {
        uri: oclc_id
        for uri, book in corpus.books.items()
        if (oclc_id := _first_oclc_id(book.version_links))
    }


def build(
    output_path:  str | None = None,
    delay:        float | None = None,
    config_path:  str | None = None,
) -> Path:
    """First-time fetch: retrieve WorldCat data for all books with OCLC links.

    Skips records already present in the output file (safe to resume after
    an interruption).  To start completely fresh, delete the output file or
    pass a new --output path.

    delay defaults to openiti.yml worldcat.request_delay if not supplied.

    Returns the output Path.
    """
    cfg          = load_config(config_path)
    openiti_cfg  = load_openiti_config()
    effective_delay = max(delay if delay is not None else openiti_cfg.worldcat.request_delay, 1.0)

    corpus   = _load_corpus(cfg)
    out_path = _resolve_output_path(output_path, openiti_cfg.corpus_version)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing for resume
    existing: dict[str, dict] = {}
    if out_path.exists():
        existing = json.loads(out_path.read_text(encoding="utf-8")).get("records", {})
        print(f"Resuming — {len(existing)} records already present.")

    all_targets = _collect_oclc_targets(corpus)
    pending = {uri: oid for uri, oid in all_targets.items() if uri not in existing}

    print(f"Books with OCLC links:     {len(all_targets)}")
    print(f"Already fetched (skipped): {len(existing)}")
    print(f"Pending:                   {len(pending)}")

    if not pending:
        print("Nothing to fetch.")
        return out_path

    records, n_failed = _fetch_targets(
        list(pending.items()), existing, effective_delay, "Fetching",
        timeout=openiti_cfg.worldcat.timeout,
    )
    _write_output(out_path, records, cfg.openiti_data_path, openiti_cfg.corpus_version)

    n_ok = len(records) - n_failed - len(existing)
    print(f"\nDone.  Fetched OK: {n_ok}  Failed: {n_failed}  Output: {out_path}")
    return out_path


def update(
    output_path:  str | None = None,
    delay:        float | None = None,
    config_path:  str | None = None,
) -> Path:
    """Incremental update: fetch only new or changed entries.

    For each book in the corpus with an OCLC link, compares the OCLC ID
    in the YML against the stored oclc_id in the enrichment file:

    - URI not in file at all → fetch (new book or new OCLC link added)
    - Stored oclc_id differs from current YML → re-fetch (link updated)
    - Stored oclc_id matches → skip (no change, even if previous fetch errored)

    Returns the output Path.
    """
    cfg          = load_config(config_path)
    openiti_cfg  = load_openiti_config()
    effective_delay = max(delay if delay is not None else openiti_cfg.worldcat.request_delay, 1.0)

    corpus   = _load_corpus(cfg)
    out_path = _resolve_output_path(output_path, openiti_cfg.corpus_version)

    if not out_path.exists():
        print(
            f"No enrichment file found at {out_path}.\n"
            f"Run `build` first to do the initial fetch."
        )
        return out_path

    existing = json.loads(out_path.read_text(encoding="utf-8")).get("records", {})
    all_targets = _collect_oclc_targets(corpus)

    pending: list[tuple[str, str]] = []
    for uri, oclc_id in all_targets.items():
        stored = existing.get(uri)
        if stored is None:
            pending.append((uri, oclc_id))        # new entry
        elif stored.get("oclc_id") != oclc_id:
            pending.append((uri, oclc_id))        # OCLC link changed in YML

    print(f"Books with OCLC links:  {len(all_targets)}")
    print(f"Already up to date:     {len(all_targets) - len(pending)}")
    print(f"New or changed:         {len(pending)}")

    if not pending:
        print("Nothing to fetch.")
        return out_path

    records, n_failed = _fetch_targets(
        pending, existing, effective_delay, "Updating",
        timeout=openiti_cfg.worldcat.timeout,
    )
    _write_output(out_path, records, cfg.openiti_data_path, openiti_cfg.corpus_version)

    print(f"\nDone.  Fetched: {len(pending) - n_failed}  Failed: {n_failed}  Output: {out_path}")
    return out_path


def _write_output(path: Path, records: dict, corpus_path: str, corpus_version: str) -> None:
    """Write the enrichment JSON with metadata header."""
    n_failed = sum(1 for r in records.values() if "error" in r)
    output = {
        "_meta": {
            "schema_version":  _SCHEMA_VERSION,
            "corpus_path":     corpus_path,
            "corpus_version":  corpus_version,
            "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "total_records":   len(records),
            "total_fetched":   len(records) - n_failed,
            "total_failed":    n_failed,
        },
        "records": records,
    }
    path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Public loader (used by the matching stage)
# ---------------------------------------------------------------------------

def load_worldcat_enrichment(path: str) -> dict[str, dict]:
    """Load a WorldCat enrichment file and return the records dict.

    Returns {book_uri: record_dict}.  Records with an "error" key had
    failed fetches and contain no usable title/author data.

    Typical use::

        from utils.enrich_worldcat import load_worldcat_enrichment
        wc = load_worldcat_enrichment("data/openiti_worldcat_corpus_2025_1_9.json")
        rec = wc.get("0110HasanBasri.FadailMakka")
        if rec and not rec.get("error"):
            arabic_title = rec["title_ar"]
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return data.get("records", {})


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_cli(args: argparse.Namespace) -> None:
    build(output_path=args.output, delay=max(args.delay, 1.0))


def _update_cli(args: argparse.Namespace) -> None:
    update(output_path=args.output, delay=max(args.delay, 1.0))


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description=(
            "WorldCat enrichment for OpenITI books with OCLC links.\n\n"
            "Output is written to data/openiti_worldcat_<corpus_version>.json\n"
            "and should be committed to the repo as a reusable artefact."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    _shared = argparse.ArgumentParser(add_help=False)
    _shared.add_argument("--output", default=None,
                         help="Override output path.")
    _shared.add_argument("--delay", type=float, default=1.0,
                         help="Seconds between requests (min 1.0; default 1.0).")

    p_build = sub.add_parser(
        "build", parents=[_shared],
        help="First-time fetch for all books with OCLC links. Resumes if output exists.",
    )
    p_build.set_defaults(func=_build_cli)

    p_update = sub.add_parser(
        "update", parents=[_shared],
        help="Fetch only new or OCLC-changed entries. Run after corpus updates.",
    )
    p_update.set_defaults(func=_update_cli)

    args = parser.parse_args()
    args.func(args)
