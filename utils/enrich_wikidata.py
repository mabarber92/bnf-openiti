"""
utils/enrich_wikidata.py

Fetch Wikidata labels and aliases for OpenITI authors that carry a Wikidata
QID in their YML EXTID field and write them into the parsed corpus JSON.

This is Stage 2 of the pipeline — run once per corpus version immediately
after parse_openiti.py.  Both stages write to the same file:

    data/openiti_<corpus_version>.json

Stage 1 (parse_openiti.py) creates the file with structural YML data.
Stage 2 (this script) enriches it in-place, adding wd_* fields to every
author record that has a Wikidata QID.  The file is then committed to the
repo as a single source of truth for all downstream stages.

Wikidata is used because:
  - ~48 % of OpenITI authors have a verified Wikidata QID in their YML.
  - The Wikidata SPARQL endpoint is fully open and explicitly supports
    programmatic access without authentication.
  - Arabic-script name labels and English transliteration aliases on Wikidata
    supplement the YML name fields, improving author-name matching coverage.

Subcommands
-----------
    build   Fetch all authors with QIDs not yet enriched.  Batched SPARQL
            queries; typically completes in under two minutes.

    update  Re-fetch authors whose QID has changed since last enrichment.
            Run after a corpus version bump that introduces new author YMLs.

Typical usage
-------------
    # After running parse_openiti.py build:
    python utils/enrich_wikidata.py build

    # After a corpus update:
    python utils/enrich_wikidata.py update
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.config import load_openiti_config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
_BATCH_SIZE      = 400
_REQUEST_DELAY   = 2.0   # seconds between batch requests
_TIMEOUT         = 60    # seconds per SPARQL request

_USER_AGENT = (
    "OpenITI-BNF-pipeline/1.0 "
    "(https://github.com/mathewbarber/bnf-openiti; "
    "mailto:mathew.barber92@googlemail.com)"
)

# ---------------------------------------------------------------------------
# SPARQL helpers
# ---------------------------------------------------------------------------

_SPARQL_TEMPLATE = """\
SELECT ?item
  (SAMPLE(?lAr) AS ?labelAr)
  (SAMPLE(?lEn) AS ?labelEn)
  (GROUP_CONCAT(DISTINCT ?aAr; SEPARATOR="||") AS ?aliasesAr)
  (GROUP_CONCAT(DISTINCT ?aEn; SEPARATOR="||") AS ?aliasesEn)
  (SAMPLE(?death) AS ?deathDate)
WHERE {{
  VALUES ?item {{ {qids} }}
  OPTIONAL {{ ?item rdfs:label     ?lAr  FILTER(LANG(?lAr)  = "ar") }}
  OPTIONAL {{ ?item rdfs:label     ?lEn  FILTER(LANG(?lEn)  = "en") }}
  OPTIONAL {{ ?item skos:altLabel  ?aAr  FILTER(LANG(?aAr)  = "ar") }}
  OPTIONAL {{ ?item skos:altLabel  ?aEn  FILTER(LANG(?aEn)  = "en") }}
  OPTIONAL {{ ?item wdt:P570 ?death }}
}}
GROUP BY ?item
"""


def _sparql_fetch(qids: list[str]) -> list[dict]:
    values_str = " ".join(f"wd:{qid}" for qid in qids)
    query      = _SPARQL_TEMPLATE.format(qids=values_str)
    data = urllib.parse.urlencode({"query": query, "format": "json"}).encode()
    req  = urllib.request.Request(
        _SPARQL_ENDPOINT,
        data    = data,
        method  = "POST",
        headers = {
            "User-Agent":   _USER_AGENT,
            "Accept":       "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))["results"]["bindings"]


def _val(row: dict, key: str) -> str | None:
    v = row.get(key, {}).get("value") or None
    return v.strip() if v and v.strip() else None


def _split_concat(s: str | None) -> list[str]:
    if not s:
        return []
    return list(dict.fromkeys(p.strip() for p in s.split("||") if p.strip()))


def _extract_year(date_str: str | None) -> int | None:
    if not date_str:
        return None
    try:
        return int(date_str.lstrip("+").split("-")[0])
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Corpus file helpers
# ---------------------------------------------------------------------------

def _corpus_path(corpus_version: str, output_path: str | None) -> Path:
    if output_path:
        return Path(output_path)
    if not corpus_version:
        raise ValueError(
            "corpus_version is not set in openiti.yml.\n"
            "Run `python utils/parse_openiti.py build --dir <path>` first."
        )
    return _ROOT / "data" / f"openiti_{corpus_version}.json"


def _load_corpus_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"Corpus file not found: {path}\n"
            f"Run `python utils/parse_openiti.py build --dir <path>` first."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def _write_corpus_json(path: Path, data: dict) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Core fetch and merge
# ---------------------------------------------------------------------------

def _collect_targets(
    authors: dict,
    mode: str,   # "build" skips already enriched; "update" skips only if QID unchanged
) -> list[tuple[str, str]]:
    """Return (author_uri, wikidata_qid) pairs that need fetching."""
    targets = []
    for uri, author in authors.items():
        qid = author.get("wikidata_id")
        if not qid:
            continue
        if mode == "build" and author.get("wd_label_ar") is not None:
            continue   # already enriched
        if mode == "update" and author.get("wd_label_ar") is not None:
            stored_qid = author.get("wikidata_id")
            if stored_qid == qid:
                continue   # up to date
        targets.append((uri, qid))
    return targets


def _fetch_and_merge(
    targets: list[tuple[str, str]],
    authors: dict,
    label: str,
) -> tuple[int, int]:
    """Batch-fetch Wikidata and write wd_* fields into the authors dict in-place.

    Returns (n_fetched, n_failed).
    """
    qid_to_uris: dict[str, list[str]] = {}
    for uri, qid in targets:
        qid_to_uris.setdefault(qid, []).append(uri)

    unique_qids = list(qid_to_uris.keys())
    chunks      = [unique_qids[i : i + _BATCH_SIZE]
                   for i in range(0, len(unique_qids), _BATCH_SIZE)]

    print(f"{label}: {len(targets)} authors  ({len(unique_qids)} unique QIDs)  "
          f"in {len(chunks)} batch(es)")

    n_fetched = n_failed = 0
    now_iso   = lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")

    for chunk_idx, chunk in enumerate(chunks, 1):
        try:
            rows = _sparql_fetch(chunk)
        except Exception as exc:
            for qid in chunk:
                for uri in qid_to_uris[qid]:
                    authors[uri]["wd_error"]      = str(exc)
                    authors[uri]["wd_fetched_at"] = now_iso()
                n_failed += len(qid_to_uris[qid])
            print(f"  Chunk {chunk_idx}/{len(chunks)} ERROR: {exc}")
            time.sleep(_REQUEST_DELAY)
            continue

        results_by_qid = {}
        for row in rows:
            qid_url = _val(row, "item") or ""
            results_by_qid[qid_url.split("/")[-1]] = row

        for qid in chunk:
            row = results_by_qid.get(qid)
            fetched_at = now_iso()
            if row is None:
                for uri in qid_to_uris[qid]:
                    authors[uri]["wd_error"]      = "QID not found in SPARQL results"
                    authors[uri]["wd_fetched_at"] = fetched_at
                n_failed += len(qid_to_uris[qid])
            else:
                death_str = _val(row, "deathDate")
                patch = {
                    "wd_label_ar":   _val(row, "labelAr"),
                    "wd_label_en":   _val(row, "labelEn"),
                    "wd_aliases_ar": _split_concat(_val(row, "aliasesAr")),
                    "wd_aliases_en": _split_concat(_val(row, "aliasesEn")),
                    "wd_death_year": _extract_year(death_str),
                    "wd_fetched_at": fetched_at,
                }
                for uri in qid_to_uris[qid]:
                    authors[uri].update(patch)
                n_fetched += len(qid_to_uris[qid])

        print(f"  Chunk {chunk_idx}/{len(chunks)} — {len(rows)} results")
        if chunk_idx < len(chunks):
            time.sleep(_REQUEST_DELAY)

    return n_fetched, n_failed


# ---------------------------------------------------------------------------
# Public pipeline functions
# ---------------------------------------------------------------------------

def build(output_path: str | None = None) -> Path:
    """Enrich all authors with Wikidata QIDs that have not yet been enriched.

    Reads and updates data/openiti_<corpus_version>.json in-place.
    Safe to resume — already-enriched authors are skipped.

    Returns the corpus file Path.
    """
    openiti_cfg = load_openiti_config()
    path        = _corpus_path(openiti_cfg.corpus_version, output_path)
    data        = _load_corpus_json(path)
    authors     = data.get("authors", {})

    total_with_qid = sum(1 for a in authors.values() if a.get("wikidata_id"))
    targets = _collect_targets(authors, mode="build")

    print(f"Authors with Wikidata QIDs: {total_with_qid}")
    print(f"Already enriched (skipped): {total_with_qid - len(targets)}")
    print(f"Pending:                    {len(targets)}")

    if not targets:
        print("Nothing to fetch.")
        return path

    n_fetched, n_failed = _fetch_and_merge(targets, authors, "Fetching")

    data["_meta"]["wikidata_enriched_at"] = (
        datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    data["_meta"]["wikidata_total_fetched"] = n_fetched
    data["_meta"]["wikidata_total_failed"]  = n_failed

    _write_corpus_json(path, data)
    print(f"\nDone.  Enriched: {n_fetched}  Failed: {n_failed}  File: {path}")
    return path


def update(output_path: str | None = None) -> Path:
    """Re-fetch authors whose Wikidata QID has changed since last enrichment.

    Reads and updates data/openiti_<corpus_version>.json in-place.

    Returns the corpus file Path.
    """
    openiti_cfg = load_openiti_config()
    path        = _corpus_path(openiti_cfg.corpus_version, output_path)
    data        = _load_corpus_json(path)
    authors     = data.get("authors", {})

    targets = _collect_targets(authors, mode="update")

    print(f"New or QID-changed authors: {len(targets)}")

    if not targets:
        print("Nothing to fetch.")
        return path

    n_fetched, n_failed = _fetch_and_merge(targets, authors, "Updating")

    data["_meta"]["wikidata_enriched_at"]    = (
        datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    data["_meta"]["wikidata_total_fetched"]  = n_fetched
    data["_meta"]["wikidata_total_failed"]   = n_failed

    _write_corpus_json(path, data)
    print(f"\nDone.  Enriched: {n_fetched}  Failed: {n_failed}  File: {path}")
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_cli(args: argparse.Namespace) -> None:
    build(output_path=args.output)


def _update_cli(args: argparse.Namespace) -> None:
    update(output_path=args.output)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description=(
            "Wikidata enrichment for OpenITI authors with QIDs.\n\n"
            "Reads and updates data/openiti_<corpus_version>.json in-place.\n"
            "Run parse_openiti.py build first to create the corpus file."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    _shared = argparse.ArgumentParser(add_help=False)
    _shared.add_argument(
        "--output", default=None,
        help="Override corpus file path (default: data/openiti_<version>.json).",
    )

    p_build = sub.add_parser(
        "build", parents=[_shared],
        help="Enrich all un-enriched authors with Wikidata QIDs.",
    )
    p_build.set_defaults(func=_build_cli)

    p_update = sub.add_parser(
        "update", parents=[_shared],
        help="Re-fetch authors whose QID has changed. Run after corpus updates.",
    )
    p_update.set_defaults(func=_update_cli)

    args = parser.parse_args()
    args.func(args)
