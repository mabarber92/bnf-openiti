"""
utils/parse_bnf.py

Parse BNF Gallica OAI-PMH XML files into a versioned JSON store.

This is Stage 5 of the pipeline — run after the BNF survey (Stage 3) and
boilerplate review + apply-review (Stage 4).  Requires:

    outputs/bnf_survey/boilerplate.json   (produced by survey_bnf.py apply-review)

Subcommands
-----------
    build   Parse the full collection.  Overwrites any existing output.

    update  Parse only XML files not yet in the output.  Use after new
            OAI-PMH files are downloaded without re-parsing everything.

    sample  Parse a random sample of N records (default 50) and print a
            summary of matching_data() output to stdout.  Does not write
            any output file — for inspection only.

Typical usage
-------------
    # First time:
    python utils/parse_bnf.py build

    # After new XML files added:
    python utils/parse_bnf.py update

    # Quick inspection after a config change:
    python utils/parse_bnf.py sample --n 30

Output
------
    <pipeline_out_dir>/bnf_parsed.json   (default: outputs/bnf_parsed.json)

Loading in downstream stages
-----------------------------
    from utils.parse_bnf import load_bnf_records
    records = load_bnf_records("outputs/bnf_parsed.json")
    rec = records.get("OAI_11000434")
    print(rec.matching_data())
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import parsers.bnf as _bnf
from parsers.bnf import BNFMetadata, BNFRecord, DetectedRelation, load_boilerplate_file, load_signal_ngrams
from utils.config import load_config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _record_to_dict(record: BNFRecord) -> dict:
    return asdict(record)


def _record_from_dict(d: dict) -> BNFRecord:
    relations = [DetectedRelation(**r) for r in d.pop("detected_relations", [])]
    return BNFRecord(**{**d, "detected_relations": relations})


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _resolve_paths(cfg, output_path: str | None) -> tuple[Path, Path, Path]:
    """Return (bnf_data_path, boilerplate_path, output_path)."""
    if not cfg.bnf_data_path:
        raise ValueError(
            "bnf_data_path is not set in config.yml.\n"
            "Add:  bnf_data_path: /path/to/BNF_data"
        )
    data_path       = Path(cfg.bnf_data_path)
    boilerplate_path = Path(cfg.resolved_bnf_survey_dir()) / "boilerplate.json"
    out_path         = Path(output_path) if output_path else (
        Path(cfg.pipeline_out_dir) / "bnf_parsed.json"
    )
    return data_path, boilerplate_path, out_path


def _load_boilerplate(boilerplate_path: Path) -> tuple[dict, list[dict]]:
    """Load boilerplate ngrams and signal ngrams. Returns (ngrams_dict, signals_list)."""
    if not boilerplate_path.exists():
        print(
            f"Warning: boilerplate.json not found at {boilerplate_path}.\n"
            f"Run `python utils/survey_bnf.py apply-review` first.\n"
            f"Parsing without boilerplate filtering."
        )
        return {}, []
    bp_ngrams = load_boilerplate_file(str(boilerplate_path))
    signals   = load_signal_ngrams(str(boilerplate_path))
    return bp_ngrams, signals


def _signals_to_relation_terms(signals: list[dict]) -> dict[str, tuple[str, str | None]]:
    """Convert signal entries to the {pattern: (signal_type, field_constraint)} dict BNFXml expects."""
    return {
        entry["ngram"]: (entry["signal_type"], entry.get("field"))
        for entry in signals
    }


def _write_output(
    path: Path,
    records: dict[str, dict],
    data_path: str,
    boilerplate_path: str,
    bp_ngrams: dict,
    signals: list[dict],
    failed: list[dict],
) -> None:
    n_bp = sum(len(v) for v in bp_ngrams.values())
    output = {
        "_meta": {
            "schema_version":   _SCHEMA_VERSION,
            "bnf_data_path":    data_path,
            "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "total_records":    len(records),
            "total_failed":     len(failed),
            "boilerplate_path": boilerplate_path,
            "boilerplate_count": n_bp,
            "signal_count":     len(signals),
            "parse_errors":     failed,
        },
        "records": records,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Public pipeline functions
# ---------------------------------------------------------------------------

def build(output_path: str | None = None) -> Path:
    """Parse the full BNF collection and write the JSON store.

    Loads boilerplate.json and signals from the survey output directory.
    Overwrites any existing output file.

    Returns the output Path.
    """
    cfg = load_config()
    data_path, boilerplate_path, out_path = _resolve_paths(cfg, output_path)
    bp_ngrams, signals = _load_boilerplate(boilerplate_path)
    relation_terms     = _signals_to_relation_terms(signals)

    print(f"Parsing BNF XML from {data_path} …")
    print(f"Boilerplate ngrams: {sum(len(v) for v in bp_ngrams.values())}  "
          f"Signals: {len(signals)}")

    metadata = BNFMetadata(
        str(data_path),
        relation_terms     = relation_terms,
        boilerplate_ngrams = bp_ngrams or None,
    )
    print(metadata)

    records = {r.bnf_id: _record_to_dict(r) for r in metadata}
    _write_output(
        out_path,
        records,
        str(data_path),
        str(boilerplate_path),
        bp_ngrams,
        signals,
        metadata.failed,
    )

    print(f"\nDone.  Records: {len(records)}  Failed: {len(metadata.failed)}  "
          f"Output: {out_path}")
    return out_path


def update(output_path: str | None = None) -> Path:
    """Parse only XML files not yet in the existing output.

    Loads the existing output file and parses only records whose bnf_id
    is absent.  Use after downloading new OAI-PMH files without re-parsing
    the full collection.

    Returns the output Path.
    """
    cfg = load_config()
    data_path, boilerplate_path, out_path = _resolve_paths(cfg, output_path)

    if not out_path.exists():
        print(f"No existing output at {out_path}. Running full build instead.")
        return build(output_path)

    existing_data = json.loads(out_path.read_text(encoding="utf-8"))
    existing      = existing_data.get("records", {})

    bp_ngrams, signals = _load_boilerplate(boilerplate_path)
    relation_terms     = _signals_to_relation_terms(signals)

    # Find XML files not already parsed
    xml_files = sorted(Path(data_path).glob("**/OAI_*.xml"))
    pending   = [p for p in xml_files if p.stem not in existing]

    print(f"Total XML files:  {len(xml_files)}")
    print(f"Already parsed:   {len(xml_files) - len(pending)}")
    print(f"Pending:          {len(pending)}")

    if not pending:
        print("Nothing to parse.")
        return out_path

    new_records: dict[str, dict] = {}
    failed: list[dict] = []

    for path in pending:
        try:
            xml = _bnf.BNFXml(
                str(path),
                relation_terms     = relation_terms,
                boilerplate_ngrams = bp_ngrams or None,
            )
            new_records[xml.record.bnf_id] = _record_to_dict(xml.record)
        except Exception as exc:
            failed.append({"path": str(path), "error": str(exc)})

    merged = {**existing, **new_records}
    all_failed = existing_data.get("_meta", {}).get("parse_errors", []) + failed
    _write_output(
        out_path,
        merged,
        str(data_path),
        str(boilerplate_path),
        bp_ngrams,
        signals,
        all_failed,
    )

    print(f"\nDone.  Added: {len(new_records)}  Failed: {len(failed)}  "
          f"Total: {len(merged)}  Output: {out_path}")
    return out_path


def sample(n: int = 50, seed: int | None = None) -> None:
    """Parse a random sample of N records and print matching_data() summaries.

    Writes no output files — for inspection and debugging only.
    """
    cfg = load_config()
    data_path, boilerplate_path, _ = _resolve_paths(cfg, None)
    bp_ngrams, signals = _load_boilerplate(boilerplate_path)
    relation_terms     = _signals_to_relation_terms(signals)

    xml_files = sorted(Path(data_path).glob("**/OAI_*.xml"))
    rng = random.Random(seed)
    chosen = rng.sample(xml_files, min(n, len(xml_files)))

    print(f"Sampling {len(chosen)} records (seed={seed}) …\n")

    stats = {"with_lat": 0, "with_ar": 0, "with_creator": 0,
             "with_title": 0, "with_desc_cands": 0, "with_relations": 0,
             "failed": 0}

    for path in chosen:
        try:
            xml = _bnf.BNFXml(
                str(path),
                relation_terms     = relation_terms,
                boilerplate_ngrams = bp_ngrams or None,
            )
            r  = xml.record
            md = r.matching_data()

            stats["with_lat"]        += bool(md["lat"])
            stats["with_ar"]         += bool(md["ar"])
            stats["with_creator"]    += bool(r.creator_lat or r.creator_ar)
            stats["with_title"]      += bool(r.title_lat or r.title_ar)
            stats["with_desc_cands"] += bool(r.description_candidates)
            stats["with_relations"]  += bool(r.detected_relations)

        except Exception as exc:
            stats["failed"] += 1
            print(f"  FAILED {path.name}: {exc}")

    total = len(chosen) - stats["failed"]
    print(f"Sample size:          {len(chosen)}")
    print(f"Parse failures:       {stats['failed']}")
    print(f"With Latin matches:   {stats['with_lat']} / {total}")
    print(f"With Arabic matches:  {stats['with_ar']} / {total}")
    print(f"With creator:         {stats['with_creator']} / {total}")
    print(f"With title:           {stats['with_title']} / {total}")
    print(f"With desc candidates: {stats['with_desc_cands']} / {total}")
    print(f"With relations:       {stats['with_relations']} / {total}")

    # Print full record details for example records
    print("\n--- Example records (full detail) ---")
    shown = 0
    for path in chosen:
        if shown >= 5:
            break
        try:
            xml = _bnf.BNFXml(
                str(path),
                relation_terms     = relation_terms,
                boilerplate_ngrams = bp_ngrams or None,
            )
            r  = xml.record
            md = r.matching_data()
            if not (md["lat"] or md["ar"]):
                continue
            print(f"\n{'='*60}")
            print(f"ID:          {r.bnf_id}")
            print(f"signal_count={r.signal_count}  composite={r.is_composite}")
            print(f"title_lat:   {r.title_lat!r}")
            print(f"title_ar:    {r.title_ar!r}")
            print(f"creator_lat: {r.creator_lat!r}")
            print(f"creator_ar:  {r.creator_ar!r}")
            print(f"subject:     {r.subject}")
            print(f"copy_date:   {r.copy_date_raw!r}  from={r.date_from}  to={r.date_to}")
            if r.description_candidates:
                print(f"desc_cands ({len(r.description_candidates)}):")
                for cand in r.description_candidates:
                    print(f"  {cand!r}")
            if r.detected_relations:
                print(f"relations ({len(r.detected_relations)}):")
                for rel in r.detected_relations:
                    st = rel.relation_type
                    print(f"  [{st}] term={rel.matched_term!r}  context={rel.context!r}")
            print(f"matching lat ({len(md['lat'])}):")
            for item in md["lat"]:
                print(f"  {item!r}")
            print(f"matching ar  ({len(md['ar'])}):")
            for item in md["ar"]:
                print(f"  {item!r}")
            shown += 1
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------------

def load_bnf_records(path: str) -> dict[str, BNFRecord]:
    """Load a parsed BNF JSON store and return {bnf_id: BNFRecord}.

    Typical use::

        from utils.parse_bnf import load_bnf_records
        records = load_bnf_records("outputs/bnf_parsed.json")
        rec = records["OAI_11000434"]
        print(rec.matching_data())
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {
        bnf_id: _record_from_dict(dict(d))
        for bnf_id, d in data.get("records", {}).items()
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_cli(args: argparse.Namespace) -> None:
    build(output_path=args.output)


def _update_cli(args: argparse.Namespace) -> None:
    update(output_path=args.output)


def _sample_cli(args: argparse.Namespace) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    sample(n=args.n, seed=args.seed)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description=(
            "Parse BNF Gallica OAI-PMH XML files into a JSON store.\n\n"
            "Requires outputs/bnf_survey/boilerplate.json — run\n"
            "`python utils/survey_bnf.py apply-review` first."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    _shared = argparse.ArgumentParser(add_help=False)
    _shared.add_argument(
        "--output", default=None,
        help="Override output path (default: <pipeline_out_dir>/bnf_parsed.json).",
    )

    p_build = sub.add_parser(
        "build", parents=[_shared],
        help="Parse the full BNF collection. Overwrites any existing output.",
    )
    p_build.set_defaults(func=_build_cli)

    p_update = sub.add_parser(
        "update", parents=[_shared],
        help="Parse only XML files not yet in the existing output.",
    )
    p_update.set_defaults(func=_update_cli)

    p_sample = sub.add_parser(
        "sample",
        help="Parse a random sample and print matching_data() summaries (no file output).",
    )
    p_sample.add_argument("--n",    type=int, default=50,   help="Sample size (default 50).")
    p_sample.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility.")
    p_sample.set_defaults(func=_sample_cli)

    args = parser.parse_args()
    args.func(args)
