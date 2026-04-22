"""
utils/survey_bnf.py

Survey BNF OAI-PMH XML records and build the n-gram vocabulary used for
boilerplate detection.  Part of the BNF–OpenITI matching pipeline.

Subcommands
-----------
    build          Scan XML records; write summary.json, ngrams.json, and an
                   initial boilerplate_review.csv for manual review.
    apply-review   Read the reviewed CSV and write boilerplate.json.

All output paths and parameters are read from config.yml by default;
CLI flags override config for experimentation.

Typical pipeline sequence
-------------------------
    python utils/survey_bnf.py build
    # → open <survey_dir>/boilerplate_review.csv, set keep=no on false positives
    python utils/survey_bnf.py apply-review

Importable API
--------------
    from utils.survey_bnf import build, apply_review, print_summary
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

from tqdm import tqdm
import yaml  # pip install pyyaml

# Ensure project root is on sys.path when running as a script.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.config import load_config, PipelineConfig, FieldBoilerplateConfig  # noqa: E402
from utils.tokens import (  # noqa: E402
    has_arabic  as _has_arabic,
    has_latin   as _has_latin,
    tokenize_lat as _tokenize_lat,
    tokenize_ar  as _tokenize_ar,
    make_ngrams  as _make_ngrams,
)

# ---------------------------------------------------------------------------
# XML namespace map
# ---------------------------------------------------------------------------
NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc":     "http://purl.org/dc/elements/1.1/",
}


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _pct(n: int, total: int) -> float:
    return round(100 * n / total, 1) if total else 0.0


def _filter_digit_only_tokens(tokens: list[str]) -> list[str]:
    """Filter out pure-digit tokens (e.g., '1666', '42').

    Numerals are critical signals for the parser (years, folio numbers),
    but create noise in the boilerplate review CSV when used for n-gram
    analysis. This function removes tokens that are entirely digits, not
    alphanumeric (e.g., 'page123' is kept; '1666' is removed).

    Survey purposes only—the parser keeps all numerals via tokenize_lat/tokenize_ar.
    """
    return [t for t in tokens if not t.isdigit()]


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _load_manifest(survey_dir: Path) -> dict:
    path = survey_dir / "manifest.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"created": _now(), "config_snapshot": {}, "stages": {}}


def _save_manifest(survey_dir: Path, manifest: dict) -> None:
    manifest["last_updated"] = _now()
    (survey_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _config_snapshot(cfg: PipelineConfig) -> dict:
    """Serialise the full resolved config for audit storage in the manifest."""
    return {
        "bnf_data_path":           cfg.bnf_data_path,
        "pipeline_out_dir":        cfg.pipeline_out_dir,
        "bnf_survey_dir":          cfg.bnf_survey_dir,
        "survey_max_n":            cfg.survey.max_n,
        "survey_keep_abbrev_dots": cfg.survey.keep_abbrev_dots,
        "boilerplate_fields": {
            fname: {
                "mode":                fcfg.mode,
                "min_doc_freq_pct":    fcfg.min_doc_freq_pct,
                "max_repeats_per_doc": fcfg.max_repeats_per_doc,
            }
            for fname, fcfg in cfg.boilerplate.fields.items()
        },
        "parsing_overwrite_existing": cfg.parsing.overwrite_existing,
    }


# ---------------------------------------------------------------------------
# N-gram ranking (full, no truncation — display limits belong in print_ngrams)
# ---------------------------------------------------------------------------

def _rank_ngrams(tf: Counter, df: Counter, n_docs: int) -> dict:
    """Return three full sorted rankings from term and document frequency data.

    All lists are untruncated so load_boilerplate_ngrams() can apply threshold
    criteria across the complete observed vocabulary.

    TF-IDF: term_freq * log(n_docs / doc_freq).
    """
    all_ngrams = set(tf.keys()) | set(df.keys())
    rows = []
    for ng in all_ngrams:
        tf_val = tf[ng]
        df_val = df[ng]
        tfidf  = tf_val * math.log(n_docs / df_val) if df_val > 0 else 0.0
        rows.append({
            "ngram":     ng,
            "term_freq": tf_val,
            "doc_freq":  df_val,
            "tfidf":     round(tfidf, 3),
        })
    return {
        "by_doc_freq":  sorted(rows, key=lambda x: -x["doc_freq"]),
        "by_term_freq": sorted(rows, key=lambda x: -x["term_freq"]),
        "by_tfidf":     sorted(rows, key=lambda x: -x["tfidf"]),
    }


# ---------------------------------------------------------------------------
# Core scan (shared by both build outputs)
# ---------------------------------------------------------------------------

def _scan(
    directory: str,
    glob: str = "**/OAI_*.xml",
    max_n: int = 4,
    keep_abbrev_dots: bool = False,
    scan_fields: list[str] | None = None,
    sample: int | None = None,
    seed: int = 42,
) -> tuple[dict, dict]:
    """Parse all XML files and return (field_stats, ngram_data).

    Runs once per build invocation; both summary.json and ngrams.json
    are derived from this single pass.

    scan_fields
        DC field names to accumulate n-grams for (e.g. ["description", "format"]).
        Defaults to ["description"] if not supplied.
    """
    import random as _random

    if scan_fields is None:
        scan_fields = ["description"]

    root = Path(directory)
    paths = sorted(root.glob(glob))
    if not paths:
        raise FileNotFoundError(f"No XML files matched '{glob}' in {directory!r}")

    n_found = len(paths)
    if sample is not None and sample < n_found:
        rng = _random.Random(seed)
        paths = rng.sample(paths, sample)

    n_parsed = n_failed = 0
    parse_errors: list[dict] = []

    # Per-field n-gram accumulators
    # field_tf[field][script] = list of tokens (for term-frequency counting)
    # field_df[field][script][n] = Counter of n-grams (for doc-frequency counting)
    field_tf: dict[str, dict[str, list[str]]] = {
        f: {"lat": [], "ar": []} for f in scan_fields
    }
    field_df: dict[str, dict[str, dict[int, Counter]]] = {
        f: {
            "lat": {n: Counter() for n in range(2, max_n + 1)},
            "ar":  {n: Counter() for n in range(2, max_n + 1)},
        }
        for f in scan_fields
    }

    # Field-level coverage accumulators
    field_present: Counter     = Counter()
    field_counts:  defaultdict = defaultdict(list)
    field_arabic:  Counter     = Counter()
    field_latin:   Counter     = Counter()
    field_both:    Counter     = Counter()
    field_empty:   Counter     = Counter()
    field_attrs:   defaultdict = defaultdict(Counter)
    field_samples: defaultdict = defaultdict(list)

    for path in tqdm(paths, desc="Scanning"):
        try:
            tree = ET.parse(path)
        except ET.ParseError as exc:
            parse_errors.append({"path": str(path), "error": str(exc)})
            n_failed += 1
            continue

        dc = tree.find(".//oai_dc:dc", NS)
        if dc is None:
            parse_errors.append({"path": str(path), "error": "No oai_dc:dc element"})
            n_failed += 1
            continue

        n_parsed += 1

        # --- Per-field n-gram accumulation ---
        # Collect tokens per field per script for this record, then update
        # term-freq lists and doc-freq counters (doc-freq uses a set so each
        # n-gram is counted once per record regardless of how many elements).
        rec_tokens: dict[str, dict[str, list[str]]] = {
            f: {"lat": [], "ar": []} for f in scan_fields
        }
        for el in dc:
            local = _strip_ns(el.tag)
            if local not in scan_fields:
                continue
            text = (el.text or "").strip()
            if not text:
                continue
            if _has_arabic(text):
                tokens = _tokenize_ar(text)
                # Filter numeric tokens for survey (they're noise in boilerplate review)
                tokens = _filter_digit_only_tokens(tokens)
                rec_tokens[local]["ar"].extend(tokens)
            else:
                tokens = _tokenize_lat(text, keep_abbrev_dots=keep_abbrev_dots)
                # Filter numeric tokens for survey (they're noise in boilerplate review)
                tokens = _filter_digit_only_tokens(tokens)
                rec_tokens[local]["lat"].extend(tokens)

        for fname in scan_fields:
            for script_key, tokens in rec_tokens[fname].items():
                field_tf[fname][script_key].extend(tokens)
                for n in range(2, max_n + 1):
                    field_df[fname][script_key][n].update(
                        set(_make_ngrams(tokens, n))
                    )

        # --- Field-level coverage accumulation ---
        per_record: Counter = Counter()
        record_has_arabic: set[str] = set()
        record_has_latin:  set[str] = set()
        record_has_both:   set[str] = set()
        record_all_empty:  dict[str, bool] = {}

        for el in dc:
            local = _strip_ns(el.tag)
            per_record[local] += 1
            record_all_empty.setdefault(local, True)
            text = (el.text or "").strip()

            if text:
                record_all_empty[local] = False
                ar  = _has_arabic(text)
                lat = _has_latin(text)
                if ar:  record_has_arabic.add(local)
                if lat: record_has_latin.add(local)
                if ar and lat: record_has_both.add(local)

            for attr_key, attr_val in el.attrib.items():
                field_attrs[local][f"{_strip_ns(attr_key)}={attr_val!r}"] += 1

            if text and len(field_samples[local]) < 5:
                field_samples[local].append(text[:140])

        for f, count in per_record.items():
            field_present[f] += 1
            field_counts[f].append(count)
        for f in record_has_arabic: field_arabic[f] += 1
        for f in record_has_latin:  field_latin[f]  += 1
        for f in record_has_both:   field_both[f]   += 1
        for f, empty in record_all_empty.items():
            if empty: field_empty[f] += 1

    # --- Build structured field stats ---
    fields: dict = {}
    for f in sorted(field_present.keys()):
        counts  = field_counts[f]
        present = field_present[f]
        is_multi = max(counts) > 1
        fields[f] = {
            "records_present": present,
            "coverage_pct":    round(100 * present / n_parsed, 1) if n_parsed else 0,
            "always_single":   not is_multi,
            "per_record_count": {
                "min": min(counts), "max": max(counts),
                "avg": round(sum(counts) / len(counts), 2),
            } if is_multi else None,
            "script": {
                "arabic_only_pct": _pct(field_arabic[f] - field_both[f], present),
                "latin_only_pct":  _pct(field_latin[f]  - field_both[f], present),
                "mixed_pct":       _pct(field_both[f],                    present),
                "empty_pct":       _pct(field_empty[f],                   present),
            },
            "attributes": dict(field_attrs[f]) if field_attrs[f] else None,
            "samples":    field_samples[f],
        }

    _NGRAM_NAMES = {2: "bigrams", 3: "trigrams", 4: "quadgrams"}

    def _key(n: int) -> str:
        return _NGRAM_NAMES.get(n, f"{n}grams")

    # Build ngram_data: "fields" → field_name → script → size_key → rankings
    ngrams_by_field: dict[str, dict] = {}
    for fname in scan_fields:
        tf_lat = {n: Counter(_make_ngrams(field_tf[fname]["lat"], n)) for n in range(2, max_n + 1)}
        tf_ar  = {n: Counter(_make_ngrams(field_tf[fname]["ar"],  n)) for n in range(2, max_n + 1)}
        ngrams_by_field[fname] = {
            "latin": {
                _key(n): _rank_ngrams(tf_lat[n], field_df[fname]["lat"][n], n_parsed)
                for n in range(2, max_n + 1)
            },
            "arabic": {
                _key(n): _rank_ngrams(tf_ar[n], field_df[fname]["ar"][n], n_parsed)
                for n in range(2, max_n + 1)
            },
        }

    field_stats = {
        "files_found":   n_found,
        "files_parsed":  n_parsed,
        "files_failed":  n_failed,
        "parse_errors":  parse_errors,
        "fields":        fields,
    }
    ngram_data = {
        "files_parsed":     n_parsed,
        "max_n":            max_n,
        "keep_abbrev_dots": keep_abbrev_dots,
        "scan_fields":      scan_fields,
        "fields":           ngrams_by_field,
    }
    return field_stats, ngram_data


# ---------------------------------------------------------------------------
# Boilerplate suggestion (applied after scanning)
# ---------------------------------------------------------------------------

def _suggest_boilerplate(
    ngram_data: dict,
    field_configs: dict[str, FieldBoilerplateConfig],
) -> list[dict]:
    """Return boilerplate candidates sorted by source_field then repeats_per_doc.

    Applies per-field criteria based on the field's mode:
    - "full":      doc_freq_pct >= min AND repeats_per_doc <= max
    - "freq_only": doc_freq_pct >= min only (repeats criterion not applied)

    Each row: {ngram, source_field, script, n, doc_freq_pct, repeats_per_doc,
               keep, signal_type}
    """
    n_docs = ngram_data["files_parsed"]
    if n_docs == 0:
        return []

    _NGRAM_SIZES = {
        "bigrams": 2, "trigrams": 3, "quadgrams": 4,
        **{f"{i}grams": i for i in range(5, 20)},
    }

    candidates: list[dict] = []
    seen: set[tuple[str, str]] = set()  # (field, ngram) dedup

    for fname, fcfg in field_configs.items():
        field_ngrams = ngram_data.get("fields", {}).get(fname)
        if field_ngrams is None:
            continue

        for script in ("latin", "arabic"):
            for size_key, size_data in field_ngrams[script].items():
                n = _NGRAM_SIZES.get(size_key, 0)
                for row in size_data["by_doc_freq"]:
                    df     = row["doc_freq"]
                    tf     = row["term_freq"]
                    df_pct = round(100 * df / n_docs, 2)
                    repeats = round(tf / df, 3) if df > 0 else 0.0
                    ngram  = row["ngram"]

                    key = (fname, ngram)
                    if key in seen:
                        continue

                    passes = df_pct >= fcfg.min_doc_freq_pct
                    if fcfg.mode == "full":
                        passes = passes and repeats <= fcfg.max_repeats_per_doc

                    if passes:
                        seen.add(key)
                        candidates.append({
                            "ngram":           ngram,
                            "source_field":    fname,
                            "script":          script,
                            "n":               n,
                            "doc_freq_pct":    df_pct,
                            "repeats_per_doc": repeats,
                            "keep":            "yes",
                            "signal_type":     "",
                        })

    return sorted(candidates, key=lambda x: (x["source_field"], x["repeats_per_doc"]))


# ---------------------------------------------------------------------------
# Public pipeline functions
# ---------------------------------------------------------------------------

def build(
    data_dir:         str | None = None,
    survey_dir:       str | None = None,
    max_n:            int | None = None,
    keep_abbrev_dots: bool | None = None,
    sample:           int | None = None,
    seed:             int = 42,
    config_path:      str | None = None,
) -> Path:
    """Scan XML records; write summary.json, ngrams.json, boilerplate_review.csv.

    Returns the survey directory Path.

    All parameters fall back to config.yml values if not supplied.
    Per-field boilerplate thresholds are always read from config — use
    config.yml to tune them rather than CLI flags.
    """
    cfg = load_config(config_path)

    data_dir  = data_dir  or cfg.bnf_data_path
    out_dir   = Path(survey_dir or cfg.resolved_bnf_survey_dir())
    max_n     = max_n            if max_n is not None            else cfg.survey.max_n
    abbrev    = keep_abbrev_dots if keep_abbrev_dots is not None else cfg.survey.keep_abbrev_dots

    if not data_dir:
        raise ValueError(
            "No data directory specified. Set bnf_data_path in config.yml or pass --dir."
        )

    out_dir.mkdir(parents=True, exist_ok=True)

    scan_fields = list(cfg.boilerplate.fields.keys())

    print(f"Scanning: {data_dir}")
    print(f"  n-gram fields: {', '.join(scan_fields)}")
    field_stats, ngram_data = _scan(
        data_dir,
        max_n=max_n,
        keep_abbrev_dots=abbrev,
        scan_fields=scan_fields,
        sample=sample,
        seed=seed,
    )

    # Write summary.json
    summary_path = out_dir / "summary.json"
    summary_path.write_text(
        json.dumps(field_stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"  summary.json        → {summary_path}")

    # Write ngrams.json
    ngrams_path = out_dir / "ngrams.json"
    ngrams_path.write_text(
        json.dumps(ngram_data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"  ngrams.json         → {ngrams_path}")

    # Write boilerplate_review.csv
    candidates = _suggest_boilerplate(ngram_data, cfg.boilerplate.fields)
    review_path = out_dir / "boilerplate_review.csv"
    with review_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "ngram", "source_field", "script", "n",
                "doc_freq_pct", "repeats_per_doc", "keep", "signal_type",
            ],
        )
        writer.writeheader()
        writer.writerows(candidates)
    print(f"  boilerplate_review.csv → {review_path}  ({len(candidates)} candidates)")
    print(f"  Review instructions:")
    print(f"    keep=yes, signal_type=''            → pure boilerplate (discarded)")
    print(f"    keep=yes, signal_type=agent:copyist → signal phrase (routed to relations)")
    print(f"    keep=no                             → content, left in field text")
    print(f"  Valid signal_type values: agent:copyist, agent:commentator, agent:owner,")
    print(f"                            relation:commentary, relation:abridgement,")
    print(f"                            relation:continuation, date:copy")

    # Record per-field thresholds used in manifest parameters
    field_params = {
        fname: {
            "mode":                fcfg.mode,
            "min_doc_freq_pct":    fcfg.min_doc_freq_pct,
            "max_repeats_per_doc": fcfg.max_repeats_per_doc,
        }
        for fname, fcfg in cfg.boilerplate.fields.items()
    }

    # Update manifest
    manifest = _load_manifest(out_dir)
    manifest["config_snapshot"] = _config_snapshot(cfg)
    manifest["stages"]["build"] = {
        "completed":  True,
        "timestamp":  _now(),
        "parameters": {
            "max_n":          max_n,
            "keep_abbrev_dots": abbrev,
            "scan_fields":    scan_fields,
            "boilerplate_fields": field_params,
            "sample":         sample,
            "seed":           seed if sample is not None else None,
        },
        "outputs": ["summary.json", "ngrams.json", "boilerplate_review.csv"],
    }
    manifest["stages"].pop("apply_review", None)   # invalidated by a new build
    _save_manifest(out_dir, manifest)

    print(f"\nNext: review {review_path}")
    print(f"      set keep=no for false positives, then run: apply-review")
    return out_dir


def apply_review(
    survey_dir:  str | None = None,
    config_path: str | None = None,
) -> Path:
    """Read the reviewed CSV and write boilerplate.json.

    boilerplate.json contains two sections:
    - "boilerplate": list of {ngram, field} dicts — phrases stripped from field text
    - "signals":     list of {ngram, field, signal_type} dicts — phrases routed to
                     relation/agent detection

    Returns the survey directory Path.
    """
    cfg     = load_config(config_path)
    out_dir = Path(survey_dir or cfg.resolved_bnf_survey_dir())

    review_path = out_dir / "boilerplate_review.csv"
    if not review_path.exists():
        raise FileNotFoundError(
            f"{review_path} not found. Run `build` first."
        )

    boilerplate: list[dict] = []
    signals:     list[dict] = []

    with review_path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("keep", "yes").strip().lower() == "no":
                continue
            ngram       = row["ngram"]
            field       = row.get("source_field", "description").strip() or "description"
            signal_type = row.get("signal_type", "").strip()
            if signal_type:
                signals.append({"ngram": ngram, "field": field, "signal_type": signal_type})
            else:
                boilerplate.append({"ngram": ngram, "field": field})

    boilerplate_path = out_dir / "boilerplate.json"
    boilerplate_path.write_text(
        json.dumps(
            {"boilerplate": boilerplate, "signals": signals},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"boilerplate.json → {boilerplate_path}")
    print(f"  {len(boilerplate)} boilerplate phrases")
    print(f"  {len(signals)} signal phrases")

    manifest = _load_manifest(out_dir)
    manifest["stages"]["apply_review"] = {
        "completed":         True,
        "timestamp":         _now(),
        "boilerplate_count": len(boilerplate),
        "signal_count":      len(signals),
        "outputs":           ["boilerplate.json"],
    }
    _save_manifest(out_dir, manifest)

    return out_dir


# ---------------------------------------------------------------------------
# Human-readable summary report
# ---------------------------------------------------------------------------

def print_summary(results: dict, top_n: int = 20) -> None:
    """Print a structured, readable summary of field_stats to stdout."""
    n   = results["files_parsed"]
    sep = "-" * 78

    print(f"\nBNF Field Survey")
    print(sep)
    print(f"  Files found:  {results['files_found']}")
    print(f"  Files parsed: {n}")
    print(f"  Files failed: {results['files_failed']}")

    if results["parse_errors"]:
        print(f"\n  Parse errors (first 5):")
        for err in results["parse_errors"][:5]:
            print(f"    {err['path']}\n    → {err['error']}")

    print(
        f"\n  {'Field':<22} {'Coverage':>14}  {'Multi?':>18}  "
        f"{'Ar-only':>7}  {'Lat-only':>8}  {'Mixed':>5}  {'Empty':>5}"
    )
    print(sep)

    for f, s in results["fields"].items():
        multi_str = "no" if s["always_single"] else (
            f"yes  avg {s['per_record_count']['avg']}  max {s['per_record_count']['max']}"
        )
        sc = s["script"]
        print(
            f"  dc:{f:<18} "
            f"{s['records_present']:>5}/{n} ({s['coverage_pct']:>5}%)  "
            f"{multi_str:>18}  "
            f"{sc['arabic_only_pct']:>6}%  {sc['latin_only_pct']:>7}%  "
            f"{sc['mixed_pct']:>4}%  {sc['empty_pct']:>4}%"
        )
        if s["attributes"]:
            for attr, count in sorted(s["attributes"].items(), key=lambda x: -x[1]):
                print(f"      attr  {attr}  ({count:,}×)")
        for sample in s["samples"]:
            print(f"      > {sample}")
    print()


def print_ngrams(ngram_data: dict, top_n: int = 20) -> None:
    """Print top n-grams per ranking per script per size, grouped by field."""
    sep = "-" * 78
    rankings = [
        ("by_doc_freq",  "by doc_freq",  lambda r: f"df={r['doc_freq']:>5}  tf={r['term_freq']:>6}  tfidf={r['tfidf']:>9.3f}"),
        ("by_term_freq", "by term_freq", lambda r: f"tf={r['term_freq']:>6}  df={r['doc_freq']:>5}  tfidf={r['tfidf']:>9.3f}"),
        ("by_tfidf",     "by TF-IDF",   lambda r: f"tfidf={r['tfidf']:>9.3f}  tf={r['term_freq']:>6}  df={r['doc_freq']:>5}"),
    ]

    for fname, field_ngrams in ngram_data.get("fields", {}).items():
        print(f"\n  dc:{fname} n-grams (top {top_n} per ranking)")
        print(sep)
        for script in ("latin", "arabic"):
            for size_key, size_data in field_ngrams[script].items():
                print(f"\n  {script.upper()} {size_key}")
                for rank_key, rank_label, fmt in rankings:
                    entries = size_data[rank_key][:top_n]
                    if not entries:
                        continue
                    print(f"    --- {rank_label} ---")
                    for row in entries:
                        print(f"      {row['ngram']:<42}  {fmt(row)}")
    print()


# ---------------------------------------------------------------------------
# Config loader (local, for scripts that don't import utils.config directly)
# ---------------------------------------------------------------------------

def _load_config_yml() -> dict:
    path = _ROOT / "config.yml"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_cli(args: argparse.Namespace) -> None:
    build(
        data_dir=args.dir,
        survey_dir=args.survey_dir,
        max_n=args.max_n,
        keep_abbrev_dots=args.keep_abbrev_dots if args.keep_abbrev_dots else None,
        sample=args.sample,
        seed=args.seed,
    )
    if args.print_summary or args.print_ngrams:
        cfg     = load_config()
        out_dir = Path(args.survey_dir or cfg.resolved_bnf_survey_dir())
        if args.print_summary:
            field_stats = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
            print_summary(field_stats, top_n=args.top_n)
        if args.print_ngrams:
            ngram_data = json.loads((out_dir / "ngrams.json").read_text(encoding="utf-8"))
            print_ngrams(ngram_data, top_n=args.top_n)


def _apply_review_cli(args: argparse.Namespace) -> None:
    apply_review(survey_dir=args.survey_dir)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="BNF survey and boilerplate vocabulary pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- build ---
    p_build = sub.add_parser(
        "build",
        help="Scan XML records; write summary.json, ngrams.json, boilerplate_review.csv.",
    )
    p_build.add_argument("--dir",          default=None, help="BNF data directory (overrides config).")
    p_build.add_argument("--survey-dir",   default=None, help="Output directory (overrides config).")
    p_build.add_argument("--max-n",        type=int,   default=None, help="Largest n-gram size (default from config).")
    p_build.add_argument("--keep-abbrev-dots", action="store_true", default=False,
                         help="Retain trailing dots on abbreviation tokens.")
    p_build.add_argument("--sample",       type=int,   default=None, help="Random sample size.")
    p_build.add_argument("--seed",         type=int,   default=42,   help="Random seed (default 42).")
    p_build.add_argument("--print-summary",  action="store_true", default=False,
                         help="Print field coverage report after build.")
    p_build.add_argument("--print-ngrams",   action="store_true", default=False,
                         help="Print n-gram rankings after build.")
    p_build.add_argument("--top-n",        type=int,   default=20,
                         help="Number of n-grams to display (report only, default 20).")
    p_build.set_defaults(func=_build_cli)

    # --- apply-review ---
    p_review = sub.add_parser(
        "apply-review",
        help="Read reviewed boilerplate_review.csv; write boilerplate.json.",
    )
    p_review.add_argument("--survey-dir", default=None, help="Survey directory (overrides config).")
    p_review.set_defaults(func=_apply_review_cli)

    args = parser.parse_args()
    args.func(args)
