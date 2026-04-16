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

from utils.config import load_config, PipelineConfig  # noqa: E402
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
        "bnf_data_path":              cfg.bnf_data_path,
        "openiti_data_path":          cfg.openiti_data_path,
        "pipeline_out_dir":           cfg.pipeline_out_dir,
        "bnf_survey_dir":             cfg.bnf_survey_dir,
        "survey_max_n":               cfg.survey.max_n,
        "survey_keep_abbrev_dots":    cfg.survey.keep_abbrev_dots,
        "boilerplate_min_doc_freq_pct":    cfg.boilerplate.min_doc_freq_pct,
        "boilerplate_max_repeats_per_doc": cfg.boilerplate.max_repeats_per_doc,
        "parsing_overwrite_existing": cfg.parsing.overwrite_existing,
    }


# ---------------------------------------------------------------------------
# N-gram ranking (full, no truncation — display limits belong in print_summary)
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
    sample: int | None = None,
    seed: int = 42,
) -> tuple[dict, dict]:
    """Parse all XML files and return (field_stats, ngram_data).

    Runs once per build invocation; both summary.json and ngrams.json
    are derived from this single pass.
    """
    import random as _random

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

    # N-gram accumulators
    desc_tf_lat: list[str] = []
    desc_tf_ar:  list[str] = []
    desc_df_lat: dict[int, Counter] = {n: Counter() for n in range(2, max_n + 1)}
    desc_df_ar:  dict[int, Counter] = {n: Counter() for n in range(2, max_n + 1)}

    # Field-level accumulators
    field_present: Counter            = Counter()
    field_counts:  defaultdict        = defaultdict(list)
    field_arabic:  Counter            = Counter()
    field_latin:   Counter            = Counter()
    field_both:    Counter            = Counter()
    field_empty:   Counter            = Counter()
    field_attrs:   defaultdict        = defaultdict(Counter)
    field_samples: defaultdict        = defaultdict(list)

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

        # --- N-gram accumulation ---
        rec_lat: list[str] = []
        rec_ar:  list[str] = []
        for el in dc:
            if _strip_ns(el.tag) != "description":
                continue
            text = (el.text or "").strip()
            if not text:
                continue
            if _has_arabic(text):
                rec_ar.extend(_tokenize_ar(text))
            else:
                rec_lat.extend(_tokenize_lat(text, keep_abbrev_dots=keep_abbrev_dots))

        desc_tf_lat.extend(rec_lat)
        desc_tf_ar.extend(rec_ar)
        for n in range(2, max_n + 1):
            desc_df_lat[n].update(set(_make_ngrams(rec_lat, n)))
            desc_df_ar[n].update(set(_make_ngrams(rec_ar, n)))

        # --- Field-level accumulation ---
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

    tf_lat = {n: Counter(_make_ngrams(desc_tf_lat, n)) for n in range(2, max_n + 1)}
    tf_ar  = {n: Counter(_make_ngrams(desc_tf_ar,  n)) for n in range(2, max_n + 1)}

    field_stats = {
        "files_found":   n_found,
        "files_parsed":  n_parsed,
        "files_failed":  n_failed,
        "parse_errors":  parse_errors,
        "fields":        fields,
    }
    ngram_data = {
        "files_parsed":      n_parsed,
        "max_n":             max_n,
        "keep_abbrev_dots":  keep_abbrev_dots,
        "ngrams": {
            "latin": {
                _key(n): _rank_ngrams(tf_lat[n], desc_df_lat[n], n_parsed)
                for n in range(2, max_n + 1)
            },
            "arabic": {
                _key(n): _rank_ngrams(tf_ar[n], desc_df_ar[n], n_parsed)
                for n in range(2, max_n + 1)
            },
        },
    }
    return field_stats, ngram_data


# ---------------------------------------------------------------------------
# Boilerplate suggestion (applied after scanning)
# ---------------------------------------------------------------------------

def _suggest_boilerplate(
    ngram_data: dict,
    min_doc_freq_pct: float,
    max_repeats_per_doc: float,
) -> list[dict]:
    """Return boilerplate candidates sorted by repeats_per_doc ascending.

    Rows with repeats_per_doc close to 1.0 appear first — they are the most
    reliable boilerplate.  Rows with higher values (name fragments, rare
    phrases) appear last, making review top-to-bottom efficient.

    Each row: {ngram, script, n, doc_freq_pct, repeats_per_doc, keep}
    """
    n_docs = ngram_data["files_parsed"]
    if n_docs == 0:
        return []

    _NGRAM_SIZES = {
        "bigrams": 2, "trigrams": 3, "quadgrams": 4,
        **{f"{i}grams": i for i in range(5, 20)},
    }

    candidates: list[dict] = []
    seen: set[str] = set()

    for script in ("latin", "arabic"):
        for size_key, size_data in ngram_data["ngrams"][script].items():
            n = _NGRAM_SIZES.get(size_key, 0)
            for row in size_data["by_doc_freq"]:
                df       = row["doc_freq"]
                tf       = row["term_freq"]
                df_pct   = round(100 * df / n_docs, 2)
                repeats  = round(tf / df, 3) if df > 0 else 0.0
                ngram    = row["ngram"]

                if ngram in seen:
                    continue
                if df_pct >= min_doc_freq_pct and repeats <= max_repeats_per_doc:
                    seen.add(ngram)
                    candidates.append({
                        "ngram":           ngram,
                        "script":          script,
                        "n":               n,
                        "doc_freq_pct":    df_pct,
                        "repeats_per_doc": repeats,
                        "keep":            "yes",
                    })

    return sorted(candidates, key=lambda x: x["repeats_per_doc"])


# ---------------------------------------------------------------------------
# Public pipeline functions
# ---------------------------------------------------------------------------

def build(
    data_dir:            str | None = None,
    survey_dir:          str | None = None,
    max_n:               int | None = None,
    keep_abbrev_dots:    bool | None = None,
    min_doc_freq_pct:    float | None = None,
    max_repeats_per_doc: float | None = None,
    sample:              int | None = None,
    seed:                int = 42,
    config_path:         str | None = None,
) -> Path:
    """Scan XML records; write summary.json, ngrams.json, boilerplate_review.csv.

    Returns the survey directory Path.

    All parameters fall back to config.yml values if not supplied.
    """
    cfg = load_config(config_path)

    data_dir  = data_dir  or cfg.bnf_data_path
    out_dir   = Path(survey_dir or cfg.resolved_bnf_survey_dir())
    max_n     = max_n            if max_n is not None            else cfg.survey.max_n
    abbrev    = keep_abbrev_dots if keep_abbrev_dots is not None else cfg.survey.keep_abbrev_dots
    df_pct    = min_doc_freq_pct    if min_doc_freq_pct is not None    else cfg.boilerplate.min_doc_freq_pct
    repeats   = max_repeats_per_doc if max_repeats_per_doc is not None else cfg.boilerplate.max_repeats_per_doc

    if not data_dir:
        raise ValueError(
            "No data directory specified. Set bnf_data_path in config.yml or pass --dir."
        )

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Scanning: {data_dir}")
    field_stats, ngram_data = _scan(
        data_dir,
        max_n=max_n,
        keep_abbrev_dots=abbrev,
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
    candidates = _suggest_boilerplate(ngram_data, df_pct, repeats)
    review_path = out_dir / "boilerplate_review.csv"
    with review_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ngram", "script", "n", "doc_freq_pct", "repeats_per_doc", "keep"],
        )
        writer.writeheader()
        writer.writerows(candidates)
    print(f"  boilerplate_review.csv → {review_path}  ({len(candidates)} candidates)")

    # Update manifest
    manifest = _load_manifest(out_dir)
    manifest["config_snapshot"] = _config_snapshot(cfg)
    manifest["stages"]["build"] = {
        "completed":  True,
        "timestamp":  _now(),
        "parameters": {
            "max_n": max_n, "keep_abbrev_dots": abbrev,
            "min_doc_freq_pct": df_pct, "max_repeats_per_doc": repeats,
            "sample": sample, "seed": seed,
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

    boilerplate.json is a flat list of strings — all n-grams where keep=yes.
    Returns the survey directory Path.
    """
    cfg     = load_config(config_path)
    out_dir = Path(survey_dir or cfg.resolved_bnf_survey_dir())

    review_path = out_dir / "boilerplate_review.csv"
    if not review_path.exists():
        raise FileNotFoundError(
            f"{review_path} not found. Run `build` first."
        )

    kept: list[str] = []
    with review_path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("keep", "yes").strip().lower() != "no":
                kept.append(row["ngram"])

    boilerplate_path = out_dir / "boilerplate.json"
    boilerplate_path.write_text(
        json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"boilerplate.json → {boilerplate_path}  ({len(kept)} entries)")

    manifest = _load_manifest(out_dir)
    manifest["stages"]["apply_review"] = {
        "completed":  True,
        "timestamp":  _now(),
        "kept_count": len(kept),
        "outputs":    ["boilerplate.json"],
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
    """Print top n-grams per ranking per script per size."""
    sep = "-" * 78
    rankings = [
        ("by_doc_freq",  "by doc_freq",  lambda r: f"df={r['doc_freq']:>5}  tf={r['term_freq']:>6}  tfidf={r['tfidf']:>9.3f}"),
        ("by_term_freq", "by term_freq", lambda r: f"tf={r['term_freq']:>6}  df={r['doc_freq']:>5}  tfidf={r['tfidf']:>9.3f}"),
        ("by_tfidf",     "by TF-IDF",   lambda r: f"tfidf={r['tfidf']:>9.3f}  tf={r['term_freq']:>6}  df={r['doc_freq']:>5}"),
    ]
    print(f"\n  dc:description n-grams (top {top_n} per ranking)")
    print(sep)
    for script in ("latin", "arabic"):
        for size_key, size_data in ngram_data["ngrams"][script].items():
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
        min_doc_freq_pct=args.min_doc_freq_pct,
        max_repeats_per_doc=args.max_repeats_per_doc,
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
    p_build.add_argument("--min-doc-freq-pct",    type=float, default=None,
                         help="Min doc-freq %% for boilerplate candidates (default from config).")
    p_build.add_argument("--max-repeats-per-doc", type=float, default=None,
                         help="Max avg occurrences per record (default from config).")
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
