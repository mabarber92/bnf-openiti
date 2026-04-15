"""
utils/survey_bnf.py

Analyse field coverage and content patterns across BNF OAI-PMH XML records
before writing the parser. Run this against the full dataset to understand
exactly which fields exist, how often, whether they carry Arabic or Latin
content, and what attribute variants appear.

Importable:
    from utils.survey_bnf import survey, print_report
    results = survey("/path/to/bnf/xml/directory")

CLI (reads bnf_data_path from config.yml by default):
    python utils/survey_bnf.py
    python utils/survey_bnf.py --out results.json
    python utils/survey_bnf.py --dir /custom/path --out results.json
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET
from tqdm import tqdm

import yaml  # pip install pyyaml

# ---------------------------------------------------------------------------
# Namespace map for OAI-PMH / Dublin Core XML
# ---------------------------------------------------------------------------
NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc":     "http://purl.org/dc/elements/1.1/",
}

# Unicode ranges covering Arabic script (Arabic, Arabic Supplement,
# Arabic Extended-A, Arabic Presentation Forms A & B)
_ARABIC_RE = re.compile(
    r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]"
)
_LATIN_RE = re.compile(r"[A-Za-z]")


def _has_arabic(text: str) -> bool:
    return bool(_ARABIC_RE.search(text))


def _has_latin(text: str) -> bool:
    return bool(_LATIN_RE.search(text))


def _strip_ns(tag: str) -> str:
    """Remove Clark-notation namespace from an element tag.

    '{http://purl.org/dc/elements/1.1/}title' -> 'title'
    """
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _pct(n: int, total: int) -> float:
    return round(100 * n / total, 1) if total else 0.0


# ---------------------------------------------------------------------------
# N-gram helpers
# ---------------------------------------------------------------------------

def _tokenize_lat(text: str) -> list[str]:
    """Tokenise Latin-script text into lowercase word tokens.

    Covers ASCII Latin, extended Latin (accented characters), and the
    precomposed Latin Extended Additional block used by ALA-LC transliteration
    (ā, ī, ū, ḍ, ṣ, ḥ, etc.). Tokens shorter than 2 characters are dropped.
    """
    return [
        t for t in re.findall(
            r"[a-zA-ZÀ-ÖØ-öø-ÿ\u02b0-\u02ff\u1e00-\u1eff]+",
            text.lower(),
        )
        if len(t) >= 2
    ]


def _tokenize_ar(text: str) -> list[str]:
    """Tokenise Arabic-script text into word tokens.

    Arabic particles (و، في، من ...) are kept — they form part of
    real multi-word phrases and are needed for meaningful bigrams/trigrams.
    """
    return re.findall(
        r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]+",
        text,
    )


def _make_ngrams(tokens: list[str], n: int) -> list[str]:
    return [" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]


def _rank_ngrams(
    tf: Counter, df: Counter, n_docs: int, top_n: int
) -> dict:
    """Produce three independent top-N rankings from term and document frequency data.

    by_doc_freq  — sorted by records containing the n-gram (broadly distributed patterns)
    by_term_freq — sorted by total corpus occurrences (high-volume phrases)
    by_tfidf     — sorted by TF-IDF score (distinctive phrases concentrated in few records)

    The three lists are independent: a phrase can appear in any combination.
    High TF-IDF entries that do not make either frequency list are the most
    interesting for pattern discovery — frequent within certain records but
    not uniformly spread across the corpus.

    TF-IDF here is corpus-level: term_freq * log(n_docs / doc_freq).
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
        "by_doc_freq":  sorted(rows, key=lambda x: -x["doc_freq"])[:top_n],
        "by_term_freq": sorted(rows, key=lambda x: -x["term_freq"])[:top_n],
        "by_tfidf":     sorted(rows, key=lambda x: -x["tfidf"])[:top_n],
    }


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    config_path = Path(__file__).parent.parent / "config.yml"
    if not config_path.exists():
        return {}
    with config_path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ---------------------------------------------------------------------------
# Core survey function
# ---------------------------------------------------------------------------

def survey(directory: str, glob: str = "**/OAI_*.xml", top_n: int = 100) -> dict:
    """Walk *directory*, parse every OAI_*.xml file, and return coverage stats.

    Returns a dict with keys:
        files_found          int
        files_parsed         int
        files_failed         int
        parse_errors         list of {path, error}
        fields               dict keyed by DC field name (without 'dc:' prefix)
        description_ngrams   bigram/trigram analysis of dc:description text,
                             split by script, with term_freq and doc_freq per
                             n-gram so TF-IDF can be computed downstream.

    top_n controls how many n-grams are returned per size per script.
    Unigrams are omitted — at corpus scale they are dominated by function words.
    """
    root = Path(directory)
    paths = sorted(root.glob(glob))

    if not paths:
        raise FileNotFoundError(f"No XML files matched '{glob}' in {directory!r}")

    n_found  = len(paths)
    n_parsed = 0
    n_failed = 0
    parse_errors: list[dict] = []

    # N-gram accumulators for dc:description
    # Term frequency: every token occurrence across the whole corpus
    desc_tf_lat: list[str] = []
    desc_tf_ar:  list[str] = []
    # Document frequency: count each unique n-gram once per record
    desc_df_bi_lat:  Counter = Counter()
    desc_df_tri_lat: Counter = Counter()
    desc_df_bi_ar:   Counter = Counter()
    desc_df_tri_ar:  Counter = Counter()

    # Per-field accumulators
    field_present: Counter     = Counter()        # records that have this field
    field_counts:  defaultdict = defaultdict(list) # per-record element count (multi-value)
    field_arabic:  Counter     = Counter()        # records where field has Arabic content
    field_latin:   Counter     = Counter()        # records where field has Latin content
    field_both:    Counter     = Counter()        # records where field has both scripts
    field_empty:   Counter     = Counter()        # field present but blank
    field_attrs:   defaultdict = defaultdict(Counter)  # attribute key=value frequencies
    field_samples: defaultdict = defaultdict(list)     # up to 5 example values per field

    for path in tqdm(paths):
        try:
            tree = ET.parse(path)
        except ET.ParseError as exc:
            parse_errors.append({"path": str(path), "error": str(exc)})
            n_failed += 1
            continue

        # Locate the oai_dc:dc container
        dc_container = tree.find(".//oai_dc:dc", NS)
        if dc_container is None:
            parse_errors.append({"path": str(path), "error": "No oai_dc:dc element found"})
            n_failed += 1
            continue

        n_parsed += 1
        per_record: Counter = Counter()

        # Collect description text for n-gram analysis.
        # Tokens are accumulated per-record first so that document frequency
        # (unique n-grams per record) can be tracked separately from term
        # frequency (total occurrences across the corpus).
        rec_desc_tokens_lat: list[str] = []
        rec_desc_tokens_ar:  list[str] = []
        for desc_el in dc_container:
            if _strip_ns(desc_el.tag) != "description":
                continue
            desc_text = (desc_el.text or "").strip()
            if not desc_text:
                continue
            if _has_arabic(desc_text):
                rec_desc_tokens_ar.extend(_tokenize_ar(desc_text))
            else:
                rec_desc_tokens_lat.extend(_tokenize_lat(desc_text))

        # Commit to corpus-level accumulators
        desc_tf_lat.extend(rec_desc_tokens_lat)
        desc_tf_ar.extend(rec_desc_tokens_ar)
        desc_df_bi_lat.update(set(_make_ngrams(rec_desc_tokens_lat, 2)))
        desc_df_tri_lat.update(set(_make_ngrams(rec_desc_tokens_lat, 3)))
        desc_df_bi_ar.update(set(_make_ngrams(rec_desc_tokens_ar, 2)))
        desc_df_tri_ar.update(set(_make_ngrams(rec_desc_tokens_ar, 3)))

        # Accumulate script presence per field at the record level.
        # For multi-value fields (e.g. 8× dc:description) we want to know
        # whether *any* element has Arabic, not count each element separately.
        record_has_arabic: set[str] = set()
        record_has_latin:  set[str] = set()
        record_has_both:   set[str] = set()
        record_all_empty:  dict[str, bool] = {}  # True until a non-blank value seen

        for el in dc_container:
            local = _strip_ns(el.tag)
            per_record[local] += 1
            record_all_empty.setdefault(local, True)

            text = (el.text or "").strip()

            if not text:
                pass  # leave record_all_empty[local] as True
            else:
                record_all_empty[local] = False
                ar  = _has_arabic(text)
                lat = _has_latin(text)
                if ar:
                    record_has_arabic.add(local)
                if lat:
                    record_has_latin.add(local)
                if ar and lat:
                    record_has_both.add(local)

            # Collect every attribute variant seen (e.g. xml:lang="fre")
            for attr_key, attr_val in el.attrib.items():
                field_attrs[local][f"{_strip_ns(attr_key)}={attr_val!r}"] += 1

            # Keep a small sample of real values for human inspection
            if text and len(field_samples[local]) < 5:
                field_samples[local].append(text[:140])

        # Commit per-record counts to field-level accumulators
        for field, count in per_record.items():
            field_present[field] += 1
            field_counts[field].append(count)

        for field in record_has_arabic:
            field_arabic[field] += 1
        for field in record_has_latin:
            field_latin[field] += 1
        for field in record_has_both:
            field_both[field] += 1
        for field, all_empty in record_all_empty.items():
            if all_empty:
                field_empty[field] += 1

    # ---------------------------------------------------------------------------
    # Build structured output
    # ---------------------------------------------------------------------------
    fields: dict = {}
    for field in sorted(field_present.keys()):
        counts  = field_counts[field]
        present = field_present[field]
        is_multi = max(counts) > 1

        fields[field] = {
            "records_present": present,
            "coverage_pct":    round(100 * present / n_parsed, 1) if n_parsed else 0,
            "always_single":   not is_multi,
            "per_record_count": {
                "min": min(counts),
                "max": max(counts),
                "avg": round(sum(counts) / len(counts), 2),
            } if is_multi else None,
            "script": {
                "arabic_only_pct": _pct(field_arabic[field] - field_both[field], present),
                "latin_only_pct":  _pct(field_latin[field]  - field_both[field], present),
                "mixed_pct":       _pct(field_both[field],                        present),
                "empty_pct":       _pct(field_empty[field],                       present),
            },
            "attributes": dict(field_attrs[field]) if field_attrs[field] else None,
            "samples":    field_samples[field],
        }

    # Build term-frequency counters from the accumulated token lists
    tf_bi_lat  = Counter(_make_ngrams(desc_tf_lat, 2))
    tf_tri_lat = Counter(_make_ngrams(desc_tf_lat, 3))
    tf_bi_ar   = Counter(_make_ngrams(desc_tf_ar,  2))
    tf_tri_ar  = Counter(_make_ngrams(desc_tf_ar,  3))

    return {
        "files_found":  n_found,
        "files_parsed": n_parsed,
        "files_failed": n_failed,
        "parse_errors": parse_errors,
        "fields":       fields,
        "description_ngrams": {
            "note": (
                "Bigram and trigram frequencies across all dc:description text. "
                "Latin and Arabic tokenised separately. "
                "Sorted by doc_freq (records containing the n-gram) descending. "
                "term_freq = total corpus occurrences. "
                "Unigrams omitted — dominated by function words at this scale. "
                "TF-IDF: term_freq * log(files_parsed / doc_freq)."
            ),
            "latin": {
                "bigrams":  _rank_ngrams(tf_bi_lat,  desc_df_bi_lat,  n_parsed, top_n),
                "trigrams": _rank_ngrams(tf_tri_lat, desc_df_tri_lat, n_parsed, top_n),
            },
            "arabic": {
                "bigrams":  _rank_ngrams(tf_bi_ar,  desc_df_bi_ar,  n_parsed, top_n),
                "trigrams": _rank_ngrams(tf_tri_ar, desc_df_tri_ar, n_parsed, top_n),
            },
        },
    }


# ---------------------------------------------------------------------------
# Human-readable report
# ---------------------------------------------------------------------------

def print_report(results: dict) -> None:
    """Print a structured, readable summary of survey() results to stdout."""
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
            print(f"    {err['path']}")
            print(f"    → {err['error']}")

    print(
        f"\n  {'Field':<22} {'Coverage':>14}  {'Multi?':>18}  "
        f"{'Ar-only':>7}  {'Lat-only':>8}  {'Mixed':>5}  {'Empty':>5}"
    )
    print(sep)

    for field, s in results["fields"].items():
        if s["always_single"]:
            multi_str = "no"
        else:
            c = s["per_record_count"]
            multi_str = f"yes  avg {c['avg']}  max {c['max']}"

        sc = s["script"]
        print(
            f"  dc:{field:<18} "
            f"{s['records_present']:>5}/{n} ({s['coverage_pct']:>5}%)  "
            f"{multi_str:>18}  "
            f"{sc['arabic_only_pct']:>6}%  "
            f"{sc['latin_only_pct']:>7}%  "
            f"{sc['mixed_pct']:>4}%  "
            f"{sc['empty_pct']:>4}%"
        )

        # Attribute variants (e.g. xml:lang="fre" 32000×)
        if s["attributes"]:
            for attr, count in sorted(s["attributes"].items(), key=lambda x: -x[1]):
                print(f"      attr  {attr}  ({count:,}×)")

        # Sample values
        for sample in s["samples"]:
            print(f"      > {sample}")

    # N-gram summary — top 20 per ranking per script per size
    ngrams = results.get("description_ngrams")
    if ngrams:
        rankings = [
            ("by_doc_freq",  "by doc_freq",  lambda r: f"df={r['doc_freq']:>5}  tf={r['term_freq']:>6}  tfidf={r['tfidf']:>9.3f}"),
            ("by_term_freq", "by term_freq", lambda r: f"tf={r['term_freq']:>6}  df={r['doc_freq']:>5}  tfidf={r['tfidf']:>9.3f}"),
            ("by_tfidf",     "by TF-IDF",   lambda r: f"tfidf={r['tfidf']:>9.3f}  tf={r['term_freq']:>6}  df={r['doc_freq']:>5}"),
        ]
        print(f"\n  dc:description n-grams (top 20 per ranking)")
        print(sep)
        for script in ("latin", "arabic"):
            for size in ("bigrams", "trigrams"):
                data = ngrams[script][size]
                print(f"\n  {script.upper()} {size}")
                for rank_key, rank_label, fmt in rankings:
                    entries = data[rank_key][:20]
                    if not entries:
                        continue
                    print(f"    --- {rank_label} ---")
                    for row in entries:
                        print(f"      {row['ngram']:<42}  {fmt(row)}")
    print()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Windows terminals default to cp1252; Arabic script needs UTF-8.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        description="Survey field coverage across BNF OAI-PMH XML records."
    )
    parser.add_argument(
        "--dir",
        default=None,
        help="Path to BNF data directory. Defaults to bnf_data_path in config.yml.",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional path to save full results as JSON.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=100,
        help="Number of n-grams to return per size per script (default: 100).",
    )
    args = parser.parse_args()

    # Resolve directory: CLI arg > config.yml > error
    data_dir = args.dir
    if data_dir is None:
        cfg = _load_config()
        data_dir = cfg.get("bnf_data_path")
    if not data_dir:
        print("Error: no directory specified. Use --dir or set bnf_data_path in config.yml.")
        sys.exit(1)

    print(f"Surveying: {data_dir}")
    results = survey(data_dir, top_n=args.top_n)
    print_report(results)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(
            json.dumps(results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Full results saved to {args.out}")
