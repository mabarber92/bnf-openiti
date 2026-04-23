"""
Greedy search test with comprehensive author matching.

Uses all available author name variants from both datasets:
- OpenITI: name_slug, wd_label_en, wd_aliases_en, all name components
- BNF: creator + contributor + description_candidates

Separates title and author matching signals.
"""

from __future__ import annotations

import csv
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fuzzywuzzy import fuzz
from matching.normalize import normalize_transliteration

# Load data
openiti_path = Path("data/openiti_corpus_2025_1_9.json")
bnf_path = Path("outputs/bnf_parsed.json")

print("Loading OpenITI corpus...")
with open(openiti_path, encoding="utf-8") as f:
    openiti_data = json.load(f)
    openiti_books = openiti_data["books"]
    openiti_authors = openiti_data["authors"]

print(f"  {len(openiti_books)} books loaded")
print(f"  {len(openiti_authors)} authors loaded")

print("Loading BNF records...")
with open(bnf_path, encoding="utf-8") as f:
    bnf_data = json.load(f)
    bnf_records = bnf_data["records"]

print(f"  {len(bnf_records)} records loaded")

# Load correspondences
print("Loading test correspondences...")
with open("data_samplers/correspondence.json", encoding="utf-8") as f:
    correspondences = json.load(f)

test_pairs = {}
for item in correspondences:
    for openiti_uri, bnf_id in item.items():
        if bnf_id not in test_pairs:
            test_pairs[bnf_id] = []
        test_pairs[bnf_id].append(openiti_uri)

print(f"  {len(test_pairs)} unique BNF records with {sum(len(v) for v in test_pairs.values())} correspondences")


@dataclass
class SearchResult:
    """Result of searching one BNF record against OpenITI."""
    bnf_id: str
    openiti_uri: str
    matched_uris_by_title: list[str]
    matched_uris_by_author: list[str]
    matched_uris_combined: list[str]
    threshold: float
    is_correct_title: bool
    is_correct_author: bool
    is_correct_combined: bool
    num_false_positives_title: int
    num_false_positives_author: int
    num_false_positives_combined: int
    search_time_ms: float


def build_bnf_title_candidates(bnf_record: dict) -> dict[str, list[str]]:
    """Extract title candidates from BNF record."""
    candidates = {"lat": [], "ara": []}

    for title in bnf_record.get("title_lat", []):
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["lat"]:
                candidates["lat"].append(part)

    for title in bnf_record.get("title_ara", []):
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["ara"]:
                candidates["ara"].append(part)

    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)

    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)

    return candidates


def build_bnf_author_candidates(bnf_record: dict) -> dict[str, list[str]]:
    """
    Extract author candidates from BNF record.

    Uses creator, contributor, and description_candidates fields
    since author info often appears in descriptions for composite manuscripts.
    """
    candidates = {"lat": [], "ara": []}

    # Creator fields (primary source)
    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)

    for creator in bnf_record.get("creator_ara", []):
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)

    # Contributor fields (secondary source)
    for contrib in bnf_record.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)

    for contrib in bnf_record.get("contributor_ara", []):
        if contrib and contrib not in candidates["ara"]:
            candidates["ara"].append(contrib)

    # Description candidates (often contain author names for composites)
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)

    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)

    return candidates


def build_openiti_title_candidates(book: dict) -> dict[str, list[str]]:
    """Extract title candidates from OpenITI book."""
    candidates = {"lat": [], "ara": []}

    if book.get("title_lat"):
        for part in book["title_lat"].split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["lat"]:
                candidates["lat"].append(part)

    if book.get("title_ara"):
        for part in book["title_ara"].split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["ara"]:
                candidates["ara"].append(part)

    return candidates


def build_openiti_author_candidates(author_uri: str) -> dict[str, list[str]]:
    """
    Extract all available author name variants from OpenITI author record.

    Sources (Latin/transliterated):
    - name_slug: primary slug
    - wd_label_en: Wikidata English label (if available)
    - wd_aliases_en: Wikidata English aliases (if available)
    - name_*_lat: structured name components (shuhra, ism, kunya, laqab, nisba)

    Sources (Arabic):
    - wd_label_ar: Wikidata Arabic label
    - wd_aliases_ar: Wikidata Arabic aliases
    """
    candidates = {"lat": [], "ara": []}

    author = openiti_authors.get(author_uri)
    if not author:
        return candidates

    # Latin/Transliterated sources
    if author.get("name_slug"):
        candidates["lat"].append(author["name_slug"])

    if author.get("wd_label_en"):
        candidates["lat"].append(author["wd_label_en"])

    if author.get("wd_aliases_en"):
        aliases = author["wd_aliases_en"]
        if isinstance(aliases, list):
            candidates["lat"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["lat"].append(aliases)

    # Structured name components (Latin)
    for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
        if author.get(field):
            candidates["lat"].append(author[field])

    # Arabic sources
    if author.get("wd_label_ar"):
        candidates["ara"].append(author["wd_label_ar"])

    if author.get("wd_aliases_ar"):
        aliases = author["wd_aliases_ar"]
        if isinstance(aliases, list):
            candidates["ara"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["ara"].append(aliases)

    return candidates


def search_bnf_against_all_openiti(
    bnf_id: str, expected_uris: list[str], threshold: float
) -> SearchResult:
    """Search one BNF record against all OpenITI books, tracking title/author separately."""
    bnf_record = bnf_records.get(bnf_id)
    if not bnf_record:
        return None

    bnf_titles = build_bnf_title_candidates(bnf_record)
    bnf_authors = build_bnf_author_candidates(bnf_record)

    if not bnf_titles.get("lat") and not bnf_titles.get("ara"):
        if not bnf_authors.get("lat") and not bnf_authors.get("ara"):
            return None

    # Normalize
    bnf_titles_norm = {
        "lat": [normalize_transliteration(c) for c in bnf_titles.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in bnf_titles.get("ara", [])],
    }
    bnf_authors_norm = {
        "lat": [normalize_transliteration(c) for c in bnf_authors.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in bnf_authors.get("ara", [])],
    }

    matched_by_title = []
    matched_by_author = []
    matched_combined = []

    start_time = time.time()

    # Search against all OpenITI books
    for openiti_uri, openiti_book in openiti_books.items():
        openiti_titles = build_openiti_title_candidates(openiti_book)
        openiti_titles_norm = {
            "lat": [normalize_transliteration(c) for c in openiti_titles.get("lat", [])],
            "ara": [normalize_transliteration(c) for c in openiti_titles.get("ara", [])],
        }

        # Try title matching
        title_matched = False
        for script in ["lat", "ara"]:
            if not bnf_titles_norm.get(script) or not openiti_titles_norm.get(script):
                continue
            for bnf_str in bnf_titles_norm[script]:
                for openiti_str in openiti_titles_norm[script]:
                    score = fuzz.token_set_ratio(bnf_str, openiti_str)
                    if score >= threshold * 100:
                        title_matched = True
                        break

        # Try author matching
        author_matched = False
        author_uri = openiti_book.get("author_uri")
        if author_uri:
            openiti_authors_cands = build_openiti_author_candidates(author_uri)
            openiti_authors_norm = {
                "lat": [normalize_transliteration(c) for c in openiti_authors_cands.get("lat", [])],
                "ara": [normalize_transliteration(c) for c in openiti_authors_cands.get("ara", [])],
            }

            for script in ["lat", "ara"]:
                if not bnf_authors_norm.get(script) or not openiti_authors_norm.get(script):
                    continue
                for bnf_str in bnf_authors_norm[script]:
                    for openiti_str in openiti_authors_norm[script]:
                        score = fuzz.token_set_ratio(bnf_str, openiti_str)
                        if score >= threshold * 100:
                            author_matched = True
                            break

        # Record matches
        if title_matched:
            matched_by_title.append(openiti_uri)
        if author_matched:
            matched_by_author.append(openiti_uri)
        if title_matched and author_matched:
            matched_combined.append(openiti_uri)

    elapsed_ms = (time.time() - start_time) * 1000

    # Check correctness
    correct_title = any(uri in matched_by_title for uri in expected_uris)
    correct_author = any(uri in matched_by_author for uri in expected_uris)
    correct_combined = any(uri in matched_combined for uri in expected_uris)

    # Count false positives
    fp_title = len(matched_by_title) - sum(1 for uri in matched_by_title if uri in expected_uris)
    fp_author = len(matched_by_author) - sum(1 for uri in matched_by_author if uri in expected_uris)
    fp_combined = len(matched_combined) - sum(1 for uri in matched_combined if uri in expected_uris)

    for expected_uri in expected_uris:
        return SearchResult(
            bnf_id=bnf_id,
            openiti_uri=expected_uri,
            matched_uris_by_title=matched_by_title,
            matched_uris_by_author=matched_by_author,
            matched_uris_combined=matched_combined,
            threshold=threshold,
            is_correct_title=correct_title,
            is_correct_author=correct_author,
            is_correct_combined=correct_combined,
            num_false_positives_title=fp_title,
            num_false_positives_author=fp_author,
            num_false_positives_combined=fp_combined,
            search_time_ms=elapsed_ms,
        )


def run_searches(thresholds: list[float] = None) -> list[SearchResult]:
    """Search all BNF records against full OpenITI corpus."""
    if thresholds is None:
        thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]

    all_results = []

    print(f"\nSearching {len(test_pairs)} BNF records (comprehensive author matching)...")
    print(f"Thresholds: {thresholds}\n")

    for i, (bnf_id, expected_uris) in enumerate(test_pairs.items(), 1):
        for threshold in thresholds:
            result = search_bnf_against_all_openiti(bnf_id, expected_uris, threshold)
            if result:
                all_results.append(result)
                title_status = "OK" if result.is_correct_title else "--"
                author_status = "OK" if result.is_correct_author else "--"
                combined_status = "OK" if result.is_correct_combined else "--"
                print(
                    f"  [{i}/{len(test_pairs)}] {bnf_id} @ {threshold}: "
                    f"title={title_status} author={author_status} combined={combined_status}"
                )

    return all_results


def write_results(results: list[SearchResult], output_dir: Path = Path("matching")) -> None:
    """Write results and generate reports (separate CSVs for title and author)."""
    output_dir.mkdir(exist_ok=True)

    # Write title matching results
    title_csv = output_dir / "matching_results_title.csv"
    print(f"\nWriting title matching results to {title_csv}...")
    with open(title_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "bnf_id", "expected_uri", "threshold",
            "matched_uris", "is_correct", "num_matches", "false_positives",
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "bnf_id": r.bnf_id,
                "expected_uri": r.openiti_uri,
                "threshold": r.threshold,
                "matched_uris": "|".join(r.matched_uris_by_title),
                "is_correct": r.is_correct_title,
                "num_matches": len(r.matched_uris_by_title),
                "false_positives": r.num_false_positives_title,
            })

    # Write author matching results
    author_csv = output_dir / "matching_results_author.csv"
    print(f"Writing author matching results to {author_csv}...")
    with open(author_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "bnf_id", "expected_uri", "threshold",
            "matched_uris", "is_correct", "num_matches", "false_positives",
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "bnf_id": r.bnf_id,
                "expected_uri": r.openiti_uri,
                "threshold": r.threshold,
                "matched_uris": "|".join(r.matched_uris_by_author),
                "is_correct": r.is_correct_author,
                "num_matches": len(r.matched_uris_by_author),
                "false_positives": r.num_false_positives_author,
            })

    # One report for each signal
    for signal in ["title", "author", "combined"]:
        report_path = output_dir / f"signal_diagnosis_comprehensive_{signal}.txt"

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"SIGNAL DIAGNOSIS (COMPREHENSIVE): {signal.upper()}\n")
            f.write("="*80 + "\n\n")

            # Get results for this signal
            if signal == "title":
                correct_attr = "is_correct_title"
                fp_attr = "num_false_positives_title"
                uri_attr = "matched_uris_by_title"
            elif signal == "author":
                correct_attr = "is_correct_author"
                fp_attr = "num_false_positives_author"
                uri_attr = "matched_uris_by_author"
            else:
                correct_attr = "is_correct_combined"
                fp_attr = "num_false_positives_combined"
                uri_attr = "matched_uris_combined"

            thresholds = sorted(set(r.threshold for r in results))
            metrics_by_threshold = {}

            for threshold in thresholds:
                threshold_results = [r for r in results if r.threshold == threshold]
                correct = sum(1 for r in threshold_results if getattr(r, correct_attr))
                total = len(threshold_results)
                total_false_positives = sum(getattr(r, fp_attr) for r in threshold_results)
                avg_time = sum(r.search_time_ms for r in threshold_results) / total if total > 0 else 0

                all_matches = sum(len(getattr(r, uri_attr)) for r in threshold_results)
                correct_matches = all_matches - total_false_positives
                precision = correct_matches / all_matches if all_matches > 0 else 0
                recall = correct / total if total > 0 else 0
                f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

                metrics_by_threshold[threshold] = {
                    "correct": correct,
                    "total": total,
                    "recall": recall,
                    "precision": precision,
                    "f1": f1,
                    "total_false_pos": total_false_positives,
                    "avg_time_ms": avg_time,
                }

            f.write("PERFORMANCE BY THRESHOLD\n")
            f.write("-"*80 + "\n")
            f.write(f"{'Threshold':<12} {'Recall':<10} {'Precision':<12} {'F1-Score':<10} {'False Pos':<12}\n")
            f.write("-"*80 + "\n")

            for threshold in sorted(thresholds):
                m = metrics_by_threshold[threshold]
                f.write(
                    f"{threshold:<12.2f} "
                    f"{m['recall']*100:<9.1f}% "
                    f"{m['precision']*100:<11.1f}% "
                    f"{m['f1']:<10.3f} "
                    f"{m['total_false_pos']:<12d}\n"
                )

            f.write("\n")

    print(f"Diagnostic reports written to {output_dir}/signal_diagnosis_comprehensive_*.txt")


if __name__ == "__main__":
    thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]
    results = run_searches(thresholds)
    write_results(results)
