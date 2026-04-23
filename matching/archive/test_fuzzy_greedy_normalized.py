"""
Greedy search test with transliteration normalization.

Same as test_fuzzy_greedy_search.py but applies normalization to candidates
before fuzzy matching to handle diacritics, ayn variants, case, etc.
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

print(f"  {len(openiti_books)} books loaded")

print("Loading BNF records...")
with open(bnf_path, encoding="utf-8") as f:
    bnf_data = json.load(f)
    bnf_records = bnf_data["records"]

print(f"  {len(bnf_records)} records loaded")

# Load correspondences
print("Loading test correspondences...")
with open("data_samplers/correspondence.json", encoding="utf-8") as f:
    correspondences = json.load(f)

test_pairs = {}  # {bnf_id: [openiti_uris]}
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
    openiti_uri: str  # Which OpenITI URI should match
    matched_uris: list[str]  # Which OpenITI URIs actually matched
    threshold: float
    score_best: float
    is_correct: bool  # Did the search find the correct URI?
    num_false_positives: int
    search_time_ms: float


def build_matching_candidates(bnf_record: dict) -> dict[str, list[str]]:
    """Extract all matching candidates from a BNF record."""
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

    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)

    for creator in bnf_record.get("creator_ara", []):
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)

    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)

    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)

    return candidates


def build_openiti_candidates(book: dict) -> dict[str, list[str]]:
    """Extract matching candidates from an OpenITI book."""
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


def search_bnf_against_all_openiti(
    bnf_id: str, expected_uris: list[str], threshold: float
) -> SearchResult:
    """
    Search one BNF record against all OpenITI books.
    Returns which URIs match above threshold (using normalized matching).
    """
    bnf_record = bnf_records.get(bnf_id)
    if not bnf_record:
        return None

    bnf_cands = build_matching_candidates(bnf_record)
    if not bnf_cands.get("lat") and not bnf_cands.get("ara"):
        return None

    # Normalize candidates
    bnf_cands_norm = {
        "lat": [normalize_transliteration(c) for c in bnf_cands.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in bnf_cands.get("ara", [])],
    }

    matched_uris = []
    best_score = 0

    start_time = time.time()

    # Search against all OpenITI books
    for openiti_uri, openiti_book in openiti_books.items():
        openiti_cands = build_openiti_candidates(openiti_book)

        # Normalize OpenITI candidates
        openiti_cands_norm = {
            "lat": [normalize_transliteration(c) for c in openiti_cands.get("lat", [])],
            "ara": [normalize_transliteration(c) for c in openiti_cands.get("ara", [])],
        }

        # Try script-matched comparisons
        for script in ["lat", "ara"]:
            bnf_list = bnf_cands_norm.get(script, [])
            openiti_list = openiti_cands_norm.get(script, [])

            if not bnf_list or not openiti_list:
                continue

            # Find best match in this script
            for bnf_str in bnf_list:
                for openiti_str in openiti_list:
                    score = fuzz.token_set_ratio(bnf_str, openiti_str)
                    if score >= threshold * 100:
                        if openiti_uri not in matched_uris:
                            matched_uris.append(openiti_uri)
                        if score > best_score:
                            best_score = score
                        break  # Found a match in this script, move to next URI

    elapsed_ms = (time.time() - start_time) * 1000

    # Check if we found the correct URI(s)
    correct_found = any(uri in matched_uris for uri in expected_uris)
    false_positives = len(matched_uris) - sum(1 for uri in matched_uris if uri in expected_uris)

    # Return result for each expected URI
    for expected_uri in expected_uris:
        return SearchResult(
            bnf_id=bnf_id,
            openiti_uri=expected_uri,
            matched_uris=matched_uris,
            threshold=threshold,
            score_best=best_score,
            is_correct=expected_uri in matched_uris,
            num_false_positives=false_positives,
            search_time_ms=elapsed_ms,
        )


def run_searches(thresholds: list[float] = None) -> list[SearchResult]:
    """Search all BNF records against full OpenITI corpus."""
    if thresholds is None:
        thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]

    all_results = []

    print(f"\nSearching {len(test_pairs)} BNF records against {len(openiti_books)} OpenITI books (with normalization)...")
    print(f"Thresholds: {thresholds}\n")

    for i, (bnf_id, expected_uris) in enumerate(test_pairs.items(), 1):
        for threshold in thresholds:
            result = search_bnf_against_all_openiti(bnf_id, expected_uris, threshold)
            if result:
                all_results.append(result)
                status = "PASS" if result.is_correct else "FAIL"
                print(
                    f"  [{i}/{len(test_pairs)}] {bnf_id} @ {threshold}: "
                    f"{status} found={result.is_correct} false_pos={result.num_false_positives} "
                    f"time={result.search_time_ms:.1f}ms"
                )

    return all_results


def write_results(results: list[SearchResult], output_dir: Path = Path("matching")) -> None:
    """Write results and generate report."""
    output_dir.mkdir(exist_ok=True)

    csv_path = output_dir / "fuzzy_greedy_normalized_results.csv"
    report_path = output_dir / "fuzzy_greedy_normalized_report.txt"

    # Write CSV
    print(f"\nWriting detailed results to {csv_path}...")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "bnf_id",
            "expected_openiti_uri",
            "matched_uris",
            "threshold",
            "best_score",
            "found_correct",
            "num_false_positives",
            "search_time_ms",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for result in results:
            writer.writerow({
                "bnf_id": result.bnf_id,
                "expected_openiti_uri": result.openiti_uri,
                "matched_uris": "|".join(result.matched_uris),
                "threshold": result.threshold,
                "best_score": f"{result.score_best:.2f}",
                "found_correct": result.is_correct,
                "num_false_positives": result.num_false_positives,
                "search_time_ms": f"{result.search_time_ms:.1f}",
            })

    # Generate report
    print(f"Generating summary report to {report_path}...")

    thresholds = sorted(set(r.threshold for r in results))
    metrics_by_threshold = {}

    for threshold in thresholds:
        threshold_results = [r for r in results if r.threshold == threshold]
        correct = sum(1 for r in threshold_results if r.is_correct)
        total = len(threshold_results)
        total_false_positives = sum(r.num_false_positives for r in threshold_results)
        avg_time = sum(r.search_time_ms for r in threshold_results) / total if total > 0 else 0

        # Precision: of all matches found, how many were correct?
        all_matches_found = sum(len(r.matched_uris) for r in threshold_results)
        correct_matches = all_matches_found - total_false_positives
        precision = correct_matches / all_matches_found if all_matches_found > 0 else 0

        # Recall: did we find all the correct matches?
        recall = correct / total if total > 0 else 0

        # F1-score
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

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("GREEDY SEARCH TEST REPORT (WITH NORMALIZATION)\n")
        f.write("="*80 + "\n\n")

        f.write(f"Test setup:\n")
        f.write(f"  BNF records searched: {len(test_pairs)}\n")
        f.write(f"  OpenITI books in corpus: {len(openiti_books)}\n")
        f.write(f"  Total search operations: {len(test_pairs) * len(thresholds)}\n\n")

        f.write("PERFORMANCE BY THRESHOLD\n")
        f.write("-"*80 + "\n")
        f.write(f"{'Threshold':<12} {'Recall':<10} {'Precision':<12} {'F1-Score':<10} {'Avg Time':<15} {'False Pos':<12}\n")
        f.write("-"*80 + "\n")

        for threshold in sorted(thresholds):
            m = metrics_by_threshold[threshold]
            f.write(
                f"{threshold:<12.2f} "
                f"{m['recall']*100:<9.1f}% "
                f"{m['precision']*100:<11.1f}% "
                f"{m['f1']:<10.3f} "
                f"{m['avg_time_ms']:<14.1f}ms "
                f"{m['total_false_pos']:<12d}\n"
            )

        f.write("\nMETRICS EXPLAINED\n")
        f.write("-"*80 + "\n")
        f.write("  Recall: % of correct matches found (high = fewer missed matches)\n")
        f.write("  Precision: % of matches that were correct (high = fewer false positives)\n")
        f.write("  F1-Score: harmonic mean of precision and recall (best = 1.0)\n")
        f.write("  Avg Time: average search time per BNF record (ms)\n")
        f.write("  False Pos: total incorrect matches found\n\n")

        f.write("RECOMMENDATION\n")
        f.write("-"*80 + "\n")

        best_threshold = max(thresholds, key=lambda t: metrics_by_threshold[t]["f1"])
        best_metrics = metrics_by_threshold[best_threshold]

        f.write(f"Best threshold: {best_threshold:.2f}\n")
        f.write(f"  Recall: {best_metrics['recall']*100:.1f}% (correct matches found)\n")
        f.write(f"  Precision: {best_metrics['precision']*100:.1f}% (accuracy of matches)\n")
        f.write(f"  F1-Score: {best_metrics['f1']:.3f}\n")
        f.write(f"  Avg search time: {best_metrics['avg_time_ms']:.1f}ms per record\n\n")

        if best_metrics["precision"] >= 0.95 and best_metrics["recall"] >= 0.95:
            f.write("[PASS] EXCELLENT: High precision and recall, no optimization needed.\n")
        elif best_metrics["precision"] >= 0.90 or best_metrics["recall"] >= 0.90:
            f.write("[NOTE] GOOD: One metric is strong, but review the weaker one.\n")
            if best_metrics["precision"] < 0.90:
                f.write("  False positives are a concern - threshold may be too loose.\n")
            if best_metrics["recall"] < 0.90:
                f.write("  Missed matches are a concern - threshold may be too strict.\n")
        else:
            f.write("[FAIL] POOR: Both metrics need improvement.\n")
            f.write("  Consider additional matching signals or embedding-based approach.\n")

        f.write(f"\nTiming analysis:\n")
        for threshold in sorted(thresholds):
            m = metrics_by_threshold[threshold]
            est_full_time = m["avg_time_ms"] * 7800  # Rough estimate for full BNF corpus
            est_full_time_sec = est_full_time / 1000
            f.write(f"  @ {threshold}: {m['avg_time_ms']:.1f}ms per record -> ~{est_full_time_sec:.0f}s for {len(bnf_records)} records\n")

    print("Done!")


if __name__ == "__main__":
    thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]
    results = run_searches(thresholds)
    write_results(results)
