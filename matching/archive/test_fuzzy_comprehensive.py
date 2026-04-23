"""
Comprehensive fuzzy matching test for BNF-OpenITI correspondence.

Tests real correspondence pairs from data_samplers/correspondence.json
against actual matching_data() from both sides, covering all available fields
(titles, creators, description candidates).

Handles both book-level and author-level OpenITI URIs.

Usage:
    python matching/test_fuzzy_comprehensive.py

Output:
    matching/fuzzy_comprehensive_results.csv — detailed results per pair/field
    matching/fuzzy_comprehensive_report.txt — summary and recommendations
"""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from fuzzywuzzy import fuzz

# Load data
openiti_path = Path("data/openiti_corpus_2025_1_9.json")
bnf_path = Path("outputs/bnf_parsed.json")

print("Loading OpenITI corpus...")
with open(openiti_path, encoding="utf-8") as f:
    openiti_data = json.load(f)
    openiti_books = openiti_data["books"]

print("Loading BNF records...")
with open(bnf_path, encoding="utf-8") as f:
    bnf_data = json.load(f)
    bnf_records = bnf_data["records"]

# Load correspondences
print("Loading test correspondences...")
with open("data_samplers/correspondence.json", encoding="utf-8") as f:
    correspondences = json.load(f)

test_pairs = []
for item in correspondences:
    for openiti_uri, bnf_id in item.items():
        test_pairs.append((openiti_uri, bnf_id))


@dataclass
class Match:
    """Result of a single fuzzy match attempt."""
    openiti_uri: str
    bnf_id: str
    bnf_field: str  # Which BNF field matched (title_lat, creator_lat, etc.)
    openiti_field: str  # Which OpenITI field matched
    query: str  # What we searched for
    target: str  # What we searched against
    score_simple: float
    score_token_set: float
    threshold: float
    matched: bool
    is_expected: bool = True


def get_openiti_books_for_uri(uri: str) -> list[tuple[str, dict]]:
    """
    Get OpenITI books for a URI.

    If uri is book-level (e.g., "0911Suyuti.HusnMuhadara"), return that book.
    If uri is author-level (e.g., "0911Suyuti"), return all books by that author.
    """
    # Try as book-level first
    if uri in openiti_books:
        return [(uri, openiti_books[uri])]

    # Try as author-level
    author_prefix = uri + "."
    matching_books = [
        (book_uri, book_data)
        for book_uri, book_data in openiti_books.items()
        if book_uri.startswith(author_prefix)
    ]

    if matching_books:
        return matching_books

    return []


def build_matching_candidates(bnf_record: dict) -> dict[str, list[str]]:
    """
    Extract all matching candidates from a BNF record.
    Returns {script: [candidates]} where script is 'lat' or 'ara'.
    """
    candidates = {"lat": [], "ara": []}

    # Titles
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

    # Creators
    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)

    for creator in bnf_record.get("creator_ara", []):
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)

    # Description candidates
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)

    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)

    return candidates


def build_openiti_candidates(book: dict) -> dict[str, list[str]]:
    """Extract all matching candidates from an OpenITI book."""
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


def test_correspondence_pair(
    openiti_uri: str, bnf_id: str, thresholds: list[float]
) -> list[Match]:
    """Test a single correspondence pair, returning all match attempts."""
    results = []

    # Get BNF record
    bnf_record = bnf_records.get(bnf_id)
    if not bnf_record:
        return results

    # Get BNF candidates
    bnf_cands = build_matching_candidates(bnf_record)

    # Get OpenITI books
    openiti_books_list = get_openiti_books_for_uri(openiti_uri)
    if not openiti_books_list:
        return results

    # Test against each OpenITI book
    for openiti_book_uri, openiti_book in openiti_books_list:
        openiti_cands = build_openiti_candidates(openiti_book)

        # Try script-matched comparisons (lat→lat, ara→ara)
        for script in ["lat", "ara"]:
            bnf_list = bnf_cands.get(script, [])
            openiti_list = openiti_cands.get(script, [])

            if not bnf_list or not openiti_list:
                continue

            # Find best match between lists
            best_score_simple = 0
            best_score_token_set = 0
            best_pair = None

            for bnf_str in bnf_list:
                for openiti_str in openiti_list:
                    score_simple = fuzz.ratio(bnf_str, openiti_str)
                    score_token_set = fuzz.token_set_ratio(bnf_str, openiti_str)

                    if score_token_set > best_score_token_set:
                        best_score_token_set = score_token_set
                        best_score_simple = score_simple
                        best_pair = (bnf_str, openiti_str)

            if best_pair:
                bnf_str, openiti_str = best_pair
                for threshold in thresholds:
                    matched = best_score_token_set >= threshold
                    results.append(
                        Match(
                            openiti_uri=openiti_uri,
                            bnf_id=bnf_id,
                            bnf_field=f"all_{script}",
                            openiti_field=f"all_{script}",
                            query=bnf_str,
                            target=openiti_str,
                            score_simple=best_score_simple,
                            score_token_set=best_score_token_set,
                            threshold=threshold,
                            matched=matched,
                            is_expected=True,
                        )
                    )

    return results


def run_tests(thresholds: list[float] = None) -> list[Match]:
    """Run fuzzy matching tests on all correspondence pairs."""
    if thresholds is None:
        thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]

    all_results = []

    print(f"\nTesting {len(test_pairs)} correspondence pairs...")
    for i, (openiti_uri, bnf_id) in enumerate(test_pairs, 1):
        results = test_correspondence_pair(openiti_uri, bnf_id, thresholds)
        all_results.extend(results)
        if results:
            print(f"  [{i}/{len(test_pairs)}] {bnf_id} -> {openiti_uri}: {len(results)} attempts")
        else:
            print(f"  [{i}/{len(test_pairs)}] {bnf_id} -> {openiti_uri}: NO DATA")

    return all_results


def write_results(results: list[Match], output_dir: Path = Path("matching"), thresholds: list[float] = None) -> None:
    """Write test results to CSV and generate report."""
    if thresholds is None:
        thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]

    output_dir.mkdir(exist_ok=True)

    csv_path = output_dir / "fuzzy_comprehensive_results.csv"
    report_path = output_dir / "fuzzy_comprehensive_report.txt"

    # Write CSV
    print(f"\nWriting results to {csv_path}...")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "openiti_uri",
            "bnf_id",
            "bnf_field",
            "openiti_field",
            "query",
            "target",
            "score_simple",
            "score_token_set",
        ]
        fieldnames.extend([f"match_{t}" for t in thresholds])
        fieldnames.append("is_expected")

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for result in results:
            row = {
                "openiti_uri": result.openiti_uri,
                "bnf_id": result.bnf_id,
                "bnf_field": result.bnf_field,
                "openiti_field": result.openiti_field,
                "query": result.query,
                "target": result.target,
                "score_simple": f"{result.score_simple:.2f}",
                "score_token_set": f"{result.score_token_set:.2f}",
                "is_expected": result.is_expected,
            }
            for t in thresholds:
                row[f"match_{t}"] = result.matched if result.threshold == t else ""

            writer.writerow(row)

    # Generate report
    print(f"Generating report to {report_path}...")

    if not results:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("NO TEST RESULTS\n")
            f.write("="*70 + "\n\n")
            f.write("No matching data was found in the datasets.\n")
            f.write("Check that:\n")
            f.write("  1. BNF records have been parsed (outputs/bnf_parsed.json)\n")
            f.write("  2. OpenITI corpus has been loaded (data/openiti_corpus_2025_1_9.json)\n")
            f.write("  3. Correspondence pairs exist in data_samplers/correspondence.json\n")
        return

    # Calculate success rates by threshold
    success_by_threshold = {t: 0 for t in thresholds}
    for result in results:
        if result.matched:
            success_by_threshold[result.threshold] += 1

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("COMPREHENSIVE FUZZY MATCHING TEST REPORT\n")
        f.write("="*70 + "\n\n")

        f.write(f"Test pairs: {len(test_pairs)}\n")
        f.write(f"Match attempts: {len(results)}\n")
        f.write(f"Unique BNF-OpenITI comparisons: {len(set((r.bnf_id, r.openiti_uri) for r in results))}\n\n")

        f.write("SUCCESS RATE BY THRESHOLD (token_set_ratio)\n")
        f.write("-"*70 + "\n")
        for t in thresholds:
            attempts = sum(1 for r in results if r.threshold == t)
            successes = success_by_threshold[t]
            if attempts > 0:
                pct = 100 * successes / attempts
                f.write(f"  {t}: {successes}/{attempts} ({pct:.1f}%)\n")

        f.write("\nFIELDS TESTED\n")
        f.write("-"*70 + "\n")
        fields = set((r.bnf_field, r.openiti_field) for r in results)
        for bnf_field, openiti_field in sorted(fields):
            count = sum(1 for r in results if r.bnf_field == bnf_field and r.openiti_field == openiti_field)
            f.write(f"  {bnf_field} → {openiti_field}: {count} attempts\n")

        f.write("\nRECOMMENDATION\n")
        f.write("-"*70 + "\n")
        overall_success_rate = sum(1 for r in results if r.matched) / len(results) * 100 if results else 0
        f.write(f"Overall success rate at 0.80 threshold: {overall_success_rate:.1f}%\n\n")

        if overall_success_rate >= 80:
            f.write("✓ Fuzzy matching is performing well.\n")
            f.write("Proceed with surface matching using threshold 0.80–0.85.\n")
        elif overall_success_rate >= 60:
            f.write("⚠ Fuzzy matching has moderate performance.\n")
            f.write("Consider adjusting threshold or adding transliteration normalization.\n")
        else:
            f.write("✗ Fuzzy matching performance is low.\n")
            f.write("Transliteration normalization is likely needed.\n")

    print("Done!")


if __name__ == "__main__":
    thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]
    results = run_tests(thresholds)
    write_results(results, thresholds=thresholds)
