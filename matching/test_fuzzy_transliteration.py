"""
Test suite for fuzzy matching on transliterated Arabic/Latin titles.

This script validates whether fuzzywuzzy alone can handle transliteration
variation (diacritics, word order, hyphenation) or if we need explicit
transliteration normalization.

It tests against known correspondences from data_samplers/correspondence.json
plus synthetic test cases covering edge cases.

Usage:
    python matching/test_fuzzy_transliteration.py

Output:
    matching/fuzzy_test_results.csv — detailed test results
    matching/fuzzy_test_report.txt — summary and recommendations
"""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fuzzywuzzy import fuzz

# Load OpenITI and BNF data
openiti_path = Path("data/openiti_corpus_2025_1_9.json")
bnf_path = Path("outputs/bnf_parsed.json")

if not openiti_path.exists():
    print(f"Error: {openiti_path} not found. Run parse_openiti.py first.")
    sys.exit(1)

if not bnf_path.exists():
    print(f"Error: {bnf_path} not found. Run parse_bnf.py first.")
    sys.exit(1)

print("Loading OpenITI corpus...")
with open(openiti_path, encoding="utf-8") as f:
    openiti_data = json.load(f)
    openiti_books = openiti_data["books"]

print("Loading BNF records...")
with open(bnf_path, encoding="utf-8") as f:
    bnf_data = json.load(f)
    bnf_records = bnf_data["records"]

# Load known correspondences
print("Loading test correspondences...")
with open("data_samplers/correspondence.json", encoding="utf-8") as f:
    correspondences = json.load(f)

# Flatten correspondences (they're dicts)
test_pairs = []
for item in correspondences:
    for openiti_uri, bnf_id in item.items():
        test_pairs.append((openiti_uri, bnf_id))


@dataclass
class TestCase:
    """A single fuzzy matching test case."""
    name: str
    query_string: str  # What we're searching for (BNF title/creator)
    target_string: str  # What we're searching in (OpenITI title/creator)
    expected_match: bool = True  # Should this match?
    category: str = "unknown"  # known_correspondence, diacritic, word_order, hyphenation, etc.


def extract_titles_and_creators(openiti_uri: str, bnf_id: str) -> tuple[list[str], list[str]]:
    """Extract all titles and creators for comparison."""
    openiti_titles = []
    openiti_creators = []

    book = openiti_books.get(openiti_uri)
    if book:
        if book.get("title_lat"):
            openiti_titles.append(book["title_lat"])
        if book.get("title_ara"):
            openiti_titles.append(book["title_ara"])

    bnf_record = bnf_records.get(bnf_id)
    if bnf_record:
        if bnf_record.get("title_lat"):
            bnf_titles = bnf_record["title_lat"]
            if isinstance(bnf_titles, list):
                openiti_titles.extend(bnf_titles)
            else:
                openiti_titles.append(bnf_titles)

    return openiti_titles, openiti_creators


def build_test_cases() -> list[TestCase]:
    """Build test cases from known correspondences and synthetic variations."""
    cases = []

    # 1. Real known correspondences
    print("Building test cases from known correspondences...")
    for openiti_uri, bnf_id in test_pairs:
        book = openiti_books.get(openiti_uri)
        record = bnf_records.get(bnf_id)

        if not book or not record:
            print(f"  Skipping {openiti_uri}/{bnf_id}: data not found")
            continue

        # Test title matches
        if book.get("title_lat") and record.get("title_lat"):
            bnf_titles = record["title_lat"]
            if not isinstance(bnf_titles, list):
                bnf_titles = [bnf_titles]
            for bnf_title in bnf_titles:
                cases.append(
                    TestCase(
                        name=f"{bnf_id} → {openiti_uri} (Latin title)",
                        query_string=bnf_title,
                        target_string=book["title_lat"],
                        expected_match=True,
                        category="known_correspondence_lat",
                    )
                )

        if book.get("title_ara") and record.get("title_ara"):
            bnf_titles_ara = record["title_ara"]
            if not isinstance(bnf_titles_ara, list):
                bnf_titles_ara = [bnf_titles_ara]
            for bnf_title_ara in bnf_titles_ara:
                cases.append(
                    TestCase(
                        name=f"{bnf_id} → {openiti_uri} (Arabic title)",
                        query_string=bnf_title_ara,
                        target_string=book["title_ara"],
                        expected_match=True,
                        category="known_correspondence_ara",
                    )
                )

    # 2. Synthetic test cases for edge cases
    print("Building synthetic test cases...")
    synthetic_cases = [
        # Diacritics
        TestCase(
            name="Diacritic: ā vs a",
            query_string="Kitab al-Tabari",
            target_string="Kitab al-Ṭabarī",
            expected_match=True,
            category="diacritic_macron",
        ),
        TestCase(
            name="Diacritic: ṣ vs s",
            query_string="Sacd al-Suud",
            target_string="Ṣaʿd al-Suʿud",
            expected_match=True,
            category="diacritic_underscore",
        ),
        # Word order
        TestCase(
            name="Word order: preserved",
            query_string="Anwar al-Tanzil wa-Asrar al-Tawil",
            target_string="Anwar al-Tanzil wa-Asrar al-Tawil",
            expected_match=True,
            category="word_order_same",
        ),
        TestCase(
            name="Word order: reversed subtitle",
            query_string="Anwar al-Tanzil wa-Asrar al-Tawil",
            target_string="Asrar al-Tawil wa-Anwar al-Tanzil",
            expected_match=False,  # Fuzzy won't fix this; needs normalization
            category="word_order_different",
        ),
        # Hyphenation
        TestCase(
            name="Hyphenation: wa- vs wa ",
            query_string="Kitab wa Risala",
            target_string="Kitab wa-Risala",
            expected_match=True,
            category="hyphenation",
        ),
        TestCase(
            name="Hyphenation: al- vs al ",
            query_string="al Fihrist",
            target_string="al-Fihrist",
            expected_match=True,
            category="hyphenation_article",
        ),
        # Case variations
        TestCase(
            name="Case: capitalization",
            query_string="AL-TABARI",
            target_string="al-Tabari",
            expected_match=True,
            category="case_variation",
        ),
        # Transliteration variants (ʿayn)
        TestCase(
            name="Ayn variant: C vs ʿ",
            query_string="Cabd al-Rahman",
            target_string="ʿAbd al-Rahman",
            expected_match=False,  # C and ʿ are different; fuzzy won't match
            category="ayn_variant",
        ),
        # Substring
        TestCase(
            name="Substring match",
            query_string="Fihrist",
            target_string="al-Fihrist al-Nadim",
            expected_match=True,
            category="substring",
        ),
    ]
    cases.extend(synthetic_cases)

    return cases


def run_tests(thresholds: list[float] = None) -> dict:
    """Run fuzzy matching tests across multiple thresholds."""
    if thresholds is None:
        thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]

    cases = build_test_cases()
    results = []

    print(f"\nRunning {len(cases)} test cases across {len(thresholds)} thresholds...")

    for case in cases:
        # Use token_set_ratio to handle word order variations better
        score_simple = fuzz.ratio(case.query_string, case.target_string)
        score_partial = fuzz.partial_ratio(case.query_string, case.target_string)
        score_token_sort = fuzz.token_sort_ratio(case.query_string, case.target_string)
        score_token_set = fuzz.token_set_ratio(case.query_string, case.target_string)

        result = {
            "test_case": case.name,
            "category": case.category,
            "query": case.query_string,
            "target": case.target_string,
            "expected_match": case.expected_match,
            "score_ratio": score_simple,
            "score_partial": score_partial,
            "score_token_sort": score_token_sort,
            "score_token_set": score_token_set,
        }

        # Add threshold-based results
        for threshold in thresholds:
            # Use token_set_ratio as primary matcher (most forgiving)
            matched = score_token_set >= threshold
            result[f"match_{threshold}"] = matched
            result[f"correct_{threshold}"] = matched == case.expected_match

        results.append(result)

    return results


def write_results(results: dict, output_dir: Path = Path("matching"), thresholds: list[float] = None) -> None:
    """Write test results to CSV and generate report."""
    if thresholds is None:
        thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]

    output_dir.mkdir(exist_ok=True)

    csv_path = output_dir / "fuzzy_test_results.csv"
    report_path = output_dir / "fuzzy_test_report.txt"

    # Write CSV
    print(f"Writing results to {csv_path}...")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "test_case",
            "category",
            "query",
            "target",
            "expected_match",
            "score_ratio",
            "score_partial",
            "score_token_sort",
            "score_token_set",
        ]
        fieldnames.extend([f"match_{t}" for t in thresholds])
        fieldnames.extend([f"correct_{t}" for t in thresholds])

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # Generate report
    print(f"Generating report to {report_path}...")
    thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]
    accuracy_by_threshold = {t: 0 for t in thresholds}
    accuracy_by_category = {}

    for result in results:
        for t in thresholds:
            if result[f"correct_{t}"]:
                accuracy_by_threshold[t] += 1

        category = result["category"]
        if category not in accuracy_by_category:
            accuracy_by_category[category] = {t: 0 for t in thresholds}
        for t in thresholds:
            if result[f"correct_{t}"]:
                accuracy_by_category[category][t] += 1

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("FUZZY MATCHING TRANSLITERATION TEST REPORT\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Test cases: {len(results)}\n")
        f.write(f"Categories: {', '.join(sorted(accuracy_by_category.keys()))}\n\n")

        f.write("ACCURACY BY THRESHOLD (token_set_ratio)\n")
        f.write("-" * 70 + "\n")
        for t in thresholds:
            correct = accuracy_by_threshold[t]
            pct = 100 * correct / len(results)
            f.write(f"  {t}: {correct}/{len(results)} ({pct:.1f}%)\n")

        f.write("\n\nACCURACY BY CATEGORY\n")
        f.write("-" * 70 + "\n")
        for category in sorted(accuracy_by_category.keys()):
            f.write(f"\n{category}:\n")
            for t in thresholds:
                correct = accuracy_by_category[category][t]
                total = sum(1 for r in results if r["category"] == category)
                pct = 100 * correct / total if total > 0 else 0
                f.write(f"  {t}: {correct}/{total} ({pct:.1f}%)\n")

        f.write("\n\nRECOMMENDATIONS\n")
        f.write("-" * 70 + "\n")

        # Analyze results
        issues = []
        for result in results:
            if not result["expected_match"] and result["score_token_set"] >= 0.75:
                issues.append(f"False positive: {result['test_case']} ({result['score_token_set']:.2f})")
            elif result["expected_match"] and result["score_token_set"] < 0.75:
                issues.append(f"False negative: {result['test_case']} ({result['score_token_set']:.2f})")

        if issues:
            f.write("\nISSUES FOUND:\n")
            for issue in issues:
                f.write(f"  • {issue}\n")

            f.write("\nRECOMMENDATION: Fuzzy matching alone may be insufficient.\n")
            f.write("Consider implementing transliteration normalization for:\n")
            for category in accuracy_by_category:
                max_acc = max(accuracy_by_category[category].values())
                total = sum(1 for r in results if r["category"] == category)
                if max_acc < total:
                    f.write(f"  • {category}\n")
        else:
            f.write("\nNo major issues found.\n")
            f.write("RECOMMENDATION: Fuzzy matching (token_set_ratio) is sufficient.\n")
            f.write("Proceed with surface matcher using threshold 0.80–0.85.\n")

    print(f"\nDone. Results saved to {csv_path} and {report_path}")


if __name__ == "__main__":
    thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]
    results = run_tests(thresholds)
    write_results(results, thresholds=thresholds)
