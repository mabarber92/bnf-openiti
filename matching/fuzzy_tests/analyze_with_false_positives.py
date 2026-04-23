"""
Analyze threshold combinations with:
1. Metrics excluding author-only cases
2. False positive match lists for inspection
3. Separate reporting of author-only cases
"""

import csv
import json
from pathlib import Path
import pandas as pd


def load_matching_csv(csv_path: str) -> dict:
    """Load matching results CSV."""
    results = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bnf_id = row["bnf_id"]
            expected_uri = row["expected_uri"]
            threshold = float(row["threshold"])
            matched = set(row["matched_uris"].split("|")) if row["matched_uris"] else set()
            is_correct = row["is_correct"].lower() == "true"

            key = (bnf_id, expected_uri, threshold)
            results[key] = (matched, is_correct)

    return results


def analyze_combination(
    author_results: dict, title_results: dict, openiti_books: dict,
    title_threshold: float, author_threshold: float
) -> dict:
    """
    Analyze a specific threshold combination.
    Returns comprehensive metrics and false positive lists.
    """
    results = {
        "title_threshold": title_threshold,
        "author_threshold": author_threshold,
        "combined_correct": 0,
        "combined_total": 0,
        "combined_false_pos": 0,
        "author_only_cases": [],
        "false_positive_matches": [],  # [(bnf_id, wrong_book_uri), ...]
    }

    # Get all unique records
    all_records = set()
    for (bnf_id, expected_uri, thresh) in author_results.keys():
        if thresh == author_threshold:
            all_records.add((bnf_id, expected_uri))

    # Analyze each record
    for bnf_id, expected_uri in all_records:
        author_key = (bnf_id, expected_uri, author_threshold)
        title_key = (bnf_id, expected_uri, title_threshold)

        matched_authors, _ = author_results.get(author_key, (set(), False))
        matched_books, _ = title_results.get(title_key, (set(), False))

        # Case 1: Author-only (no title match)
        if matched_authors and not matched_books:
            results["author_only_cases"].append({
                "bnf_id": bnf_id,
                "expected_uri": expected_uri,
                "matched_authors": list(matched_authors),
            })
            continue

        # Case 2: Combined matching (both author and title)
        results["combined_total"] += 1

        combined_uris = set()
        for book_uri in matched_books:
            book = openiti_books.get(book_uri)
            if book and book.get("author_uri") in matched_authors:
                combined_uris.add(book_uri)

        is_correct = expected_uri in combined_uris
        if is_correct:
            results["combined_correct"] += 1

        # Track false positives
        false_pos_count = len(combined_uris) - (1 if is_correct else 0)
        results["combined_false_pos"] += false_pos_count

        for book_uri in combined_uris:
            if book_uri != expected_uri:
                results["false_positive_matches"].append({
                    "bnf_id": bnf_id,
                    "expected_uri": expected_uri,
                    "wrong_match_uri": book_uri,
                })

    # Calculate metrics
    if results["combined_total"] > 0:
        results["combined_recall"] = results["combined_correct"] / results["combined_total"]
        # Precision: correct matches / total matched
        total_combined_matches = len(results["false_positive_matches"]) + results["combined_correct"]
        results["combined_precision"] = (
            results["combined_correct"] / total_combined_matches
            if total_combined_matches > 0 else 0
        )
    else:
        results["combined_recall"] = 0
        results["combined_precision"] = 0

    # F1
    p = results["combined_precision"]
    r = results["combined_recall"]
    results["combined_f1"] = (
        2 * (p * r) / (p + r) if (p + r) > 0 else 0
    )

    return results


def get_false_positive_details(
    author_results: dict, title_results: dict, openiti_books: dict,
    openiti_authors: dict,
    title_threshold: float, author_threshold: float
) -> pd.DataFrame:
    """
    Create detailed CSV of false positive matches for a parameter combination.
    """
    analysis = analyze_combination(
        author_results, title_results, openiti_books,
        title_threshold, author_threshold
    )

    fp_data = []
    for fp in analysis["false_positive_matches"]:
        wrong_book = openiti_books.get(fp["wrong_match_uri"], {})
        wrong_author = openiti_authors.get(wrong_book.get("author_uri", ""), {})

        fp_data.append({
            "bnf_id": fp["bnf_id"],
            "expected_uri": fp["expected_uri"],
            "wrong_match_uri": fp["wrong_match_uri"],
            "wrong_author_uri": wrong_book.get("author_uri", ""),
            "wrong_author_name": wrong_author.get("name_slug", ""),
            "wrong_book_title_lat": wrong_book.get("title_lat", ""),
            "wrong_book_title_ara": wrong_book.get("title_ara", ""),
        })

    return pd.DataFrame(fp_data)


def main():
    """Run comprehensive analysis with false positive details."""
    print("Loading data...")
    with open("data/openiti_corpus_2025_1_9.json", encoding="utf-8") as f:
        openiti_data = json.load(f)
        openiti_books = openiti_data["books"]
        openiti_authors = openiti_data["authors"]

    print("Loading matching results...")
    author_results = load_matching_csv("matching/matching_results_author.csv")
    title_results = load_matching_csv("matching/matching_results_title.csv")

    author_thresholds = sorted(set(thresh for (_, _, thresh) in author_results.keys()))
    title_thresholds = sorted(set(thresh for (_, _, thresh) in title_results.keys()))

    print(f"\nTesting {len(title_thresholds) * len(author_thresholds)} combinations...\n")

    results = []
    for title_thresh in title_thresholds:
        for author_thresh in author_thresholds:
            analysis = analyze_combination(
                author_results, title_results, openiti_books,
                title_thresh, author_thresh
            )

            results.append({
                "title_threshold": title_thresh,
                "author_threshold": author_thresh,
                "combined_recall": analysis["combined_recall"],
                "combined_precision": analysis["combined_precision"],
                "combined_f1": analysis["combined_f1"],
                "combined_correct": analysis["combined_correct"],
                "combined_total": analysis["combined_total"],
                "combined_false_pos": analysis["combined_false_pos"],
                "author_only_cases": len(analysis["author_only_cases"]),
            })

            print(
                f"title={title_thresh:.2f} author={author_thresh:.2f}: "
                f"combined(recall={analysis['combined_recall']*100:.0f}% precision={analysis['combined_precision']*100:.1f}% "
                f"f1={analysis['combined_f1']:.3f}) | "
                f"author_only_cases={len(analysis['author_only_cases'])} | "
                f"combined_fp={analysis['combined_false_pos']}"
            )

    # Write summary
    summary_csv = Path("matching/threshold_analysis_excluding_author_only.csv")
    print(f"\nWriting summary to {summary_csv}...")
    summary_df = pd.DataFrame(results)
    summary_df.to_csv(summary_csv, index=False, encoding="utf-8")

    # Print best combinations (by combined F1 only, excluding author-only cases)
    print("\n" + "="*80)
    print("BEST COMBINATIONS (excluding author-only cases from metric)")
    print("="*80)

    sorted_by_f1 = sorted(results, key=lambda r: r["combined_f1"], reverse=True)
    print("\nBy Combined F1-Score:")
    for r in sorted_by_f1[:5]:
        print(
            f"  title={r['title_threshold']:.2f} author={r['author_threshold']:.2f}: "
            f"F1={r['combined_f1']:.3f} recall={r['combined_recall']*100:.0f}% "
            f"precision={r['combined_precision']*100:.1f}% "
            f"combined_fp={r['combined_false_pos']} author_only={r['author_only_cases']}"
        )

    # Generate false positive CSVs for best combinations
    print("\n" + "="*80)
    print("FALSE POSITIVE MATCH DETAILS")
    print("="*80)

    for r in sorted_by_f1[:3]:
        fp_csv = Path(f"matching/false_positives_title{r['title_threshold']:.2f}_author{r['author_threshold']:.2f}.csv")
        fp_df = get_false_positive_details(
            author_results, title_results, openiti_books, openiti_authors,
            r["title_threshold"], r["author_threshold"]
        )

        if len(fp_df) > 0:
            fp_df.to_csv(fp_csv, index=False, encoding="utf-8")
            print(f"\nWrote {len(fp_df)} false positives to {fp_csv.name}")
        else:
            print(f"\nNo false positives for title={r['title_threshold']:.2f} author={r['author_threshold']:.2f}")


if __name__ == "__main__":
    main()
