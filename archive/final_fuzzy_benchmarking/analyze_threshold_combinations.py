"""
Analyze threshold combinations for author and title matching.

Loads separate author and title matching results CSVs and combines them:
- Stage 1 author matching returns author URIs
- Stage 2 title matching returns book URIs
- Stage 3 combined: books where BOTH author matches AND title matches

For each book, check if its author_uri appears in matched author URIs.
"""

import csv
from pathlib import Path


def load_matching_csv(csv_path: str) -> dict:
    """
    Load matching results CSV.
    Returns: {(bnf_id, expected_uri, threshold): (matched_list, is_correct)}
    """
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


def combine_results_at_thresholds(
    author_results: dict, title_results: dict,
    openiti_books: dict,
    title_threshold: float, author_threshold: float
) -> tuple[int, int, int]:
    """
    Combine author and title results at given thresholds.
    Stage 3: book must have both author match AND title match.
    Returns: (correct_count, total_count, total_false_pos)
    """
    correct_count = 0
    total_count = 0
    total_false_pos = 0

    # Get all unique records
    all_records = set()
    for (bnf_id, expected_uri, thresh) in author_results.keys():
        if thresh == author_threshold:
            all_records.add((bnf_id, expected_uri))

    # For each record, combine at this threshold combination
    for bnf_id, expected_uri in all_records:
        total_count += 1

        # Get matched author URIs
        author_key = (bnf_id, expected_uri, author_threshold)
        matched_authors, _ = author_results.get(author_key, (set(), False))

        # Get matched book URIs
        title_key = (bnf_id, expected_uri, title_threshold)
        matched_books, _ = title_results.get(title_key, (set(), False))

        # Stage 3: intersection - books whose authors matched AND whose titles matched
        combined_uris = set()
        for book_uri in matched_books:
            book = openiti_books.get(book_uri)
            if book:
                author_uri = book.get("author_uri")
                if author_uri in matched_authors:
                    combined_uris.add(book_uri)

        # Check if expected URI is in combined results
        is_correct_combined = expected_uri in combined_uris
        if is_correct_combined:
            correct_count += 1

        # False positives in combined
        combined_false_pos = len(combined_uris) - (1 if is_correct_combined else 0)
        total_false_pos += combined_false_pos

    recall = correct_count / total_count if total_count > 0 else 0
    precision = 0.0

    # Calculate total precision across all combinations
    total_combined_matches = 0
    for (bnf_id, expected_uri, t_thresh), (matched_books, _) in title_results.items():
        if t_thresh == title_threshold:
            author_key = (bnf_id, expected_uri, author_threshold)
            matched_authors, _ = author_results.get(author_key, (set(), False))

            for book_uri in matched_books:
                book = openiti_books.get(book_uri)
                if book and book.get("author_uri") in matched_authors:
                    total_combined_matches += 1

    correct_matches = total_combined_matches - total_false_pos
    precision = correct_matches / total_combined_matches if total_combined_matches > 0 else 0

    return correct_count, total_count, total_false_pos, precision


def main():
    """Analyze all threshold combinations."""
    import json

    print("Loading data...")
    with open("data/openiti_corpus_2025_1_9.json", encoding="utf-8") as f:
        openiti_data = json.load(f)
        openiti_books = openiti_data["books"]

    print("Loading matching results...")
    author_results = load_matching_csv("matching/matching_results_author.csv")
    title_results = load_matching_csv("matching/matching_results_title.csv")

    # Get unique thresholds
    author_thresholds = sorted(set(thresh for (_, _, thresh) in author_results.keys()))
    title_thresholds = sorted(set(thresh for (_, _, thresh) in title_results.keys()))

    print(f"Author thresholds: {author_thresholds}")
    print(f"Title thresholds: {title_thresholds}")

    # Test all combinations
    results = []
    print(f"\nTesting {len(title_thresholds) * len(author_thresholds)} combinations...\n")

    for title_thresh in title_thresholds:
        for author_thresh in author_thresholds:
            correct, total, false_pos, precision = combine_results_at_thresholds(
                author_results, title_results, openiti_books,
                title_thresh, author_thresh
            )
            recall = correct / total if total > 0 else 0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

            results.append({
                "title_threshold": title_thresh,
                "author_threshold": author_thresh,
                "recall": recall,
                "precision": precision,
                "f1": f1,
                "correct": correct,
                "total": total,
                "false_pos": false_pos,
            })

            print(
                f"title={title_thresh:.2f} author={author_thresh:.2f}: "
                f"recall={recall*100:.0f}% precision={precision*100:.1f}% f1={f1:.3f} fp={false_pos}"
            )

    # Write results to CSV
    import pandas as pd
    output_csv = Path("matching/threshold_combination_analysis.csv")
    print(f"\nWriting results to {output_csv}...")
    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False, encoding="utf-8")

    # Print best combinations
    print("\n" + "="*80)
    print("BEST COMBINATIONS")
    print("="*80)

    sorted_by_f1 = sorted(results, key=lambda r: r["f1"], reverse=True)
    print("\nBy F1-Score:")
    for r in sorted_by_f1[:3]:
        print(
            f"  title={r['title_threshold']:.2f} author={r['author_threshold']:.2f}: "
            f"F1={r['f1']:.3f} recall={r['recall']*100:.0f}% precision={r['precision']*100:.1f}% fp={r['false_pos']}"
        )

    sorted_by_recall = sorted(results, key=lambda r: r["recall"], reverse=True)
    print("\nBy Recall (best recall):")
    for r in sorted_by_recall[:3]:
        print(
            f"  title={r['title_threshold']:.2f} author={r['author_threshold']:.2f}: "
            f"recall={r['recall']*100:.0f}% precision={r['precision']*100:.1f}% F1={r['f1']:.3f} fp={r['false_pos']}"
        )

    sorted_by_precision = sorted(results, key=lambda r: r["precision"], reverse=True)
    print("\nBy Precision (best precision):")
    for r in sorted_by_precision[:3]:
        print(
            f"  title={r['title_threshold']:.2f} author={r['author_threshold']:.2f}: "
            f"precision={r['precision']*100:.1f}% recall={r['recall']*100:.0f}% F1={r['f1']:.3f} fp={r['false_pos']}"
        )


if __name__ == "__main__":
    main()
