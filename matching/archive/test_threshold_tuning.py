"""
Threshold tuning test: find optimal combination of title and author thresholds.

Tests combinations where:
- Title threshold (looser): controls recall on title matching
- Author threshold (stricter): controls precision on author matching
- Combined matching requires both to exceed their respective thresholds

Produces CSV showing performance at each combination.
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

print(f"  {len(openiti_books)} books loaded, {len(openiti_authors)} authors")

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

print(f"  {len(test_pairs)} test records\n")


@dataclass
class TuningResult:
    """Result of testing a threshold combination."""
    title_threshold: float
    author_threshold: float
    recall: float
    precision: float
    f1: float
    correct: int
    total: int
    total_false_pos: int
    avg_time_ms: float


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
    """Extract author candidates from BNF record."""
    candidates = {"lat": [], "ara": []}

    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)

    for creator in bnf_record.get("creator_ara", []):
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)

    for contrib in bnf_record.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)

    for contrib in bnf_record.get("contributor_ara", []):
        if contrib and contrib not in candidates["ara"]:
            candidates["ara"].append(contrib)

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
    """Extract all author name variants from OpenITI author record."""
    candidates = {"lat": [], "ara": []}

    author = openiti_authors.get(author_uri)
    if not author:
        return candidates

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

    for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
        if author.get(field):
            candidates["lat"].append(author[field])

    if author.get("wd_label_ar"):
        candidates["ara"].append(author["wd_label_ar"])

    if author.get("wd_aliases_ar"):
        aliases = author["wd_aliases_ar"]
        if isinstance(aliases, list):
            candidates["ara"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["ara"].append(aliases)

    return candidates


def search_with_thresholds(
    bnf_id: str, expected_uris: list[str],
    title_threshold: float, author_threshold: float
) -> tuple[bool, int, int, float]:
    """
    Search one BNF record with separate title/author thresholds.
    Returns: (found_correct, total_matches, false_positives, time_ms)
    """
    bnf_record = bnf_records.get(bnf_id)
    if not bnf_record:
        return False, 0, 0, 0.0

    bnf_titles = build_bnf_title_candidates(bnf_record)
    bnf_authors = build_bnf_author_candidates(bnf_record)

    if not bnf_titles.get("lat") and not bnf_titles.get("ara"):
        if not bnf_authors.get("lat") and not bnf_authors.get("ara"):
            return False, 0, 0, 0.0

    # Normalize
    bnf_titles_norm = {
        "lat": [normalize_transliteration(c) for c in bnf_titles.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in bnf_titles.get("ara", [])],
    }
    bnf_authors_norm = {
        "lat": [normalize_transliteration(c) for c in bnf_authors.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in bnf_authors.get("ara", [])],
    }

    matched_combined = []
    start_time = time.time()

    # Search against all OpenITI books
    for openiti_uri, openiti_book in openiti_books.items():
        openiti_titles = build_openiti_title_candidates(openiti_book)
        openiti_titles_norm = {
            "lat": [normalize_transliteration(c) for c in openiti_titles.get("lat", [])],
            "ara": [normalize_transliteration(c) for c in openiti_titles.get("ara", [])],
        }

        # Check title match at title_threshold
        title_matched = False
        for script in ["lat", "ara"]:
            if not bnf_titles_norm.get(script) or not openiti_titles_norm.get(script):
                continue
            for bnf_str in bnf_titles_norm[script]:
                for openiti_str in openiti_titles_norm[script]:
                    score = fuzz.token_set_ratio(bnf_str, openiti_str)
                    if score >= title_threshold * 100:
                        title_matched = True
                        break

        # Check author match at author_threshold
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
                        if score >= author_threshold * 100:
                            author_matched = True
                            break

        # Combined match requires both
        if title_matched and author_matched:
            matched_combined.append(openiti_uri)

    elapsed_ms = (time.time() - start_time) * 1000

    correct = any(uri in matched_combined for uri in expected_uris)
    false_pos = len(matched_combined) - sum(1 for uri in matched_combined if uri in expected_uris)

    return correct, len(matched_combined), false_pos, elapsed_ms


def run_tuning():
    """Test different threshold combinations."""
    results = []

    # Test combinations: title_threshold (looser) and author_threshold (stricter)
    title_thresholds = [0.65, 0.70, 0.75, 0.80]
    author_thresholds = [0.75, 0.80, 0.85, 0.90]

    print(f"Testing {len(title_thresholds) * len(author_thresholds)} threshold combinations...\n")

    for title_threshold in title_thresholds:
        for author_threshold in author_thresholds:
            correct_count = 0
            total_matches = 0
            total_false_pos = 0
            total_time = 0

            for bnf_id, expected_uris in test_pairs.items():
                found, num_matches, false_pos, elapsed = search_with_thresholds(
                    bnf_id, expected_uris, title_threshold, author_threshold
                )
                if found:
                    correct_count += 1
                total_matches += num_matches
                total_false_pos += false_pos
                total_time += elapsed

            recall = correct_count / len(test_pairs) if test_pairs else 0
            precision = (total_matches - total_false_pos) / total_matches if total_matches > 0 else 0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            avg_time = total_time / len(test_pairs) if test_pairs else 0

            result = TuningResult(
                title_threshold=title_threshold,
                author_threshold=author_threshold,
                recall=recall,
                precision=precision,
                f1=f1,
                correct=correct_count,
                total=len(test_pairs),
                total_false_pos=total_false_pos,
                avg_time_ms=avg_time,
            )
            results.append(result)

            print(
                f"title={title_threshold} author={author_threshold}: "
                f"recall={recall*100:.0f}% precision={precision*100:.1f}% f1={f1:.3f} "
                f"fp={total_false_pos} time={avg_time:.0f}ms"
            )

    return results


def write_results(results: list[TuningResult], output_dir: Path = Path("matching")) -> None:
    """Write tuning results to CSV."""
    output_dir.mkdir(exist_ok=True)
    csv_path = output_dir / "threshold_tuning_results.csv"

    print(f"\nWriting results to {csv_path}...")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "title_threshold", "author_threshold",
            "recall", "precision", "f1",
            "correct_matches", "total_records",
            "total_false_positives", "avg_time_ms",
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "title_threshold": f"{r.title_threshold:.2f}",
                "author_threshold": f"{r.author_threshold:.2f}",
                "recall": f"{r.recall:.3f}",
                "precision": f"{r.precision:.3f}",
                "f1": f"{r.f1:.3f}",
                "correct_matches": r.correct,
                "total_records": r.total,
                "total_false_positives": r.total_false_pos,
                "avg_time_ms": f"{r.avg_time_ms:.1f}",
            })

    # Print best combinations
    print("\nBest combinations by metric:\n")
    sorted_by_f1 = sorted(results, key=lambda r: r.f1, reverse=True)
    print("By F1-Score:")
    for r in sorted_by_f1[:3]:
        print(f"  title={r.title_threshold} author={r.author_threshold}: "
              f"F1={r.f1:.3f} recall={r.recall*100:.0f}% precision={r.precision*100:.1f}%")

    sorted_by_recall = sorted(results, key=lambda r: r.recall, reverse=True)
    print("\nBy Recall:")
    for r in sorted_by_recall[:3]:
        print(f"  title={r.title_threshold} author={r.author_threshold}: "
              f"recall={r.recall*100:.0f}% precision={r.precision*100:.1f}% F1={r.f1:.3f}")

    sorted_by_precision = sorted(results, key=lambda r: r.precision, reverse=True)
    print("\nBy Precision:")
    for r in sorted_by_precision[:3]:
        print(f"  title={r.title_threshold} author={r.author_threshold}: "
              f"precision={r.precision*100:.1f}% recall={r.recall*100:.0f}% F1={r.f1:.3f}")


if __name__ == "__main__":
    results = run_tuning()
    write_results(results)
