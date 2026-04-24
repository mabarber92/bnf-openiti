"""
Surface-form fuzzy matching test for BNF-OpenITI correspondence.

Three-stage architecture:
1. Author matching: BNF all fields → OpenITI author URIs
2. Title matching: BNF titles → OpenITI book URIs
3. Combined: Author URIs → their books → check title overlap

Uses normalized fuzzy matching with separate thresholds for each stage.
Outputs separate CSVs for reproducibility and threshold tuning analysis.
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fuzzywuzzy import fuzz
from matching.normalize import normalize_transliteration

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("Warning: pandas not available, using CSV fallback")

# Load data
openiti_path = Path("data/openiti_corpus_2025_1_9.json")
bnf_path = Path("outputs/bnf_parsed.json")

print("Loading OpenITI corpus...")
with open(openiti_path, encoding="utf-8") as f:
    openiti_data = json.load(f)
    openiti_books = openiti_data["books"]
    openiti_authors = openiti_data["authors"]

print(f"  {len(openiti_books)} books, {len(openiti_authors)} authors")

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
class MatchingResult:
    """Result from one matching test."""
    bnf_id: str
    expected_uri: str
    threshold: float
    stage: str  # "title" or "author"
    matched_uris: list[str]
    is_correct: bool
    num_matches: int
    false_positives: int
    search_time_ms: float


def build_bnf_author_candidates(bnf_record: dict) -> dict[str, list[str]]:
    """
    Extract author candidates from BNF record (Stage 1).
    Uses ALL fields: creators + titles + description_candidates
    (author info appears across multiple fields in BNF data)
    """
    candidates = {"lat": [], "ara": []}

    # Creator fields (primary)
    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)

    for creator in bnf_record.get("creator_ara", []):
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)

    # Contributor fields
    for contrib in bnf_record.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)

    for contrib in bnf_record.get("contributor_ara", []):
        if contrib and contrib not in candidates["ara"]:
            candidates["ara"].append(contrib)

    # Titles (sometimes contain author names)
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

    # Description candidates
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)

    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)

    return candidates


def build_bnf_title_candidates(bnf_record: dict) -> dict[str, list[str]]:
    """
    Extract title candidates from BNF record (Stage 2).
    """
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


def build_openiti_author_candidates(author_uri: str) -> dict[str, list[str]]:
    """
    Extract all author name variants from OpenITI author record.
    Name components are now stored in _lat and _ara fields separately.
    """
    candidates = {"lat": [], "ara": []}

    author = openiti_authors.get(author_uri)
    if not author:
        return candidates

    # Transliterated variants
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

    # Structured name components (Latin transliteration)
    for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
        if author.get(field):
            candidates["lat"].append(author[field])

    # Structured name components (Arabic script)
    for field in ["name_shuhra_ara", "name_ism_ara", "name_kunya_ara", "name_laqab_ara", "name_nasab_ara", "name_nisba_ara"]:
        if author.get(field):
            candidates["ara"].append(author[field])

    # Arabic variants from Wikidata
    if author.get("wd_label_ar"):
        candidates["ara"].append(author["wd_label_ar"])

    if author.get("wd_aliases_ar"):
        aliases = author["wd_aliases_ar"]
        if isinstance(aliases, list):
            candidates["ara"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["ara"].append(aliases)

    return candidates


def build_openiti_title_candidates(book: dict) -> dict[str, list[str]]:
    """
    Extract title candidates from OpenITI book.
    Title fields are now pre-split lists (from TSV separator splitting).
    """
    candidates = {"lat": [], "ara": []}

    # Handle title_lat (which is now a list after TSV splitting)
    title_lat = book.get("title_lat")
    if title_lat:
        if isinstance(title_lat, list):
            for part in title_lat:
                part = part.strip().rstrip(".") if part else ""
                if part and part not in candidates["lat"]:
                    candidates["lat"].append(part)
        else:
            # Fallback for string (shouldn't happen, but be safe)
            for part in title_lat.split(". "):
                part = part.strip().rstrip(".")
                if part and part not in candidates["lat"]:
                    candidates["lat"].append(part)

    # Handle title_ara (which is now a list after TSV splitting)
    title_ara = book.get("title_ara")
    if title_ara:
        if isinstance(title_ara, list):
            for part in title_ara:
                part = part.strip().rstrip(".") if part else ""
                if part and part not in candidates["ara"]:
                    candidates["ara"].append(part)
        else:
            # Fallback for string (shouldn't happen, but be safe)
            for part in title_ara.split(". "):
                part = part.strip().rstrip(".")
                if part and part not in candidates["ara"]:
                    candidates["ara"].append(part)

    return candidates


def search_authors(bnf_id: str, threshold: float) -> tuple[list[str], float]:
    """
    Stage 1: Find matching OpenITI author URIs.
    Returns: (matched_author_uris, time_ms)
    """
    bnf_record = bnf_records.get(bnf_id)
    if not bnf_record:
        return [], 0.0

    bnf_cands = build_bnf_author_candidates(bnf_record)
    if not bnf_cands.get("lat") and not bnf_cands.get("ara"):
        return [], 0.0

    # Normalize
    bnf_norm = {
        "lat": [normalize_transliteration(c) for c in bnf_cands.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in bnf_cands.get("ara", [])],
    }

    matched_authors = []
    start_time = time.time()

    # Match against all OpenITI authors
    for author_uri, author in openiti_authors.items():
        author_cands = build_openiti_author_candidates(author_uri)
        author_norm = {
            "lat": [normalize_transliteration(c) for c in author_cands.get("lat", [])],
            "ara": [normalize_transliteration(c) for c in author_cands.get("ara", [])],
        }

        # Check if any candidate matches at threshold
        for script in ["lat", "ara"]:
            if not bnf_norm.get(script) or not author_norm.get(script):
                continue
            for bnf_str in bnf_norm[script]:
                for author_str in author_norm[script]:
                    score = fuzz.token_set_ratio(bnf_str, author_str)
                    if score >= threshold * 100:
                        if author_uri not in matched_authors:
                            matched_authors.append(author_uri)
                        break

    elapsed_ms = (time.time() - start_time) * 1000
    return matched_authors, elapsed_ms


def search_titles(bnf_id: str, threshold: float) -> tuple[list[str], float]:
    """
    Stage 2: Find matching OpenITI book URIs by title.
    Returns: (matched_book_uris, time_ms)
    """
    bnf_record = bnf_records.get(bnf_id)
    if not bnf_record:
        return [], 0.0

    bnf_cands = build_bnf_title_candidates(bnf_record)
    if not bnf_cands.get("lat") and not bnf_cands.get("ara"):
        return [], 0.0

    # Normalize
    bnf_norm = {
        "lat": [normalize_transliteration(c) for c in bnf_cands.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in bnf_cands.get("ara", [])],
    }

    matched_books = []
    start_time = time.time()

    # Match against all OpenITI books
    for book_uri, book in openiti_books.items():
        book_cands = build_openiti_title_candidates(book)
        book_norm = {
            "lat": [normalize_transliteration(c) for c in book_cands.get("lat", [])],
            "ara": [normalize_transliteration(c) for c in book_cands.get("ara", [])],
        }

        # Check if any candidate matches at threshold
        for script in ["lat", "ara"]:
            if not bnf_norm.get(script) or not book_norm.get(script):
                continue
            for bnf_str in bnf_norm[script]:
                for book_str in book_norm[script]:
                    score = fuzz.token_set_ratio(bnf_str, book_str)
                    if score >= threshold * 100:
                        if book_uri not in matched_books:
                            matched_books.append(book_uri)
                        break

    elapsed_ms = (time.time() - start_time) * 1000
    return matched_books, elapsed_ms


def run_tests(thresholds: list[float] = None) -> tuple[list[MatchingResult], list[MatchingResult]]:
    """Run author and title matching tests at all thresholds."""
    if thresholds is None:
        thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]

    author_results = []
    title_results = []

    print(f"Testing {len(test_pairs)} BNF records at {len(thresholds)} thresholds\n")

    for i, (bnf_id, expected_uris) in enumerate(test_pairs.items(), 1):
        for threshold in thresholds:
            # Stage 1: Author matching
            matched_authors, author_time = search_authors(bnf_id, threshold)
            author_correct = any(
                uri.split(".")[0] == expected_uri.split(".")[0]  # Compare author prefix
                for expected_uri in expected_uris
                for uri in matched_authors
            )
            author_false_pos = len(matched_authors) - (1 if author_correct else 0)

            author_results.append(MatchingResult(
                bnf_id=bnf_id,
                expected_uri=expected_uris[0] if expected_uris else "",
                threshold=threshold,
                stage="author",
                matched_uris=matched_authors,
                is_correct=author_correct,
                num_matches=len(matched_authors),
                false_positives=author_false_pos,
                search_time_ms=author_time,
            ))

            # Stage 2: Title matching
            matched_books, title_time = search_titles(bnf_id, threshold)
            title_correct = any(uri in matched_books for uri in expected_uris)
            title_false_pos = len(matched_books) - (1 if title_correct else 0)

            title_results.append(MatchingResult(
                bnf_id=bnf_id,
                expected_uri=expected_uris[0] if expected_uris else "",
                threshold=threshold,
                stage="title",
                matched_uris=matched_books,
                is_correct=title_correct,
                num_matches=len(matched_books),
                false_positives=title_false_pos,
                search_time_ms=title_time,
            ))

            author_status = "OK" if author_correct else "--"
            title_status = "OK" if title_correct else "--"
            print(f"  [{i}/{len(test_pairs)}] {bnf_id} @ {threshold}: author={author_status} title={title_status}")

    return author_results, title_results


def write_results(
    author_results: list[MatchingResult],
    title_results: list[MatchingResult],
    output_dir: Path = Path("matching")
) -> None:
    """Write results to CSV using pandas or fallback."""
    output_dir.mkdir(exist_ok=True)

    if HAS_PANDAS:
        # Author results
        author_data = []
        for r in author_results:
            author_data.append({
                "bnf_id": r.bnf_id,
                "expected_uri": r.expected_uri,
                "threshold": r.threshold,
                "matched_uris": "|".join(r.matched_uris),
                "is_correct": r.is_correct,
                "num_matches": r.num_matches,
                "false_positives": r.false_positives,
                "search_time_ms": f"{r.search_time_ms:.1f}",
            })
        author_df = pd.DataFrame(author_data)
        author_csv = output_dir / "matching_results_author.csv"
        author_df.to_csv(author_csv, index=False, encoding="utf-8")
        print(f"\nWrote author results to {author_csv}")

        # Title results
        title_data = []
        for r in title_results:
            title_data.append({
                "bnf_id": r.bnf_id,
                "expected_uri": r.expected_uri,
                "threshold": r.threshold,
                "matched_uris": "|".join(r.matched_uris),
                "is_correct": r.is_correct,
                "num_matches": r.num_matches,
                "false_positives": r.false_positives,
                "search_time_ms": f"{r.search_time_ms:.1f}",
            })
        title_df = pd.DataFrame(title_data)
        title_csv = output_dir / "matching_results_title.csv"
        title_df.to_csv(title_csv, index=False, encoding="utf-8")
        print(f"Wrote title results to {title_csv}")
    else:
        print("Warning: pandas not available, results not written")


if __name__ == "__main__":
    thresholds = [0.70, 0.75, 0.80, 0.85, 0.90]
    author_results, title_results = run_tests(thresholds)
    write_results(author_results, title_results)
