"""Diagnose matching results to understand threshold behavior."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from matching.config import BNF_SAMPLE_PATH, OPENITI_CORPUS_PATH
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus


def diagnose_record(bnf_id, bnf_records, openiti_index, pipeline):
    """Examine a single record's candidates and matches."""
    record = bnf_records[bnf_id]

    # Get raw candidates
    raw_candidates = record.matching_candidates(norm_strategy="raw")
    print(f"\nRecord {bnf_id}:")
    print(f"  Raw author candidates (lat): {len(raw_candidates.get('lat', []))} items")
    print(f"  Raw author candidates (ara): {len(raw_candidates.get('ara', []))} items")

    # Show first few
    if raw_candidates.get("lat"):
        print(f"    Examples: {raw_candidates['lat'][:3]}")

    # Get fuzzy candidates
    fuzzy_candidates = record.matching_candidates(norm_strategy="fuzzy")
    print(f"  Fuzzy author candidates (lat): {len(fuzzy_candidates.get('lat', []))} items")
    print(f"  Fuzzy author candidates (ara): {len(fuzzy_candidates.get('ara', []))} items")

    if fuzzy_candidates.get("lat"):
        print(f"    Examples: {fuzzy_candidates['lat'][:3]}")

    # Get pipeline results
    stage1 = pipeline.get_stage1_result(bnf_id) or []
    stage2 = pipeline.get_stage2_result(bnf_id) or []
    stage3 = pipeline.get_stage3_result(bnf_id) or []

    print(f"  Stage 1 matches: {len(stage1)} authors")
    print(f"  Stage 2 matches: {len(stage2)} books")
    print(f"  Stage 3 matches: {len(stage3)} (intersection)")

    # Sample a few matches
    if stage1:
        sample_author_uri = stage1[0]
        author = openiti_index.get_author(sample_author_uri)
        author_name = author.get("name_slug") if isinstance(author, dict) else author.name_slug
        print(f"  Sample Stage 1: {sample_author_uri} ({author_name})")

    if stage3:
        sample_book_uri = stage3[0]
        book = openiti_index.get_book(sample_book_uri)
        book_title = book.get("title_slug") if isinstance(book, dict) else book.title_slug
        print(f"  Sample Stage 3: {sample_book_uri} ({book_title})")


def main():
    """Run diagnostic."""
    print("=" * 60)
    print("MATCHING DIAGNOSIS")
    print("=" * 60)

    # Load data
    bnf_records = load_bnf_records(BNF_SAMPLE_PATH)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

    # Create pipeline
    pipeline = MatchingPipeline(
        bnf_records,
        openiti_data,
        run_id="diagnosis",
        norm_strategy="fuzzy",
        verbose=False,
    )

    # Run stages
    print("\nRunning matching stages...")
    pipeline.register_stage(AuthorMatcher(verbose=False))
    pipeline.register_stage(TitleMatcher(verbose=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Diagnose a few records
    test_records = list(bnf_records.keys())[:3]

    for bnf_id in test_records:
        diagnose_record(bnf_id, bnf_records, pipeline.openiti_index, pipeline)

    # Summary stats
    print(f"\n\nResults summary ({len(bnf_records)} records):")
    stage1_counts = []
    stage3_counts = []

    for bnf_id in bnf_records.keys():
        s1 = pipeline.get_stage1_result(bnf_id) or []
        s3 = pipeline.get_stage3_result(bnf_id) or []
        stage1_counts.append(len(s1))
        stage3_counts.append(len(s3))

    print(f"  Stage 1 matches: avg={sum(stage1_counts)/len(stage1_counts):.0f}, "
          f"min={min(stage1_counts)}, max={max(stage1_counts)}")
    print(f"  Stage 3 matches: avg={sum(stage3_counts)/len(stage3_counts):.0f}, "
          f"min={min(stage3_counts)}, max={max(stage3_counts)}")


if __name__ == "__main__":
    main()
