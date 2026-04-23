"""Debug a single record to understand scoring behavior."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from matching.config import BNF_SAMPLE_PATH, OPENITI_CORPUS_PATH
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.fuzzy_scorer import FuzzyScorer
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus


def main():
    """Debug OAI_10030933 which should match IbnCadim.BughyatTalab."""
    print("=" * 60)
    print("DEBUG: OAI_10030933")
    print("=" * 60)
    print("Expected match: 0660IbnCadim.BughyatTalab")

    # Load data
    all_bnf = load_bnf_records(BNF_SAMPLE_PATH)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

    bnf_id = "OAI_10030933"
    if bnf_id not in all_bnf:
        print(f"ERROR: {bnf_id} not in sample")
        return 1

    record = all_bnf[bnf_id]

    # Examine candidates
    print(f"\nBNF Record Candidates:")
    raw_cands = record.matching_candidates(norm_strategy="raw")
    fuzzy_cands = record.matching_candidates(norm_strategy="fuzzy")

    print(f"  Raw author candidates (lat): {len(raw_cands.get('lat', []))} items")
    print(f"  Raw author candidates (ara): {len(raw_cands.get('ara', []))} items")
    print(f"  Fuzzy author candidates (lat): {len(fuzzy_cands.get('lat', []))} items")
    print(f"  Fuzzy author candidates (ara): {len(fuzzy_cands.get('ara', []))} items")

    # Expected OpenITI record
    expected_uri = "0660IbnCadim.BughyatTalab"
    if expected_uri in openiti_data["books"]:
        expected_book = openiti_data["books"][expected_uri]
        expected_book_title = expected_book.get("title_slug") if isinstance(expected_book, dict) else expected_book.title_slug
        expected_author_uri = expected_book.get("author_uri") if isinstance(expected_book, dict) else expected_book.author_uri
        expected_author = openiti_data["authors"].get(expected_author_uri)
        expected_author_name = expected_author.get("name_slug") if isinstance(expected_author, dict) else expected_author.name_slug

        print(f"\nExpected OpenITI Match:")
        print(f"  Book URI: {expected_uri}")
        print(f"  Book title: {expected_book_title}")
        print(f"  Author URI: {expected_author_uri}")
        print(f"  Author name: {expected_author_name}")

        # Score each candidate against expected match
        scorer = FuzzyScorer()
        print(f"\nScoring BNF candidates against expected match:")
        print(f"  Against author (sample scores):")

        for i, cand in enumerate(fuzzy_cands.get("lat", [])[:3]):
            score = scorer.score(cand, expected_author_name)
            print(f"    Candidate {i}: {score:.1f}")

        print(f"  Against title (sample scores):")
        for i, cand in enumerate(fuzzy_cands.get("lat", [])[:3]):
            score = scorer.score(cand, expected_book_title)
            print(f"    Candidate {i}: {score:.1f}")

    # Run pipeline on just this record
    print(f"\nRunning pipeline on {bnf_id}...")
    test_bnf = {bnf_id: record}

    pipeline = MatchingPipeline(
        test_bnf,
        openiti_data,
        run_id="debug_single",
        norm_strategy="fuzzy",
        verbose=False,
    )

    pipeline.register_stage(AuthorMatcher(verbose=False))
    pipeline.register_stage(TitleMatcher(verbose=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Results
    stage1 = pipeline.get_stage1_result(bnf_id) or []
    stage2 = pipeline.get_stage2_result(bnf_id) or []
    stage3 = pipeline.get_stage3_result(bnf_id) or []

    print(f"\nResults:")
    print(f"  Stage 1 (authors): {len(stage1)} matches")
    print(f"  Stage 2 (books): {len(stage2)} matches")
    print(f"  Stage 3 (intersection): {len(stage3)} matches")
    print(f"  Classification: {pipeline.get_classification(bnf_id)}")

    # Check if expected is in results
    expected_author_uri_actual = "0660IbnCadim"
    expected_in_s1 = expected_author_uri_actual in stage1
    expected_in_s3 = expected_uri in stage3

    print(f"\nValidation:")
    print(f"  Expected author {expected_author_uri_actual} in Stage 1: {expected_in_s1}")
    print(f"  Expected book {expected_uri} in Stage 3: {expected_in_s3}")

    if expected_in_s3:
        print("\n[PASS] Record validated successfully")
        return 0
    else:
        print("\n[FAIL] Record validation FAILED")
        if expected_in_s1:
            print("  Author matched but book not in intersection")
        return 1


if __name__ == "__main__":
    sys.exit(main())
