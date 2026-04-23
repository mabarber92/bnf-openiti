"""Integration tests for matching pipeline stages."""

import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from matching.openiti_index import OpenITIIndex
from matching.bnf_index import BNFCandidateIndex
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.config import BNF_SAMPLE_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus


def test_full_pipeline():
    """Test all stages running together."""
    print("\n--- Test: Full Matching Pipeline ---")

    # Load data
    print("Loading test data...")
    bnf_records = load_bnf_records(BNF_SAMPLE_PATH)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

    # Create pipeline
    print(f"Creating pipeline with {len(bnf_records)} BNF records...")
    pipeline = MatchingPipeline(
        bnf_records,
        openiti_data,
        run_id="stage_test",
        norm_strategy="fuzzy",
        verbose=False,
    )

    # Register and execute stages
    print("Registering and executing matching stages...")
    pipeline.register_stage(AuthorMatcher(verbose=False))
    pipeline.register_stage(TitleMatcher(verbose=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))

    pipeline.run()

    # Validate results
    print("Validating results...")

    # Pick a test record
    test_bnf_id = list(bnf_records.keys())[0]

    stage1 = pipeline.get_stage1_result(test_bnf_id)
    stage2 = pipeline.get_stage2_result(test_bnf_id)
    stage3 = pipeline.get_stage3_result(test_bnf_id)
    classification = pipeline.get_classification(test_bnf_id)

    assert stage1 is not None, "Stage 1 result should exist"
    assert stage2 is not None, "Stage 2 result should exist"
    assert stage3 is not None, "Stage 3 result should exist"
    assert classification is not None, "Classification should exist"
    assert classification in [
        "high_confidence",
        "author_only",
        "title_only",
        "unmatched",
    ], f"Invalid classification: {classification}"

    print(f"  Test record {test_bnf_id}:")
    print(f"    Stage 1 (authors):     {len(stage1)} matches")
    print(f"    Stage 2 (titles):      {len(stage2)} matches")
    print(f"    Stage 3 (intersection): {len(stage3)} matches")
    print(f"    Classification:        {classification}")
    print(f"  Pipeline test: PASSED [OK]")

    # Count results across all records
    high_conf = 0
    author_only = 0
    title_only = 0
    unmatched = 0

    for bnf_id in bnf_records.keys():
        tier = pipeline.get_classification(bnf_id)
        if tier == "high_confidence":
            high_conf += 1
        elif tier == "author_only":
            author_only += 1
        elif tier == "title_only":
            title_only += 1
        else:
            unmatched += 1

    total = high_conf + author_only + title_only + unmatched
    assert total == len(bnf_records), f"Classification count mismatch: {total} != {len(bnf_records)}"

    print(f"\nClassification summary (all {len(bnf_records)} records):")
    print(f"  High confidence: {high_conf} ({100*high_conf/total:.1f}%)")
    print(f"  Author only:     {author_only} ({100*author_only/total:.1f}%)")
    print(f"  Title only:      {title_only} ({100*title_only/total:.1f}%)")
    print(f"  Unmatched:       {unmatched} ({100*unmatched/total:.1f}%)")

    print("Full pipeline tests PASSED [OK]")
    return True


def main():
    """Run integration tests."""
    print("=" * 60)
    print("MATCHING PIPELINE INTEGRATION TESTS")
    print("=" * 60)

    try:
        test_full_pipeline()

        print("\n" + "=" * 60)
        print("ALL INTEGRATION TESTS PASSED")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
