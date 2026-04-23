"""Test matching on known correspondence pairs (validation set)."""

import json
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


def test_correspondence():
    """Test on known BNF→OpenITI matches."""
    print("\n--- Test: Correspondence Validation ---")

    # Load correspondence (known correct matches)
    with open("data_samplers/correspondence.json") as f:
        correspondence = json.load(f)

    # Extract BNF IDs to test
    bnf_ids_to_test = set()
    expected_matches = {}  # {bnf_id: [expected_openiti_uris]}

    for item in correspondence:
        for openiti_uri, bnf_id in item.items():
            bnf_ids_to_test.add(bnf_id)
            if bnf_id not in expected_matches:
                expected_matches[bnf_id] = []
            expected_matches[bnf_id].append(openiti_uri)

    print(f"Testing {len(bnf_ids_to_test)} BNF records with known matches")

    # Load full dataset
    all_bnf = load_bnf_records(BNF_SAMPLE_PATH)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

    # Filter to correspondence records
    test_bnf = {k: v for k, v in all_bnf.items() if k in bnf_ids_to_test}

    if len(test_bnf) < len(bnf_ids_to_test):
        print(f"Warning: only {len(test_bnf)} / {len(bnf_ids_to_test)} records found in sample")

    # Run pipeline
    pipeline = MatchingPipeline(
        test_bnf,
        openiti_data,
        run_id="correspondence_test",
        norm_strategy="fuzzy",
        verbose=False,
    )

    pipeline.register_stage(AuthorMatcher(verbose=False))
    pipeline.register_stage(TitleMatcher(verbose=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Validate results
    print("\nValidation results:")
    correct = 0
    total = 0

    for bnf_id in test_bnf.keys():
        expected = set(expected_matches.get(bnf_id, []))
        stage3 = set(pipeline.get_stage3_result(bnf_id) or [])
        classification = pipeline.get_classification(bnf_id)

        # Check if expected matches are in Stage 3 results
        found = expected.issubset(stage3)
        total += 1

        if found:
            correct += 1
            status = "PASS"
        else:
            status = "FAIL"
            missing = expected - stage3

        print(f"  {bnf_id}: {status}")
        if not found:
            print(f"    Expected: {expected}")
            print(f"    Got: {stage3}")
            print(f"    Missing: {missing}")
            print(f"    Classification: {classification}")

    print(f"\nResult: {correct}/{total} records have expected matches in Stage 3")
    return correct == total


def main():
    """Run correspondence validation."""
    print("=" * 60)
    print("CORRESPONDENCE VALIDATION TEST")
    print("=" * 60)

    try:
        success = test_correspondence()
        print("\n" + "=" * 60)
        if success:
            print("CORRESPONDENCE TEST PASSED")
        else:
            print("CORRESPONDENCE TEST FAILED")
        print("=" * 60)
        return 0 if success else 1
    except Exception as e:
        print(f"\nTEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
