"""
Validate full pipeline against all correspondences.json records.

Measures precision and recall across the entire dataset.
"""

import json
import sys
from collections import defaultdict

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
import matching.config as cfg
from tqdm import tqdm

def main():
    # Load data
    print("Loading BNF and OpenITI data...")
    bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
    openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

    # Load ground truth
    print("Loading correspondences.json...")
    with open('data_samplers/correspondence.json') as f:
        correspondences = json.load(f)

    # Build ground truth mapping: bnf_id → expected book_uri
    expected_matches = {}
    for item in correspondences:
        for book_uri, bnf_id in item.items():
            expected_matches[bnf_id] = book_uri

    print(f"\nGround truth: {len(expected_matches)} BNF records with known matches\n")

    # Run full pipeline
    print("Running matching pipeline on all records...")
    pipeline = MatchingPipeline(bnf_records, openiti_data, verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=True, use_parallel=False))
    pipeline.register_stage(TitleMatcher(verbose=True, use_parallel=False))
    pipeline.register_stage(CombinedMatcher(verbose=True))
    pipeline.register_stage(Classifier(verbose=True))
    pipeline.run()

    # Evaluate
    print("\nEvaluating results...\n")

    correct = 0
    incorrect = 0
    missing = 0
    false_positives = 0

    results_by_confidence = defaultdict(lambda: {"correct": 0, "incorrect": 0, "missing": 0, "total": 0})

    for bnf_id, expected_book_uri in expected_matches.items():
        classification = pipeline.get_classification(bnf_id)

        results_by_confidence[classification]["total"] += 1

        if classification is None:
            missing += 1
            results_by_confidence[classification]["missing"] += 1
        else:
            # Get the actual matched book
            stage3_books = pipeline.get_stage3_result(bnf_id) or []

            if expected_book_uri in stage3_books:
                correct += 1
                results_by_confidence[classification]["correct"] += 1
            else:
                incorrect += 1
                results_by_confidence[classification]["incorrect"] += 1
                if stage3_books:
                    false_positives += 1

    # Calculate metrics
    total = len(expected_matches)
    precision = correct / (correct + false_positives) if (correct + false_positives) > 0 else 0
    recall = correct / total if total > 0 else 0

    print("="*80)
    print("FULL VALIDATION RESULTS")
    print("="*80)
    print(f"\nDataset size: {total} BNF records with ground truth")
    print(f"\nResults:")
    print(f"  Correct matches:    {correct:>4} ({correct/total*100:>5.1f}%)")
    print(f"  Incorrect matches:  {incorrect:>4} ({incorrect/total*100:>5.1f}%)")
    print(f"  Missing (no match): {missing:>4} ({missing/total*100:>5.1f}%)")
    print(f"  False positives:    {false_positives:>4} (of {incorrect} incorrect)")

    print(f"\nMetrics:")
    print(f"  Precision: {precision:.1%} (correct / (correct + false_positives))")
    print(f"  Recall:    {recall:.1%} (correct / total)")
    print(f"  F1 Score:  {2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0:.1%}")

    print(f"\nBreakdown by classification confidence:")
    print(f"{'Classification':<20} {'Total':>6} {'Correct':>8} {'Incorrect':>10} {'Missing':>8} {'Accuracy':>10}")
    print("-" * 80)

    for conf in sorted(results_by_confidence.keys(), key=lambda x: (x is None, x)):
        stats = results_by_confidence[conf]
        conf_str = str(conf) if conf is not None else "None"
        total_conf = stats["total"]
        correct_conf = stats["correct"]
        accuracy = correct_conf / total_conf if total_conf > 0 else 0
        print(f"{conf_str:<20} {total_conf:>6} {correct_conf:>8} {stats['incorrect']:>10} {stats['missing']:>8} {accuracy:>10.1%}")

    print("\n" + "="*80)
    print(f"Config settings used:")
    print(f"  AUTHOR_THRESHOLD: {cfg.AUTHOR_THRESHOLD}")
    print(f"  TITLE_THRESHOLD: {cfg.TITLE_THRESHOLD}")
    print(f"  COMBINED_THRESHOLD: {cfg.COMBINED_THRESHOLD}")
    print(f"  COMBINED_FLOOR: {cfg.COMBINED_FLOOR}")
    print(f"  TOKEN_RARITY_THRESHOLD: {cfg.TOKEN_RARITY_THRESHOLD}")
    print(f"  RARE_TOKEN_BOOST_FACTOR: {cfg.RARE_TOKEN_BOOST_FACTOR}")
    print("="*80)


if __name__ == "__main__":
    main()
