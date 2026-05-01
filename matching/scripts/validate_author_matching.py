"""
Validate two-stage author matching with creator field weighting.

Tests different AUTHOR_CREATOR_FIELD_THRESHOLD values to ensure:
1. No regression on existing 13 records (recall >= 11)
2. Improvement on the new discriminator test case (0852IbnHajarCasqalani.InbaGhumr)
3. Metrics: recall, best-match accuracy, per-record precision, FP rate
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


def evaluate_author_threshold(author_threshold, test_bnf_records, openiti_data, expected_matches):
    """Test a single author creator-field threshold value."""
    cfg.AUTHOR_CREATOR_FIELD_THRESHOLD = author_threshold

    # Run pipeline
    pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Evaluate
    correct_total = 0
    fp_records = 0
    perfect_records = 0
    per_record_precisions = []
    total_candidates = 0
    records_no_match = 0
    record_details = {}

    for bnf_id in expected_matches.keys():
        if bnf_id not in test_bnf_records:
            continue

        expected_uri = expected_matches[bnf_id]
        stage3_results = pipeline.get_stage3_result(bnf_id) or []

        # Record details for later inspection
        is_correct_in_results = expected_uri in stage3_results
        is_top_match = stage3_results[0] == expected_uri if stage3_results else False
        rank = stage3_results.index(expected_uri) + 1 if is_correct_in_results else None

        record_details[bnf_id] = {
            'matched': len(stage3_results) > 0,
            'correct': is_correct_in_results,
            'top_match': is_top_match,
            'rank': rank,
            'candidates': len(stage3_results),
        }

        if not stage3_results:
            records_no_match += 1
            continue

        total_candidates += len(stage3_results)

        # Count correct matches for this record
        correct_in_record = sum(1 for uri in stage3_results if uri == expected_uri)
        correct_total += correct_in_record

        # Per-record precision
        record_precision = correct_in_record / len(stage3_results)
        per_record_precisions.append(record_precision)

        # Check if has false positives
        if len(stage3_results) > correct_in_record:
            fp_records += 1

        # Check if top match is correct
        if stage3_results[0] == expected_uri:
            perfect_records += 1

    total_records = len([bid for bid in expected_matches.keys() if bid in test_bnf_records])

    global_precision = correct_total / total_candidates if total_candidates > 0 else 0
    mean_per_record_precision = sum(per_record_precisions) / len(per_record_precisions) if per_record_precisions else 0
    best_match_accuracy = perfect_records / total_records if total_records > 0 else 0
    recall = correct_total / total_records if total_records > 0 else 0
    fp_rate = fp_records / (total_records - records_no_match) if (total_records - records_no_match) > 0 else 0

    return {
        'threshold': author_threshold,
        'recall': recall,
        'best_match_accuracy': best_match_accuracy,
        'global_precision': global_precision,
        'mean_per_record_precision': mean_per_record_precision,
        'fp_rate': fp_rate,
        'fp_count': fp_records,
        'perfect_count': perfect_records,
        'total_candidates': total_candidates,
        'total_records': total_records,
        'records_no_match': records_no_match,
        'record_details': record_details,
    }


if __name__ == '__main__':
    # Load ground truth
    print("Loading test data...")
    with open('data_samplers/correspondence.json') as f:
        correspondences = json.load(f)

    expected_matches = {}
    for item in correspondences:
        for book_uri, bnf_id in item.items():
            expected_matches[bnf_id] = book_uri

    print(f"Found {len(expected_matches)} test records")
    print(f"Including discriminator case: 0852IbnHajarCasqalani.InbaGhumr\n")

    # Load BNF and OpenITI
    all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
    openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
    test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches.keys() if bnf_id in all_bnf}

    print(f"Loaded {len(test_bnf_records)} test BNF records\n")

    # Test threshold range
    thresholds = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]

    print("=" * 130)
    print("AUTHOR CREATOR-FIELD THRESHOLD EVALUATION")
    print("=" * 130)
    print(f"{'THRESHOLD':<12} {'RECALL':<12} {'BEST_MATCH%':<12} {'GLOBAL_PREC%':<14} {'MEAN_PREC%':<12} {'FP_RATE%':<10}")
    print("-" * 130)

    results = []
    for threshold in thresholds:
        result = evaluate_author_threshold(threshold, test_bnf_records, openiti_data, expected_matches)
        results.append(result)

        print(f"{result['threshold']:<12.2f} {result['recall']:<12.1%} {result['best_match_accuracy']:<12.1%} {result['global_precision']:<14.1%} {result['mean_per_record_precision']:<12.1%} {result['fp_rate']:<10.1%}")

    print("\n" + "=" * 130)
    print("DISCRIMINATOR CASE ANALYSIS")
    print("=" * 130)

    discriminator_id = "0852IbnHajarCasqalani.InbaGhumr"
    print(f"\nNew test case: {discriminator_id}")
    print(f"Expected match: {expected_matches[discriminator_id]}\n")

    print(f"{'THRESHOLD':<12} {'MATCHED':<12} {'CORRECT':<12} {'TOP_MATCH':<12} {'RANK':<8} {'CANDIDATES':<10}")
    print("-" * 66)

    for result in results:
        details = result['record_details'].get(discriminator_id, {})
        matched = "Yes" if details.get('matched') else "No"
        correct = "Yes" if details.get('correct') else "No"
        top_match = "Yes" if details.get('top_match') else "No"
        rank = str(details.get('rank')) if details.get('rank') else "N/A"
        candidates = str(details.get('candidates', 0))

        print(f"{result['threshold']:<12.2f} {matched:<12} {correct:<12} {top_match:<12} {rank:<8} {candidates:<10}")

    print("\n" + "=" * 130)
    print("SUMMARY")
    print("=" * 130)

    best_by_recall = max(results, key=lambda x: x['recall'])
    best_by_best_match = max(results, key=lambda x: x['best_match_accuracy'])
    best_by_global_prec = max(results, key=lambda x: x['global_precision'])

    print(f"\nBest by recall:")
    print(f"  Threshold={best_by_recall['threshold']:.2f} -> {best_by_recall['recall']:.1%} ({best_by_recall['perfect_count']}/{best_by_recall['total_records']} top matches)")

    print(f"\nBest by best-match accuracy:")
    print(f"  Threshold={best_by_best_match['threshold']:.2f} -> {best_by_best_match['best_match_accuracy']:.1%}")

    print(f"\nBest by global precision:")
    print(f"  Threshold={best_by_global_prec['threshold']:.2f} -> {best_by_global_prec['global_precision']:.1%}")

    print(f"\n--- Recommendation ---")
    print(f"Ensure recall doesn't drop below 11/14 (78.6%) on existing records.")
    print(f"Verify discriminator case matches correctly with selected threshold.")
