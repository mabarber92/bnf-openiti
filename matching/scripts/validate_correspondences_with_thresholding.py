"""
Validate matching pipeline on correspondence.json with configurable thresholding.

Tests different thresholds and reports metrics that surface per-record false positives:
- Global precision: correct / total_candidates (can mask FPs in individual records)
- Per-record precision: average of (correct/returned per record)
- Best-match accuracy: % of records where top match is correct
- FP rate: % of records with at least one false positive
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


def evaluate_threshold(combined_threshold, test_bnf_records, openiti_data, expected_matches):
    """Test a single threshold value."""
    cfg.COMBINED_THRESHOLD = combined_threshold

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

    for bnf_id in expected_matches.keys():
        if bnf_id not in test_bnf_records:
            continue

        expected_uri = expected_matches[bnf_id]
        stage3_results = pipeline.get_stage3_result(bnf_id) or []

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
    fp_rate = fp_records / (total_records - records_no_match) if (total_records - records_no_match) > 0 else 0

    return {
        'threshold': combined_threshold,
        'global_precision': global_precision,
        'mean_per_record_precision': mean_per_record_precision,
        'best_match_accuracy': best_match_accuracy,
        'fp_rate': fp_rate,
        'fp_count': fp_records,
        'perfect_count': perfect_records,
        'total_candidates': total_candidates,
        'total_records': total_records,
        'records_no_match': records_no_match,
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

    print(f"Found {len(expected_matches)} test records\n")

    # Load BNF and OpenITI
    all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
    openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
    test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches.keys() if bnf_id in all_bnf}

    print(f"Loaded {len(test_bnf_records)} test BNF records\n")

    # Test threshold range
    thresholds = [0.88, 0.90, 0.92, 0.94, 0.96, 0.98]

    print("="*120)
    print("THRESHOLD EVALUATION")
    print("="*120)
    print(f"{'THRESHOLD':<12} {'BEST_MATCH%':<12} {'MEAN_PREC%':<12} {'GLOBAL_PREC%':<14} {'FP_RATE%':<10} {'FP_COUNT':<8} {'CANDIDATES':<10}")
    print("-"*120)

    results = []
    for threshold in thresholds:
        result = evaluate_threshold(threshold, test_bnf_records, openiti_data, expected_matches)
        results.append(result)

        print(f"{result['threshold']:<12.2f} {result['best_match_accuracy']:<12.1%} {result['mean_per_record_precision']:<12.1%} {result['global_precision']:<14.1%} {result['fp_rate']:<10.1%} {result['fp_count']:<8} {result['total_candidates']:<10}")

    print("\n" + "="*120)
    print("ANALYSIS")
    print("="*120)

    best_by_best_match = max(results, key=lambda x: x['best_match_accuracy'])
    best_by_mean_prec = max(results, key=lambda x: x['mean_per_record_precision'])
    best_by_global_prec = max(results, key=lambda x: x['global_precision'])
    fewest_fps = min(results, key=lambda x: x['fp_count'])

    print(f"\nBest by best-match accuracy:")
    print(f"  Threshold={best_by_best_match['threshold']:.2f} -> {best_by_best_match['best_match_accuracy']:.1%} ({best_by_best_match['perfect_count']}/{best_by_best_match['total_records']} records)")

    print(f"\nBest by mean per-record precision:")
    print(f"  Threshold={best_by_mean_prec['threshold']:.2f} -> {best_by_mean_prec['mean_per_record_precision']:.1%}")

    print(f"\nBest by global precision:")
    print(f"  Threshold={best_by_global_prec['threshold']:.2f} -> {best_by_global_prec['global_precision']:.1%}")

    print(f"\nFewest false positives:")
    print(f"  Threshold={fewest_fps['threshold']:.2f} -> {fewest_fps['fp_count']} records with FPs ({fewest_fps['fp_rate']:.1%})")
    print(f"    Returns {fewest_fps['total_candidates']} total candidates, {fewest_fps['records_no_match']} records get no match")
