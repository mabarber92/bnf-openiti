"""
Parameter sweep to optimize matching precision.

Tests combinations of:
- COMBINED_THRESHOLD: [0.88, 0.90, 0.92, 0.94, 0.96]
- AUTHOR_RARE_TOKEN_BOOST_FACTOR: [1.10, 1.15, 1.20, 1.25]
- TITLE_RARE_TOKEN_BOOST_FACTOR: [1.15, 1.20, 1.25, 1.30]

Evaluates on 11-record validation set (correspondences.json).
Uses multiprocessing pool (4 workers) for sweep parallelization.
"""

import json
import sys
from collections import defaultdict
from multiprocessing import Pool

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
import matching.config as cfg


def test_params(args):
    """Test a single parameter combination."""
    combined_thresh, author_boost, title_boost, test_bnf_records, openiti_data, expected_matches = args

    # Update config
    cfg.COMBINED_THRESHOLD = combined_thresh
    cfg.AUTHOR_RARE_TOKEN_BOOST_FACTOR = author_boost
    cfg.TITLE_RARE_TOKEN_BOOST_FACTOR = title_boost

    # Run pipeline (sequential - no nested multiprocessing from sweep workers)
    pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Evaluate
    correct = 0
    total_candidates = 0

    for bnf_id, expected_book_uri in expected_matches.items():
        if bnf_id not in test_bnf_records:
            continue

        stage3_books = pipeline.get_stage3_result(bnf_id) or []
        is_correct = expected_book_uri in stage3_books

        if is_correct:
            correct += 1

        total_candidates += len(stage3_books)

    precision = correct / total_candidates if total_candidates > 0 else 0
    recall = correct / len(expected_matches)
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    return {
        'combined_thresh': combined_thresh,
        'author_boost': author_boost,
        'title_boost': title_boost,
        'correct': correct,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'candidates': total_candidates,
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

    # Parameter ranges
    combined_thresholds = [0.88, 0.90, 0.92, 0.94, 0.96]
    author_boosts = [1.10, 1.15, 1.20, 1.25]
    title_boosts = [1.15, 1.20, 1.25, 1.30]

    # Build parameter combinations
    params_list = []
    for combined_thresh in combined_thresholds:
        for author_boost in author_boosts:
            for title_boost in title_boosts:
                params_list.append((combined_thresh, author_boost, title_boost, test_bnf_records, openiti_data, expected_matches))

    total = len(params_list)
    print(f"Testing {total} parameter combinations with 12 workers...\n")

    # Run sweep in parallel (12 cores, no nested parallelization)
    with Pool(processes=12) as pool:
        results = pool.map(test_params, params_list)

    print(f"\n{'='*100}")
    print("PARAMETER SWEEP RESULTS")
    print(f"{'='*100}\n")

    # Sort by precision (descending)
    results_by_precision = sorted(results, key=lambda x: x['precision'], reverse=True)

    print("Top 10 by Precision:\n")
    print(f"{'COMBINED':<10} {'AUTH_BOOST':<12} {'TITLE_BOOST':<12} {'PRECISION':<12} {'RECALL':<10} {'F1':<10} {'CORRECT':<8}")
    print("-" * 100)

    for i, r in enumerate(results_by_precision[:10]):
        print(f"{r['combined_thresh']:<10.2f} {r['author_boost']:<12.2f} {r['title_boost']:<12.2f} {r['precision']:<12.1%} {r['recall']:<10.1%} {r['f1']:<10.1%} {r['correct']:<8}")

    # Find best by F1
    results_by_f1 = sorted(results, key=lambda x: x['f1'], reverse=True)

    print(f"\n\nTop 10 by F1 Score:\n")
    print(f"{'COMBINED':<10} {'AUTH_BOOST':<12} {'TITLE_BOOST':<12} {'F1':<10} {'PRECISION':<12} {'RECALL':<10} {'CORRECT':<8}")
    print("-" * 100)

    for i, r in enumerate(results_by_f1[:10]):
        print(f"{r['combined_thresh']:<10.2f} {r['author_boost']:<12.2f} {r['title_boost']:<12.2f} {r['f1']:<10.1%} {r['precision']:<12.1%} {r['recall']:<10.1%} {r['correct']:<8}")

    print(f"\n\nBest Precision Overall:")
    best_precision = results_by_precision[0]
    print(f"  COMBINED_THRESHOLD = {best_precision['combined_thresh']}")
    print(f"  AUTHOR_RARE_TOKEN_BOOST_FACTOR = {best_precision['author_boost']}")
    print(f"  TITLE_RARE_TOKEN_BOOST_FACTOR = {best_precision['title_boost']}")
    print(f"  => Precision: {best_precision['precision']:.1%}, Recall: {best_precision['recall']:.1%}, F1: {best_precision['f1']:.1%}")

    print(f"\nBest F1 Overall:")
    best_f1 = results_by_f1[0]
    print(f"  COMBINED_THRESHOLD = {best_f1['combined_thresh']}")
    print(f"  AUTHOR_RARE_TOKEN_BOOST_FACTOR = {best_f1['author_boost']}")
    print(f"  TITLE_RARE_TOKEN_BOOST_FACTOR = {best_f1['title_boost']}")
    print(f"  => F1: {best_f1['f1']:.1%}, Precision: {best_f1['precision']:.1%}, Recall: {best_f1['recall']:.1%}")

    # Export full results to CSV
    import csv
    csv_path = 'parameter_sweep_results.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['combined_thresh', 'author_boost', 'title_boost', 'correct', 'candidates', 'precision', 'recall', 'f1'])
        writer.writeheader()
        writer.writerows(sorted(results, key=lambda x: x['precision'], reverse=True))
    print(f"\nFull results saved to: {csv_path}")

    # Analyze recall variance
    recalls = set(r['recall'] for r in results)
    precisions = set(r['precision'] for r in results)
    print(f"\nRecall values observed: {sorted(recalls)}")
    print(f"Precision range: {min(precisions):.1%} to {max(precisions):.1%}")

    print(f"\n{'='*100}")
