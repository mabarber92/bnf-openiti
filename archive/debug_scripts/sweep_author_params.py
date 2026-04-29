"""
Parameter sweep: optimize AUTHOR_THRESHOLD and RARE_TOKEN_BOOST_FACTOR.

Goal: reduce false positives by being stricter on author matching.
"""

import json
import sys
from collections import defaultdict
from itertools import product

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
import matching.config as cfg

# Load ground truth
with open('data_samplers/correspondence.json') as f:
    correspondences = json.load(f)

expected_matches = {}
for item in correspondences:
    for book_uri, bnf_id in item.items():
        expected_matches[bnf_id] = book_uri

# Load test records
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches.keys() if bnf_id in all_bnf}

# Parameter sweep ranges
author_thresholds = [0.80]  # Fixed to baseline for faster iteration
boost_factors = [1.15, 1.25, 1.35, 1.45, 1.55, 1.75, 2.00]

results = []

print("Running parameter sweep...")
print(f"Combinations to test: {len(author_thresholds)} × {len(boost_factors)} = {len(author_thresholds) * len(boost_factors)}\n")

for author_thresh, boost in product(author_thresholds, boost_factors):
    # Temporarily override config
    original_auth_thresh = cfg.AUTHOR_THRESHOLD
    original_boost = cfg.RARE_TOKEN_BOOST_FACTOR

    cfg.AUTHOR_THRESHOLD = author_thresh
    cfg.RARE_TOKEN_BOOST_FACTOR = boost

    # Run pipeline
    pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Evaluate
    correct = 0
    incorrect = 0
    false_positives = 0

    for bnf_id, expected_book_uri in expected_matches.items():
        if bnf_id not in test_bnf_records:
            continue

        classification = pipeline.get_classification(bnf_id)
        stage3_books = pipeline.get_stage3_result(bnf_id) or []

        if classification is not None:
            if expected_book_uri in stage3_books:
                correct += 1
            else:
                incorrect += 1
                if stage3_books:
                    false_positives += 1

    total = len(expected_matches)
    precision = correct / (correct + false_positives) if (correct + false_positives) > 0 else 0
    recall = correct / total if total > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    results.append({
        'author_thresh': author_thresh,
        'boost': boost,
        'correct': correct,
        'incorrect': incorrect,
        'fp': false_positives,
        'precision': precision,
        'recall': recall,
        'f1': f1
    })

    print(f"AUTHOR_THRESH={author_thresh:.2f} BOOST={boost:.2f} | Correct={correct:2d} FP={false_positives:2d} | P={precision:.1%} R={recall:.1%} F1={f1:.1%}")

    # Restore config
    cfg.AUTHOR_THRESHOLD = original_auth_thresh
    cfg.RARE_TOKEN_BOOST_FACTOR = original_boost

# Sort by F1 score descending
results.sort(key=lambda x: x['f1'], reverse=True)

print("\n" + "="*120)
print("TOP 10 PARAMETER COMBINATIONS (by F1 score)")
print("="*120)
print(f"{'Rank':<5} {'AUTHOR_THRESH':>13} {'BOOST':>8} {'Correct':>8} {'FP':>4} {'Precision':>10} {'Recall':>10} {'F1':>10}")
print("-" * 120)

for rank, r in enumerate(results[:10], 1):
    print(f"{rank:<5} {r['author_thresh']:>13.2f} {r['boost']:>8.2f} {r['correct']:>8d} {r['fp']:>4d} {r['precision']:>10.1%} {r['recall']:>10.1%} {r['f1']:>10.1%}")

print("\n" + "="*120)
print("ANALYSIS")
print("="*120)

best = results[0]
baseline = results[-1]  # Worst F1 for comparison

print(f"\nBest config (AUTHOR_THRESH={best['author_thresh']:.2f}, BOOST={best['boost']:.2f}):")
print(f"  Precision: {best['precision']:.1%}")
print(f"  Recall: {best['recall']:.1%}")
print(f"  F1: {best['f1']:.1%}")
print(f"  FP: {best['fp']}")

print(f"\nWorst config (AUTHOR_THRESH={baseline['author_thresh']:.2f}, BOOST={baseline['boost']:.2f}):")
print(f"  Precision: {baseline['precision']:.1%}")
print(f"  Recall: {baseline['recall']:.1%}")
print(f"  F1: {baseline['f1']:.1%}")
print(f"  FP: {baseline['fp']}")

if __name__ == "__main__":
    pass
