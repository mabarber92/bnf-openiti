"""
Test confidence-dependent filtering in Stage 3.

Runs validation with and without confidence filtering to see if it
improves precision without hurting recall.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH

def run_test(use_confidence_filtering: bool) -> dict:
    """Run validation test with given configuration."""

    # Load data
    bnf_records = load_bnf_records(BNF_FULL_PATH)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

    with open("data_samplers/correspondence.json", encoding="utf-8") as f:
        correspondences = json.load(f)

    test_pairs = {}
    for item in correspondences:
        for openiti_uri, bnf_id in item.items():
            if bnf_id not in test_pairs:
                test_pairs[bnf_id] = []
            test_pairs[bnf_id].append(openiti_uri)

    # Filter to test records only
    test_bnf_ids = [bid for bid in test_pairs.keys() if bid in bnf_records]
    bnf_records_test = {bid: bnf_records[bid] for bid in test_bnf_ids}

    # Run pipeline
    run_id = "confidence_test" if use_confidence_filtering else "baseline_test"
    pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id=run_id, verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(CombinedMatcher(verbose=False, use_confidence_filtering=use_confidence_filtering))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Calculate metrics
    correct_matches = 0
    false_positives = 0

    results = []

    for bnf_id in sorted(test_bnf_ids):
        expected_uris = test_pairs[bnf_id]
        pipeline_stage3 = set(pipeline.get_stage3_result(bnf_id) or [])

        # Check if any expected URI is in Stage 3
        found = any(uri in pipeline_stage3 for uri in expected_uris)

        # Count false positives
        extra_matches = pipeline_stage3 - set(expected_uris)

        if found:
            correct_matches += 1

        false_positives += len(extra_matches)

        results.append({
            "bnf_id": bnf_id,
            "found": found,
            "matched_count": len(pipeline_stage3),
            "extra_count": len(extra_matches),
            "extra": list(extra_matches) if extra_matches else [],
        })

    return {
        "use_confidence_filtering": use_confidence_filtering,
        "recall": 100 * correct_matches / len(test_bnf_ids),
        "precision": (100 * correct_matches / (correct_matches + false_positives)) if (correct_matches + false_positives) > 0 else 0,
        "correct_matches": correct_matches,
        "false_positives": false_positives,
        "total_records": len(test_bnf_ids),
        "results": results,
    }

print("="*80)
print("CONFIDENCE-DEPENDENT FILTERING TEST")
print("="*80)

# Test baseline (no filtering)
print("\nRunning baseline test (no confidence filtering)...")
baseline = run_test(use_confidence_filtering=False)

# Test with confidence filtering
print("Running test with confidence filtering...")
with_filtering = run_test(use_confidence_filtering=True)

# Compare results
print("\n" + "="*80)
print("RESULTS COMPARISON")
print("="*80)

print(f"\n{'Metric':<30} {'Baseline':<15} {'With Filtering':<15} {'Change'}")
print("-" * 75)

for key in ["recall", "precision"]:
    baseline_val = baseline[key]
    filtered_val = with_filtering[key]
    change = filtered_val - baseline_val
    symbol = "UP" if change > 0 else "DN" if change < 0 else "SAME"
    print(f"{key.upper():<30} {baseline_val:>6.1f}%{'':<7} {filtered_val:>6.1f}%{'':<7} {symbol:>4} {abs(change):+.1f}%")

print(f"{'False Positives':<30} {baseline['false_positives']:<15} {with_filtering['false_positives']:<15} {with_filtering['false_positives'] - baseline['false_positives']:+d}")
print(f"{'Correct Matches':<30} {baseline['correct_matches']:<15} {with_filtering['correct_matches']:<15} {with_filtering['correct_matches'] - baseline['correct_matches']:+d}")

# Show detailed results
print("\n" + "="*80)
print("DETAILED RESULTS WITH FILTERING")
print("="*80)

for r in with_filtering["results"]:
    if r["extra"]:
        status = "EXTRA" if r["found"] else "EXTRA (MISS)"
        print(f"\n{r['bnf_id']}: {status}")
        print(f"  Extra matches: {len(r['extra'])}")

# Analysis
print("\n" + "="*80)
print("ANALYSIS")
print("="*80)

if with_filtering['false_positives'] < baseline['false_positives']:
    reduced = baseline['false_positives'] - with_filtering['false_positives']
    print(f"\nConfidence filtering REDUCED false positives by {reduced}")
    if with_filtering['correct_matches'] == baseline['correct_matches']:
        print("[GOOD] Recall unchanged - this is a pure improvement!")
    else:
        print(f"[WARN] Recall changed: {baseline['correct_matches']} -> {with_filtering['correct_matches']}")
elif with_filtering['false_positives'] > baseline['false_positives']:
    print(f"\nConfidence filtering INCREASED false positives - not recommended")
else:
    print(f"\nConfidence filtering had NO EFFECT on false positives")

print(f"\nRecommendation: {'ENABLE confidence filtering' if with_filtering['false_positives'] <= baseline['false_positives'] and with_filtering['recall'] >= baseline['recall'] else 'DISABLE confidence filtering'}")
