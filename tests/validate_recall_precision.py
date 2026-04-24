"""
Validate recall and precision of the matching pipeline.

Checks:
1. Are we finding all expected matches (recall)?
2. Are the extra matches correct or false positives (precision)?
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

# Load data
print("Loading data...")
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

print(f"Test set: {len(test_pairs)} records")

# Filter to test records only
test_bnf_ids = [bid for bid in test_pairs.keys() if bid in bnf_records]
bnf_records_test = {bid: bnf_records[bid] for bid in test_bnf_ids}

# Run pipeline
print("\nRunning pipeline...")
pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="recall_test", verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Validate recall and precision
print("\n" + "="*80)
print("RECALL AND PRECISION ANALYSIS")
print("="*80)

correct_matches = 0
false_positives = 0
false_negatives = 0

results = []

for bnf_id in sorted(test_bnf_ids):
    expected_uris = test_pairs[bnf_id]
    pipeline_stage3 = set(pipeline.get_stage3_result(bnf_id) or [])

    # Check if any expected URI is in Stage 3
    found = any(uri in pipeline_stage3 for uri in expected_uris)

    # Count false positives (matches not in expected set)
    extra_matches = pipeline_stage3 - set(expected_uris)

    result = {
        "bnf_id": bnf_id,
        "expected": expected_uris,
        "found": found,
        "matched_books": list(pipeline_stage3),
        "extra_matches": list(extra_matches),
        "num_extra": len(extra_matches),
    }
    results.append(result)

    if found:
        correct_matches += 1
    else:
        false_negatives += 1

    false_positives += len(extra_matches)

# Report
print(f"\nRecall: {correct_matches}/{len(test_bnf_ids)} ({100*correct_matches/len(test_bnf_ids):.1f}%)")
print(f"False Negatives: {false_negatives}")
print(f"False Positives: {false_positives}")
print(f"Precision: {(correct_matches/(correct_matches + false_positives)*100) if (correct_matches + false_positives) > 0 else 0:.1f}%")

# Detailed results
print("\n" + "="*80)
print("DETAILED RESULTS")
print("="*80)

for r in results:
    status = "CORRECT" if r["found"] else "MISSED"
    print(f"\n{r['bnf_id']}: {status}")
    print(f"  Expected: {r['expected']}")
    print(f"  Matched:  {r['matched_books']}")
    if r["extra_matches"]:
        print(f"  Extra:    {r['extra_matches']}")

# Summary table
print("\n" + "="*80)
print("SUMMARY TABLE")
print("="*80)
print(f"{'BNF_ID':<15} {'Status':<12} {'Expected':<30} {'Found':<30} {'Extra':<5}")
print("-" * 92)

for r in results:
    status = "CORRECT" if r["found"] else "MISSED"
    expected_str = r['expected'][0] if r['expected'] else "N/A"
    found_str = r['matched_books'][0] if r['matched_books'] else "None"
    print(f"{r['bnf_id']:<15} {status:<12} {expected_str:<30} {found_str:<30} {r['num_extra']:<5}")
