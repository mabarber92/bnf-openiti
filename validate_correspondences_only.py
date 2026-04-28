"""
Validate matching on just the 11 records in correspondences.json.

IDF is computed from full OpenITI dataset, but we only match the test records.
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

# Load ground truth
print("Loading correspondences.json...")
with open('data_samplers/correspondence.json') as f:
    correspondences = json.load(f)

# Build ground truth mapping: bnf_id → expected book_uri
expected_matches = {}
for item in correspondences:
    for book_uri, bnf_id in item.items():
        expected_matches[bnf_id] = book_uri

print(f"Found {len(expected_matches)} test records\n")

# Load only the BNF records we need
print("Loading BNF and OpenITI data...")
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

# Extract only test records
test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches.keys() if bnf_id in all_bnf}
print(f"Loaded {len(test_bnf_records)} test BNF records\n")

# Run pipeline on test records only
# (IDF is computed from full OpenITI)
print("Running matching pipeline...")
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
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

results_by_id = {}

for bnf_id, expected_book_uri in expected_matches.items():
    if bnf_id not in test_bnf_records:
        continue

    classification = pipeline.get_classification(bnf_id)
    stage3_books = pipeline.get_stage3_result(bnf_id) or []

    is_correct = expected_book_uri in stage3_books

    results_by_id[bnf_id] = {
        'expected': expected_book_uri,
        'stage3': stage3_books,
        'classification': classification,
        'correct': is_correct
    }

    if classification is None:
        missing += 1
    elif is_correct:
        correct += 1
    else:
        incorrect += 1

# Calculate metrics
total = len(expected_matches)
# Precision: of all candidates that passed the threshold, how many are correct?
total_candidates_passed = sum(len(r['stage3']) for r in results_by_id.values())
precision = correct / total_candidates_passed if total_candidates_passed > 0 else 0
recall = correct / total if total > 0 else 0
f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

print("="*100)
print("VALIDATION RESULTS - Correspondences Only")
print("="*100)
print(f"\nDataset: {total} records")
print(f"\nResults:")
print(f"  Correct:     {correct:>3} ({correct/total*100:>5.1f}%)")
print(f"  Incorrect:   {incorrect:>3} ({incorrect/total*100:>5.1f}%)")
print(f"  Missing:     {missing:>3} ({missing/total*100:>5.1f}%)")

print(f"\nMetrics:")
print(f"  Precision: {precision:.1%}")
print(f"  Recall:    {recall:.1%}")
print(f"  F1 Score:  {f1:.1%}")

print(f"\n{'ID':<20} {'Expected':<35} {'Got':<35} {'Result':<10}")
print("-" * 100)

for bnf_id in sorted(expected_matches.keys()):
    if bnf_id not in results_by_id:
        continue
    r = results_by_id[bnf_id]
    expected = r['expected'][:34]
    got = (r['stage3'][0] if r['stage3'] else "NONE")[:34]
    status = "CORRECT" if r['correct'] else "WRONG" if r['classification'] else "MISSING"
    print(f"{bnf_id:<20} {expected:<35} {got:<35} {status:<10}")

print("\n" + "="*100)
print(f"Config:")
print(f"  AUTHOR_THRESHOLD: {cfg.AUTHOR_THRESHOLD}")
print(f"  TITLE_THRESHOLD: {cfg.TITLE_THRESHOLD}")
print(f"  COMBINED_THRESHOLD: {cfg.COMBINED_THRESHOLD}")
print(f"  COMBINED_FLOOR: {cfg.COMBINED_FLOOR}")
print(f"  TOKEN_RARITY_THRESHOLD: {cfg.TOKEN_RARITY_THRESHOLD}")
print(f"  RARE_TOKEN_BOOST_FACTOR: {cfg.RARE_TOKEN_BOOST_FACTOR}")
print("="*100)


if __name__ == "__main__":
    pass
