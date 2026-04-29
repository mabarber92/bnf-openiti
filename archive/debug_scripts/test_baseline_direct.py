"""
Direct test of baseline pipeline (0.80/0.85, IDF^3).

Runs outside of the parameter sweep to isolate variables.
"""

import json
import sys
from pathlib import Path

repo_root = Path(__file__).parent
sys.path.insert(0, str(repo_root))

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH

# Configure baseline parameters
import matching.config as cfg
cfg.AUTHOR_THRESHOLD = 0.80
cfg.TITLE_THRESHOLD = 0.85
cfg.USE_TOKEN_IDF_WEIGHTING = True
cfg.TOKEN_IDF_PENALTY_EXPONENT = 3

# Load data
print("Loading data...")
bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

# Load test correspondences
with open("data_samplers/correspondence.json") as f:
    correspondences = json.load(f)

test_pairs = {}
for item in correspondences:
    for openiti_uri, bnf_id in item.items():
        if bnf_id not in test_pairs:
            test_pairs[bnf_id] = []
        test_pairs[bnf_id].append(openiti_uri)

# Filter to test records
test_bnf_ids = [bid for bid in test_pairs.keys() if bid in bnf_records]
bnf_records_test = {bid: bnf_records[bid] for bid in test_bnf_ids}

print(f"Test set: {len(bnf_records_test)} BNF records, {len(test_pairs)} expected matches")

# Run pipeline
pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="baseline_direct", verbose=True)
pipeline.register_stage(AuthorMatcher(verbose=True, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=True, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=True))
pipeline.register_stage(Classifier(verbose=True))
pipeline.run()

# Evaluate
print("\n" + "="*60)
print("RESULTS (Author=0.80, Title=0.85, IDF^3)")
print("="*60)

correct = 0
extra_total = 0
for bnf_id, expected_uris in sorted(test_pairs.items()):
    result = pipeline.get_stage3_result(bnf_id)
    matched = set(result) if result else set()
    expected = set(expected_uris)

    is_match = matched == expected
    if is_match:
        correct += 1

    status = "✓" if is_match else "✗"
    extra = len(matched - expected)
    missing = len(expected - matched)
    extra_total += extra

    print(f"{status} {bnf_id}: expected {len(expected)}, got {len(matched)} (extra={extra}, missing={missing})")

recall = correct / len(test_pairs) if test_pairs else 0
total_matched = sum(len(pipeline.get_stage3_result(bid) or []) for bid in test_pairs.keys())
precision = (total_matched - extra_total) / total_matched if total_matched > 0 else 0
f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

print("\n" + "-"*60)
print(f"Precision: {precision:.3f} ({int(precision*total_matched)}/{total_matched} correct)")
print(f"Recall: {recall:.3f} ({correct}/{len(test_pairs)} exact matches)")
print(f"F1: {f1:.3f}")
