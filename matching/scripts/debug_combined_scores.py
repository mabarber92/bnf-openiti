"""
Debug script: show combined scores for each validation record.

Helps diagnose precision ceiling by showing what scores were assigned
to correct vs incorrect matches.
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
with open('data_samplers/correspondence.json') as f:
    correspondences = json.load(f)

expected_matches = {}
for item in correspondences:
    for book_uri, bnf_id in item.items():
        expected_matches[bnf_id] = book_uri

# Load data
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches.keys() if bnf_id in all_bnf}

# Run pipeline
print("Running pipeline with optimal parameters...")
print(f"  COMBINED_THRESHOLD = {cfg.COMBINED_THRESHOLD}")
print(f"  AUTHOR_IDF_BOOST_SCALE = {cfg.AUTHOR_IDF_BOOST_SCALE}, AUTHOR_MAX_BOOST = {cfg.AUTHOR_MAX_BOOST}")
print(f"  TITLE_IDF_BOOST_SCALE = {cfg.TITLE_IDF_BOOST_SCALE}, TITLE_MAX_BOOST = {cfg.TITLE_MAX_BOOST}\n")

pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Extract scores for analysis
# Note: We need to calculate combined scores ourselves since they're not stored
# We have stage 1 and stage 2 scores, combined is their average

print("="*120)
print("COMBINED SCORES BY RECORD")
print("="*120)
print(f"{'BNF_ID':<20} {'EXPECTED':<40} {'RESULT':<10} {'SCORE':<8} {'STAGE1':<8} {'STAGE2':<8}")
print("-"*120)

for bnf_id in sorted(expected_matches.keys()):
    if bnf_id not in test_bnf_records:
        continue

    expected_uri = expected_matches[bnf_id]
    stage3_results = pipeline.get_stage3_result(bnf_id) or []
    stage1_results = pipeline.get_stage1_result(bnf_id) or []
    stage2_results = pipeline.get_stage2_result(bnf_id) or []

    stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
    stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}

    # Find author and book for expected match
    expected_author = None
    for author_uri, book_uris in pipeline.openiti_index._author_books.items():
        if expected_uri in book_uris:
            expected_author = author_uri
            break

    # Get normalized combined score from stage 3, or calculate from components if not available
    stage3_scores = pipeline.get_stage3_scores(bnf_id) or {}
    if expected_uri in stage3_scores:
        expected_combined = stage3_scores[expected_uri]
    else:
        expected_author_score = stage1_scores.get(expected_author, 0) if expected_author else 0
        expected_title_score = stage2_scores.get(expected_uri, 0)
        expected_combined = (expected_author_score + expected_title_score) / 2 if (expected_author_score or expected_title_score) else 0

    # Check if it's in stage 3 results
    is_correct = expected_uri in stage3_results
    result = "CORRECT" if is_correct else "WRONG"

    print(f"{bnf_id:<20} {expected_uri:<40} {result:<10} {expected_combined:<8.3f} {expected_author_score:<8.3f} {expected_title_score:<8.3f}")

    # Show top stage 3 candidates if wrong
    if not is_correct and stage3_results:
        print(f"  -> Got instead: {stage3_results[0][:40]}")
        # Get score for wrong match from stage 3
        stage3_scores = pipeline.get_stage3_scores(bnf_id) or {}
        wrong_combined = stage3_scores.get(stage3_results[0], 0)
        print(f"     Wrong score: {wrong_combined:.3f}")
    elif not is_correct and not stage3_results:
        print(f"  -> No matches returned (combined score {expected_combined:.3f} below threshold {cfg.COMBINED_THRESHOLD})")

print("\n" + "="*120)
print("ANALYSIS")
print("="*120)

# Check for scores > 1.0
max_score = 0
for bnf_id in expected_matches.keys():
    if bnf_id not in test_bnf_records:
        continue
    stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
    stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}
    for score in stage1_scores.values():
        max_score = max(max_score, score)
    for score in stage2_scores.values():
        max_score = max(max_score, score)

# Calculate representative metrics
correct_total = 0
fp_records = 0  # Records with false positives
perfect_records = 0  # Records where top match is correct
per_record_precisions = []

for bnf_id in expected_matches.keys():
    if bnf_id not in test_bnf_records:
        continue

    expected_uri = expected_matches[bnf_id]
    stage3_results = pipeline.get_stage3_result(bnf_id) or []

    if not stage3_results:
        continue

    # Count correct matches for this record
    correct_in_record = sum(1 for uri in stage3_results if uri == expected_uri)
    correct_total += correct_in_record

    # Per-record precision
    if stage3_results:
        record_precision = correct_in_record / len(stage3_results)
        per_record_precisions.append(record_precision)

    # Check if has false positives
    if len(stage3_results) > correct_in_record:
        fp_records += 1

    # Check if top match is correct
    if stage3_results[0] == expected_uri:
        perfect_records += 1

total_records = len([bid for bid in expected_matches.keys() if bid in test_bnf_records])
total_candidates = sum(len(pipeline.get_stage3_result(bid) or []) for bid in expected_matches.keys() if bid in test_bnf_records)

print(f"\nGlobal Metrics:")
print(f"  Total candidates returned: {total_candidates}")
print(f"  Correct matches: {correct_total}")
print(f"  Global precision (correct / total candidates): {correct_total / total_candidates if total_candidates > 0 else 0:.1%}")

print(f"\nPer-Record Metrics:")
print(f"  Records evaluated: {total_records}")
print(f"  Records with correct top match: {perfect_records}/{total_records} ({perfect_records / total_records:.1%})")
print(f"  Records with FPs (wrong matches returned): {fp_records}/{total_records}")
print(f"  Mean per-record precision: {sum(per_record_precisions) / len(per_record_precisions) if per_record_precisions else 0:.1%}")
print(f"  Problem areas: ", end="")
if fp_records > 0:
    print(f"{fp_records} record(s) with FPs")
else:
    print("None - all returned matches are correct!")
