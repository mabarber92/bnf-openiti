"""
Detailed analysis of failures and false positives in validation.
"""

import json
import sys

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

# Load only test records
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches.keys() if bnf_id in all_bnf}

# Run pipeline
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Identify failures and FP
failure_ids = ['OAI_10884186', 'OAI_11000928']  # The 2 wrong results
fp_id = 'OAI_10884186'  # The false positive

print("="*120)
print("FAILURE AND FALSE POSITIVE ANALYSIS")
print("="*120)

for test_id in failure_ids:
    if test_id not in expected_matches:
        continue

    expected_book = expected_matches[test_id]

    print(f"\n\n{'='*120}")
    print(f"RECORD: {test_id}")
    print(f"EXPECTED: {expected_book}")
    print(f"{'='*120}")

    # Get stage results
    stage1_authors = pipeline.get_stage1_result(test_id) or []
    stage1_scores = pipeline.get_stage1_scores(test_id) or {}

    stage2_books = pipeline.get_stage2_result(test_id) or []
    stage2_scores = pipeline.get_stage2_scores(test_id) or {}

    stage3_books = pipeline.get_stage3_result(test_id) or []

    classification = pipeline.get_classification(test_id)

    # Extract expected author
    expected_author = expected_book.split('.')[0]

    print(f"\nSTAGE 1 (Author Matching):")
    print(f"  Total matches: {len(stage1_authors)}")
    print(f"  Expected author '{expected_author}' in matches: {expected_author in stage1_authors}")

    if stage1_authors:
        sorted_s1 = sorted(stage1_authors, key=lambda x: stage1_scores.get(x, 0), reverse=True)
        print(f"\n  Top 5 author matches:")
        for rank, author_uri in enumerate(sorted_s1[:5], 1):
            score = stage1_scores.get(author_uri, 0)
            match = "[*]" if author_uri == expected_author else "[ ]"
            print(f"    {rank}. {match} {author_uri:20s} {score:.3f}")
    else:
        print("  [X] No author matches!")

    print(f"\nSTAGE 2 (Title Matching):")
    print(f"  Total matches: {len(stage2_books)}")
    print(f"  Expected book in matches: {expected_book in stage2_books}")

    if stage2_books:
        sorted_s2 = sorted(stage2_books, key=lambda x: stage2_scores.get(x, 0), reverse=True)
        print(f"\n  Top 5 title matches:")
        for rank, book_uri in enumerate(sorted_s2[:5], 1):
            score = stage2_scores.get(book_uri, 0)
            match = "[*]" if book_uri == expected_book else " "
            print(f"    {rank}. [{match}] {book_uri[:60]:60s} {score:.3f}")
    else:
        print("  [X] No title matches!")

    print(f"\nSTAGE 3 (Combined Scoring):")
    print(f"  Total valid author+book pairs: {len(stage3_books)}")
    print(f"  Expected book in stage 3: {expected_book in stage3_books}")

    if stage3_books:
        print(f"\n  Stage 3 pairs (author+title combos that passed):")
        for rank, book_uri in enumerate(stage3_books[:5], 1):
            author_uri = book_uri.split('.')[0]
            author_score = stage1_scores.get(author_uri, 0)
            title_score = stage2_scores.get(book_uri, 0)
            combined = (author_score + title_score) / 2
            match = "[*]" if book_uri == expected_book else " "
            print(f"    {rank}. [{match}] {book_uri[:50]:50s}")
            print(f"       Author: {author_uri} ({author_score:.3f}), Title: ({title_score:.3f}), Combined: ({combined:.3f})")
    else:
        print("  [X] No pairs passed stage 3!")

    print(f"\nFINAL CLASSIFICATION: {classification}")

print(f"\n\n{'='*120}")
print("SUMMARY")
print(f"{'='*120}")
print(f"""
OAI_10884186 (WRONG - Got Nasai instead of Maqrizi):
  This is a FALSE POSITIVE - we matched to the wrong author.
  Root cause: Title matching may be too permissive, or author stage failed to distinguish.

OAI_11000928 (MISSING - Got no match):
  This is a MISSED MATCH - no stage 3 pairs formed.
  Root cause: Either author or title stage (or both) failed to match, or combined threshold too high.
""")
