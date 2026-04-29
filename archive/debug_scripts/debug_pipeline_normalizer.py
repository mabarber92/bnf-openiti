"""
Debug: Run pipeline on sample records with table ON and OFF.

This tests actual pipeline behavior, not just the normalizer function.
"""

import json
import sys

sys.stdout.reconfigure(encoding='utf-8')
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

# Test records
test_ids = list(expected_matches.keys())[:5]

print("="*120)
print("PIPELINE BEHAVIOR WITH TABLE ON vs OFF")
print("="*120)

for use_table in [False, True]:
    cfg.USE_DIACRITIC_CONVERSION_TABLE = use_table

    print(f"\n\n{'='*120}")
    print(f"RUN WITH USE_DIACRITIC_CONVERSION_TABLE = {use_table}")
    print(f"{'='*120}\n")

    # Run pipeline
    pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Show results for each test record
    for bnf_id in test_ids:
        expected_book = expected_matches[bnf_id]
        classification = pipeline.get_classification(bnf_id)
        stage3_books = pipeline.get_stage3_result(bnf_id) or []

        stage1_authors = pipeline.get_stage1_result(bnf_id) or []
        stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
        stage2_books = pipeline.get_stage2_result(bnf_id) or []
        stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}

        print(f"{bnf_id}:")
        print(f"  Expected: {expected_book}")
        print(f"  Got:      {stage3_books[0] if stage3_books else 'NONE'}")
        print(f"  Status:   {'CORRECT' if expected_book in stage3_books else 'WRONG' if stage3_books else 'MISSING'}")

        if stage1_authors:
            top_author = max(stage1_authors, key=lambda x: stage1_scores.get(x, 0))
            print(f"  Stage 1:  {len(stage1_authors)} authors, top: {top_author} @ {stage1_scores[top_author]:.3f}")
        else:
            print(f"  Stage 1:  NO AUTHORS MATCHED")

        if stage2_books:
            top_book = max(stage2_books, key=lambda x: stage2_scores.get(x, 0))
            print(f"  Stage 2:  {len(stage2_books)} books, top: {top_book} @ {stage2_scores[top_book]:.3f}")
        else:
            print(f"  Stage 2:  NO BOOKS MATCHED")

        print()
