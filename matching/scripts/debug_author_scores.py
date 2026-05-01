"""
Debug two-stage author matching by showing per-record author scores.

Inspects stage 1 scores to see:
1. How many author candidates were found per BNF record
2. What scores creator field matching produced for the discriminator case
3. Whether creator field reweighting was applied
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

# Load BNF and OpenITI
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches.keys() if bnf_id in all_bnf}

# Run pipeline
print("Running matching pipeline...")
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Analyze stage 1 (author) results
print("\n" + "="*130)
print("STAGE 1 AUTHOR MATCHING ANALYSIS")
print("="*130)

for bnf_id in sorted(expected_matches.keys()):
    if bnf_id not in test_bnf_records:
        continue

    stage1_authors = pipeline.get_stage1_result(bnf_id) or []
    stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}

    expected_book = expected_matches[bnf_id]
    expected_author_uri = pipeline.openiti_index.get_book(expected_book).author_uri if pipeline.openiti_index.get_book(expected_book) else None

    is_discriminator = (bnf_id == "0852IbnHajarCasqalani.InbaGhumr")

    print(f"\n{bnf_id}")
    print(f"  Expected book: {expected_book}")
    print(f"  Expected author: {expected_author_uri}")
    print(f"  Matched authors: {len(stage1_authors)}")

    if stage1_authors:
        print(f"  Author scores:")
        # Sort by score descending
        sorted_authors = sorted(stage1_scores.items(), key=lambda x: x[1], reverse=True)
        for author_uri, score in sorted_authors[:5]:  # Show top 5
            author_obj = openiti_data.get(author_uri, {})
            author_name = author_obj.get('name', 'Unknown')[:40] if isinstance(author_obj, dict) else (author_obj.name[:40] if hasattr(author_obj, 'name') else 'Unknown')
            is_expected = "*** EXPECTED ***" if author_uri == expected_author_uri else ""
            print(f"    {author_uri}: {score:.3f} ({author_name}) {is_expected}")

        if is_discriminator:
            print(f"\n  [DISCRIMINATOR CASE] Creator field matching should have helped rank {expected_author_uri}")
    else:
        print(f"  NO AUTHORS MATCHED!")

print("\n" + "="*130)
print("CONFIGURATION")
print("="*130)
print(f"USE_AUTHOR_CREATOR_FIELD_MATCHING: {cfg.USE_AUTHOR_CREATOR_FIELD_MATCHING}")
print(f"AUTHOR_CREATOR_FIELD_THRESHOLD: {cfg.AUTHOR_CREATOR_FIELD_THRESHOLD}")
print(f"AUTHOR_FULL_STRING_WEIGHT: {cfg.AUTHOR_FULL_STRING_WEIGHT}")
print(f"AUTHOR_CREATOR_FIELD_WEIGHT: {cfg.AUTHOR_CREATOR_FIELD_WEIGHT}")
print(f"USE_AUTHOR_IDF_WEIGHTING: {cfg.USE_AUTHOR_IDF_WEIGHTING}")
print("="*130)
