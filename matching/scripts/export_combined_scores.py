"""
Export all combined-stage matches showing author scores pre and post reweighting.

Uses the pipeline's stored scores - no re-computation.
Shows pre-reweighting vs post-reweighting to measure creator field impact.
"""

import json
import sys
import csv

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

# Run pipeline ONCE with production parameters
print("Running pipeline with production parameters...")
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Extract results from pipeline's stored data
rows = []

for bnf_id in sorted(expected_matches.keys()):
    if bnf_id not in test_bnf_records:
        continue

    expected_uri = expected_matches[bnf_id]
    stage3_results = pipeline.get_stage3_result(bnf_id) or []
    stage1_scores_post = pipeline.get_stage1_scores(bnf_id) or {}
    stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}
    stage3_scores = pipeline.get_stage3_scores(bnf_id) or {}

    # Get pre-reweighting scores from pipeline
    stage1_scores_pre = getattr(pipeline, '_stage1_scores_pre_reweighting', {}).get(bnf_id, {})

    # Check if BNF record has creator fields
    bnf_record = test_bnf_records.get(bnf_id)
    bnf_creator_lat = None
    bnf_creator_ara = None
    if bnf_record:
        bnf_creator_lat = bnf_record.get('creator_lat') if isinstance(bnf_record, dict) else getattr(bnf_record, 'creator_lat', None)
        bnf_creator_ara = bnf_record.get('creator_ara') if isinstance(bnf_record, dict) else getattr(bnf_record, 'creator_ara', None)

    # For each stage 3 result, get the scores
    for rank, book_uri in enumerate(stage3_results, 1):
        # Find author of this book
        book_author = None
        for author_uri, book_uris in pipeline.openiti_index._author_books.items():
            if book_uri in book_uris:
                book_author = author_uri
                break

        # Get scores from pipeline's stored data
        author_score_post = stage1_scores_post.get(book_author, 0) if book_author else 0
        author_score_pre = stage1_scores_pre.get(book_author, author_score_post) if book_author else 0
        title_score = stage2_scores.get(book_uri, 0)
        combined_score = stage3_scores.get(book_uri, (author_score_post + title_score) / 2)

        # Get author name
        author_name = 'NONE'
        if book_author:
            author_obj = openiti_data.get(book_author, {})
            if isinstance(author_obj, dict):
                author_name = author_obj.get('name', 'Unknown')[:40]
            else:
                author_name = getattr(author_obj, 'name', 'Unknown')[:40]

        is_expected = (book_uri == expected_uri)

        rows.append({
            'bnf_id': bnf_id,
            'expected_uri': expected_uri,
            'matched_uri': book_uri,
            'is_correct': is_expected,
            'rank': rank,
            'author_uri': book_author or 'NONE',
            'author_name': author_name,
            'author_score_pre': author_score_pre,
            'author_score_post': author_score_post,
            'bnf_has_creator_lat': bool(bnf_creator_lat),
            'bnf_has_creator_ara': bool(bnf_creator_ara),
            'title_score': title_score,
            'combined_score': combined_score,
        })

# Write CSV
csv_path = 'data_samplers/combined_scores_with_author.csv'
with open(csv_path, 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['bnf_id', 'expected_uri', 'matched_uri', 'is_correct', 'rank', 'author_uri', 'author_name',
                  'author_score_pre', 'author_score_post', 'bnf_has_creator_lat', 'bnf_has_creator_ara', 'title_score', 'combined_score']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Exported {len(rows)} matches to {csv_path}")
print(f"\nConfig:")
print(f"  AUTHOR_THRESHOLD = {cfg.AUTHOR_THRESHOLD}")
print(f"  COMBINED_THRESHOLD = {cfg.COMBINED_THRESHOLD}")
print(f"  USE_AUTHOR_CREATOR_FIELD_MATCHING = {cfg.USE_AUTHOR_CREATOR_FIELD_MATCHING}")
print(f"  AUTHOR_CREATOR_FIELD_THRESHOLD = {cfg.AUTHOR_CREATOR_FIELD_THRESHOLD}")
