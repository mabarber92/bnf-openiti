"""
Export all combined-stage matches to CSV for manual inspection.

Shows every book that passed the combined threshold for each BNF record,
with its combined score (and component author/title scores).
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

# Run pipeline
print("Running pipeline...")
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Export to CSV
rows = []

for bnf_id in sorted(expected_matches.keys()):
    if bnf_id not in test_bnf_records:
        continue

    expected_uri = expected_matches[bnf_id]
    stage3_results = pipeline.get_stage3_result(bnf_id) or []
    stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
    stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}

    # For each stage 3 result, get the normalized combined score
    stage3_scores = pipeline.get_stage3_scores(bnf_id) or {}
    for rank, book_uri in enumerate(stage3_results, 1):
        # Find author of this book
        book_author = None
        for author_uri, book_uris in pipeline.openiti_index._author_books.items():
            if book_uri in book_uris:
                book_author = author_uri
                break

        author_score = stage1_scores.get(book_author, 0) if book_author else 0
        title_score = stage2_scores.get(book_uri, 0)
        # Use normalized combined score from stage 3
        combined_score = stage3_scores.get(book_uri, (author_score + title_score) / 2)

        is_expected = (book_uri == expected_uri)

        rows.append({
            'bnf_id': bnf_id,
            'expected_uri': expected_uri,
            'matched_uri': book_uri,
            'is_correct': is_expected,
            'rank': rank,
            'author_score': author_score,
            'title_score': title_score,
            'combined_score': combined_score,
        })

# Write CSV
csv_path = 'combined_scores.csv'
with open(csv_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['bnf_id', 'expected_uri', 'matched_uri', 'is_correct', 'rank', 'author_score', 'title_score', 'combined_score'])
    writer.writeheader()
    writer.writerows(rows)

print(f"Exported {len(rows)} matches to {csv_path}")
print(f"\nConfig:")
print(f"  COMBINED_THRESHOLD = {cfg.COMBINED_THRESHOLD}")
print(f"  AUTHOR_RARE_TOKEN_BOOST_FACTOR = {cfg.AUTHOR_RARE_TOKEN_BOOST_FACTOR}")
print(f"  TITLE_RARE_TOKEN_BOOST_FACTOR = {cfg.TITLE_RARE_TOKEN_BOOST_FACTOR}")
