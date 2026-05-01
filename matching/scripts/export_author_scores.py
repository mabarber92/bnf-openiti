"""
Export matching results showing scores across all three pipeline stages.

Each row is a (bnf_id, book_uri) pair. For each book, shows:
- Stage 1 (author): score_raw, score_idf, score_final for the book's author
- Stage 2 (title):  title_score for this specific book
- Stage 3 (combined): combined_score if the book reached stage 3

Rows include: all stage 3 books, all stage 2 books, plus any book by an author
that reached stage 1 (even if it didn't make title matching), so the expected
match is always visible regardless of where it dropped out.
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

rows = []

for bnf_id in sorted(expected_matches.keys()):
    if bnf_id not in test_bnf_records:
        continue

    expected_book_uri = expected_matches[bnf_id]

    # Stage 1: author scores
    stage1_scores_final = pipeline.get_stage1_scores(bnf_id) or {}
    stage1_scores_raw = getattr(pipeline, '_stage1_scores_pre_reweighting', {}).get(bnf_id, {})
    stage1_scores_idf = getattr(pipeline, '_stage1_scores_post_idf', {}).get(bnf_id, {})
    stage1_authors = set(pipeline.get_stage1_result(bnf_id) or [])

    # Stage 2: title scores (book_uri → score)
    stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}
    stage2_books = set(pipeline.get_stage2_result(bnf_id) or [])

    # Stage 3: combined scores (book_uri → combined_score)
    stage3_scores = pipeline.get_stage3_scores(bnf_id) or {}
    stage3_books = set(pipeline.get_stage3_result(bnf_id) or [])

    # BNF creator fields
    bnf_record = test_bnf_records[bnf_id]
    bnf_creator_lat = bnf_record.get('creator_lat') if isinstance(bnf_record, dict) else getattr(bnf_record, 'creator_lat', None)
    bnf_creator_ara = bnf_record.get('creator_ara') if isinstance(bnf_record, dict) else getattr(bnf_record, 'creator_ara', None)
    bnf_has_creator_lat = bool(bnf_creator_lat)
    bnf_has_creator_ara = bool(bnf_creator_ara)

    # Collect candidate books: stage2 + stage3 + books by stage1 authors + expected book
    candidate_books = stage2_books | stage3_books | {expected_book_uri}

    # Add all books by stage1-matched authors so we can see where expected book dropped out
    for author_uri in stage1_authors:
        for book_uri, book in pipeline.openiti_index.books.items():
            b_author = book.get('author_uri') if isinstance(book, dict) else book.author_uri
            if b_author == author_uri:
                candidate_books.add(book_uri)

    for book_uri in candidate_books:
        book = pipeline.openiti_index.get_book(book_uri)
        if not book:
            continue

        author_uri = book.get('author_uri') if isinstance(book, dict) else book.author_uri
        book_title = book.get('title_lat') or book.get('title_ara') or '' if isinstance(book, dict) else (getattr(book, 'title_lat', '') or getattr(book, 'title_ara', '') or '')

        # Stage 1 author scores
        score_final = stage1_scores_final.get(author_uri)
        score_raw = stage1_scores_raw.get(author_uri, score_final)
        score_idf = stage1_scores_idf.get(author_uri, score_raw)
        idf_boosted = (score_idf is not None and score_raw is not None and score_idf > score_raw)
        creator_reweighted = (score_final is not None and score_idf is not None and abs(score_final - score_idf) > 1e-6)

        author_obj = pipeline.openiti_index.authors.get(author_uri, {})
        author_name = author_obj.get('name', 'Unknown') if isinstance(author_obj, dict) else getattr(author_obj, 'name', 'Unknown')

        # Stage 2 title score
        title_score = stage2_scores.get(book_uri)

        # Stage 3 combined score
        combined_score = stage3_scores.get(book_uri)

        rows.append({
            'bnf_id': bnf_id,
            'expected_book': expected_book_uri,
            'book_uri': book_uri,
            'book_title': str(book_title)[:50],
            'author_uri': author_uri,
            'author_name': str(author_name)[:40],
            'score_raw': round(score_raw, 4) if score_raw is not None else '',
            'score_idf': round(score_idf, 4) if score_idf is not None else '',
            'score_final': round(score_final, 4) if score_final is not None else '',
            'idf_boosted': idf_boosted,
            'creator_reweighted': creator_reweighted,
            'title_score': round(title_score, 4) if title_score is not None else '',
            'combined_score': round(combined_score, 4) if combined_score is not None else '',
            'in_stage1': author_uri in stage1_authors,
            'in_stage2': book_uri in stage2_books,
            'in_stage3': book_uri in stage3_books,
            'is_expected': book_uri == expected_book_uri,
            'bnf_has_creator_lat': bnf_has_creator_lat,
            'bnf_has_creator_ara': bnf_has_creator_ara,
        })

# Write CSV
csv_path = 'data_samplers/stage1_author_scores.csv'
with open(csv_path, 'w', newline='', encoding='utf-8') as f:
    fieldnames = [
        'bnf_id', 'expected_book', 'book_uri', 'book_title',
        'author_uri', 'author_name',
        'score_raw', 'score_idf', 'score_final', 'idf_boosted', 'creator_reweighted',
        'title_score', 'combined_score',
        'in_stage1', 'in_stage2', 'in_stage3', 'is_expected',
        'bnf_has_creator_lat', 'bnf_has_creator_ara',
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Exported {len(rows)} book candidates to {csv_path}\n")
print(f"Config:")
print(f"  AUTHOR_THRESHOLD:               {cfg.AUTHOR_THRESHOLD}")
print(f"  TITLE_THRESHOLD:                {cfg.TITLE_THRESHOLD}")
print(f"  COMBINED_THRESHOLD:             {cfg.COMBINED_THRESHOLD}")
print(f"  COMBINED_FLOOR:                 {cfg.COMBINED_FLOOR}")
print(f"  TITLE_FLOOR:                    {cfg.TITLE_FLOOR}")
print(f"  AUTHOR_CREATOR_IDF_THRESHOLD:   {cfg.AUTHOR_CREATOR_IDF_THRESHOLD}")
print(f"  AUTHOR_FULL_STRING_WEIGHT:      {cfg.AUTHOR_FULL_STRING_WEIGHT}")
print(f"  AUTHOR_CREATOR_FIELD_WEIGHT:    {cfg.AUTHOR_CREATOR_FIELD_WEIGHT}")
print(f"  AUTHOR_IDF_BOOST_SCALE:         {cfg.AUTHOR_IDF_BOOST_SCALE}")
print(f"  AUTHOR_MAX_BOOST:               {cfg.AUTHOR_MAX_BOOST}")
print(f"  TITLE_IDF_BOOST_SCALE:          {cfg.TITLE_IDF_BOOST_SCALE}")
print(f"  TITLE_MAX_BOOST:                {cfg.TITLE_MAX_BOOST}")
print(f"  USE_AUTHOR_IDF_WEIGHTING:       {cfg.USE_AUTHOR_IDF_WEIGHTING}")
print(f"  USE_TITLE_IDF_WEIGHTING:        {cfg.USE_TITLE_IDF_WEIGHTING}")
