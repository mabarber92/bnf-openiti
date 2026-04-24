"""
Debug: Check actual scores for Ibn Hanbal false positive.
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
bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

# Just test OAI_11001068
bnf_id = "OAI_11001068"
record = bnf_records[bnf_id]

bnf_records_test = {bnf_id: record}
pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="debug_scores", verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.run()

stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}
stage1_authors = pipeline.get_stage1_result(bnf_id) or []
stage2_books = pipeline.get_stage2_result(bnf_id) or []

print("="*80)
print(f"SCORES FOR {bnf_id}")
print("="*80)

print(f"\nStage 1 (Author Matching): {len(stage1_authors)} authors")
print(f"Stage 2 (Title Matching): {len(stage2_books)} books")

# Find Ibn Hanbal
ibn_hanbal_score = stage1_scores.get("0241IbnHanbal")
print(f"\nIbn Hanbal author score: {ibn_hanbal_score}")

if "0241IbnHanbal.MasailRiwayatCabdAllah" in stage2_books:
    ibn_hanbal_book_score = stage2_scores.get("0241IbnHanbal.MasailRiwayatCabdAllah")
    print(f"Ibn Hanbal book score: {ibn_hanbal_book_score}")

    # Check thresholds
    print(f"\nConfidence filtering thresholds:")
    if ibn_hanbal_score and ibn_hanbal_book_score:
        if ibn_hanbal_score >= 0.90:
            print(f"  Author score {ibn_hanbal_score:.3f} >= 0.90: Accept ANY title match")
            print(f"  Title score {ibn_hanbal_book_score:.3f}: PASS (no requirement)")
            print(f"  Result: MATCH")
        elif ibn_hanbal_score >= 0.85:
            print(f"  Author score {ibn_hanbal_score:.3f} in [0.85, 0.90): Need title >= 0.90")
            print(f"  Title score {ibn_hanbal_book_score:.3f}: {'PASS' if ibn_hanbal_book_score >= 0.90 else 'FAIL'}")
            print(f"  Result: {'MATCH' if ibn_hanbal_book_score >= 0.90 else 'NO MATCH'}")
        elif ibn_hanbal_score >= 0.80:
            print(f"  Author score {ibn_hanbal_score:.3f} in [0.80, 0.85): Need title >= 0.95")
            print(f"  Title score {ibn_hanbal_book_score:.3f}: {'PASS' if ibn_hanbal_book_score >= 0.95 else 'FAIL'}")
            print(f"  Result: {'MATCH' if ibn_hanbal_book_score >= 0.95 else 'NO MATCH'}")

# Show top author scores
print(f"\nTop 5 author scores:")
sorted_authors = sorted(stage1_scores.items(), key=lambda x: x[1], reverse=True)
for author_uri, score in sorted_authors[:5]:
    marker = " <-- Ibn Hanbal" if author_uri == "0241IbnHanbal" else ""
    print(f"  {author_uri}: {score:.3f}{marker}")

# Show top title scores
print(f"\nTop 5 book scores:")
sorted_books = sorted(stage2_scores.items(), key=lambda x: x[1], reverse=True)
for book_uri, score in sorted_books[:5]:
    marker = " <-- Ibn Hanbal book" if "IbnHanbal" in book_uri else ""
    print(f"  {book_uri}: {score:.3f}{marker}")
