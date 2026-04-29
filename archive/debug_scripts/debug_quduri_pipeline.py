"""
Run pipeline on just OAI_11000520 to see actual scores
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
import matching.config as cfg

all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

test_bnf = {'OAI_11000520': all_bnf['OAI_11000520']}

pipeline = MatchingPipeline(test_bnf, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.run()

bnf_id = 'OAI_11000520'

# Get stage 1
authors = pipeline.get_stage1_result(bnf_id) or []
author_scores = pipeline.get_stage1_scores(bnf_id) or {}

print("=" * 120)
print(f"OAI_11000520 - Pipeline Results")
print("=" * 120)
print()

print("STAGE 1: Author Matches")
print("-" * 120)
for author in authors:
    score = author_scores.get(author, 0)
    print(f"  {author:40} score={score:.3f}")
print()

# Get stage 2
books = pipeline.get_stage2_result(bnf_id) or []
book_scores = pipeline.get_stage2_scores(bnf_id) or {}

print("STAGE 2: Book Matches")
print("-" * 120)
for book in books:
    score = book_scores.get(book, 0)
    print(f"  {book:60} score={score:.3f}")
print()

# Get stage 3
combined = pipeline.get_stage3_result(bnf_id) or []

print("STAGE 3: Combined (Normalized & Ranked)")
print("-" * 120)
for i, book in enumerate(combined):
    print(f"  {i+1}. {book}")
print()

# Show why Juz might rank above Mukhtasar
quduri_juz = "0428AbuHusaynQuduri.Juz"
quduri_mukhtasar = "0428AbuHusaynQuduri.Mukhtasar"

if quduri_juz in books and quduri_mukhtasar in books:
    author_uri = "0428AbuHusaynQuduri"
    author_score = author_scores.get(author_uri, 0)
    juz_title_score = book_scores.get(quduri_juz, 0)
    mukhtasar_title_score = book_scores.get(quduri_mukhtasar, 0)
    
    juz_combined = (author_score + juz_title_score) / 2.0
    mukhtasar_combined = (author_score + mukhtasar_title_score) / 2.0
    
    print("SCORE COMPARISON")
    print("-" * 120)
    print(f"Author score (0428AbuHusaynQuduri): {author_score:.3f}")
    print()
    print(f"Juz combined:        ({author_score:.3f} + {juz_title_score:.3f}) / 2 = {juz_combined:.3f}")
    print(f"Mukhtasar combined:  ({author_score:.3f} + {mukhtasar_title_score:.3f}) / 2 = {mukhtasar_combined:.3f}")
    print()
    print(f"Max: {max(juz_combined, mukhtasar_combined):.3f}")
    
    # Apply normalization
    max_score = max(juz_combined, mukhtasar_combined)
    if max_score > 0:
        juz_norm = juz_combined / max_score
        mukhtasar_norm = mukhtasar_combined / max_score
        print()
        print("After normalization (divide by max):")
        print(f"Juz normalized:        {juz_norm:.3f}")
        print(f"Mukhtasar normalized:  {mukhtasar_norm:.3f}")

