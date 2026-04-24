"""
Investigate why OAI_11001068 is matching Ibn Hanbal's Masail (false positive).
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.normalize import normalize_transliteration
from fuzzywuzzy import fuzz
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH

def safe_get(obj, key):
    """Get attribute from dict or dataclass."""
    if isinstance(obj, dict):
        return obj.get(key)
    else:
        return getattr(obj, key, None)

# Load data
bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

bnf_id = "OAI_11001068"
record = bnf_records[bnf_id]
false_positive_book = "0241IbnHanbal.MasailRiwayatCabdAllah"
expected_book = "0697IbnWasil.MufarrijKurub"

# Get book details
book_fp = openiti_data["books"][false_positive_book]
book_expected = openiti_data["books"][expected_book]

print("="*80)
print(f"FALSE POSITIVE INVESTIGATION: {bnf_id}")
print("="*80)
print(f"\nExpected match: {expected_book}")
print(f"False positive: {false_positive_book}")
print(f"\nFalse positive book author: {safe_get(book_fp, 'author_uri')}")
print(f"Expected book author: {safe_get(book_expected, 'author_uri')}")

# Extract BNF candidates (author + title)
bnf_author_cands = []
bnf_title_cands = []

# Authors
for creator in safe_get(record, 'creator_lat') or []:
    if creator and creator not in bnf_author_cands:
        bnf_author_cands.append(creator)
for contrib in safe_get(record, 'contributor_lat') or []:
    if contrib and contrib not in bnf_author_cands:
        bnf_author_cands.append(contrib)
for title in safe_get(record, 'title_lat') or []:
    for part in title.split(". "):
        part = part.strip().rstrip(".")
        if part and part not in bnf_author_cands:
            bnf_author_cands.append(part)
for desc in safe_get(record, 'description_candidates_lat') or []:
    if desc and desc not in bnf_author_cands:
        bnf_author_cands.append(desc)

# Titles
for title in safe_get(record, 'title_lat') or []:
    for part in title.split(". "):
        part = part.strip().rstrip(".")
        if part and part not in bnf_title_cands:
            bnf_title_cands.append(part)
for desc in safe_get(record, 'description_candidates_lat') or []:
    if desc and desc not in bnf_title_cands:
        bnf_title_cands.append(desc)

print(f"\n\nBNF HAS {len(bnf_author_cands)} AUTHOR CANDIDATES")
print(f"BNF HAS {len(bnf_title_cands)} TITLE CANDIDATES")

# Run pipeline to get Stage 1 and Stage 2 results
bnf_records_test = {bnf_id: record}
pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="fp_debug", verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.run()

stage1 = pipeline.get_stage1_result(bnf_id) or []
stage2 = pipeline.get_stage2_result(bnf_id) or []

print(f"\n\nPIPELINE RESULTS:")
print(f"  Stage 1 (authors): {len(stage1)} authors matched")
print(f"    False positive author (0241IbnHanbal): {'IN' if '0241IbnHanbal' in stage1 else 'NOT IN'} stage 1")
print(f"  Stage 2 (books): {len(stage2)} books matched")
print(f"    False positive book: {'IN' if false_positive_book in stage2 else 'NOT IN'} stage 2")
print(f"    Expected book: {'IN' if expected_book in stage2 else 'NOT IN'} stage 2")

if false_positive_book in stage2:
    print(f"\n>>> FALSE POSITIVE IS IN STAGE 2")
    print(f"One of the BNF title candidates matched Ibn Hanbal book title")

    # Find which title candidate matched
    book_titles = []
    for title in safe_get(book_fp, 'title_lat') or []:
        title_stripped = title.strip().rstrip(".")
        if title_stripped:
            book_titles.append(title_stripped)

    print(f"\nIbn Hanbal book has {len(book_titles)} titles")

    THRESHOLD = 0.85
    match_count = 0
    for bnf_cand in bnf_title_cands:
        norm_bnf = normalize_transliteration(bnf_cand)
        if not norm_bnf:
            continue

        for book_title in book_titles:
            norm_book = normalize_transliteration(book_title)
            if not norm_book:
                continue

            score = fuzz.token_set_ratio(norm_bnf, norm_book)
            if score >= THRESHOLD * 100:
                match_count += 1
                print(f"\nMatch #{match_count}:")
                print(f"  BNF candidate: '{bnf_cand}' (norm: '{norm_bnf}')")
                print(f"  Book title: '{book_title}' (norm: '{norm_book}')")
                print(f"  Score: {score}/100 >= {THRESHOLD*100}")

    if match_count == 0:
        print("\nNo title matches found - check if this is correct")
