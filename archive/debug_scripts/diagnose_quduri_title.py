"""
Detailed diagnostic for OAI_11000520 - why is Tahawi's Mukhtasar winning the title match?
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
from collections import defaultdict
import math

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.normalize import normalize_for_matching
from matching.candidate_builders import build_book_candidates_by_script
from fuzzywuzzy import fuzz
import matching.config as cfg

bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

# Build IDF weights
def build_token_idf_weights(books_candidates):
    token_doc_freq = defaultdict(set)
    total_docs = 0
    for book_uri, book_candidates_by_script in books_candidates.items():
        total_docs += 1
        tokens_seen = set()
        for script in ["lat", "ara"]:
            for book_str in book_candidates_by_script.get(script, []):
                if book_str:
                    norm_str = normalize_for_matching(book_str, split_camelcase=True, is_openiti=True)
                    if norm_str:
                        for token in norm_str.lower().split():
                            tokens_seen.add(token)
        for token in tokens_seen:
            token_doc_freq[token].add(book_uri)
    
    idf_weights = {}
    for token, doc_set in token_doc_freq.items():
        doc_freq = len(doc_set)
        idf = math.log(total_docs / max(1, doc_freq)) if doc_freq > 0 else 0
        idf_weights[token] = idf
    return idf_weights

# Build book candidates
books_candidates = {}
for book_uri, book_data in openiti_data['books'].items():
    books_candidates[book_uri] = build_book_candidates_by_script(book_data)

idf_weights = build_token_idf_weights(books_candidates)

# Get the specific BNF record
target_bnf_id = 'OAI_11000520'
bnf_record = bnf_records[target_bnf_id]

titles_ara = getattr(bnf_record, "title_ara", []) or []
titles_lat = getattr(bnf_record, "title_lat", []) or []
title = (titles_ara[0] if titles_ara else None) or (titles_lat[0] if titles_lat else "")

norm_title = normalize_for_matching(title, split_camelcase=False)

print("=" * 120)
print(f"DIAGNOSTIC: OAI_11000520 - Quduri vs Tahawi Book Title Matching")
print("=" * 120)
print(f"BNF Title: {title[:100]}")
print(f"Normalized: {norm_title}")
print(f"Tokens: {set(norm_title.split())}")
print()

print(f"TOKEN_RARITY_THRESHOLD: {cfg.TOKEN_RARITY_THRESHOLD}")
print(f"RARE_TOKEN_BOOST_FACTOR: {cfg.RARE_TOKEN_BOOST_FACTOR}")
print()

# Find Quduri and Tahawi books
quduri_uri = "0428AbuHusaynQuduri.Mukhtasar"
tahawi_uri = "0321Tahawi.Mukhtasar"

if quduri_uri in openiti_data['books']:
    quduri_book = openiti_data['books'][quduri_uri]
    quduri_candidates = build_book_candidates_by_script(quduri_book)
    quduri_titles = quduri_candidates.get("lat", []) + quduri_candidates.get("ara", [])
else:
    quduri_titles = ["NOT FOUND"]
    print(f"WARNING: {quduri_uri} not found in books")

if tahawi_uri in openiti_data['books']:
    tahawi_book = openiti_data['books'][tahawi_uri]
    tahawi_candidates = build_book_candidates_by_script(tahawi_book)
    tahawi_titles = tahawi_candidates.get("lat", []) + tahawi_candidates.get("ara", [])
else:
    tahawi_titles = ["NOT FOUND"]
    print(f"WARNING: {tahawi_uri} not found in books")

print()
print(f"Quduri book variants: {[t for t in quduri_titles if t]}")
print(f"Tahawi book variants: {[t for t in tahawi_titles if t]}")
print()

# Score each variant
print("=" * 120)
print("QUDURI BOOK VARIANTS")
print("=" * 120)

best_quduri = (0, "", "")
for variant in quduri_titles:
    if not variant:
        continue
    norm_variant = normalize_for_matching(variant, split_camelcase=True)
    fuzzy_score = fuzz.token_set_ratio(norm_title, norm_variant)
    
    title_tokens = set(norm_title.split())
    variant_tokens = set(norm_variant.split())
    matched_tokens = title_tokens & variant_tokens
    
    rare_tokens = [t for t in matched_tokens if idf_weights.get(t, 0.1) >= cfg.TOKEN_RARITY_THRESHOLD]
    
    if rare_tokens:
        boosted_score = fuzzy_score * cfg.RARE_TOKEN_BOOST_FACTOR
        boost_status = f"BOOSTED ({rare_tokens})"
    else:
        boosted_score = fuzzy_score
        boost_status = "no boost"
    
    if boosted_score > best_quduri[0]:
        best_quduri = (boosted_score, variant, boost_status)
    
    print(f"Variant: {variant[:80]}")
    print(f"  Normalized: {norm_variant[:80]}")
    print(f"  Matched tokens: {matched_tokens}")
    print(f"  Rare tokens: {rare_tokens}")
    print(f"  Fuzzy: {fuzzy_score:.1f} → {boosted_score:.1f} ({boost_status})")
    print()

print("=" * 120)
print("TAHAWI BOOK VARIANTS")
print("=" * 120)

best_tahawi = (0, "", "")
for variant in tahawi_titles:
    if not variant:
        continue
    norm_variant = normalize_for_matching(variant, split_camelcase=True)
    fuzzy_score = fuzz.token_set_ratio(norm_title, norm_variant)
    
    title_tokens = set(norm_title.split())
    variant_tokens = set(norm_variant.split())
    matched_tokens = title_tokens & variant_tokens
    
    rare_tokens = [t for t in matched_tokens if idf_weights.get(t, 0.1) >= cfg.TOKEN_RARITY_THRESHOLD]
    
    if rare_tokens:
        boosted_score = fuzzy_score * cfg.RARE_TOKEN_BOOST_FACTOR
        boost_status = f"BOOSTED ({rare_tokens})"
    else:
        boosted_score = fuzzy_score
        boost_status = "no boost"
    
    if boosted_score > best_tahawi[0]:
        best_tahawi = (boosted_score, variant, boost_status)
    
    print(f"Variant: {variant[:80]}")
    print(f"  Normalized: {norm_variant[:80]}")
    print(f"  Matched tokens: {matched_tokens}")
    print(f"  Rare tokens: {rare_tokens}")
    print(f"  Fuzzy: {fuzzy_score:.1f} → {boosted_score:.1f} ({boost_status})")
    print()

print("=" * 120)
print("SUMMARY")
print("=" * 120)
print(f"Best Quduri book score: {best_quduri[0]:.1f} ({best_quduri[2]})")
print(f"Best Tahawi book score: {best_tahawi[0]:.1f} ({best_tahawi[2]})")
print()

print("=" * 120)
print("IDF VALUES FOR TITLE TOKENS")
print("=" * 120)

for token in norm_title.split():
    idf = idf_weights.get(token, 0.1)
    is_rare = "RARE" if idf >= cfg.TOKEN_RARITY_THRESHOLD else "common"
    print(f"  {token:20} IDF={idf:7.3f} ({is_rare})")
