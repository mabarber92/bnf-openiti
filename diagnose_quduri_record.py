"""
Detailed diagnostic for OAI_11000520 - why is Tahawi scoring higher than Quduri?
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
from collections import defaultdict
import math

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.normalize import normalize_for_matching
from matching.candidate_builders import build_author_candidates_by_script
from fuzzywuzzy import fuzz
import matching.config as cfg

bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

# Build IDF weights
def build_token_idf_weights(authors_candidates):
    token_doc_freq = defaultdict(set)
    total_docs = 0
    for author_uri, author_candidates_by_script in authors_candidates.items():
        total_docs += 1
        tokens_seen = set()
        for script in ["lat", "ara"]:
            for author_str in author_candidates_by_script.get(script, []):
                if author_str:
                    norm_str = normalize_for_matching(author_str, split_camelcase=True, is_openiti=True)
                    if norm_str:
                        for token in norm_str.lower().split():
                            tokens_seen.add(token)
        for token in tokens_seen:
            token_doc_freq[token].add(author_uri)
    
    idf_weights = {}
    for token, doc_set in token_doc_freq.items():
        doc_freq = len(doc_set)
        idf = math.log(total_docs / max(1, doc_freq)) if doc_freq > 0 else 0
        idf_weights[token] = idf
    return idf_weights

# Build author candidates
authors_candidates = {}
for author_uri, author_data in openiti_data['authors'].items():
    authors_candidates[author_uri] = build_author_candidates_by_script(author_data)

idf_weights = build_token_idf_weights(authors_candidates)

# Get the specific BNF record
target_bnf_id = 'OAI_11000520'
bnf_record = bnf_records[target_bnf_id]

creators_lat = getattr(bnf_record, "creator_lat", []) or []
creator = creators_lat[0] if creators_lat else ""

norm_creator = normalize_for_matching(creator, split_camelcase=False)

print("=" * 120)
print(f"DIAGNOSTIC: OAI_11000520 - Quduri vs Tahawi")
print("=" * 120)
print(f"BNF Creator: {creator[:80]}")
print(f"Normalized: {norm_creator}")
print(f"Tokens: {set(norm_creator.split())}")
print()

print(f"TOKEN_RARITY_THRESHOLD: {cfg.TOKEN_RARITY_THRESHOLD}")
print(f"RARE_TOKEN_BOOST_FACTOR: {cfg.RARE_TOKEN_BOOST_FACTOR}")
print()

# Find Quduri and Tahawi
quduri_uri = "0428AbuHusaynQuduri"
tahawi_uri = "0321Tahawi"

quduri_data = openiti_data['authors'][quduri_uri]
tahawi_data = openiti_data['authors'][tahawi_uri]

quduri_candidates = build_author_candidates_by_script(quduri_data)
tahawi_candidates = build_author_candidates_by_script(tahawi_data)

quduri_variants = quduri_candidates.get("lat", []) + quduri_candidates.get("ara", [])
tahawi_variants = tahawi_candidates.get("lat", []) + tahawi_candidates.get("ara", [])

print(f"Quduri variants: {quduri_variants}")
print(f"Tahawi variants: {tahawi_variants}")
print()

# Score each variant
print("=" * 120)
print("QUDURI VARIANTS")
print("=" * 120)

best_quduri = (0, "", "")
for variant in quduri_variants:
    if not variant:
        continue
    norm_variant = normalize_for_matching(variant, split_camelcase=True)
    fuzzy_score = fuzz.token_set_ratio(norm_creator, norm_variant)
    
    creator_tokens = set(norm_creator.split())
    variant_tokens = set(norm_variant.split())
    matched_tokens = creator_tokens & variant_tokens
    
    rare_tokens = [t for t in matched_tokens if idf_weights.get(t, 0.1) >= cfg.TOKEN_RARITY_THRESHOLD]
    
    if rare_tokens:
        boosted_score = fuzzy_score * cfg.RARE_TOKEN_BOOST_FACTOR
        boost_status = f"BOOSTED ({rare_tokens})"
    else:
        boosted_score = fuzzy_score
        boost_status = "no boost"
    
    if boosted_score > best_quduri[0]:
        best_quduri = (boosted_score, variant, boost_status)
    
    print(f"Variant: {variant}")
    print(f"  Normalized: {norm_variant}")
    print(f"  Matched tokens: {matched_tokens}")
    print(f"  Rare tokens: {rare_tokens}")
    print(f"  Fuzzy: {fuzzy_score:.1f} → {boosted_score:.1f} ({boost_status})")
    print()

print("=" * 120)
print("TAHAWI VARIANTS")
print("=" * 120)

best_tahawi = (0, "", "")
for variant in tahawi_variants:
    if not variant:
        continue
    norm_variant = normalize_for_matching(variant, split_camelcase=True)
    fuzzy_score = fuzz.token_set_ratio(norm_creator, norm_variant)
    
    creator_tokens = set(norm_creator.split())
    variant_tokens = set(norm_variant.split())
    matched_tokens = creator_tokens & variant_tokens
    
    rare_tokens = [t for t in matched_tokens if idf_weights.get(t, 0.1) >= cfg.TOKEN_RARITY_THRESHOLD]
    
    if rare_tokens:
        boosted_score = fuzzy_score * cfg.RARE_TOKEN_BOOST_FACTOR
        boost_status = f"BOOSTED ({rare_tokens})"
    else:
        boosted_score = fuzzy_score
        boost_status = "no boost"
    
    if boosted_score > best_tahawi[0]:
        best_tahawi = (boosted_score, variant, boost_status)
    
    print(f"Variant: {variant}")
    print(f"  Normalized: {norm_variant}")
    print(f"  Matched tokens: {matched_tokens}")
    print(f"  Rare tokens: {rare_tokens}")
    print(f"  Fuzzy: {fuzzy_score:.1f} → {boosted_score:.1f} ({boost_status})")
    print()

print("=" * 120)
print("SUMMARY")
print("=" * 120)
print(f"Best Quduri score:  {best_quduri[0]:.1f} ({best_quduri[2]})")
print(f"Best Tahawi score:  {best_tahawi[0]:.1f} ({best_tahawi[2]})")
print()

print("=" * 120)
print("IDF VALUES FOR CREATOR TOKENS")
print("=" * 120)

for token in norm_creator.split():
    idf = idf_weights.get(token, 0.1)
    is_rare = "RARE" if idf >= cfg.TOKEN_RARITY_THRESHOLD else "common"
    print(f"  {token:15} IDF={idf:7.3f} ({is_rare})")
