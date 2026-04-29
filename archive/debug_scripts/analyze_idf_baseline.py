"""
Analyze IDF baseline for all tokens in OpenITI corpus.

Shows:
1. All tokens with IDF scores
2. Current threshold (2.5) and which tokens trigger boosts
3. For OAI_11000520 specifically, which tokens matched and their scores
4. Distribution analysis to help choose better threshold
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import _build_token_idf_weights
from matching.candidate_builders import build_author_candidates_by_script
from matching.normalize import normalize_for_matching
import matching.config as cfg

# Load data
bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

# Get OAI_11000520
with open('data_samplers/correspondence.json') as f:
    correspondences = json.load(f)

target_bnf_id = None
for item in correspondences:
    for book_uri, bnf_id in item.items():
        if bnf_id == 'OAI_11000520':
            target_bnf_id = bnf_id
            break

bnf_records_test = {target_bnf_id: bnf_records[target_bnf_id]}

# Build OpenITI author candidates
pipeline = MatchingPipeline(bnf_records_test, openiti_data, verbose=False)
authors_candidates = {}
for author_uri, author_data in pipeline.openiti_index.authors.items():
    candidates = build_author_candidates_by_script(author_data)
    if candidates["lat"] or candidates["ara"]:
        authors_candidates[author_uri] = candidates

# Build IDF weights from OpenITI authors only
idf_weights = _build_token_idf_weights(authors_candidates)

print("="*80)
print("IDF BASELINE ANALYSIS")
print("="*80)
print(f"\nOpenITI authors in corpus: {len(authors_candidates)}")
print(f"Unique tokens: {len(idf_weights)}")
print(f"Current rarity threshold: {cfg.TOKEN_RARITY_THRESHOLD}")

# Find tokens that trigger the boost
rare_tokens = {t: idf for t, idf in idf_weights.items() if idf >= cfg.TOKEN_RARITY_THRESHOLD}
print(f"Tokens at or above threshold ({cfg.TOKEN_RARITY_THRESHOLD}): {len(rare_tokens)}")

# Get BNF record
bnf_record = bnf_records_test[target_bnf_id]
creators_lat = getattr(bnf_record, "creator_lat", []) or []
all_creators = creators_lat  # Just use Latin for simplicity

print(f"\n" + "="*80)
print(f"OAI_11000520 ANALYSIS")
print("="*80)

for creator in all_creators:
    # BNF creators are regular names, not OpenITI slugs - don't split camelcase
    norm_creator = normalize_for_matching(creator, split_camelcase=False)
    c_safe = creator.encode('ascii', 'replace').decode('ascii')[:60]
    cn_safe = norm_creator.encode('ascii', 'replace').decode('ascii')[:60]

    print(f"\nBNF Creator: {c_safe}")
    print(f"Normalized: {cn_safe}")

    creator_tokens = set(norm_creator.lower().split())
    print(f"Tokens: {creator_tokens}")

    print(f"\nToken IDF Analysis:")
    print(f"{'Token':<20} {'IDF':>10} {'Above Threshold?':>18}")
    print("-"*50)

    for token in sorted(creator_tokens):
        idf = idf_weights.get(token, 0.1)
        above = "YES" if idf >= cfg.TOKEN_RARITY_THRESHOLD else "NO"
        print(f"{token:<20} {idf:>10.4f} {above:>18}")

# Show IDF distribution
print(f"\n" + "="*80)
print("IDF DISTRIBUTION ACROSS ALL TOKENS")
print("="*80)

idf_values = sorted(idf_weights.values())
percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]

print(f"\nPercentile Analysis:")
for p in percentiles:
    idx = int(len(idf_values) * (p / 100))
    val = idf_values[idx]
    print(f"  {p:3d}th percentile: {val:.4f}")

print(f"\nCommon tokens (IDF < 1.5):")
common = {t: idf for t, idf in sorted(idf_weights.items(), key=lambda x: x[1]) if idf < 1.5}
for token, idf in list(common.items())[:20]:
    print(f"  '{token}': {idf:.4f}")

print(f"\nRare tokens (IDF >= 2.5):")
rare = {t: idf for t, idf in sorted(idf_weights.items(), key=lambda x: x[1], reverse=True) if idf >= 2.5}
print(f"Total: {len(rare)}")
for token, idf in list(rare.items())[:30]:
    print(f"  '{token}': {idf:.4f}")

# Recommendation
print(f"\n" + "="*80)
print("RECOMMENDATION")
print("="*80)

threshold_counts = {}
for t in [1.5, 2.0, 2.5, 3.0, 3.5]:
    count = len([idf for idf in idf_weights.values() if idf >= t])
    threshold_counts[t] = count
    pct = 100 * count / len(idf_weights)
    print(f"Threshold {t}: {count:5d} tokens ({pct:5.1f}%) would trigger boost")

print(f"\nFor OAI_11000520 BNF author tokens:")
for token in creator_tokens:
    idf = idf_weights.get(token, 0.1)
    print(f"  '{token}' IDF={idf:.4f}")
