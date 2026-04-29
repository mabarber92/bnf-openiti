"""
Diagnose which URIs are getting rare token boosts in stage 1 matching.

Shows for each matched author URI:
- Raw fuzzy score
- Whether it has rare tokens
- Which rare tokens it has
- Boosted score
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher, _build_token_idf_weights
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.normalize import normalize_for_matching
from matching.candidate_builders import build_author_candidates_by_script
from fuzzywuzzy import fuzz
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

# Build IDF weights
pipeline = MatchingPipeline(bnf_records_test, openiti_data, verbose=False)
authors_candidates = {}
for author_uri, author_data in pipeline.openiti_index.authors.items():
    candidates = build_author_candidates_by_script(author_data)
    if candidates["lat"] or candidates["ara"]:
        authors_candidates[author_uri] = candidates

idf_weights = _build_token_idf_weights(authors_candidates)

# Get BNF creators
bnf_record = bnf_records_test[target_bnf_id]
creators_lat = getattr(bnf_record, "creator_lat", []) or []
creators = [c for c in creators_lat if c][:1]  # Just first creator

print("="*100)
print(f"RARE TOKEN BOOST ANALYSIS: {target_bnf_id}")
print("="*100)

if creators:
    creator = creators[0]
    norm_creator = normalize_for_matching(creator, split_camelcase=False)
    creator_safe = creator.encode('ascii', 'replace').decode('ascii')[:60]
    norm_safe = norm_creator.encode('ascii', 'replace').decode('ascii')
    print(f"\nBNF Creator: {creator_safe}")
    print(f"Normalized: {norm_safe}")
    creator_tokens = set(norm_creator.lower().split())
    print(f"Tokens: {creator_tokens}\n")

    print("Analyzing scores for all OpenITI authors:")
    print(f"{'Rank':<5} {'URI':<25} {'Raw Score':>10} {'Rare Tokens':>30} {'Boosted Score':>15}")
    print("-" * 100)

    scored_authors = []

    for author_uri, author_candidates_by_script in authors_candidates.items():
        best_raw = 0
        best_boosted = 0
        rare_tokens_found = []

        for script in ["lat", "ara"]:
            for author_str in author_candidates_by_script.get(script, []):
                if not author_str:
                    continue

                norm_author = normalize_for_matching(author_str, split_camelcase=True)
                if not norm_author:
                    continue

                # Get raw score
                raw_score = fuzz.token_set_ratio(norm_creator, norm_author)

                # Check for rare tokens
                author_tokens = set(norm_author.lower().split())
                matched_tokens = creator_tokens & author_tokens
                rare_in_match = [t for t in matched_tokens if idf_weights.get(t, 0.1) >= cfg.TOKEN_RARITY_THRESHOLD]

                # Calculate boosted score
                if rare_in_match:
                    boosted_score = raw_score * cfg.RARE_TOKEN_BOOST_FACTOR
                else:
                    boosted_score = raw_score

                if raw_score > best_raw:
                    best_raw = raw_score
                    best_boosted = boosted_score
                    rare_tokens_found = rare_in_match

        if best_raw >= cfg.AUTHOR_THRESHOLD * 100:
            scored_authors.append((author_uri, best_raw, best_boosted, rare_tokens_found))

    # Sort by boosted score descending
    scored_authors.sort(key=lambda x: x[2], reverse=True)

    for rank, (author_uri, raw, boosted, rare_tokens) in enumerate(scored_authors[:25], 1):
        rare_str = f"{rare_tokens}" if rare_tokens else "(no rare tokens)"
        boosted_status = "[BOOSTED]" if boosted > raw else ""
        print(f"{rank:<5} {author_uri:<25} {raw:>10.1f} {rare_str:>30} {boosted:>14.1f} {boosted_status}")

    print(f"\n" + "="*100)
    print(f"Summary:")
    print(f"  Total authors passing threshold: {len(scored_authors)}")
    boosted_count = sum(1 for _, raw, boosted, _ in scored_authors if boosted > raw)
    print(f"  Authors with rare token boost: {boosted_count}")
    print(f"  Authors without boost (common-only match): {len(scored_authors) - boosted_count}")

    # Find Quduri specifically
    for rank, (author_uri, raw, boosted, rare_tokens) in enumerate(scored_authors, 1):
        if 'quduri' in author_uri.lower():
            print(f"\n  Quduri match found at rank {rank}: {author_uri}")
            print(f"    Raw score: {raw:.1f}, Boosted score: {boosted:.1f}")
            print(f"    Rare tokens: {rare_tokens}")
            break
