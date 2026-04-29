"""
Diagnose if IDF rarity_threshold (1.1) is actually being hit in test matches.

For each Stage 2 (title) match, calculate avg_matched_idf and show how many
matches would be penalized vs how many are above threshold.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
import matching.config as cfg

# Load data
bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

with open('data_samplers/correspondence.json') as f:
    correspondences = json.load(f)

test_pairs = {}
for item in correspondences:
    for openiti_uri, bnf_id in item.items():
        if bnf_id not in test_pairs:
            test_pairs[bnf_id] = []
        test_pairs[bnf_id].append(openiti_uri)

test_bnf_ids = [bid for bid in test_pairs.keys() if bid in bnf_records]
bnf_records_test = {bid: bnf_records[bid] for bid in test_bnf_ids}

# Set config for BOTH Author and Title IDF enabled
cfg.AUTHOR_THRESHOLD = 0.80
cfg.TITLE_THRESHOLD = 0.85
cfg.USE_AUTHOR_IDF_WEIGHTING = True
cfg.USE_TITLE_IDF_WEIGHTING = True
cfg.TOKEN_IDF_PENALTY_EXPONENT = 3

# Clear module cache
for mod in ['matching.pipeline', 'matching.author_matcher', 'matching.title_matcher', 'matching.combined_matcher', 'matching.classifier']:
    if mod in sys.modules:
        del sys.modules[mod]

from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier

# Run pipeline
pipeline = MatchingPipeline(bnf_records_test, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Now we need to inspect the IDF calculation inside TitleMatcher
# Run it again but capture intermediate IDF data

print("="*70)
print("IDF RARITY THRESHOLD DIAGNOSIS (BOTH AUTHOR AND TITLE)")
print("="*70)
print(f"\nRarity threshold: 1.1")
print(f"Penalty exponent: 3")
print(f"\nAnalyzing author and title matches for test set...\n")

# Re-run author matching with instrumentation
from matching.candidate_builders import build_author_candidates_by_script
from matching.normalize import normalize_for_matching
from matching.author_matcher import _build_token_idf_weights
import math
from collections import defaultdict

# Prepare author candidates
authors_candidates = {}
for author_uri, author_data in pipeline.openiti_index.authors.items():
    candidates = build_author_candidates_by_script(author_data)
    if candidates["lat"] or candidates["ara"]:
        authors_candidates[author_uri] = candidates

# Build IDF weights (same as in the pipeline)
from matching.config import BNF_FULL_PATH
full_bnf_records = load_bnf_records(BNF_FULL_PATH)
bnf_candidates_for_idf = {}
for bnf_id, bnf_record in full_bnf_records.items():
    creators_lat = getattr(bnf_record, "creator_lat", []) or []
    creators_ara = getattr(bnf_record, "creator_ara", []) or []
    for creator in creators_lat:
        if creator:
            key = f"bnf_{bnf_id}_lat_{len(bnf_candidates_for_idf)}"
            bnf_candidates_for_idf[key] = {"lat": [creator], "ara": []}
    for creator in creators_ara:
        if creator:
            key = f"bnf_{bnf_id}_ara_{len(bnf_candidates_for_idf)}"
            bnf_candidates_for_idf[key] = {"lat": [], "ara": [creator]}

combined_candidates = {**authors_candidates, **bnf_candidates_for_idf}
idf_weights = _build_token_idf_weights(combined_candidates)

# Prepare book candidates for title analysis
from matching.candidate_builders import build_book_candidates_by_script
books_candidates = {}
for book_uri, book_data in pipeline.openiti_index.books.items():
    candidates = build_book_candidates_by_script(book_data)
    if candidates["lat"] or candidates["ara"]:
        books_candidates[book_uri] = candidates

# Build combined IDF weights (both authors and titles, books and bnf)
bnf_candidates_for_idf_authors = {}
bnf_candidates_for_idf_titles = {}
for bnf_id, bnf_record in full_bnf_records.items():
    creators_lat = getattr(bnf_record, "creator_lat", []) or []
    creators_ara = getattr(bnf_record, "creator_ara", []) or []
    for creator in creators_lat:
        if creator:
            key = f"bnf_{bnf_id}_lat_{len(bnf_candidates_for_idf_authors)}"
            bnf_candidates_for_idf_authors[key] = {"lat": [creator], "ara": []}
    for creator in creators_ara:
        if creator:
            key = f"bnf_{bnf_id}_ara_{len(bnf_candidates_for_idf_authors)}"
            bnf_candidates_for_idf_authors[key] = {"lat": [], "ara": [creator]}

    titles_lat = getattr(bnf_record, "title_lat", []) or []
    titles_ara = getattr(bnf_record, "title_ara", []) or []
    for title in titles_lat:
        if title:
            key = f"bnf_{bnf_id}_lat_{len(bnf_candidates_for_idf_titles)}"
            bnf_candidates_for_idf_titles[key] = {"lat": [title], "ara": []}
    for title in titles_ara:
        if title:
            key = f"bnf_{bnf_id}_ara_{len(bnf_candidates_for_idf_titles)}"
            bnf_candidates_for_idf_titles[key] = {"lat": [], "ara": [title]}

combined_authors = {**authors_candidates, **bnf_candidates_for_idf_authors}
combined_titles = {**books_candidates, **bnf_candidates_for_idf_titles}
idf_weights_authors = _build_token_idf_weights(combined_authors)
idf_weights_titles = _build_token_idf_weights(combined_titles)

# Now analyze both author and title matches
threshold_hits_author = 0
above_threshold_author = 0
threshold_hits_title = 0
above_threshold_title = 0
match_details = []

for bnf_id, expected_uris in test_pairs.items():
    bnf_record = bnf_records_test[bnf_id]

    # AUTHOR MATCHES
    stage1_matches = pipeline.get_stage1_result(bnf_id)
    if stage1_matches:
        creators_lat = getattr(bnf_record, "creator_lat", []) or []
        creators_ara = getattr(bnf_record, "creator_ara", []) or []
        all_creators = creators_lat + creators_ara

        for creator_str in all_creators:
            # BNF creators are regular names, not OpenITI slugs - don't split camelcase
            norm_creator = normalize_for_matching(creator_str, split_camelcase=False)
            if not norm_creator:
                continue
            creator_tokens = set(norm_creator.lower().split())

            for author_uri in stage1_matches:
                author_data = pipeline.openiti_index.authors[author_uri]
                author_candidates = build_author_candidates_by_script(author_data)

                for script in ["lat", "ara"]:
                    for author_name in author_candidates.get(script, []):
                        if not author_name:
                            continue
                        # OpenITI author names may be slugs like IbnKhayyat - use camelcase splitting
                        norm_author_name = normalize_for_matching(author_name, split_camelcase=True)
                        if not norm_author_name:
                            continue

                        author_tokens = set(norm_author_name.lower().split())
                        matched_tokens = creator_tokens & author_tokens

                        if matched_tokens:
                            matched_idf_values = [idf_weights_authors.get(t, 0.1) for t in matched_tokens]
                            avg_matched_idf = sum(matched_idf_values) / len(matched_tokens)

                            rarity_threshold = 1.1
                            would_penalize = avg_matched_idf < rarity_threshold

                            match_details.append({
                                'type': 'AUTHOR',
                                'bnf_id': bnf_id,
                                'bnf_value': creator_str[:40],
                                'openiti_uri': author_uri,
                                'openiti_value': author_name[:40],
                                'avg_idf': avg_matched_idf,
                                'would_penalize': would_penalize,
                                'matched_tokens': len(matched_tokens),
                            })

                            if would_penalize:
                                threshold_hits_author += 1
                            else:
                                above_threshold_author += 1

    # TITLE MATCHES
    stage2_matches = pipeline.get_stage2_result(bnf_id)
    if stage2_matches:
        titles_lat = getattr(bnf_record, "title_lat", []) or []
        titles_ara = getattr(bnf_record, "title_ara", []) or []
        all_titles = titles_lat + titles_ara

        for title_str in all_titles:
            # BNF titles are regular text, not OpenITI slugs - don't split camelcase
            norm_title = normalize_for_matching(title_str, split_camelcase=False)
            if not norm_title:
                continue
            title_tokens = set(norm_title.lower().split())

            for book_uri in stage2_matches:
                book_data = pipeline.openiti_index.books[book_uri]
                book_candidates = build_book_candidates_by_script(book_data)

                for script in ["lat", "ara"]:
                    for book_title in book_candidates.get(script, []):
                        if not book_title:
                            continue
                        # OpenITI book titles may be slugs - use camelcase splitting
                        norm_book_title = normalize_for_matching(book_title, split_camelcase=True)
                        if not norm_book_title:
                            continue

                        book_tokens = set(norm_book_title.lower().split())
                        matched_tokens = title_tokens & book_tokens

                        if matched_tokens:
                            matched_idf_values = [idf_weights_titles.get(t, 0.1) for t in matched_tokens]
                            avg_matched_idf = sum(matched_idf_values) / len(matched_tokens)

                            rarity_threshold = 1.1
                            would_penalize = avg_matched_idf < rarity_threshold

                            match_details.append({
                                'type': 'TITLE',
                                'bnf_id': bnf_id,
                                'bnf_value': title_str[:40],
                                'openiti_uri': book_uri,
                                'openiti_value': book_title[:40],
                                'avg_idf': avg_matched_idf,
                                'would_penalize': would_penalize,
                                'matched_tokens': len(matched_tokens),
                            })

                            if would_penalize:
                                threshold_hits_title += 1
                            else:
                                above_threshold_title += 1

print(f"Total matches analyzed: {len(match_details)}")
print(f"\nAUTHOR MATCHES:")
print(f"  BELOW threshold (1.1): {threshold_hits_author} ({100*threshold_hits_author/(threshold_hits_author+above_threshold_author) if (threshold_hits_author+above_threshold_author) > 0 else 0:.1f}%)")
print(f"  ABOVE threshold (1.1): {above_threshold_author} ({100*above_threshold_author/(threshold_hits_author+above_threshold_author) if (threshold_hits_author+above_threshold_author) > 0 else 0:.1f}%)")

print(f"\nTITLE MATCHES:")
print(f"  BELOW threshold (1.1): {threshold_hits_title} ({100*threshold_hits_title/(threshold_hits_title+above_threshold_title) if (threshold_hits_title+above_threshold_title) > 0 else 0:.1f}%)")
print(f"  ABOVE threshold (1.1): {above_threshold_title} ({100*above_threshold_title/(threshold_hits_title+above_threshold_title) if (threshold_hits_title+above_threshold_title) > 0 else 0:.1f}%)")

# Split by type
author_matches = [m for m in match_details if m['type'] == 'AUTHOR']
title_matches = [m for m in match_details if m['type'] == 'TITLE']

print(f"\n" + "-"*70)
print("AUTHOR MATCHES BELOW THRESHOLD (would be penalized):")
print("-"*70)

below_author = [m for m in author_matches if m['would_penalize']]
for m in sorted(below_author, key=lambda x: x['avg_idf']):
    bnf_safe = m['bnf_value'][:30].encode('ascii', 'replace').decode('ascii')
    ot_safe = m['openiti_value'][:30].encode('ascii', 'replace').decode('ascii')
    print(f"  IDF={m['avg_idf']:.3f} ({m['matched_tokens']} tokens): {bnf_safe} -> {ot_safe}")

if not below_author:
    print("  (None - no author matches hit the threshold!)")

print(f"\n" + "-"*70)
print("TITLE MATCHES BELOW THRESHOLD (would be penalized):")
print("-"*70)

below_title = [m for m in title_matches if m['would_penalize']]
for m in sorted(below_title, key=lambda x: x['avg_idf']):
    bnf_safe = m['bnf_value'][:30].encode('ascii', 'replace').decode('ascii')
    ot_safe = m['openiti_value'][:30].encode('ascii', 'replace').decode('ascii')
    print(f"  IDF={m['avg_idf']:.3f} ({m['matched_tokens']} tokens): {bnf_safe} -> {ot_safe}")

if not below_title:
    print("  (None - no title matches hit the threshold!)")

print(f"\n" + "-"*70)
print("TOP AUTHOR MATCHES ABOVE THRESHOLD:")
print("-"*70)

above_author = [m for m in author_matches if not m['would_penalize']]
for m in sorted(above_author, key=lambda x: x['avg_idf'], reverse=True)[:5]:
    bnf_safe = m['bnf_value'][:30].encode('ascii', 'replace').decode('ascii')
    ot_safe = m['openiti_value'][:30].encode('ascii', 'replace').decode('ascii')
    print(f"  IDF={m['avg_idf']:.3f} ({m['matched_tokens']} tokens): {bnf_safe} -> {ot_safe}")

print(f"\n" + "-"*70)
print("TOP TITLE MATCHES ABOVE THRESHOLD:")
print("-"*70)

above_title = [m for m in title_matches if not m['would_penalize']]
for m in sorted(above_title, key=lambda x: x['avg_idf'], reverse=True)[:5]:
    bnf_safe = m['bnf_value'][:30].encode('ascii', 'replace').decode('ascii')
    ot_safe = m['openiti_value'][:30].encode('ascii', 'replace').decode('ascii')
    print(f"  IDF={m['avg_idf']:.3f} ({m['matched_tokens']} tokens): {bnf_safe} -> {ot_safe}")
