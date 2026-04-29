"""
Diagnose which rare tokens are triggering boosts in OAI_11000520 stage 1 matching.

For each of the top 20 stage 1 matches, analyze:
- Which tokens matched between BNF and OpenITI
- Which of those are "rare" (IDF >= TOKEN_RARITY_THRESHOLD)
- Why they got 100% score
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

# Build IDF weights (same as stage 1 uses)
full_bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
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

# Prepare OpenITI authors
from matching.pipeline import MatchingPipeline as MP
# Use the OpenITI index from pipeline
pipeline_temp = MP(bnf_records_test, openiti_data, verbose=False)
authors_candidates = {}
for author_uri, author_data in pipeline_temp.openiti_index.authors.items():
    candidates = build_author_candidates_by_script(author_data)
    if candidates["lat"] or candidates["ara"]:
        authors_candidates[author_uri] = candidates

# Build IDF weights
combined_candidates = {**authors_candidates, **bnf_candidates_for_idf}
idf_weights = _build_token_idf_weights(combined_candidates)

print("="*80)
print(f"RARE TOKEN ANALYSIS: {target_bnf_id}")
print("="*80)

# Get BNF record and show creators
bnf_record = bnf_records_test[target_bnf_id]
creators_lat = getattr(bnf_record, "creator_lat", []) or []
creators_ara = getattr(bnf_record, "creator_ara", []) or []
all_creators = creators_lat + creators_ara

print(f"\nBNF CREATORS:")
for creator in all_creators[:2]:
    c_safe = creator.encode('ascii', 'replace').decode('ascii') if creator else "None"
    # BNF creators are regular names, not OpenITI slugs - don't split camelcase
    norm = normalize_for_matching(creator, split_camelcase=False)
    norm_safe = norm.encode('ascii', 'replace').decode('ascii') if norm else "None"
    print(f"  Original: {c_safe[:60]}")
    print(f"  Normalized: {norm_safe}")
    print(f"  Tokens: {set(norm_safe.lower().split())}")

# Run pipeline to get stage 1 results
pipeline = MatchingPipeline(bnf_records_test, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

stage1_authors = pipeline.get_stage1_result(target_bnf_id) or []
stage1_scores = pipeline.get_stage1_scores(target_bnf_id) or {}

print(f"\n" + "="*80)
print("TOP 20 STAGE 1 MATCHES - RARE TOKEN ANALYSIS")
print("="*80)

sorted_authors = sorted(stage1_authors, key=lambda x: stage1_scores.get(x, 0), reverse=True)

for rank, author_uri in enumerate(sorted_authors[:20], 1):
    score = stage1_scores.get(author_uri, 0)
    author = pipeline_temp.openiti_index.authors.get(author_uri)

    # Get author candidates
    author_candidates = build_author_candidates_by_script(author)

    print(f"\n{rank:2d}. Score={score:.1%} | {author_uri}")

    # Check against all creator variants
    for creator in all_creators:
        # BNF creators are regular names, not OpenITI slugs - don't split camelcase
        norm_creator = normalize_for_matching(creator, split_camelcase=False)
        creator_tokens = set(norm_creator.lower().split())

        # Check against all author variants
        for script in ["lat", "ara"]:
            for author_name in author_candidates.get(script, []):
                if not author_name:
                    continue
                # OpenITI author names may be slugs like IbnKhayyat - use camelcase splitting
                norm_author = normalize_for_matching(author_name, split_camelcase=True)
                author_tokens = set(norm_author.lower().split())

                # Find matched tokens
                matched_tokens = creator_tokens & author_tokens

                if matched_tokens:
                    # Check which are rare
                    rare_tokens = [t for t in matched_tokens if idf_weights.get(t, 0.1) >= cfg.TOKEN_RARITY_THRESHOLD]

                    print(f"    {script}: matched={matched_tokens}")
                    if rare_tokens:
                        print(f"      RARE TOKENS FOUND: {rare_tokens}")
                        for t in rare_tokens:
                            idf = idf_weights.get(t, 0.1)
                            print(f"        '{t}' IDF={idf:.3f}")
                    else:
                        print(f"      (no rare tokens, common match)")

print(f"\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Total stage 1 matches: {len(stage1_authors)}")
print(f"All at 100%: {sum(1 for s in stage1_scores.values() if s >= 0.999)}")
print(f"Range: {min(stage1_scores.values()):.1%} - {max(stage1_scores.values()):.1%}")
