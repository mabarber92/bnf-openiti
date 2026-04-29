"""
Debug: show individual component scores for specific authors.
"""

import json
import sys
sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.normalize import normalize_for_matching
from matching.candidate_builders import build_author_candidates_by_script
from fuzzywuzzy import fuzz
import matching.config as cfg

bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

with open('data_samplers/correspondence.json') as f:
    correspondences = json.load(f)
target_bnf_id = None
for item in correspondences:
    for book_uri, bnf_id in item.items():
        if bnf_id == 'OAI_11000520':
            target_bnf_id = bnf_id
            break

bnf_record = bnf_records[target_bnf_id]
creators_lat = getattr(bnf_record, "creator_lat", []) or []
creator = creators_lat[0] if creators_lat else ""
norm_creator = normalize_for_matching(creator, split_camelcase=False)

pipeline = MatchingPipeline({}, openiti_data, verbose=False)

# Check specific authors
test_uris = ["0428AbuHusaynQuduri", "0207Waqidi"]

for author_uri in test_uris:
    author_data = pipeline.openiti_index.authors.get(author_uri)
    if not author_data:
        continue

    candidates = build_author_candidates_by_script(author_data)

    print(f"\n{'='*100}")
    print(f"Author: {author_uri}")
    print(f"{'='*100}")

    all_scores = []
    for script in ["lat", "ara"]:
        for author_str in candidates.get(script, []):
            if not author_str:
                continue
            norm_author = normalize_for_matching(author_str, split_camelcase=True)
            if not norm_author:
                continue

            score = fuzz.token_set_ratio(norm_creator, norm_author)
            author_safe = author_str.encode('ascii', 'replace').decode('ascii')[:50]
            norm_safe = norm_author.encode('ascii', 'replace').decode('ascii')[:50]

            all_scores.append(score)
            print(f"  {author_safe:<50} > {norm_safe:<50} Score: {score:>6.1f}")

    print(f"\nComponent scores: {all_scores}")

    # Average ALL scores (not just good ones)
    if all_scores:
        combined = sum(all_scores) / len(all_scores)
    else:
        combined = 0

    print(f"Combined (average of ALL scores): {combined:.1f}")
    print(f"In 0-1 range: {combined/100:.3f}")
