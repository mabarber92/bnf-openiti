"""
Debug fuzzy matching scores - show actual BNF vs OpenITI strings and their scores.
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

# Get target
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
creator_safe = creator.encode('ascii', 'replace').decode('ascii')[:80]
norm_safe = norm_creator.encode('ascii', 'replace').decode('ascii')[:80]

print("="*120)
print(f"FUZZY MATCH ANALYSIS - BNF Author")
print("="*120)
print(f"BNF Creator: {creator_safe}")
print(f"Normalized: {norm_safe}")
print(f"\nShowing actual fuzzy_score for each OpenITI author match:\n")

pipeline = MatchingPipeline({}, openiti_data, verbose=False)
matches = []

for author_uri, author_data in pipeline.openiti_index.authors.items():
    candidates = build_author_candidates_by_script(author_data)

    for script in ["lat", "ara"]:
        for author_str in candidates.get(script, []):
            if not author_str:
                continue

            norm_author = normalize_for_matching(author_str, split_camelcase=True)
            if not norm_author:
                continue

            # Calculate fuzzy score
            fuzzy_score = fuzz.token_set_ratio(norm_creator, norm_author)

            if fuzzy_score >= 80:
                author_safe = author_str.encode('ascii', 'replace').decode('ascii')[:40]
                norm_author_safe = norm_author.encode('ascii', 'replace').decode('ascii')[:40]
                matches.append((author_uri, author_safe, norm_author_safe, fuzzy_score))

# Sort by score descending
matches.sort(key=lambda x: x[3], reverse=True)

print(f"{'Score':>6} {'Author URI':<20} {'OpenITI Variant':<40} {'Normalized':<40}")
print("-" * 120)

for fuzzy_score, author_uri, author_str, norm_str in [(m[3], m[0], m[1], m[2]) for m in matches[:30]]:
    print(f"{fuzzy_score:>6.0f} {author_uri:<20} {author_str:<40} {norm_str:<40}")

print(f"\n\nTotal matches >= 80%: {len(matches)}")
print(f"At 100%: {sum(1 for m in matches if m[3] == 100.0)}")
print(f"At 90-99%: {sum(1 for m in matches if 90 <= m[3] < 100)}")
print(f"At 80-89%: {sum(1 for m in matches if 80 <= m[3] < 90)}")
