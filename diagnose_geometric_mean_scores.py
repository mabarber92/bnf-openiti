"""
Show how geometric mean combining of component scores affects author matching.
"""

import json
import sys
sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import _match_author_candidate, _build_token_idf_weights
from matching.candidate_builders import build_author_candidates_by_script
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

# Build author candidates
pipeline = MatchingPipeline({}, openiti_data, verbose=False)
authors_candidates = {}
for author_uri, author_data in pipeline.openiti_index.authors.items():
    candidates = build_author_candidates_by_script(author_data)
    if candidates["lat"] or candidates["ara"]:
        authors_candidates[author_uri] = candidates

# Build IDF weights
idf_weights = _build_token_idf_weights(authors_candidates)

# Run matching with geometric mean
candidate, matched_authors = _match_author_candidate(
    creator,
    authors_candidates,
    threshold=cfg.AUTHOR_THRESHOLD,
    idf_weights=idf_weights
)

creator_safe = creator.encode('ascii', 'replace').decode('ascii')[:80]
print("="*120)
print(f"GEOMETRIC MEAN COMBINED SCORES - BNF Author")
print("="*120)
print(f"BNF Creator: {creator_safe}\n")

# Sort by score descending
sorted_matches = sorted(matched_authors.items(), key=lambda x: x[1], reverse=True)

print(f"{'Rank':<5} {'Author URI':<20} {'Combined Score':>15} {'Status':<15}")
print("-" * 60)

for rank, (author_uri, score) in enumerate(sorted_matches[:30], 1):
    is_quduri = "CORRECT" if "quduri" in author_uri.lower() else ""
    print(f"{rank:<5} {author_uri:<20} {score:>15.3f}  {is_quduri:<15}")

print(f"\n\nTotal matches >= threshold: {len(matched_authors)}")
print(f"\nQuduri rank: ", end="")
for rank, (author_uri, score) in enumerate(sorted_matches, 1):
    if "quduri" in author_uri.lower():
        print(f"{rank} with score {score:.3f}")
        break
else:
    print("NOT FOUND")
