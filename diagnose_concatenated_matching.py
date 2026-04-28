"""
Show fuzzy matching with concatenated name components per script.
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

creator_safe = creator.encode('ascii', 'replace').decode('ascii')[:80]
norm_safe = norm_creator.encode('ascii', 'replace').decode('ascii')[:80]

print("="*120)
print(f"CONCATENATED COMPONENT MATCHING - BNF Author")
print("="*120)
print(f"BNF Creator: {creator_safe}")
print(f"Normalized: {norm_safe}\n")

pipeline = MatchingPipeline({}, openiti_data, verbose=False)
matches = []

for author_uri, author_data in pipeline.openiti_index.authors.items():
    candidates = build_author_candidates_by_script(author_data)
    best_score = 0

    for script in ["lat", "ara"]:
        cands = candidates.get(script, [])
        if not cands:
            continue

        # Normalize and concatenate all candidates for this script
        normalized = []
        for c in cands:
            if c:
                norm_c = normalize_for_matching(c, split_camelcase=True)
                if norm_c:
                    normalized.append(norm_c)

        if not normalized:
            continue

        combined_str = " ".join(normalized)
        score = fuzz.token_set_ratio(norm_creator, combined_str)

        if score > best_score:
            best_score = score
            best_combined = combined_str[:80]  # for display

    if best_score >= 80:
        matches.append((author_uri, best_score, best_combined))

# Sort by score descending
matches.sort(key=lambda x: x[1], reverse=True)

print(f"{'Rank':<5} {'Author URI':<20} {'Score':>6} {'Concatenated String (first 80 chars)':<80}")
print("-" * 120)

for rank, (author_uri, score, combined) in enumerate(matches[:30], 1):
    is_quduri = " <-- CORRECT" if "quduri" in author_uri.lower() else ""
    combined_safe = combined.encode('ascii', 'replace').decode('ascii')
    print(f"{rank:<5} {author_uri:<20} {score:>6.0f} {combined_safe:<80}{is_quduri}")

print(f"\n\nTotal matches >= 80%: {len(matches)}")

for rank, (author_uri, score, _) in enumerate(matches, 1):
    if "quduri" in author_uri.lower():
        print(f"Quduri rank: {rank} with score {score:.0f}")
        break
