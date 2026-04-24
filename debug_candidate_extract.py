"""Debug: show exact candidates extracted by each method."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records

bnf_records = load_bnf_records(BNF_FULL_PATH)
test_record_id = "OAI_10030933"
bnf_record = bnf_records[test_record_id]

# Original test method
def build_bnf_author_candidates_original(bnf_record_dict):
    candidates = {"lat": []}
    for creator in bnf_record_dict.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)
    for contrib in bnf_record_dict.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)
    for desc in bnf_record_dict.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)
    return candidates

# Our method
orig_cands = build_bnf_author_candidates_original({
    "creator_lat": bnf_record.creator_lat or [],
    "contributor_lat": bnf_record.contributor_lat or [],
    "description_candidates_lat": bnf_record.description_candidates_lat or [],
})

our_cands = bnf_record.matching_candidates(norm_strategy="raw")

print(f"Original method: {len(orig_cands['lat'])} candidates")
print(f"Our method: {len(our_cands['lat'])} candidates")

# Check if they're the same
orig_set = set(orig_cands['lat'])
our_set = set(our_cands['lat'])

print(f"\nSame candidates: {orig_set == our_set}")
print(f"In original but not ours: {orig_set - our_set}")
print(f"In ours but not original: {our_set - orig_set}")
