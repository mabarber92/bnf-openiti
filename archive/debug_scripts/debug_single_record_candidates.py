"""Debug: show what candidates are extracted from one BNF record."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records

# Load test record
all_bnf = load_bnf_records(BNF_FULL_PATH)
test_record_id = "OAI_10030933"
bnf_record = all_bnf[test_record_id]

print(f"BNF Record: {test_record_id}")
print(f"Type: {type(bnf_record)}")

# Get raw candidates
raw_cands = bnf_record.matching_candidates(norm_strategy="raw")
print(f"\nRaw candidates extracted by matching_candidates():")
print(f"  Latin: {len(raw_cands['lat'])} items")
print(f"  Arabic: {len(raw_cands['ara'])} items")

# Show Latin candidates (count only, skip problematic printing)
print(f"\nLatin candidates: (skipping display due to Unicode)")

# Also check what the original test would extract
print(f"\n\n=== ORIGINAL TEST LOGIC ===")

# Replicate the original test's build_bnf_author_candidates
def build_bnf_author_candidates(record):
    candidates = {"lat": [], "ara": []}
    for creator in record.get("creator_lat", []) or []:
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)
    for creator in record.get("creator_ara", []) or []:
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)
    for contrib in record.get("contributor_lat", []) or []:
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)
    for contrib in record.get("contributor_ara", []) or []:
        if contrib and contrib not in candidates["ara"]:
            candidates["ara"].append(contrib)
    for desc in record.get("description_candidates_lat", []) or []:
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)
    for desc in record.get("description_candidates_ara", []) or []:
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)
    return candidates

# Get raw BNF data (convert dataclass back to dict for this)
bnf_dict = {
    "creator_lat": bnf_record.creator_lat or [],
    "creator_ara": bnf_record.creator_ara or [],
    "contributor_lat": bnf_record.contributor_lat or [],
    "contributor_ara": bnf_record.contributor_ara or [],
    "description_candidates_lat": bnf_record.description_candidates_lat or [],
    "description_candidates_ara": bnf_record.description_candidates_ara or [],
}

orig_cands = build_bnf_author_candidates(bnf_dict)
print(f"\nOriginal test's build_bnf_author_candidates():")
print(f"  Latin: {len(orig_cands['lat'])} items")
print(f"  Arabic: {len(orig_cands['ara'])} items")

print(f"\nLatin candidates from original logic: (skipping display due to Unicode)")

# Compare
print(f"\n\n=== COMPARISON ===")
print(f"Raw candidates (matching_candidates): {len(raw_cands['lat'])} Latin")
print(f"Original test (build_bnf_author_candidates): {len(orig_cands['lat'])} Latin")
print(f"Match: {raw_cands['lat'] == orig_cands['lat']}")
