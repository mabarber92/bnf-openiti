"""Debug: trace through matching_candidates() line by line."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records

# Load test record
all_bnf = load_bnf_records(BNF_FULL_PATH)
test_record_id = "OAI_10030933"
bnf_record = all_bnf[test_record_id]

# Replicate matching_candidates() logic exactly
lat = []
ara = []
seen_lat = set()
seen_ara = set()

def add(raw, script, dest, seen):
    raw = raw.strip().rstrip(".")
    if not raw:
        print(f"    SKIP: empty after strip")
        return
    if raw not in seen:
        seen.add(raw)
        dest.append(raw)
        print(f"    ADD")
    else:
        print(f"    SKIP: already in seen")

print(f"bnf_record.creator_lat = {type(bnf_record.creator_lat)}, len = {len(bnf_record.creator_lat or [])}")
print("Processing creator_lat:")
for i, creator in enumerate(bnf_record.creator_lat or []):
    print(f"  [{i}] (len {len(creator)})")
    add(creator, "lat", lat, seen_lat)

print(f"\nbnf_record.contributor_lat = {type(bnf_record.contributor_lat)}, len = {len(bnf_record.contributor_lat or [])}")
print("Processing contributor_lat:")
for i, contrib in enumerate(bnf_record.contributor_lat or []):
    print(f"  [{i}] (len {len(contrib)})")
    add(contrib, "lat", lat, seen_lat)

print(f"\nbnf_record.description_candidates_lat = {type(bnf_record.description_candidates_lat)}, len = {len(bnf_record.description_candidates_lat or [])}")
print("Processing description_candidates_lat:")
for i, desc in enumerate(bnf_record.description_candidates_lat or []):
    print(f"  [{i}] (len {len(desc)})")
    add(desc, "lat", lat, seen_lat)

print(f"\nTotal: {len(lat)} candidates")
print(f"Expected: 17")
