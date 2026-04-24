"""Debug: count candidates without printing problematic strings."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records
from utils.normalize import normalize

# Load test record
all_bnf = load_bnf_records(BNF_FULL_PATH)
test_record_id = "OAI_10030933"
bnf_record = all_bnf[test_record_id]

print(f"Replicating matching_candidates() logic:\n")

lat = []
ara = []
seen_lat = set()
seen_ara = set()

def add(raw, script, dest, seen):
    raw = raw.strip().rstrip(".")
    if not raw:
        return False
    if raw not in seen:
        seen.add(raw)
        norm = normalize(raw, script, "fuzzy")
        if norm:
            dest.append(norm)
            return True
    return False

added = 0

print(f"creator_lat ({len(bnf_record.creator_lat or [])}):")
for creator in (bnf_record.creator_lat or []):
    if add(creator, "lat", lat, seen_lat):
        added += 1
        print(f"  + added (total so far: {len(lat)})")
print(f"Total added: {added}")

added = 0
print(f"\ncontributor_lat ({len(bnf_record.contributor_lat or [])}):")
for contributor in (bnf_record.contributor_lat or []):
    if add(contributor, "lat", lat, seen_lat):
        added += 1
        print(f"  + added (total so far: {len(lat)})")
print(f"Total added: {added}")

added = 0
print(f"\ndescription_candidates_lat ({len(bnf_record.description_candidates_lat or [])}):")
for desc in (bnf_record.description_candidates_lat or []):
    if add(desc, "lat", lat, seen_lat):
        added += 1
        print(f"  + added (total so far: {len(lat)})")
    else:
        print(f"  - skipped or empty")
print(f"Total added: {added}")

print(f"\n\nFinal count: {len(lat)}")
print(f"Expected: 17 (6 creator + 1 contributor + 10 description)")

# Also check what matching_candidates returns
cands = bnf_record.matching_candidates(norm_strategy="raw")
print(f"matching_candidates() returns: {len(cands['lat'])} candidates")

# And compare with the original test logic
print(f"\n\nOriginal test logic:")

def build_bnf_author_candidates(bnf_record):
    candidates = {"lat": [], "ara": []}
    for creator in (bnf_record.get("creator_lat") or []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)
    for contrib in (bnf_record.get("contributor_lat") or []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)
    for desc in (bnf_record.get("description_candidates_lat") or []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)
    return candidates

bnf_dict = {
    "creator_lat": bnf_record.creator_lat or [],
    "contributor_lat": bnf_record.contributor_lat or [],
    "description_candidates_lat": bnf_record.description_candidates_lat or [],
}

orig_cands = build_bnf_author_candidates(bnf_dict)
print(f"Original test would extract: {len(orig_cands['lat'])} raw candidates")
