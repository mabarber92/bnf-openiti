"""Debug: show what each candidate normalizes to."""

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

print("Normalization results:\n")

seen = set()

added = 0
print("creator_lat (6):")
for i, creator in enumerate(bnf_record.creator_lat or []):
    norm = normalize(creator.strip().rstrip("."), "lat", "fuzzy")
    is_dup = norm in seen
    if norm:
        seen.add(norm)
        added += 1
    status = "DUP" if is_dup else ("OK" if norm else "EMPTY")
    print(f"  [{i}] {status}")
print(f"Added: {added}\n")

added = 0
print("contributor_lat (1):")
for i, contributor in enumerate(bnf_record.contributor_lat or []):
    norm = normalize(contributor.strip().rstrip("."), "lat", "fuzzy")
    is_dup = norm in seen
    if norm:
        seen.add(norm)
        added += 1
    status = "DUP" if is_dup else ("OK" if norm else "EMPTY")
    print(f"  [{i}] {status}")
print(f"Added: {added}\n")

added = 0
empty_count = 0
dup_count = 0
print("description_candidates_lat (10):")
for i, desc in enumerate(bnf_record.description_candidates_lat or []):
    raw = desc.strip().rstrip(".")
    norm = normalize(raw, "lat", "fuzzy")
    is_dup = norm in seen
    if norm:
        if not is_dup:
            seen.add(norm)
            added += 1
            status = "OK"
        else:
            dup_count += 1
            status = "DUP"
    else:
        empty_count += 1
        status = "EMPTY"
    print(f"  [{i}] {status}")
print(f"Added: {added}, Empty: {empty_count}, Duplicates: {dup_count}")

print(f"\nTotal unique normalized candidates: {len(seen)}")
