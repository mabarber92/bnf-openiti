"""Debug: identify which description_candidates are duplicates."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records

# Load test record
all_bnf = load_bnf_records(BNF_FULL_PATH)
test_record_id = "OAI_10030933"
bnf_record = all_bnf[test_record_id]

print("Finding duplicates in description_candidates_lat:\n")

creators = set(bnf_record.creator_lat or [])
contributors = set(bnf_record.contributor_lat or [])
descriptions = bnf_record.description_candidates_lat or []

print(f"creators: {len(creators)}")
print(f"contributors: {len(contributors)}")
print(f"descriptions: {len(descriptions)}")

all_raw = set(creators) | set(contributors)

for i, desc in enumerate(descriptions):
    if desc in all_raw:
        # Find what it matches
        if desc in creators:
            what = "creator"
        elif desc in contributors:
            what = "contributor"
        print(f"  [{i}] DUPLICATE of {what}: (skipped in our logic)")
    else:
        print(f"  [{i}] unique")

print(f"\nSo the original test would extract:")
print(f"  6 creators")
print(f"  1 contributor  ")
print(f"  {sum(1 for d in descriptions if d not in all_raw)} unique descriptions")
print(f"  Total: {6 + 1 + sum(1 for d in descriptions if d not in all_raw)}")
