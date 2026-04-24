"""Debug: show exact candidates from BNF index vs. matching_candidates method."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records
from matching.bnf_index import BNFCandidateIndex

bnf_records = load_bnf_records(BNF_FULL_PATH)
test_record_id = "OAI_10030933"
bnf_record = bnf_records[test_record_id]

# What matching_candidates returns
print("matching_candidates output:")
cands_raw = bnf_record.matching_candidates(norm_strategy="raw")
print(f"  lat: {len(cands_raw.get('lat', []))} candidates")
print(f"  ara: {len(cands_raw.get('ara', []))} candidates")

# What the BNF index returns (normalized)
bnf_index = BNFCandidateIndex({test_record_id: bnf_record}, norm_strategy="fuzzy")
print("\nBNFCandidateIndex output:")
print(f"  Total unique candidates: {len(bnf_index.author_index)}")

# Get all candidates from the index
for candidate, bnf_ids in bnf_index.author_index.items():
    print(f"  '{candidate}' -> {bnf_ids}")

# Check specific candidates
print("\nDirect checking what matching_candidates returns:")
for i, cand in enumerate(cands_raw.get('lat', [])[:5]):
    print(f"  [{i}] '{cand}'")
