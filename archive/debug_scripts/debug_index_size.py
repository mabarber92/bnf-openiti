"""Debug: check how many unique author candidates are in the BNF index."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from matching.bnf_index import BNFCandidateIndex
from parsers.bnf import load_bnf_records

# Load single record
bnf_records = load_bnf_records(BNF_FULL_PATH)
single_record = {
    "OAI_10030933": bnf_records["OAI_10030933"]
}

index = BNFCandidateIndex(single_record, norm_strategy="fuzzy")

print(f"Author candidates in index: {len(index.author_index)}")
print(f"First 10 candidates:")
for i, (cand, bnf_ids) in enumerate(list(index.author_index.items())[:10]):
    print(f"  [{i}] normalized candidate")
