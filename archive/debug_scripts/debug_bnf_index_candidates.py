"""Check what candidates are in the BNF index."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records
from matching.bnf_index import BNFCandidateIndex

bnf_records = load_bnf_records(BNF_FULL_PATH)
test_ids = ["OAI_10884186", "OAI_11001068"]

for bnf_id in test_ids:
    record = bnf_records[bnf_id]
    
    # Build index
    bnf_index = BNFCandidateIndex({bnf_id: record}, norm_strategy="fuzzy")
    
    # Get candidates from index
    index_cands = {}
    for candidate, bnf_ids in bnf_index.author_candidates_iter():
        # Count the characters to see if it's different
        index_cands[candidate] = len(candidate)
    
    print(f"{bnf_id}:")
    print(f"  Index has {len(index_cands)} author candidates")
    
    # Get candidates from record directly
    record_cands_raw = record.matching_candidates(norm_strategy="raw")
    print(f"  Record has {len(record_cands_raw.get('lat', []))} lat + {len(record_cands_raw.get('ara', []))} ara candidates")
