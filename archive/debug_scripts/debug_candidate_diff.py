"""Debug what candidates differ between methods."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records

bnf_records = load_bnf_records(BNF_FULL_PATH)

test_ids = ["OAI_10884186", "OAI_11001068"]

for bnf_id in test_ids:
    record = bnf_records[bnf_id]
    
    # Original method
    orig_lat = []
    orig_ara = []
    for creator in record.creator_lat or []:
        if creator and creator not in orig_lat:
            orig_lat.append(creator)
    for contrib in record.contributor_lat or []:
        if contrib and contrib not in orig_lat:
            orig_lat.append(contrib)
    for desc in record.description_candidates_lat or []:
        if desc and desc not in orig_lat:
            orig_lat.append(desc)
    for creator in record.creator_ara or []:
        if creator and creator not in orig_ara:
            orig_ara.append(creator)
    for contrib in record.contributor_ara or []:
        if contrib and contrib not in orig_ara:
            orig_ara.append(contrib)
    for desc in record.description_candidates_ara or []:
        if desc and desc not in orig_ara:
            orig_ara.append(desc)
    
    # matching_candidates method
    cands = record.matching_candidates(norm_strategy="raw")
    
    print(f"{bnf_id}:")
    print(f"  Original: {len(orig_lat)} lat, {len(orig_ara)} ara")
    print(f"  matching_candidates: {len(cands.get('lat', []))} lat, {len(cands.get('ara', []))} ara")
    
    # Check if they're the same
    if orig_lat == cands.get('lat', []) and orig_ara == cands.get('ara', []):
        print("  MATCH")
    else:
        print("  MISMATCH")
        if orig_lat != cands.get('lat', []):
            print(f"    lat differs: {len(set(orig_lat) - set(cands.get('lat', [])))} in orig only, {len(set(cands.get('lat', [])) - set(orig_lat))} in cands only")
        if orig_ara != cands.get('ara', []):
            print(f"    ara differs: {len(set(orig_ara) - set(cands.get('ara', [])))} in orig only, {len(set(cands.get('ara', [])) - set(orig_ara))} in cands only")
