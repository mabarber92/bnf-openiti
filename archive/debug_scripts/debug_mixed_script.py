"""Find mixed-script candidates in problematic records."""

import sys
import re
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records

bnf_records = load_bnf_records(BNF_FULL_PATH)

# Check problematic records
test_ids = ["OAI_10884186", "OAI_11001068"]

ara_pattern = r"[\u0600-\u06FF\u0750-\u077F]+"
lat_pattern = r"[A-Za-z0-9\u0100-\u017F\u0180-\u024F]+"

for bnf_id in test_ids:
    record = bnf_records[bnf_id]
    
    # Get raw candidates
    cands = record.matching_candidates(norm_strategy="raw")
    
    print(f"\n{bnf_id}:")
    print(f"  Total: {len(cands.get('lat', []))} lat + {len(cands.get('ara', []))} ara")
    
    # Check for mixed-script in each
    mixed_count = 0
    for script in ["lat", "ara"]:
        for cand in cands.get(script, []):
            has_ara = bool(re.search(ara_pattern, cand))
            has_lat = bool(re.search(lat_pattern, cand))
            if has_ara and has_lat:
                mixed_count += 1
    
    print(f"  Mixed-script candidates: {mixed_count}")
