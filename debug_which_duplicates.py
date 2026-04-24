"""Debug: identify which normalized candidates are duplicating."""

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

print("Tracking normalized values to find duplicates:\n")

norm_to_raw = {}

# Process creators
for creator in (bnf_record.creator_lat or []):
    raw = creator.strip().rstrip(".")
    norm = normalize(raw, "lat", "fuzzy")
    if norm:
        if norm not in norm_to_raw:
            norm_to_raw[norm] = []
        norm_to_raw[norm].append(("creator", raw[:40]))

# Process contributors
for contributor in (bnf_record.contributor_lat or []):
    raw = contributor.strip().rstrip(".")
    norm = normalize(raw, "lat", "fuzzy")
    if norm:
        if norm not in norm_to_raw:
            norm_to_raw[norm] = []
        norm_to_raw[norm].append(("contributor", raw[:40]))

# Process descriptions
for i, desc in enumerate(bnf_record.description_candidates_lat or []):
    raw = desc.strip().rstrip(".")
    norm = normalize(raw, "lat", "fuzzy")
    if norm:
        if norm not in norm_to_raw:
            norm_to_raw[norm] = []
            status = "UNIQUE"
        else:
            status = "DUPLICATE"
        norm_to_raw[norm].append(("desc[%d]" % i, len(raw)))
        print(f"  desc[{i}] (raw len {len(raw):3}) -> {status}")

print(f"\nTotal unique normalized values: {len(norm_to_raw)}")
print(f"Total sources (creator + contributor + desc): {sum(len(v) for v in norm_to_raw.values())}")
