"""Inspect what candidates are being extracted."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from matching.config import BNF_SAMPLE_PATH
from parsers.bnf import load_bnf_records

# Load one record
bnf_records = load_bnf_records(BNF_SAMPLE_PATH)
bnf_id = "OAI_10030933"
record = bnf_records[bnf_id]

print(f"Record: {bnf_id}\n")

# Check which fields are being used (source of candidates)
print(f"Source field summary:")
print(f"  title_lat: {len(record.title_lat)} items")
print(f"  title_ara: {len(record.title_ara)} items")
print(f"  creator_lat: {len(record.creator_lat)} items")
print(f"  creator_ara: {len(record.creator_ara)} items")
print(f"  description_candidates_lat: {len(record.description_candidates_lat)} items")
print(f"  description_candidates_ara: {len(record.description_candidates_ara)} items")

# Raw candidates (no normalization)
raw = record.matching_candidates(norm_strategy="raw")
print(f"\nCandidates extracted (raw):")
print(f"  author/other candidates (lat): {len(raw.get('lat', []))} items")
print(f"  author/other candidates (ara): {len(raw.get('ara', []))} items")

# Fuzzy candidates (normalized)
fuzzy = record.matching_candidates(norm_strategy="fuzzy")
print(f"\nCandidates extracted (fuzzy normalized):")
print(f"  author/other candidates (lat): {len(fuzzy.get('lat', []))} items")
print(f"  author/other candidates (ara): {len(fuzzy.get('ara', []))} items")

# Check which source fields were included
print(f"\nNote: matching_candidates() includes:")
print(f"  - All title parts (split on '. ')")
print(f"  - All creators")
print(f"  - All description candidates")
print(f"  (contributor fields are NOT included)")
print(f"\nTotal source items (lat): {len(record.title_lat) + len(record.creator_lat) + len(record.description_candidates_lat)}")
