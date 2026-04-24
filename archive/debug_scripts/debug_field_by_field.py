"""Debug: show field-by-field candidate counts."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH
from parsers.bnf import load_bnf_records

# Load test record
all_bnf = load_bnf_records(BNF_FULL_PATH)
test_record_id = "OAI_10030933"
bnf_record = all_bnf[test_record_id]

print(f"BNF Record: {test_record_id}\n")

# Check each field
print("Field counts (RAW data):")
print(f"  creator_lat: {len(bnf_record.creator_lat or [])}")
print(f"  creator_ara: {len(bnf_record.creator_ara or [])}")
print(f"  contributor_lat: {len(bnf_record.contributor_lat or [])}")
print(f"  contributor_ara: {len(bnf_record.contributor_ara or [])}")
print(f"  description_candidates_lat: {len(bnf_record.description_candidates_lat or [])}")
print(f"  description_candidates_ara: {len(bnf_record.description_candidates_ara or [])}")
print(f"  title_lat: {len(bnf_record.title_lat or [])}")
print(f"  title_ara: {len(bnf_record.title_ara or [])}")

print(f"\nTotal author-related: {len(bnf_record.creator_lat or []) + len(bnf_record.contributor_lat or []) + len(bnf_record.description_candidates_lat or [])}")

# Check for empty/None values
print(f"\n\nChecking for empty/None values:")
for creator in (bnf_record.creator_lat or []):
    if not creator or not creator.strip():
        print(f"  Empty creator_lat: '{creator}'")

for contributor in (bnf_record.contributor_lat or []):
    if not contributor or not contributor.strip():
        print(f"  Empty contributor_lat: '{contributor}'")

for desc in (bnf_record.description_candidates_lat or []):
    if not desc or not desc.strip():
        print(f"  Empty description_candidates_lat: '{desc}'")

# Test the normalization to see if any fail
from utils.normalize import normalize

print(f"\n\nTesting normalization of author candidates:")
failed = []
for creator in (bnf_record.creator_lat or []):
    try:
        norm = normalize(creator, "lat", "fuzzy")
        if not norm:
            print(f"  Normalized to empty: '{creator}' -> ''")
            failed.append(creator)
    except Exception as e:
        print(f"  Normalization failed for '{creator}': {e}")
        failed.append(creator)

for contributor in (bnf_record.contributor_lat or []):
    try:
        norm = normalize(contributor, "lat", "fuzzy")
        if not norm:
            print(f"  Normalized to empty: '{contributor}' -> ''")
            failed.append(contributor)
    except Exception as e:
        print(f"  Normalization failed: {e}")
        failed.append(contributor)

for desc in (bnf_record.description_candidates_lat or []):
    try:
        norm = normalize(desc, "lat", "fuzzy")
        if not norm:
            print(f"  Normalized to empty: '{desc[:30]}' -> ''")
            failed.append(desc)
    except Exception as e:
        print(f"  Normalization failed: {e}")
        failed.append(desc)

if not failed:
    print("  All candidates normalized successfully")

# Test matching_candidates() step-by-step
print(f"\n\nTesting matching_candidates() with detailed output:")

from utils.normalize import normalize

lat = []
ara = []
seen_lat = set()
seen_ara = set()

def add_debug(raw, script, dest, seen, context):
    raw_clean = raw.strip().rstrip(".")
    if not raw_clean:
        print(f"  {context}: SKIP empty")
        return

    if raw_clean not in seen:
        seen.add(raw_clean)
        try:
            norm = normalize(raw_clean, script, "fuzzy")
            if norm:
                dest.append(norm)
                print(f"  {context}: ADD '{raw_clean[:25]}' -> '{norm[:25]}'")
            else:
                print(f"  {context}: SKIP normalized to empty")
        except Exception as e:
            print(f"  {context}: ERROR {e}")
    else:
        print(f"  {context}: SKIP duplicate")

print("\ncreator_lat:")
for creator in (bnf_record.creator_lat or []):
    add_debug(creator, "lat", lat, seen_lat, f"  creator")

print("\ncontributor_lat:")
for contributor in (bnf_record.contributor_lat or []):
    add_debug(contributor, "lat", lat, seen_lat, f"  contributor")

print("\ndescription_candidates_lat:")
for desc in (bnf_record.description_candidates_lat or []):
    add_debug(desc, "lat", lat, seen_lat, f"  desc")

print(f"\nTotal after manual processing: {len(lat)}")
print(f"Total from matching_candidates(): {len(bnf_record.matching_candidates(norm_strategy='raw')['lat'])}")
