"""Debug mismatches between original and pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.candidate_builders import build_author_candidates_by_script

# Load data
bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

bnf_id = "OAI_10884186"
bnf_record = bnf_records[bnf_id]

# Get raw BNF candidates (original test method)
orig_candidates = []
for creator in bnf_record.creator_lat or []:
    if creator and creator not in orig_candidates:
        orig_candidates.append(creator)
for contrib in bnf_record.contributor_lat or []:
    if contrib and contrib not in orig_candidates:
        orig_candidates.append(contrib)
for desc in bnf_record.description_candidates_lat or []:
    if desc and desc not in orig_candidates:
        orig_candidates.append(desc)

# Get matching_candidates method output
matching_cands = bnf_record.matching_candidates(norm_strategy="raw")

print(f"Original test extracts {len(orig_candidates)} candidates")
print(f"matching_candidates() extracts {len(matching_cands.get('lat', []))} candidates")

# Check BNF record fields
print(f"\nBNF record {bnf_id} has:")
print(f"  creator_lat: {len(bnf_record.creator_lat or [])}")
print(f"  contributor_lat: {len(bnf_record.contributor_lat or [])}")
print(f"  description_candidates_lat: {len(bnf_record.description_candidates_lat or [])}")

# Test with a specific author
test_author = list(openiti_authors.keys())[0]
test_author_data = openiti_authors[test_author]

# Build using our method
cands_by_script = build_author_candidates_by_script(test_author_data)
print(f"\nSample author has:")
print(f"  lat candidates: {len(cands_by_script['lat'])}")
print(f"  ara candidates: {len(cands_by_script['ara'])}")

# Check for Arabic candidates that might be counted as Latin
print(f"\nChecking if matching_candidates returns correct lat/ara split:")
matching_ara = matching_cands.get('ara', [])
print(f"  lat in matching_candidates: {len(matching_cands.get('lat', []))}")
print(f"  ara in matching_candidates: {len(matching_ara)}")
