"""Debug: count actual matches for one BNF candidate."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.bnf_index import BNFCandidateIndex
from matching.normalize import normalize_transliteration
from fuzzywuzzy import fuzz

bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
test_record_id = "OAI_10030933"
bnf_record = bnf_records[test_record_id]

# Build BNF index
bnf_index = BNFCandidateIndex({test_record_id: bnf_record}, norm_strategy="fuzzy")

# Get raw candidates from the record
cands_raw = bnf_record.matching_candidates(norm_strategy="raw")
lat_candidates_raw = cands_raw.get('lat', [])

print(f"Raw BNF candidates: {len(lat_candidates_raw)}")

# Count how many matches each raw candidate gets
total_matches = 0
for candidate in lat_candidates_raw[:3]:  # Test first 3 candidates
    matches = 0
    norm_candidate = normalize_transliteration(candidate)
    
    # Check against each OpenITI author's candidates
    for author_uri, author_data in openiti_data["authors"].items():
        # Build candidates like author_matcher does
        candidates_lat = []
        if isinstance(author_data, dict):
            if author_data.get("name_slug"):
                candidates_lat.append(author_data["name_slug"])
            if author_data.get("wd_label_en"):
                candidates_lat.append(author_data["wd_label_en"])
            if author_data.get("wd_aliases_en"):
                aliases = author_data["wd_aliases_en"]
                if isinstance(aliases, list):
                    candidates_lat.extend(aliases)
                elif isinstance(aliases, str):
                    candidates_lat.append(aliases)
            for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
                if author_data.get(field):
                    candidates_lat.append(author_data[field])
        
        # Check matches
        for author_str in candidates_lat:
            if author_str:
                norm_author = normalize_transliteration(author_str)
                if norm_author:
                    score = fuzz.token_set_ratio(norm_candidate, norm_author)
                    if score >= 80:  # AUTHOR_THRESHOLD = 0.80
                        matches += 1
                        break
    
    total_matches += matches
    print(f"  Candidate {lat_candidates_raw.index(candidate)}: {matches} matches")

print(f"\nTotal from first 3 candidates: {total_matches}")
