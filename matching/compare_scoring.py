"""Compare scoring: original vs our implementation using same candidates."""

import json
import sys
from pathlib import Path
from fuzzywuzzy import fuzz

sys.path.insert(0, str(Path.cwd()))

from matching.fuzzy_scorer import FuzzyScorer
from matching.normalize import normalize_transliteration
from utils.normalize import normalize

# Load data
openiti_path = Path("data/openiti_corpus_2025_1_9.json")
bnf_path = Path("outputs/bnf_parsed.json")

with open(openiti_path, encoding="utf-8") as f:
    openiti_data = json.load(f)
    openiti_authors = openiti_data["authors"]

with open(bnf_path, encoding="utf-8") as f:
    bnf_data = json.load(f)
    bnf_records = bnf_data["records"]

# Get candidates for test record
bnf_id = "OAI_10030933"
bnf_record = bnf_records[bnf_id]

# Extract candidates (same as original)
def build_bnf_author_candidates(bnf_record):
    candidates = {"lat": [], "ara": []}
    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)
    for contrib in bnf_record.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)
    return candidates

raw_candidates = build_bnf_author_candidates(bnf_record)
print(f"Raw BNF candidates: {len(raw_candidates['lat'])} items")

# Normalize both ways
orig_norm = [normalize_transliteration(c) for c in raw_candidates['lat']]
our_norm = [normalize(c, 'lat', 'fuzzy') for c in raw_candidates['lat']]

print(f"\nNormalization comparison:")
print(f"Candidate | Original Norm | Our Norm | Match")
print("-" * 80)
for i, (raw, on, on_) in enumerate(zip(raw_candidates['lat'][:5], orig_norm[:5], our_norm[:5])):
    match = "YES" if on == on_ else "NO"
    print(f"{i} | {on:15} | {on_:15} | {match}")

# Now score against authors
scorer = FuzzyScorer()
threshold_pct = 80.0
threshold_frac = 0.80

# Sample author for testing
sample_author_uri = "0660IbnCadim"
sample_author = openiti_authors.get(sample_author_uri)
sample_author_name = sample_author.get("name_slug", "")

print(f"\n\nScoring against sample author: {sample_author_uri} ({sample_author_name})")
print(f"Original norm author: {normalize_transliteration(sample_author_name)}")
print(f"Our norm author: {normalize(sample_author_name, 'lat', 'fuzzy')}")

print(f"\nCandidate | Original Score (fuzz) | Our Score (scorer) | Threshold Met")
print("-" * 80)

for i, (raw, orig_c, our_c) in enumerate(zip(raw_candidates['lat'][:5], orig_norm[:5], our_norm[:5])):
    # Original: token_set_ratio returns 0-100
    orig_score = fuzz.token_set_ratio(orig_c, normalize_transliteration(sample_author_name))

    # Ours: returns 0-100
    our_score = scorer.score(our_c, normalize(sample_author_name, 'lat', 'fuzzy'))

    orig_pass = "YES" if orig_score >= threshold_pct else "NO"
    our_pass = "YES" if our_score >= threshold_pct else "NO"

    print(f"{i} | {orig_score:6.1f} ({orig_pass}) | {our_score:6.1f} ({our_pass}) | {orig_pass == our_pass}")
