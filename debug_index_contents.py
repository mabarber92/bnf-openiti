"""Debug: inspect what's in the BNF candidate index."""

import json
import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from matching.bnf_index import BNFCandidateIndex
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus

# Load correspondence mapping
with open("data_samplers/correspondence.json", encoding="utf-8") as f:
    correspondence = json.load(f)

bnf_ids_to_test = set()
for item in correspondence:
    for openiti_uri, bnf_id in item.items():
        bnf_ids_to_test.add(bnf_id)

# Load BNF records
all_bnf = load_bnf_records(BNF_FULL_PATH)
bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in bnf_ids_to_test if bnf_id in all_bnf}

print(f"Building index for {len(bnf_records)} records...")
index = BNFCandidateIndex(bnf_records, norm_strategy="fuzzy")

print(f"\nAuthor index statistics:")
print(f"  Total unique candidates: {len(index.author_index)}")
print(f"  Total records referencing authors: {len([bnf_ids for bnf_ids in index.author_index.values() if bnf_ids])}")

# Show candidate distribution
bnf_counts = Counter()
for candidate, bnf_ids in index.author_index.items():
    bnf_counts[len(bnf_ids)] += 1

print(f"\nCandidate frequency distribution:")
for count in sorted(bnf_counts.keys()):
    print(f"  {count} record(s) have {bnf_counts[count]} candidates")

# Now check scoring
print(f"\n\nScoring test: sample candidate against OpenITI authors")

all_openiti = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = all_openiti["authors"]

# Get a sample candidate (first Latin one)
sample_cand = None
for cand in index.author_index.keys():
    # Skip Arabic candidates (have Arabic characters)
    if all(ord(c) < 0x0600 for c in cand):
        sample_cand = cand
        break

if sample_cand:
    print(f"Sample Latin candidate: {sample_cand}")

    from matching.fuzzy_scorer import FuzzyScorer
    from utils.normalize import normalize

    scorer = FuzzyScorer()
    threshold = 0.80

    # Count total matches for sample candidate
    total_matches = 0
    for author_uri, author_data in openiti_authors.items():
        if isinstance(author_data, dict):
            author_name = author_data.get("name_slug", "")
        else:
            author_name = author_data.name_slug

        if not author_name:
            continue

        try:
            norm_author = normalize(author_name, "lat", "fuzzy")
        except:
            norm_author = author_name

        score = scorer.score(sample_cand, norm_author)
        if score >= threshold * 100:
            total_matches += 1

    print(f"Total matches for this candidate against all {len(openiti_authors)} OpenITI authors: {total_matches}")
    print(f"Match rate: {100*total_matches/len(openiti_authors):.1f}%")

    # If all 196 candidates match like this, total would be
    print(f"\nIf all 196 candidates match at this rate: {int(196 * total_matches)} total (explaining the 3618 matches)")
