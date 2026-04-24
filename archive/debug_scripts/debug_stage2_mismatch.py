"""Debug Stage 2 mismatch for OAI_11000954."""

import json
from pathlib import Path
from matching.normalize import normalize_transliteration
from fuzzywuzzy import fuzz

# Load data
with open("data/openiti_corpus_2025_1_9.json", encoding="utf-8") as f:
    openiti_data = json.load(f)
    openiti_books = openiti_data["books"]

with open("outputs/bnf_parsed.json", encoding="utf-8") as f:
    bnf_data = json.load(f)
    bnf_records = bnf_data["records"]

bnf_id = "OAI_11000954"
record = bnf_records.get(bnf_id)

if not record:
    print(f"Record {bnf_id} not found")
    exit(1)

# Extract title candidates for BNF (following benchmark logic)
bnf_candidates = []
for title in record.get("title_lat", []):
    for part in title.split(". "):
        part = part.strip().rstrip(".")
        if part:
            bnf_candidates.append(part)

for title in record.get("title_ara", []):
    for part in title.split(". "):
        part = part.strip().rstrip(".")
        if part:
            bnf_candidates.append(part)

for desc in record.get("description_candidates_lat", []):
    if desc:
        bnf_candidates.append(desc)

for desc in record.get("description_candidates_ara", []):
    if desc:
        bnf_candidates.append(desc)

print(f"BNF candidates for {bnf_id}: {len(bnf_candidates)} total")

# The extra book that pipeline found but benchmark didn't
extra_book = "1377MuhammadCabdAllahDarraz.Din"
book = openiti_books.get(extra_book)

if not book:
    print(f"\nExtra book {extra_book} not found in OpenITI")
    exit(1)

print(f"\nExtra book: {extra_book}")

# Check which BNF candidate matches
THRESHOLD = 0.85
book_titles = []
for title in book.get("title_lat", []):
    title_stripped = title.strip().rstrip(".")
    if title_stripped:
        book_titles.append(title_stripped)

print(f"Book has {len(book_titles)} title parts")

matches_found = 0
print(f"\nChecking matches at threshold {THRESHOLD}:")
for bnf_cand in bnf_candidates:
    norm_bnf = normalize_transliteration(bnf_cand)
    if not norm_bnf:
        continue

    for book_title in book_titles:
        norm_book = normalize_transliteration(book_title)
        if not norm_book:
            continue

        score = fuzz.token_set_ratio(norm_bnf, norm_book)
        if score >= THRESHOLD * 100:
            matches_found += 1
            print(f"  Match #{matches_found}: score={score}")

if matches_found == 0:
    print("  No matches found (this is the problem - pipeline shouldn't match it)")
