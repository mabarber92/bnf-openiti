"""
Diagnose what caused each false positive match.
Compare BNF candidates against matched book candidates to understand matching patterns.
"""

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from matching.normalize import normalize_transliteration
from fuzzywuzzy import fuzz

# Load data
with open("data/openiti_corpus_2025_1_9.json", encoding="utf-8") as f:
    openiti_data = json.load(f)
    openiti_books = openiti_data["books"]
    openiti_authors = openiti_data["authors"]

with open("outputs/bnf_parsed.json", encoding="utf-8") as f:
    bnf_data = json.load(f)
    bnf_records = bnf_data["records"]

# Extract candidate extraction logic from test_surface_matching.py
def build_bnf_author_candidates(bnf_record: dict) -> dict[str, list[str]]:
    candidates = {"lat": [], "ara": []}
    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)
    for creator in bnf_record.get("creator_ara", []):
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)
    for contrib in bnf_record.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)
    for contrib in bnf_record.get("contributor_ara", []):
        if contrib and contrib not in candidates["ara"]:
            candidates["ara"].append(contrib)
    for title in bnf_record.get("title_lat", []):
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["lat"]:
                candidates["lat"].append(part)
    for title in bnf_record.get("title_ara", []):
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["ara"]:
                candidates["ara"].append(part)
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)
    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)
    return candidates

def build_openiti_author_candidates(author_uri: str) -> dict[str, list[str]]:
    candidates = {"lat": [], "ara": []}
    author = openiti_authors.get(author_uri)
    if not author:
        return candidates
    if author.get("name_slug"):
        candidates["lat"].append(author["name_slug"])
    if author.get("wd_label_en"):
        candidates["lat"].append(author["wd_label_en"])
    if author.get("wd_aliases_en"):
        aliases = author["wd_aliases_en"]
        if isinstance(aliases, list):
            candidates["lat"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["lat"].append(aliases)
    for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
        if author.get(field):
            candidates["lat"].append(author[field])
    if author.get("wd_label_ar"):
        candidates["ara"].append(author["wd_label_ar"])
    if author.get("wd_aliases_ar"):
        aliases = author["wd_aliases_ar"]
        if isinstance(aliases, list):
            candidates["ara"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["ara"].append(aliases)
    return candidates

def build_openiti_title_candidates(book: dict) -> dict[str, list[str]]:
    candidates = {"lat": [], "ara": []}
    if book.get("title_lat"):
        for part in book["title_lat"].split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["lat"]:
                candidates["lat"].append(part)
    if book.get("title_ara"):
        for part in book["title_ara"].split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["ara"]:
                candidates["ara"].append(part)
    return candidates

# False positive cases
false_positives = [
    ("OAI_11000434", "0620IbnQudamaMaqdisi.DhammTawil"),
    ("OAI_10030933", "0843IbnKhatibNasiriyya.DurrMuntakhab"),
    ("OAI_10030933", "0385Daraqutni.FawaidMuntaqatDhuhli"),
    ("OAI_10882524", "0385Daraqutni.FawaidMuntaqatDhuhli"),
    ("OAI_10884186", "0385Daraqutni.FawaidMuntaqatDhuhli"),
    ("OAI_10884191", "0385Daraqutni.FawaidMuntaqatDhuhli"),
    ("OAI_11000947", "0385Daraqutni.FawaidMuntaqatDhuhli"),
    ("OAI_11001068", "0580IbnCimrani.InbaFiTarikhKhulafa"),
]

threshold = 0.80

output = []
output.append("ANALYZING FALSE POSITIVES")
output.append("=" * 100)

for bnf_id, matched_book_uri in false_positives:
    bnf_record = bnf_records.get(bnf_id)
    matched_book = openiti_books.get(matched_book_uri)

    if not bnf_record or not matched_book:
        output.append(f"\nSkipping {bnf_id} {matched_book_uri} (missing data)")
        continue

    matched_author_uri = matched_book.get("author_uri")

    # Get candidates
    bnf_author_cands = build_bnf_author_candidates(bnf_record)
    book_title_cands = build_openiti_title_candidates(matched_book)
    author_cands = build_openiti_author_candidates(matched_author_uri)

    # Normalize
    bnf_norm = {
        "lat": [normalize_transliteration(c) for c in bnf_author_cands.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in bnf_author_cands.get("ara", [])],
    }
    book_title_norm = {
        "lat": [normalize_transliteration(c) for c in book_title_cands.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in book_title_cands.get("ara", [])],
    }
    author_norm = {
        "lat": [normalize_transliteration(c) for c in author_cands.get("lat", [])],
        "ara": [normalize_transliteration(c) for c in author_cands.get("ara", [])],
    }

    output.append(f"\n{bnf_id} -> {matched_book_uri}")
    output.append(f"  Author: {matched_author_uri}")

    # Check author matching
    author_matches = []
    for script in ["lat", "ara"]:
        for bnf_str in bnf_norm[script]:
            for author_str in author_norm[script]:
                score = fuzz.token_set_ratio(bnf_str, author_str)
                if score >= threshold * 100:
                    author_matches.append((bnf_str, author_str, score))

    if author_matches:
        output.append(f"  Author matches found: {len(author_matches)} total")
        for bnf_str, author_str, score in author_matches[:3]:
            output.append(f"    '{bnf_str}' vs '{author_str}' = {score}")
    else:
        output.append(f"  No author matches (threshold {threshold})")

    # Check title matching
    title_matches = []
    for script in ["lat", "ara"]:
        for bnf_str in bnf_norm[script]:
            for book_str in book_title_norm[script]:
                score = fuzz.token_set_ratio(bnf_str, book_str)
                if score >= threshold * 100:
                    title_matches.append((bnf_str, book_str, score))

    if title_matches:
        output.append(f"  Title matches found: {len(title_matches)} total")
        for bnf_str, book_str, score in title_matches[:3]:
            output.append(f"    '{bnf_str}' vs '{book_str}' = {score}")
    else:
        output.append(f"  No title matches (threshold {threshold})")

# Check Daraqutni title structure
output.append("\n" + "=" * 100)
output.append("DARAQUTNI TITLE ANALYSIS")
output.append("=" * 100)
daraqutni_book = openiti_books.get("0385Daraqutni.FawaidMuntaqatDhuhli")
if daraqutni_book:
    title_lat = daraqutni_book.get("title_lat", "")
    output.append(f"\nDaraqutni title_lat length: {len(title_lat)} chars")
    output.append(f"  Separators: '::' count={title_lat.count('::')}, '|' count={title_lat.count('|')}")

    # Split and analyze parts
    if '::' in title_lat:
        parts = [p.strip() for p in title_lat.split("::") if p.strip()]
        output.append(f"\nSplit by '::' gives {len(parts)} parts")
        for i, part in enumerate(parts[:5]):
            output.append(f"  Part {i+1} ({len(part)} chars): {part[:60]}")

# Write to file
output_file = Path("matching/false_positives_analysis.txt")
with open(output_file, "w", encoding="utf-8") as f:
    f.write("\n".join(output))

print(f"Wrote analysis to {output_file}")
print("\nFirst 50 lines:")
for line in output[:50]:
    try:
        print(line)
    except:
        print(line.encode('ascii', errors='ignore').decode('ascii'))
