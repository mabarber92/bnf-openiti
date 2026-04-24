"""Debug: extract and display candidates from original test logic."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Load data (same as original test)
openiti_path = Path("data/openiti_corpus_2025_1_9.json")
bnf_path = Path("outputs/bnf_parsed.json")

print("Loading OpenITI...")
with open(openiti_path, encoding="utf-8") as f:
    openiti_data = json.load(f)
    openiti_authors = openiti_data["authors"]

print("Loading BNF...")
with open(bnf_path, encoding="utf-8") as f:
    bnf_data = json.load(f)
    bnf_records = bnf_data["records"]

# Use same candidate extraction as original test
def build_bnf_author_candidates(bnf_record: dict) -> dict[str, list[str]]:
    """Extract author candidates exactly as original test does."""
    candidates = {"lat": [], "ara": []}

    # Creator fields
    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)

    for creator in bnf_record.get("creator_ara", []):
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)

    # Contributor fields
    for contrib in bnf_record.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)

    for contrib in bnf_record.get("contributor_ara", []):
        if contrib and contrib not in candidates["ara"]:
            candidates["ara"].append(contrib)

    # Description candidates
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)

    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)

    return candidates


def build_openiti_author_candidates(author_uri: str) -> dict[str, list[str]]:
    """Extract OpenITI author candidates exactly as original test does."""
    candidates = {"lat": [], "ara": []}

    author = openiti_authors.get(author_uri)
    if not author:
        return candidates

    # Latin sources
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

    # Structured components
    for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
        if author.get(field):
            candidates["lat"].append(author[field])

    # Arabic sources
    if author.get("wd_label_ar"):
        candidates["ara"].append(author["wd_label_ar"])

    if author.get("wd_aliases_ar"):
        aliases = author["wd_aliases_ar"]
        if isinstance(aliases, list):
            candidates["ara"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["ara"].append(aliases)

    return candidates


# Debug one record
bnf_id = "OAI_10030933"
expected_uri = "0660IbnCadim.BughyatTalab"

if bnf_id not in bnf_records:
    print(f"ERROR: {bnf_id} not in BNF data")
    sys.exit(1)

bnf_record = bnf_records[bnf_id]
bnf_authors = build_bnf_author_candidates(bnf_record)

print(f"\n=== ORIGINAL TEST LOGIC ===")
print(f"BNF Record: {bnf_id}")
print(f"Expected OpenITI: {expected_uri}")

print(f"\nBNF Author Candidates (Latin): {len(bnf_authors.get('lat', []))} items")
for i, cand in enumerate(bnf_authors.get('lat', [])[:5]):
    try:
        print(f"  [{i}] {cand}")
    except:
        print(f"  [{i}] [non-printable]")

print(f"\nBNF Author Candidates (Arabic): {len(bnf_authors.get('ara', []))} items")

# Get expected author and show candidates
openiti_book = openiti_data["books"].get(expected_uri)
if openiti_book:
    expected_author_uri = openiti_book.get("author_uri")
    expected_author = openiti_authors.get(expected_author_uri)
    expected_author_cands = build_openiti_author_candidates(expected_author_uri)

    print(f"\nExpected OpenITI Author: {expected_author_uri}")
    print(f"OpenITI Author Candidates (Latin): {len(expected_author_cands.get('lat', []))} items")
    for i, cand in enumerate(expected_author_cands.get('lat', [])[:5]):
        try:
            print(f"  [{i}] {cand}")
        except:
            print(f"  [{i}] [non-printable]")

print(f"\n=== COMPARISON ===")
print(f"BNF total fields used: {len(bnf_record.get('creator_lat', [])) + len(bnf_record.get('contributor_lat', [])) + len(bnf_record.get('description_candidates_lat', []))}")
print(f"  creator_lat: {len(bnf_record.get('creator_lat', []))}")
print(f"  contributor_lat: {len(bnf_record.get('contributor_lat', []))}")
print(f"  description_candidates_lat: {len(bnf_record.get('description_candidates_lat', []))}")
