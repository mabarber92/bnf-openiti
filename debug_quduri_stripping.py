"""Debug why author string stripping isn't working for Quduri record."""

import json
import sys
sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.normalize import normalize_for_matching
import matching.config as cfg

# Load data
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

# Get Quduri record
quduri_bnf_id = "OAI_11000520"
quduri_bnf = all_bnf[quduri_bnf_id]

print(f"BNF Record: {quduri_bnf_id}")
print(f"\nTitle fields:")
print(f"  title_lat: {quduri_bnf.get('title_lat', [])}")
print(f"  title_ara: {quduri_bnf.get('title_ara', [])}")
print(f"\nCreator fields:")
print(f"  creator_lat: {quduri_bnf.get('creator_lat', [])}")
print(f"  creator_ara: {quduri_bnf.get('creator_ara', [])}")
print(f"\nDescription fields:")
print(f"  description_candidates_lat: {quduri_bnf.get('description_candidates_lat', [])}")
print(f"  description_candidates_ara: {quduri_bnf.get('description_candidates_ara', [])}")

# Get the Quduri author from OpenITI
# The BNF record should have matched to Quduri
quduri_author_uri = "0428AbuHusaynQuduri"
quduri_author = openiti_data["authors"].get(quduri_author_uri)

if quduri_author:
    print(f"\nOpenITI Author: {quduri_author_uri}")
    if isinstance(quduri_author, dict):
        print(f"  lat_name: {quduri_author.get('lat_name', 'N/A')}")
        print(f"  ar_name: {quduri_author.get('ar_name', 'N/A')}")
    else:
        print(f"  lat_name: {getattr(quduri_author, 'lat_name', 'N/A')}")
        print(f"  ar_name: {getattr(quduri_author, 'ar_name', 'N/A')}")

    # Now try matching with normalization
    author_names_to_strip = []
    if isinstance(quduri_author, dict):
        if quduri_author.get("lat_name"):
            author_names_to_strip.append(quduri_author["lat_name"])
        if quduri_author.get("ar_name"):
            author_names_to_strip.append(quduri_author["ar_name"])
    else:
        if hasattr(quduri_author, "lat_name") and quduri_author.lat_name:
            author_names_to_strip.append(quduri_author.lat_name)
        if hasattr(quduri_author, "ar_name") and quduri_author.ar_name:
            author_names_to_strip.append(quduri_author.ar_name)

    print(f"\nAuthor names to strip: {author_names_to_strip}")

    # Check matching in title_lat
    print(f"\nNormalization matching test (title_lat):")
    for title in quduri_bnf.get('title_lat', []):
        print(f"\n  Original title: {title}")
        norm_title = normalize_for_matching(title, split_camelcase=False, is_openiti=False)
        print(f"  Normalized title: {norm_title}")

        for author_name in author_names_to_strip:
            norm_author = normalize_for_matching(author_name, split_camelcase=True, is_openiti=True)
            print(f"    Author: {author_name}")
            print(f"    Normalized author: {norm_author}")

            if norm_author and norm_author.lower() in norm_title.lower():
                print(f"      → MATCH! Would strip this")
            else:
                print(f"      → no match")
