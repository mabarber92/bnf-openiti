"""Trace exact matching for a problematic record."""

import sys
from pathlib import Path
from fuzzywuzzy import fuzz

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH, AUTHOR_THRESHOLD
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.normalize import normalize_transliteration

bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_books = openiti_data["books"]
openiti_authors = openiti_data["authors"]

bnf_id = "OAI_10884186"
bnf_record = bnf_records[bnf_id]

# Build BNF candidates (original test style)
bnf_lat = []
bnf_ara = []
for c in bnf_record.creator_lat or []:
    if c and c not in bnf_lat:
        bnf_lat.append(c)
for c in bnf_record.contributor_lat or []:
    if c and c not in bnf_lat:
        bnf_lat.append(c)
for c in bnf_record.description_candidates_lat or []:
    if c and c not in bnf_lat:
        bnf_lat.append(c)
for c in bnf_record.creator_ara or []:
    if c and c not in bnf_ara:
        bnf_ara.append(c)
for c in bnf_record.contributor_ara or []:
    if c and c not in bnf_ara:
        bnf_ara.append(c)
for c in bnf_record.description_candidates_ara or []:
    if c and c not in bnf_ara:
        bnf_ara.append(c)

print(f"BNF candidates: {len(bnf_lat)} lat, {len(bnf_ara)} ara")

# Normalize
bnf_lat_norm = [normalize_transliteration(c) for c in bnf_lat]
bnf_ara_norm = [normalize_transliteration(c) for c in bnf_ara]

# Check specific author
test_author_uri = "0845Maqrizi"
author = openiti_authors.get(test_author_uri)

if author:
    print(f"\nAuthor {test_author_uri}:")
    
    # Build author candidates (original test style)
    author_lat = []
    author_ara = []
    
    is_dict = isinstance(author, dict)
    def get_attr(o, name):
        return o.get(name) if is_dict else getattr(o, name, None)
    
    if get_attr(author, "name_slug"):
        author_lat.append(get_attr(author, "name_slug"))
    if get_attr(author, "wd_label_en"):
        author_lat.append(get_attr(author, "wd_label_en"))
    if get_attr(author, "wd_aliases_en"):
        aliases = get_attr(author, "wd_aliases_en")
        if isinstance(aliases, list):
            author_lat.extend(aliases)
        elif isinstance(aliases, str):
            author_lat.append(aliases)
    for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
        if get_attr(author, field):
            author_lat.append(get_attr(author, field))
    
    if get_attr(author, "wd_label_ar"):
        author_ara.append(get_attr(author, "wd_label_ar"))
    if get_attr(author, "wd_aliases_ar"):
        aliases = get_attr(author, "wd_aliases_ar")
        if isinstance(aliases, list):
            author_ara.extend(aliases)
        elif isinstance(aliases, str):
            author_ara.append(aliases)
    
    print(f"  Author candidates: {len(author_lat)} lat, {len(author_ara)} ara")
    
    # Normalize author candidates
    author_lat_norm = [normalize_transliteration(c) for c in author_lat]
    author_ara_norm = [normalize_transliteration(c) for c in author_ara]
    
    # Check matching
    author_matched = False
    for script in ["lat", "ara"]:
        bnf_norm = bnf_lat_norm if script == "lat" else bnf_ara_norm
        author_norm = author_lat_norm if script == "lat" else author_ara_norm
        
        if not bnf_norm or not author_norm:
            continue
        
        for bnf_str in bnf_norm:
            for author_str in author_norm:
                score = fuzz.token_set_ratio(bnf_str, author_str)
                if score >= AUTHOR_THRESHOLD * 100:
                    author_matched = True
                    print(f"  MATCH on {script}: score={score}")
                    break
            if author_matched:
                break
        if author_matched:
            break
    
    if not author_matched:
        print(f"  NO MATCH")
else:
    print(f"Author {test_author_uri} not found")
