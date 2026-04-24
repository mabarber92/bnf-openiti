"""
Find which BNF author candidate is matching Ibn Hanbal.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.normalize import normalize_transliteration
from fuzzywuzzy import fuzz
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH, AUTHOR_THRESHOLD

def safe_get(obj, key):
    """Get attribute from dict or dataclass."""
    if isinstance(obj, dict):
        return obj.get(key)
    else:
        return getattr(obj, key, None)

def build_author_candidates_for_bn(rec):
    """Build author candidates from BNF record."""
    candidates = []
    for creator in safe_get(rec, 'creator_lat') or []:
        if creator:
            candidates.append(creator)
    for contrib in safe_get(rec, 'contributor_lat') or []:
        if contrib:
            candidates.append(contrib)
    for title in safe_get(rec, 'title_lat') or []:
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part:
                candidates.append(part)
    for desc in safe_get(rec, 'description_candidates_lat') or []:
        if desc:
            candidates.append(desc)
    return candidates

# Load data
bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

bnf_id = "OAI_11001068"
ibn_hanbal_id = "0241IbnHanbal"

record = bnf_records[bnf_id]
ibn_hanbal = openiti_data["authors"][ibn_hanbal_id]

# Get author candidates for Ibn Hanbal
ibn_hanbal_cands = []
if safe_get(ibn_hanbal, 'name_slug'):
    ibn_hanbal_cands.append(safe_get(ibn_hanbal, 'name_slug'))
if safe_get(ibn_hanbal, 'wd_label_en'):
    ibn_hanbal_cands.append(safe_get(ibn_hanbal, 'wd_label_en'))

print("="*80)
print(f"FINDING WHICH BNF CANDIDATE MATCHES IBN HANBAL")
print("="*80)
print(f"\nBNF Record: {bnf_id}")
print(f"Target Author: {ibn_hanbal_id}")
print(f"Ibn Hanbal candidates: {ibn_hanbal_cands[:2]}")  # Show first 2

# Get BNF candidates
bnf_candidates = build_author_candidates_for_bn(record)
print(f"\nBNF has {len(bnf_candidates)} author candidates")

# Check which match
threshold = AUTHOR_THRESHOLD
print(f"\nSearching for matches at threshold {threshold}:")

match_count = 0
for bnf_cand in bnf_candidates:
    norm_bnf = normalize_transliteration(bnf_cand)
    if not norm_bnf:
        continue

    for ibn_cand in ibn_hanbal_cands:
        norm_ibn = normalize_transliteration(ibn_cand)
        if not norm_ibn:
            continue

        score = fuzz.token_set_ratio(norm_bnf, norm_ibn)
        if score >= threshold * 100:
            match_count += 1
            print(f"\nMatch #{match_count}:")
            print(f"  BNF: '{bnf_cand}' (norm: '{norm_bnf}')")
            print(f"  IBN HANBAL: '{ibn_cand}' (norm: '{norm_ibn}')")
            print(f"  Score: {score}/100 >= {threshold*100}")

if match_count == 0:
    print("\nNo matches found - this shouldn't happen since Ibn Hanbal is in Stage 1")
    print("\nLet me check if he's matched via name components...")

    ibn_hanbal_cands_all = []
    for field in ['name_shuhra_lat', 'name_ism_lat', 'name_kunya_lat', 'name_laqab_lat', 'name_nasab_lat', 'name_nisba_lat']:
        val = safe_get(ibn_hanbal, field)
        if val:
            ibn_hanbal_cands_all.append((field, val))

    print(f"\nIbn Hanbal name components:")
    for field, val in ibn_hanbal_cands_all[:3]:
        print(f"  {field}: {val}")

    # Try matching again with all candidates
    print(f"\nRetrying with all candidates:")
    for bnf_cand in bnf_candidates[:3]:
        print(f"\n  BNF: '{bnf_cand}'")
        norm_bnf = normalize_transliteration(bnf_cand)
        print(f"    normalized: '{norm_bnf}'")

        for field, ibn_cand in ibn_hanbal_cands_all[:2]:
            norm_ibn = normalize_transliteration(ibn_cand)
            score = fuzz.token_set_ratio(norm_bnf, norm_ibn)
            if score >= threshold * 100:
                print(f"    MATCHES {field}: '{ibn_cand}' (score: {score})")
