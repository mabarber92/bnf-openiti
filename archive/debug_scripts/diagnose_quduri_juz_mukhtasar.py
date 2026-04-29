"""
Investigate why Quduri.Juz ranks above Quduri.Mukhtasar for OAI_11000520
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.normalize import normalize_for_matching
from matching.candidate_builders import build_author_candidates_by_script, build_book_candidates_by_script
from fuzzywuzzy import fuzz
import matching.config as cfg

bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

# Get BNF record
bnf_id = 'OAI_11000520'
bnf_record = bnf_records[bnf_id]

creators_lat = getattr(bnf_record, "creator_lat", []) or []
creator = creators_lat[0] if creators_lat else ""
norm_creator = normalize_for_matching(creator, split_camelcase=False)

titles_ara = getattr(bnf_record, "title_ara", []) or []
titles_lat = getattr(bnf_record, "title_lat", []) or []
title = (titles_ara[0] if titles_ara else None) or (titles_lat[0] if titles_lat else "")
norm_title = normalize_for_matching(title, split_camelcase=False)

print("=" * 120)
print(f"DIAGNOSTIC: OAI_11000520 - Quduri.Juz vs Quduri.Mukhtasar")
print("=" * 120)
print(f"BNF Creator: {creator[:80]}")
print(f"Normalized: {norm_creator}")
print(f"BNF Title: {title[:100]}")
print(f"Normalized: {norm_title}")
print()

# Get both books
juz_uri = "0428AbuHusaynQuduri.Juz"
mukhtasar_uri = "0428AbuHusaynQuduri.Mukhtasar"

juz_book = openiti_data['books'].get(juz_uri)
mukhtasar_book = openiti_data['books'].get(mukhtasar_uri)

print(f"Quduri.Juz found: {juz_book is not None}")
print(f"Quduri.Mukhtasar found: {mukhtasar_book is not None}")
print()

if juz_book:
    juz_candidates = build_book_candidates_by_script(juz_book)
    juz_titles = juz_candidates.get("lat", []) + juz_candidates.get("ara", [])
    
    print("Quduri.Juz title variants:")
    for variant in [v for v in juz_titles if v]:
        norm_variant = normalize_for_matching(variant, split_camelcase=True)
        fuzzy = fuzz.token_set_ratio(norm_title, norm_variant)
        print(f"  {variant[:50]:50} → {norm_variant[:50]:50} fuzzy={fuzzy:.0f}")

if mukhtasar_book:
    mukhtasar_candidates = build_book_candidates_by_script(mukhtasar_book)
    mukhtasar_titles = mukhtasar_candidates.get("lat", []) + mukhtasar_candidates.get("ara", [])
    
    print()
    print("Quduri.Mukhtasar title variants:")
    for variant in [v for v in mukhtasar_titles if v]:
        norm_variant = normalize_for_matching(variant, split_camelcase=True)
        fuzzy = fuzz.token_set_ratio(norm_title, norm_variant)
        print(f"  {variant[:50]:50} → {norm_variant[:50]:50} fuzzy={fuzzy:.0f}")

