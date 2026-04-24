"""Check what author candidates are being built."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus
from matching.candidate_builders import build_author_candidates_by_script

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

# Sample a few authors
for i, (author_uri, author_data) in enumerate(list(openiti_authors.items())[:5]):
    cands = build_author_candidates_by_script(author_data)
    
    # Count by field type
    has_name_slug = False
    has_en_fields = False
    has_lat_fields = False
    has_ar_fields = False
    
    if isinstance(author_data, dict):
        has_name_slug = bool(author_data.get("name_slug"))
        has_en_fields = bool(author_data.get("wd_label_en") or author_data.get("wd_aliases_en"))
        has_lat_fields = bool(any(author_data.get(f) for f in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]))
        has_ar_fields = bool(author_data.get("wd_label_ar") or author_data.get("wd_aliases_ar"))
    
    print(f"{i}: {len(cands['lat'])} lat ({has_name_slug=}, {has_en_fields=}, {has_lat_fields=}), {len(cands['ara'])} ara ({has_ar_fields=})")
