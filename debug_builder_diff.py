"""Compare candidate builders."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus
from matching.candidate_builders import build_author_candidates_by_script

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

# Test author
test_author_uri = "0845Maqrizi"
author = openiti_authors[test_author_uri]

# Original test method
author_lat_orig = []
author_ara_orig = []

is_dict = isinstance(author, dict)
def get_attr(o, name):
    return o.get(name) if is_dict else getattr(o, name, None)

if get_attr(author, "name_slug"):
    author_lat_orig.append(get_attr(author, "name_slug"))
if get_attr(author, "wd_label_en"):
    author_lat_orig.append(get_attr(author, "wd_label_en"))
if get_attr(author, "wd_aliases_en"):
    aliases = get_attr(author, "wd_aliases_en")
    if isinstance(aliases, list):
        author_lat_orig.extend(aliases)
    elif isinstance(aliases, str):
        author_lat_orig.append(aliases)
for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
    if get_attr(author, field):
        author_lat_orig.append(get_attr(author, field))

if get_attr(author, "wd_label_ar"):
    author_ara_orig.append(get_attr(author, "wd_label_ar"))
if get_attr(author, "wd_aliases_ar"):
    aliases = get_attr(author, "wd_aliases_ar")
    if isinstance(aliases, list):
        author_ara_orig.extend(aliases)
    elif isinstance(aliases, str):
        author_ara_orig.append(aliases)

# Pipeline builder
cands = build_author_candidates_by_script(author)

print(f"Original: {len(author_lat_orig)} lat, {len(author_ara_orig)} ara")
print(f"Builder:  {len(cands['lat'])} lat, {len(cands['ara'])} ara")

if set(author_lat_orig) == set(cands['lat']) and set(author_ara_orig) == set(cands['ara']):
    print("CANDIDATES MATCH")
else:
    print("CANDIDATES DIFFER")
    if set(author_lat_orig) != set(cands['lat']):
        print(f"  lat: in orig {set(author_lat_orig) - set(cands['lat'])} not in builder")
        print(f"  lat: in builder {set(cands['lat']) - set(author_lat_orig)} not in orig")
    if set(author_ara_orig) != set(cands['ara']):
        print(f"  ara: in orig {set(author_ara_orig) - set(cands['ara'])} not in builder")
        print(f"  ara: in builder {set(cands['ara']) - set(author_ara_orig)} not in orig")
