"""Debug: check how many author candidates we extract per author."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

# Sample 10 authors
author_count = {}
for author_uri, author_data in list(openiti_authors.items())[:10]:
    candidates = []

    if isinstance(author_data, dict):
        if author_data.get("name_slug"):
            candidates.append("name_slug")
        if author_data.get("wd_label_en"):
            candidates.append("wd_label_en")
        if author_data.get("wd_aliases_en"):
            aliases = author_data["wd_aliases_en"]
            if isinstance(aliases, list):
                candidates.extend([f"wd_aliases_en[{i}]" for i in range(len(aliases))])
            else:
                candidates.append("wd_aliases_en")
        for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
            if author_data.get(field):
                candidates.append(field)
        if author_data.get("wd_label_ar"):
            candidates.append("wd_label_ar")
        if author_data.get("wd_aliases_ar"):
            aliases = author_data["wd_aliases_ar"]
            if isinstance(aliases, list):
                candidates.extend([f"wd_aliases_ar[{i}]" for i in range(len(aliases))])
            else:
                candidates.append("wd_aliases_ar")
    else:
        if author_data.name_slug:
            candidates.append("name_slug")
        if author_data.wd_label_en:
            candidates.append("wd_label_en")
        if author_data.wd_aliases_en:
            aliases = author_data.wd_aliases_en
            if isinstance(aliases, list):
                candidates.extend([f"wd_aliases_en[{i}]" for i in range(len(aliases))])
            else:
                candidates.append("wd_aliases_en")
        for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
            val = getattr(author_data, field, None)
            if val:
                candidates.append(field)
        if author_data.wd_label_ar:
            candidates.append("wd_label_ar")
        if author_data.wd_aliases_ar:
            aliases = author_data.wd_aliases_ar
            if isinstance(aliases, list):
                candidates.extend([f"wd_aliases_ar[{i}]" for i in range(len(aliases))])
            else:
                candidates.append("wd_aliases_ar")

    print(f"{author_uri}: {len(candidates)} fields - {candidates}")
