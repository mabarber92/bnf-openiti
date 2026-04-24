"""Compare pipeline results against original test results for one record."""

import json
import sys
from pathlib import Path
from fuzzywuzzy import fuzz

sys.path.insert(0, str(Path.cwd()))

from matching.normalize import normalize_transliteration
from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus

# Load data
bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_books = openiti_data["books"]
openiti_authors = openiti_data["authors"]

test_record_id = "OAI_10030933"
expected_uri = "0660IbnCadim.BughyatTalab"
threshold = 0.80

bnf_record_dict = None
for record_id, record in bnf_records.items():
    if record_id == test_record_id:
        bnf_record_dict = {
            "creator_lat": record.creator_lat or [],
            "contributor_lat": record.contributor_lat or [],
            "description_candidates_lat": record.description_candidates_lat or [],
            "title_lat": record.title_lat or [],
        }
        break

# Original test logic
def build_bnf_author_candidates(bnf_record):
    candidates = {"lat": []}
    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)
    for contrib in bnf_record.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)
    return candidates

def build_openiti_author_candidates(author_uri):
    candidates = {"lat": []}
    author = openiti_authors.get(author_uri)
    if not author:
        return candidates

    is_dict = isinstance(author, dict)

    # Helper to get attribute
    def get_attr(a, name):
        return a.get(name) if is_dict else getattr(a, name, None)

    if get_attr(author, "name_slug"):
        candidates["lat"].append(get_attr(author, "name_slug"))
    if get_attr(author, "wd_label_en"):
        candidates["lat"].append(get_attr(author, "wd_label_en"))
    if get_attr(author, "wd_aliases_en"):
        aliases = get_attr(author, "wd_aliases_en")
        if isinstance(aliases, list):
            candidates["lat"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["lat"].append(aliases)
    for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
        if get_attr(author, field):
            candidates["lat"].append(get_attr(author, field))
    return candidates

# Extract candidates
bnf_authors = build_bnf_author_candidates(bnf_record_dict)
bnf_authors_norm = {
    "lat": [normalize_transliteration(c) for c in bnf_authors.get("lat", [])],
}

print(f"BNF candidates: {len(bnf_authors['lat'])} raw, {len(bnf_authors_norm['lat'])} normalized")

# Count matches using original test logic
matches_original = 0
for openiti_uri, openiti_book in openiti_books.items():
    # Handle both dict and dataclass
    author_uri = openiti_book["author_uri"] if isinstance(openiti_book, dict) else openiti_book.author_uri
    if not author_uri:
        continue

    openiti_authors_cands = build_openiti_author_candidates(author_uri)
    openiti_authors_norm = {
        "lat": [normalize_transliteration(c) for c in openiti_authors_cands.get("lat", [])],
    }

    author_matched = False
    for bnf_str in bnf_authors_norm["lat"]:
        for openiti_str in openiti_authors_norm["lat"]:
            score = fuzz.token_set_ratio(bnf_str, openiti_str)
            if score >= threshold * 100:
                author_matched = True
                break
        if author_matched:
            break

    if author_matched:
        matches_original += 1

print(f"\nOriginal test logic: {matches_original} total author matches")

# Check if expected match would be found
expected_found = False
for uri, book in openiti_books.items():
    if uri == expected_uri:
        author_uri = book["author_uri"] if isinstance(book, dict) else book.author_uri
        openiti_authors_cands = build_openiti_author_candidates(author_uri)
        openiti_authors_norm = {
            "lat": [normalize_transliteration(c) for c in openiti_authors_cands.get("lat", [])],
        }
        for bnf_str in bnf_authors_norm["lat"]:
            for openiti_str in openiti_authors_norm["lat"]:
                score = fuzz.token_set_ratio(bnf_str, openiti_str)
                if score >= threshold * 100:
                    expected_found = True
                    break
            if expected_found:
                break
        break

print(f"Expected {expected_uri} found: {expected_found}")

# Now check our pipeline
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.classifier import Classifier

bnf_records_test = {test_record_id: bnf_records[test_record_id]}
pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="compare", verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(Classifier(verbose=False))

pipeline.run()

s1_results = pipeline.get_stage1_result(test_record_id)
print(f"\nPipeline Stage 1: {len(s1_results or [])} author matches")
