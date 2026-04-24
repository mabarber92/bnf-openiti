"""Check if data is loaded differently."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from parsers.bnf import load_bnf_records

# Load both ways
openiti_direct = load_openiti_corpus(OPENITI_CORPUS_PATH)

# Via pipeline
bnf_records = load_bnf_records("matching/config.BNF_FULL_PATH" if False else None)
bnf_test = {}
pipeline = MatchingPipeline(bnf_test, None, run_id="debug", verbose=False)  # Will load its own openiti

# Actually, let me check what load_openiti_corpus returns
print(f"openiti_direct type: {type(openiti_direct)}")
print(f"openiti_direct keys: {openiti_direct.keys() if isinstance(openiti_direct, dict) else 'N/A'}")

# Check one author in direct vs loaded
author_uri = "0845Maqrizi"
author_direct = openiti_direct["authors"].get(author_uri)
print(f"\nAuthor {author_uri}:")
print(f"  Direct load is dict: {isinstance(author_direct, dict)}")
print(f"  Direct load type: {type(author_direct)}")

# Check if it has the _ara fields
if isinstance(author_direct, dict):
    print(f"  Direct has name_shuhra_ara: {'name_shuhra_ara' in author_direct}")
    print(f"  Direct has name_shuhra_lat: {'name_shuhra_lat' in author_direct}")
else:
    import dataclasses
    fields = [f.name for f in dataclasses.fields(author_direct)]
    print(f"  Direct has name_shuhra_ara: {'name_shuhra_ara' in fields}")
    print(f"  Direct has name_shuhra_lat: {'name_shuhra_lat' in fields}")
