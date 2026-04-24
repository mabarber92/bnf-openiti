"""Check what the pipeline actually loads."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from parsers.bnf import load_bnf_records

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
bnf_records = load_bnf_records(BNF_FULL_PATH)

# Create a small pipeline
bnf_test = {"OAI_10884186": bnf_records["OAI_10884186"]}
pipeline = MatchingPipeline(bnf_test, openiti_data, run_id="debug", verbose=False)

# Check one author in the pipeline
test_author_uri = "0660IbnCadim"
author_data = pipeline.openiti_index.authors.get(test_author_uri)

if author_data:
    print(f"Author {test_author_uri} in pipeline:")
    print(f"  Is dict: {isinstance(author_data, dict)}")
    
    if isinstance(author_data, dict):
        for key in sorted(author_data.keys()):
            val = author_data[key]
            if val:
                print(f"  {key}: {type(val).__name__} = {str(val)[:50]}")
    else:
        print(f"  Is dataclass")
        import dataclasses
        for field in dataclasses.fields(author_data):
            val = getattr(author_data, field.name)
            if val:
                print(f"  {field.name}: {str(val)[:50]}")
else:
    print(f"Author {test_author_uri} not found")
    # List first few authors
    print("First 5 authors in pipeline:")
    for uri in list(pipeline.openiti_index.authors.keys())[:5]:
        print(f"  {uri}")
