"""Check what fields an author has."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

# Get one author
author_uri, author_data = list(openiti_authors.items())[0]

print(f"Author {author_uri} fields:")
if isinstance(author_data, dict):
    for key in sorted(author_data.keys()):
        val = author_data[key]
        if val:
            if isinstance(val, list):
                print(f"  {key}: {len(val)} items - {val[:2] if len(val) > 0 else []}")
            elif isinstance(val, str):
                print(f"  {key}: '{val[:50]}...'")
            else:
                print(f"  {key}: {val}")
else:
    # It's a dataclass
    import dataclasses
    for field in dataclasses.fields(author_data):
        val = getattr(author_data, field.name)
        if val:
            if isinstance(val, list):
                print(f"  {field.name}: {len(val)} items")
            elif isinstance(val, str):
                print(f"  {field.name}: '{val[:50]}...'")
            else:
                print(f"  {field.name}: {val}")
