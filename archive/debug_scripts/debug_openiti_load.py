"""Check OpenITI data structure."""

import sys
from pathlib import Path
import dataclasses

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

# Check one author
author_uri = "0845Maqrizi"
author = openiti_authors[author_uri]

print(f"Author {author_uri}:")
print(f"  Type: {type(author)}")
print(f"  Is dict: {isinstance(author, dict)}")

if isinstance(author, dict):
    print("  Keys with values:")
    for key in sorted(author.keys()):
        if author[key]:
            print(f"    {key}")
else:
    print("  Fields with values:")
    for field in dataclasses.fields(author):
        val = getattr(author, field.name)
        if val:
            print(f"    {field.name}")
