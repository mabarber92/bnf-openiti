"""Check all field names in OpenITI authors."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

# Get all unique field names
all_fields = set()
for author_uri, author_data in openiti_authors.items():
    if isinstance(author_data, dict):
        all_fields.update(author_data.keys())
    break

print("All author fields:")
for field in sorted(all_fields):
    print(f"  {field}")
