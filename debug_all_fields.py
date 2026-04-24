"""Get all unique fields across all authors."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

all_fields = set()
for author_uri, author_data in openiti_authors.items():
    if isinstance(author_data, dict):
        all_fields.update(author_data.keys())
    else:
        import dataclasses
        all_fields.update(f.name for f in dataclasses.fields(author_data))

print(f"Total unique fields: {len(all_fields)}")
for field in sorted(all_fields):
    if 'ara' in field or 'ar' in field or 'name' in field:
        print(f"  {field}")
