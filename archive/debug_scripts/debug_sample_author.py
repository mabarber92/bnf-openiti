"""Show all fields for a sample author."""

import sys
from pathlib import Path
import json

sys.path.insert(0, str(Path.cwd()))

from matching.config import OPENITI_CORPUS_PATH
from parsers.openiti import load_openiti_corpus

openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
openiti_authors = openiti_data["authors"]

# Get one author that has many fields
test_author = "0660IbnCadim"
if test_author in openiti_authors:
    author = openiti_authors[test_author]
    if isinstance(author, dict):
        print("Author fields (dict):")
        for key in sorted(author.keys()):
            val = author[key]
            print(f"  {key}: {type(val).__name__}")
    else:
        print("Author is dataclass, converting to dict...")
        import dataclasses
        author_dict = dataclasses.asdict(author)
        print("Author fields (from dataclass):")
        for key in sorted(author_dict.keys()):
            val = author_dict[key]
            print(f"  {key}: {type(val).__name__}")
else:
    # Find a author with many fields
    for author_uri in list(openiti_authors.keys())[:20]:
        author = openiti_authors[author_uri]
        if isinstance(author, dict):
            count = len([k for k, v in author.items() if v])
        else:
            import dataclasses
            author_dict = dataclasses.asdict(author)
            count = len([k for k, v in author_dict.items() if v])
        
        if count > 10:
            print(f"Found {author_uri} with {count} non-empty fields")
            if isinstance(author, dict):
                print("Fields:")
                for key in sorted(author.keys()):
                    val = author[key]
                    if val:
                        print(f"  {key}: {type(val).__name__}")
            break
