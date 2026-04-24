import json
from matching.normalize import normalize_transliteration
from matching.normalize_diacritics import normalize_with_diacritics

# Load one of the failing records
with open("data_samplers/correspondence.json", encoding='utf-8') as f:
    data = json.load(f)

record = data[4]  # OAI_10884191 (5th record, 0-indexed)
print(f"Record: {record['bnf_id']}")
print(f"Expected: {record['expected_matches']}")
print()

# Get the author and title
author = record.get('author', '')
title = record.get('title', '')

print(f"Author (raw):  {repr(author)}")
print(f"Title (raw):   {repr(title)}")
print()

# Test both normalizers
legacy_author = normalize_transliteration(author)
legacy_title = normalize_transliteration(title)

new_author = normalize_with_diacritics(author, use_table=True)
new_title = normalize_with_diacritics(title, use_table=True)

print(f"Legacy author: {repr(legacy_author)}")
print(f"New author:    {repr(new_author)}")
print(f"Match: {legacy_author == new_author}")
print()

print(f"Legacy title:  {repr(legacy_title)}")
print(f"New title:     {repr(new_title)}")
print(f"Match: {legacy_title == new_title}")
