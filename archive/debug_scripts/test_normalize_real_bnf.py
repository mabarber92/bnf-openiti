"""
Test normalization on real BNF data.

Loads 20 BNF records and shows how authors/titles normalize with table ON vs OFF.
"""

import json
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from matching.normalize import normalize_for_matching
from matching.normalize import _apply_openiti_conversions, normalize_transliteration
import re

# Helper to normalize without table (heavy Unicode only)
def normalize_table_off(text):
    """Normalize using heavy Unicode decomposition only"""
    if not text:
        return ""
    # Strip OpenITI date (shouldn't be in BNF, but for consistency)
    # Split camelCase
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    # Apply C→ʿ
    text = _apply_openiti_conversions(text, is_openiti=False)
    # Heavy decomposition
    text = normalize_transliteration(text)
    return text

# Load BNF data
with open('outputs/bnf_parsed.json', encoding='utf-8') as f:
    bnf_data = json.load(f)

records = bnf_data['records']

print("=" * 120)
print("NORMALIZATION TEST: Real BNF Data (20 records)")
print("=" * 120)
print()

tested = 0
for uri, record in list(records.items()):
    if tested >= 20:
        break

    if not record:
        continue

    # Get author and title
    creator = record.get('creator_ara', '') or record.get('creator_lat', '')
    title = record.get('title_ara', '') or record.get('title_lat', '')

    if not creator and not title:
        continue

    # Extract text from list if needed
    if isinstance(creator, list):
        creator = creator[0] if creator else ''
    if isinstance(title, list):
        title = title[0] if title else ''

    creator = str(creator) if creator else ''
    title = str(title) if title else ''

    if not creator and not title:
        continue

    tested += 1

    print(f"Record {tested}: {uri}")
    print()

    # Normalize creator
    if creator:
        creator_off = normalize_table_off(creator)
        creator_on = normalize_for_matching(creator, is_openiti=False)
        match = "✓" if creator_off == creator_on else "✗"

        print(f"  Creator: {creator[:80]}")
        print(f"    OFF: {creator_off[:100]}")
        print(f"    ON:  {creator_on[:100]} {match}")
        print()

    # Normalize title
    if title:
        title_off = normalize_table_off(title)
        title_on = normalize_for_matching(title, is_openiti=False)
        match = "✓" if title_off == title_on else "✗"

        print(f"  Title: {title[:80]}")
        print(f"    OFF: {title_off[:100]}")
        print(f"    ON:  {title_on[:100]} {match}")
        print()

print("=" * 120)
print("Legend:")
print("  OFF = Heavy Unicode decomposition only (no table)")
print("  ON  = With diacritic conversion table")
print("  ✓   = Same result (table has no effect on this data)")
print("  ✗   = Different result (table applied specific mappings)")
print("=" * 120)
