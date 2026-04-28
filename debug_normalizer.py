"""
Debug script: Compare normalizer output with and without conversion table.

Load 20 BNF records and show how they're normalized both ways.
"""

import json
import sys

# Force UTF-8 output
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
import matching.config as cfg

# Temporarily disable for off-table run
cfg.USE_DIACRITIC_CONVERSION_TABLE = False
from matching.normalize import normalize_for_matching as normalize_off

# Now enable for on-table run
cfg.USE_DIACRITIC_CONVERSION_TABLE = True
from importlib import reload
import matching.normalize as norm_module
reload(norm_module)
from matching.normalize import normalize_for_matching as normalize_on

# Load 20 sample BNF records
print("Loading BNF records...")
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
bnf_list = list(all_bnf.items())[:20]

print(f"\n{'='*120}")
print("NORMALIZER COMPARISON: ON vs OFF (Conversion Table)")
print(f"{'='*120}")

for idx, (bnf_id, record) in enumerate(bnf_list, 1):
    creators_lat = getattr(record, "creator_lat", []) or []
    titles_lat = getattr(record, "title_lat", []) or []

    creator = creators_lat[0] if creators_lat else ""
    title = titles_lat[0] if titles_lat else ""

    if not creator and not title:
        continue

    def safe_display(text, maxlen=60):
        """Display text, truncating if needed, showing length."""
        if len(text) > maxlen:
            return f"{text[:maxlen]}... (len={len(text)})"
        return text

    print(f"\n{idx}. {bnf_id}")
    print(f"   Creator: {safe_display(creator)}")
    print(f"   Title:   {safe_display(title)}")

    # Normalize both ways
    creator_off = normalize_off(creator, split_camelcase=False)
    creator_on = normalize_on(creator, split_camelcase=False)

    title_off = normalize_off(title, split_camelcase=False)
    title_on = normalize_on(title, split_camelcase=False)

    # Show results
    creator_match = "✓" if creator_off == creator_on else "✗ DIFFER"
    title_match = "✓" if title_off == title_on else "✗ DIFFER"

    print(f"\n   Creator (OFF): {safe_display(creator_off)}")
    print(f"   Creator (ON):  {safe_display(creator_on)} {creator_match}")

    print(f"\n   Title (OFF): {safe_display(title_off)}")
    print(f"   Title (ON):  {safe_display(title_on)} {title_match}")

    if creator_off != creator_on or title_off != title_on:
        print("\n   ⚠️  DIFFERENCE DETECTED")
        if creator_off != creator_on:
            print(f"      Creator diff:")
            print(f"        OFF (len={len(creator_off)}): {safe_display(repr(creator_off), 80)}")
            print(f"        ON  (len={len(creator_on)}): {safe_display(repr(creator_on), 80)}")
        if title_off != title_on:
            print(f"      Title diff:")
            print(f"        OFF (len={len(title_off)}): {safe_display(repr(title_off), 80)}")
            print(f"        ON  (len={len(title_on)}): {safe_display(repr(title_on), 80)}")

print(f"\n{'='*120}")
print("ANALYSIS")
print(f"{'='*120}")
print(f"USE_DIACRITIC_CONVERSION_TABLE: {cfg.USE_DIACRITIC_CONVERSION_TABLE}")
print(f"Conversion table file location: outputs/bnf_survey/diacritic_conversions.csv")

# Check if conversion table file exists
from pathlib import Path
table_path = Path("outputs/bnf_survey/diacritic_conversions.csv")
if table_path.exists():
    print(f"✓ Conversion table file exists")
    # Check if it has content
    with open(table_path) as f:
        lines = f.readlines()
    print(f"  File has {len(lines)} lines (including header)")
    # Show first few rows
    if len(lines) > 1:
        print(f"  First row (header): {lines[0].strip()}")
        if len(lines) > 2:
            print(f"  First data row: {lines[1].strip()}")
else:
    print(f"✗ Conversion table file NOT found at {table_path}")
    print(f"  This means normalize_with_diacritics() is running in fallback mode")
    print(f"  (removes all unmapped non-ASCII characters)")
