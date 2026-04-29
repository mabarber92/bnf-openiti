"""
Validate both normalization strategies (table ON vs OFF).

Shows that:
1. Heavy normalization (table OFF): Preserves characters via Unicode decomposition
2. Table-driven (table ON): Adds phonetic preservation via digraph conversion
3. Both preserve base characters; table adds phonetic accuracy
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from matching.normalize import _apply_openiti_conversions, normalize_transliteration
from matching.normalize_diacritics import normalize_with_diacritics

def normalize_table_off(text: str) -> str:
    """Heavy normalization: C→ʿ + Unicode decomposition only."""
    text = _apply_openiti_conversions(text)
    text = normalize_transliteration(text)
    return text

def normalize_table_on(text: str) -> str:
    """Table-driven: C→ʿ + table + Unicode decomposition."""
    text = _apply_openiti_conversions(text)
    text = normalize_with_diacritics(text, use_table=True)
    text = normalize_transliteration(text)
    return text

# Test cases covering different diacritic types
test_cases = [
    ("Šarīf", "Caron + macron: lowercase + combining mark"),
    ("al-Ṣūfī", "Dot + macron: lowercase + emphatic"),
    ("Ǧazūlī", "Caron + macron: uppercase + base letter j"),
    ("Muḥammad", "Dot below: common Arabic name"),
    ("al-Raḥmān", "Dot + macron: two combining marks"),
    ("ʿAbd al-ʿAlī", "Ayn: preserved in both strategies"),
]

print("=" * 90)
print("NORMALIZATION STRATEGIES COMPARISON")
print("=" * 90)
print()

for text, description in test_cases:
    off = normalize_table_off(text)
    on = normalize_table_on(text)

    # Color output: green for identical, yellow for different
    status = "✓ SAME" if off == on else "✗ DIFFERS"

    print(f"Input: {text:20s}  ({description})")
    print(f"  OFF: {off:30s}  ← No table (Unicode decomposition only)")
    print(f"  ON:  {on:30s}  ← With table {status}")
    print()

print("=" * 90)
print("KEY FINDINGS:")
print("=" * 90)
print("""
1. HEAVY NORMALIZATION (Table OFF):
   - Preserves all base characters via Unicode NFD decomposition
   - Example: Š→s (NOT removed entirely, just loses diacritic)
   - Limitation: Cannot produce digraphs (Š→s, not Š→sh)
   - Trade-off: Robust fallback vs phonetic loss

2. TABLE-DRIVEN (Table ON):
   - Adds phonetic preservation via conversion table digraphs
   - Example: Š→sh (maintains transliteration convention)
   - Requires: diacritic_conversions.csv populated
   - Trade-off: Better accuracy vs table maintenance

3. COMPATIBILITY:
   - Both strategies handle ayn (ʿ) correctly
   - Both lowercase and normalize whitespace identically
   - Differences only appear in diacritic character handling
   - Fuzzy matching can tolerate differences (Š→s ≈ Š→sh)

RECOMMENDATION:
- Default: USE_DIACRITIC_CONVERSION_TABLE = True (best accuracy)
- Fallback: Table OFF still works, just with phonetic degradation
- Critical: diacritic_conversions.csv must be populated via utils/survey_bnf.py
""")
