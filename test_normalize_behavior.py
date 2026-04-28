"""
Test normalization behavior with and without the conversion table.

Verifies that:
1. Table ON applies diacritic mappings
2. Table OFF uses decomposition only
3. Both preserve base characters
4. OpenITI date stripping works
5. Results are consistent and sensible
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from matching.normalize import normalize_for_matching

print("=" * 100)
print("NORMALIZATION BEHAVIOR TEST")
print("=" * 100)
print()

# Test cases: (text, is_openiti, description)
test_cases = [
    # BNF data
    ("al-Šarīf", False, "BNF: diacritic lowercase s with caron"),
    ("Catalogue d'étoiles", False, "BNF: French title, should NOT convert C→ʿ"),
    ("al-Ṣūfī, ʿAbd al-Raḥmān", False, "BNF: mixed case author with emphatic and ayn"),

    # OpenITI data
    ("0685IbnKamal", True, "OpenITI: author with date prefix"),
    ("CAbdAlQahir", True, "OpenITI: author with C→ʿ conversion"),
    ("0845Maqrizi.Mawaciz", True, "OpenITI: book slug with date"),
]

# Test with table OFF (heavy Unicode decomposition only)
print("=" * 100)
print("TABLE OFF (Heavy Unicode Decomposition Only)")
print("=" * 100)
print()

results_off = {}
for text, is_openiti, desc in test_cases:
    # Manually disable table for this test
    from matching.normalize import _apply_openiti_conversions, normalize_transliteration
    import re

    # Replicate normalize_for_matching but skip the table
    normalized = text

    # Strip date if OpenITI
    if is_openiti:
        normalized = re.sub(r"\d{4}", "", normalized)

    # Split camelCase
    normalized = re.sub(r"([a-z])([A-Z])", r"\1 \2", normalized)

    # Apply C→ʿ
    normalized = _apply_openiti_conversions(normalized, is_openiti=is_openiti)

    # Skip table - go straight to decomposition
    normalized = normalize_transliteration(normalized)

    results_off[text] = normalized
    print(f"{desc}")
    print(f"  Input:  {text}")
    print(f"  Output: {normalized}")
    print()

# Test with table ON (if available)
print()
print("=" * 100)
print("TABLE ON (Diacritic Conversion Table + Unicode Decomposition)")
print("=" * 100)
print()

results_on = {}
for text, is_openiti, desc in test_cases:
    # Use the actual function which applies table if config allows
    normalized = normalize_for_matching(text, split_camelcase=True, is_openiti=is_openiti)
    results_on[text] = normalized

    print(f"{desc}")
    print(f"  Input:  {text}")
    print(f"  Output: {normalized}")
    print()

# Compare results
print()
print("=" * 100)
print("COMPARISON")
print("=" * 100)
print()

for text, is_openiti, desc in test_cases:
    off = results_off[text]
    on = results_on[text]

    if off == on:
        status = "SAME"
    else:
        status = "DIFFERENT"

    print(f"{status}: {desc}")
    if off != on:
        print(f"       OFF: {off}")
        print(f"       ON:  {on}")
    print()

print("=" * 100)
print("VALIDATION")
print("=" * 100)
print()

# Key checks
checks = [
    ("Catalogue stays as 'catalogue' (no C→ʿ)", "catalogue" in results_off[("Catalogue d'étoiles", False, "BNF: French title, should NOT convert C→ʿ")]),
    ("Date stripped from OpenITI", "0685" not in results_off[("0685IbnKamal", True, "OpenITI: author with date prefix")]),
    ("Ayn preserved", "ʿabd" in results_off[("al-Ṣūfī, ʿAbd al-Raḥmān", False, "BNF: mixed case author with emphatic and ayn")]),
]

for check_desc, result in checks:
    status = "✓" if result else "✗"
    print(f"{status} {check_desc}")
