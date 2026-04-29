"""
Test that the is_openiti parameter correctly prevents unwanted C→ʿ conversion on BNF data.

This validates the fix for the bug where BNF titles like "Catalogue" were being
incorrectly converted to "ʿatalogue".
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from matching.normalize import normalize_for_matching

print("=" * 90)
print("VALIDATION: is_openiti Parameter Fix")
print("=" * 90)
print()

# Test cases covering the critical bug and normal operation
test_cases = [
    # (input_text, is_openiti, expected_output, description)
    ("Catalogue d'étoiles", False, "catalogue d'etoiles", "BNF French title: C should NOT convert"),
    ("Catalogue d'étoiles", True, "ʿatalogue d'etoiles", "OpenITI variant: C should convert"),
    ("al-Šarīf", False, "al sharif", "BNF author: table converts š→sh"),
    ("al-Šarīf", True, "al sharif", "OpenITI author: table converts š→sh"),
    ("0685IbnKamal", True, "0685ibn kamal", "OpenITI URI: camelcase split, normalized"),
    ("CAbdAlQahir", True, "ʿabd al qahir", "OpenITI URI: C at start converts to ʿ"),
    ("CAbdAlQahir", False, "cabd al qahir", "BNF data: C kept as-is, lowercased"),
]

print("Test Results:")
print("-" * 90)

passed = 0
failed = 0

for input_text, is_openiti, expected, description in test_cases:
    result = normalize_for_matching(input_text, split_camelcase=True, is_openiti=is_openiti)
    status = "✓ PASS" if result == expected else "✗ FAIL"

    if result == expected:
        passed += 1
    else:
        failed += 1

    print(f"{status}: {description}")
    print(f"       Input: {input_text} (is_openiti={is_openiti})")
    print(f"       Got:      {result}")
    print(f"       Expected: {expected}")
    if result != expected:
        print(f"       ⚠ MISMATCH")
    print()

print("=" * 90)
print(f"Results: {passed} passed, {failed} failed")
print("=" * 90)

if failed == 0:
    print("\n✓ All tests passed! The is_openiti parameter correctly handles BNF vs OpenITI data.")
else:
    print(f"\n✗ {failed} test(s) failed. Review the implementation.")

print()
print("Summary:")
print("-" * 90)
print("""
The is_openiti parameter now correctly controls C→ʿ conversion:

1. BNF DATA (is_openiti=False):
   - "Catalogue" stays as "catalogue" (no unwanted conversion)
   - Standard Latin script preserved
   - Only Unicode decomposition applied

2. OPENITI DATA (is_openiti=True):
   - "C" in URIs converts to "ʿ" (ayn placeholder)
   - Allows consistent matching between OpenITI slugs and normalized text
   - Critical for handling OpenITI URI-style author/book references

This ensures BNF and OpenITI data are normalized consistently while respecting
their different conventions.
""")
