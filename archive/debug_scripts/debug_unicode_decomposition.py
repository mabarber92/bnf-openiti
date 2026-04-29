"""
Test if Unicode NFD decomposition can handle heavy diacritic stripping without a table.
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

import unicodedata

def normalize_heavy_unicode_only(text: str) -> str:
    """
    Heavy normalization using ONLY Unicode decomposition and combining mark removal.
    No conversion table needed.

    Pipeline:
    1. NFD decomposition (separate base chars from combining marks)
    2. Remove all combining marks (category Mn)
    3. NFC recomposition
    4. Handle special cases (ayn, etc.)
    """
    if not text:
        return ""

    # Step 1: NFD decomposition
    text = unicodedata.normalize("NFD", text)

    # Step 2: Remove combining marks (category Mn = Mark, nonspacing)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")

    # Step 3: NFC recomposition
    text = unicodedata.normalize("NFC", text)

    # Step 4: Lowercase and cleanup
    text = text.lower()
    text = text.replace("-", " ")  # Replace hyphens with spaces
    text = " ".join(text.split())  # Collapse whitespace

    return text


# Test cases: problem characters from debug output
test_cases = [
    ("al-Šarif", "al sharif"),          # Š should become s
    ("Allâh", "allah"),                 # â should become a
    ("ʿAbd al-Rahman", "abd al rahman"), # ʿ should be removed
    ("al-Ṣūfī", "al sufi"),             # Ṣ→s, ū→u, ī→i
    ("Maqrizi", "maqrizi"),             # No diacritics, should lowercase
    ("Kitāb", "kitab"),                 # ā→a
    ("café", "cafe"),                   # é→e
    ("naïve", "naive"),                 # ï→i
]

print("="*100)
print("UNICODE-ONLY HEAVY NORMALIZATION TEST")
print("="*100)
print()

all_pass = True
for original, expected in test_cases:
    result = normalize_heavy_unicode_only(original)
    status = "✓ PASS" if result == expected else "✗ FAIL"

    if result != expected:
        all_pass = False

    print(f"{status}: {original:20s} → {result:20s} (expected: {expected})")
    if result != expected:
        print(f"       Got: {repr(result)}")
        print(f"       Expected: {repr(expected)}")

print()
print("="*100)
if all_pass:
    print("✓ All tests passed! Unicode-only approach works.")
    print("  Can refactor normalize_transliteration to use heavy Unicode decomposition.")
else:
    print("✗ Some tests failed. Need to investigate what Unicode decomposition misses.")

print()
print("ANALYSIS: Unicode character categories")
print("="*100)

# Show what happens to each problem character
problem_chars = {
    'Š': 'LATIN CAPITAL LETTER S WITH CARON',
    'š': 'LATIN SMALL LETTER S WITH CARON',
    'â': 'LATIN SMALL LETTER A WITH CIRCUMFLEX',
    'ʿ': 'MODIFIER LETTER APOSTROPHE (ayn)',
    'Ṣ': 'LATIN CAPITAL LETTER S WITH DOT BELOW',
    'ū': 'LATIN SMALL LETTER U WITH MACRON',
}

for char, desc in problem_chars.items():
    try:
        name = unicodedata.name(char)
    except ValueError:
        name = desc

    category = unicodedata.category(char)

    # Decompose
    nfd = unicodedata.normalize("NFD", char)
    decomposed_chars = [f"{c} (U+{ord(c):04X}, {unicodedata.category(c)})" for c in nfd]

    # After removing combining marks
    cleaned = "".join(c for c in nfd if unicodedata.category(c) != "Mn")

    print(f"\n{char} (U+{ord(char):04X})")
    print(f"  Name: {name}")
    print(f"  Category: {category}")
    print(f"  NFD decomposition: {nfd} → [{', '.join(decomposed_chars)}]")
    print(f"  After removing Mn: {cleaned}")
