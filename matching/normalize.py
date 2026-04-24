"""
Transliteration normalization for fuzzy matching.

Two strategies:
1. Legacy (normalize_transliteration): converts ayn to 'ayn', removes diacritics
2. Diacritic conversion table (normalize_for_matching): uses parametrized conversions,
   preserves or converts characters based on conversion table

The main entry point is normalize_for_matching(), which conditionally uses the
conversion table based on config.USE_DIACRITIC_CONVERSION_TABLE.
"""

import re
import unicodedata


def normalize_transliteration(text: str) -> str:
    """
    Normalize transliterated Arabic text for fuzzy matching.

    Handles:
    - Ayn variants: all converted to ʿ (by earlier _apply_openiti_conversions)
    - Backtick: converted to ʿ
    - Diacritics: removes combining marks
    - Case: lowercases everything
    - Whitespace: normalizes spaces and hyphens
    """
    if not text:
        return ""

    # 1. Convert backtick to ʿ (BetaCode ayn representation)
    text = text.replace("`", "ʿ")

    # 2. Remove diacritical marks (macrons, underscores, etc.)
    # Use NFD decomposition to separate base chars from combining marks
    text = unicodedata.normalize("NFD", text)
    # Remove combining characters (category Mn = Mark, nonspacing)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    # Recompose to NFC
    text = unicodedata.normalize("NFC", text)

    # 3. Lowercase
    text = text.lower()

    # 4. Normalize whitespace and hyphens
    # Replace hyphens with spaces (so "al-" becomes "al ")
    text = re.sub(r"-+", " ", text)
    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text)
    # Strip leading/trailing whitespace
    text = text.strip()

    return text


def _apply_openiti_conversions(text: str) -> str:
    """
    Hardcoded conversions for OpenITI standards.

    Converts to OpenITI/shared representations:
    - C, c → ʿ (unify ayn: all variants to ʿ)
    - Long vowels (ā, ī, ū) → short (a, i, u)
    - Emphatics with marks (ḥ, ḍ, ṭ, ẓ, ṣ) → base (h, d, t, z, s)
    - Consonant marks (ḏ, ṯ, ḫ, ǧ, š, ġ) → two-letter forms (dh, th, kh, j, sh, gh)
    - ta marbuta (ŧ) → a

    Applied to both BNF and OpenITI data to harmonize representations before matching.
    """
    conversions = {
        "C": "ʿ",      # ayn (OpenITI URI convention)
        "c": "ʿ",      # ayn (lowercase variant)
        "ʾ": "",       # hamza - remove
        "ā": "a",      # long a
        "ī": "i",      # long i
        "ū": "u",      # long u
        "ō": "o",      # long o
        "ē": "e",      # long e
        "ḥ": "h",      # ha with dot
        "ḍ": "d",      # dad with dot
        "ṭ": "t",      # ta with dot
        "ẓ": "z",      # za with dot
        "ṣ": "s",      # sad with dot
        "ḏ": "dh",     # dhal
        "ṯ": "th",     # tha
        "ḫ": "kh",     # kha
        "ǧ": "j",      # jim
        "š": "sh",     # shin
        "ġ": "gh",     # ghayn
        "ŧ": "a",      # ta marbuta
        "á": "a",      # alif maqsura
        "ã": "a",      # dagger alif
    }

    for char, replacement in conversions.items():
        text = text.replace(char, replacement)

    return text


def normalize_for_matching(text: str) -> str:
    """
    Main entry point for normalization.

    Pipeline:
    0. Split camelCase (OpenITI URIs like IbnKhayyat need tokenization)
    1. Apply hardcoded OpenITI transliteration conversions (C→ʿ, ī→i, etc.)
    2. Apply parametrized diacritic table if enabled (for library-specific chars)
    3. Pass through legacy normalizer (handles hyphens, diacritics, spacing)

    This function is used throughout the matching pipeline (AuthorMatcher,
    TitleMatcher, etc.) on both BNF and OpenITI data.
    """
    # Import here to avoid circular dependency
    from matching.config import USE_DIACRITIC_CONVERSION_TABLE

    # Step 0: Split camelCase before conversions (e.g., "IbnKhayyat" → "Ibn Khayyat")
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)

    # Step 1: Apply hardcoded OpenITI conversions (both BNF and OpenITI)
    text = _apply_openiti_conversions(text)

    # Step 2: Apply parametrized diacritic table (optional)
    if USE_DIACRITIC_CONVERSION_TABLE:
        from matching.normalize_diacritics import normalize_with_diacritics
        text = normalize_with_diacritics(text, use_table=True)

    # Step 3: Pass through legacy normalizer (handles remaining normalization)
    return normalize_transliteration(text)


if __name__ == "__main__":
    # Quick test
    test_cases = [
        ("Kitab al-Tabari", "kitab al tabari"),
        ("Kitab al-Ṭabarī", "kitab al tabari"),
        ("ʿAbd al-Rahman", "ayn abd al rahman"),
        ("Cabd al-Rahman", "ayn abd al rahman"),
        ("AL-TABARI", "al tabari"),
        ("Anwār al-tanzīl", "anwar al tanzil"),
    ]

    for original, expected in test_cases:
        result = normalize_transliteration(original)
        status = "OK" if result == expected else "MISMATCH"
        print(f"{status}: {repr(original):30} -> {repr(result):30} (expected {repr(expected)})")
