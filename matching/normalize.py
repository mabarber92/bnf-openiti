"""
Transliteration normalization for fuzzy matching.

Standardizes:
- Ayn variants (C, ʿ, etc. -> single form)
- Diacritical marks (macrons, underscores, etc.)
- Case variation
- Whitespace
"""

import re
import unicodedata


def normalize_transliteration(text: str) -> str:
    """
    Normalize transliterated Arabic text for fuzzy matching.

    Handles:
    - Ayn variants: C, c, ʿ, ` -> single form
    - Diacritics: removes combining marks and special forms
    - Case: lowercases everything
    - Whitespace: normalizes spaces and hyphens
    """
    if not text:
        return ""

    # 1. Normalize ayn variants to 'ayn' (ʿ)
    # Handle uppercase C, lowercase c, backtick, standard ayn
    text = re.sub(r"[Cʿ`']", "ayn", text)

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
