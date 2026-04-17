"""
utils/tokens.py

Shared tokenisation and script-detection utilities.

Used by both utils/survey_bnf.py (n-gram analysis) and parsers/bnf.py
(boilerplate filtering), and by the matching pipeline for OpenITI slug
normalisation.  Centralised here so that the same normalisation logic is
applied on both sides of every comparison.

Exports
-------
    ARABIC_RE           — compiled regex matching any Arabic-script character
    has_arabic          — True if text contains any Arabic-script character
    has_latin           — True if text contains any basic Latin letter
    tokenize_lat        — Latin-script word tokeniser (with optional abbrev-dot mode)
    tokenize_ar         — Arabic-script word tokeniser
    make_ngrams         — Produce a list of n-gram strings from a token list
    split_camel         — Split a CamelCase OpenITI URI slug into word tokens
    normalise_ayn       — Normalise Unicode ʿayn variants to canonical ʿ (U+02BF)
    openiti_slug_tokens — Split a slug and apply the OpenITI C/c → ʿ convention
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Script-detection patterns
# ---------------------------------------------------------------------------

# Covers: Arabic, Arabic Supplement, Arabic Extended-A,
#         Arabic Presentation Forms A & B
ARABIC_RE = re.compile(
    r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]"
)

_LATIN_RE = re.compile(r"[A-Za-z]")

# ---------------------------------------------------------------------------
# Latin tokenisation patterns
# ---------------------------------------------------------------------------

# Character class covering ASCII + extended Latin (accented) + Latin Extended
# Additional (ALA-LC transliteration: ā, ī, ū, ḍ, ṣ, ḥ, …) +
# Spacing Modifier Letters block (superscript letters used in some transliterations)
_LAT_LETTERS = r"[a-zA-ZÀ-ÖØ-öø-ÿ\u02b0-\u02ff\u1e00-\u1eff]"

# Matches an abbreviation: 1–4 letter-chars immediately followed by a period
# that is NOT itself followed by another letter.  This lets "cf." and "ms."
# be captured with their dot while sentence-ending periods are ignored.
# "e.g." produces "e." and "g." separately — acceptable for this corpus.
_ABBREV_RE = re.compile(
    rf"({_LAT_LETTERS}{{1,4}})\."
    r"(?![a-zA-ZÀ-ÖØ-öø-ÿ\u02b0-\u02ff\u1e00-\u1eff])",
)

# Plain letter-run: one or more consecutive Latin-range letters
_WORD_RE = re.compile(rf"{_LAT_LETTERS}+")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def has_arabic(text: str) -> bool:
    """Return True if *text* contains at least one Arabic-script character."""
    return bool(ARABIC_RE.search(text)) if text else False


def has_latin(text: str) -> bool:
    """Return True if *text* contains at least one basic Latin letter (A–Z / a–z)."""
    return bool(_LATIN_RE.search(text)) if text else False


def tokenize_lat(text: str, keep_abbrev_dots: bool = False) -> list[str]:
    """Tokenise Latin-script text into lowercase word tokens.

    Covers ASCII Latin, extended Latin (accented characters), and the
    precomposed Latin Extended Additional block used in ALA-LC transliteration
    (ā, ī, ū, ḍ, ṣ, ḥ, etc.).  Tokens shorter than 2 characters are dropped.

    Parameters
    ----------
    text : str
        Raw input string (any mixture of scripts; only Latin tokens extracted).
    keep_abbrev_dots : bool
        When True, tokens of 1–4 letters immediately followed by a period
        (abbreviation pattern, e.g. "Cf.", "ms.", "no.") are emitted with the
        dot retained (e.g. "cf.", "ms.").  All other punctuation is still
        stripped.  Enables abbreviation-specific n-grams like "cf. ms. arabe".
    """
    text_lower = text.lower()
    tokens: list[str] = []
    if keep_abbrev_dots:
        pos = 0
        while pos < len(text_lower):
            abbrev = _ABBREV_RE.match(text_lower, pos)
            if abbrev:
                tokens.append(abbrev.group(0))   # e.g. "cf."
                pos = abbrev.end()
                continue
            word = _WORD_RE.match(text_lower, pos)
            if word:
                t = word.group(0)
                if len(t) >= 2:
                    tokens.append(t)
                pos = word.end()
                continue
            pos += 1
    else:
        tokens = [t for t in _WORD_RE.findall(text_lower) if len(t) >= 2]
    return tokens


def tokenize_ar(text: str) -> list[str]:
    """Tokenise Arabic-script text into word tokens.

    Arabic particles (و، في، من …) are kept — they form meaningful n-gram
    components for BNF description phrases.
    """
    return re.findall(
        r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]+",
        text,
    )


def make_ngrams(tokens: list[str], n: int) -> list[str]:
    """Return all n-grams (as space-joined strings) from *tokens*."""
    return [" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]


# ---------------------------------------------------------------------------
# OpenITI slug normalisation
# ---------------------------------------------------------------------------

# Split on camelCase boundaries:
#   lower→upper:  "NasirDin" → "Nasir" | "Din"
#   upper+lower:  "AHMad"    → "AH"   | "Mad"  (handles acronym-like prefixes)
_CAMEL_RE = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def split_camel(slug: str) -> list[str]:
    """Split an OpenITI CamelCase URI slug into word tokens.

    Splits on camelCase boundaries only — no other transformation applied.

        "NasirDinBaydawi" → ["Nasir", "Din", "Baydawi"]
        "FadailMakka"     → ["Fadail", "Makka"]
        "IbnCali"         → ["Ibn", "Cali"]

    For matching-ready tokens with ʿayn conversion applied, use
    openiti_slug_tokens() instead.
    """
    return [t for t in _CAMEL_RE.split(slug) if t]


# ʿayn Unicode variants → canonical ALA-LC form ʿ (U+02BF).
# Does NOT include hamza variants (ʾ U+02BE) — hamza and ʿayn are distinct.
_AYN_TABLE = str.maketrans({
    "\u02BB": "\u02BF",   # ʻ modifier letter turned comma
    "\u2018": "\u02BF",   # ' left single quotation mark
    "\u02BC": "\u02BF",   # ʼ modifier letter apostrophe
    "\u0060": "\u02BF",   # ` grave accent (rare)
})


def normalise_ayn(text: str) -> str:
    """Normalise Unicode ʿayn variants to the canonical ALA-LC form ʿ (U+02BF).

    Collapses ʻ (U+02BB), ' (U+2018), ʼ (U+02BC), and ` (U+0060) into ʿ.
    Hamza (ʾ U+02BE) is left unchanged — it is phonetically distinct.

    Use this on BNF transliterated text before comparing against OpenITI data.
    """
    return text.translate(_AYN_TABLE)


def openiti_slug_tokens(slug: str) -> list[str]:
    """Split an OpenITI slug and apply the C/c → ʿ ʿayn convention.

    In OpenITI URIs, a capital C before a lowercase letter represents ʿayn:

        "CamrIbnKulthum"    → ["ʿamr", "ibn", "kulthum"]
        "AbuTalibCabdManaf" → ["abu", "talib", "ʿabd", "manaf"]
        "NasirDinBaydawi"   → ["nasir", "din", "baydawi"]

    Returns lowercase tokens suitable for case-insensitive matching.
    Apply normalise_ayn() to BNF text before comparing against these tokens.
    """
    result = []
    for token in split_camel(slug):
        if len(token) >= 2 and token[0] == "C" and token[1].islower():
            result.append("\u02BF" + token[1:].lower())
        else:
            result.append(token.lower())
    return result
