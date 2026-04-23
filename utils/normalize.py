"""
Text normalization for matching pipelines.

Provides configurable normalization strategies for Latin and Arabic scripts.
Used by matching stages to normalize candidates before fuzzy matching or embedding.

Arabic normalization uses the external openiti library (pip install OpenITI).
Latin normalization uses custom functions optimized for our corpus.

Strategies
----------
- "fuzzy": aggressive normalization for fuzzy matching
  - Arabic: light (strip vocalization, preserve hamza)
  - Latin: lowercase, normalize ayn, strip diacritics
- "embedding": moderate normalization for embedding similarity
  - Arabic: vocalization only (preserve hamza, case, structure)
  - Latin: lowercase only
- "raw": no normalization
"""

from __future__ import annotations

import unicodedata
import re

from utils.tokens import normalise_ayn

try:
    from openiti.helper.ara import normalize_ara_light, normalize_ara_heavy
except ImportError as e:
    raise ImportError(
        "OpenITI library required: pip install OpenITI"
    ) from e


# ============================================================================
# LATIN NORMALIZATION
# ============================================================================

def _normalize_latin_fuzzy(text: str) -> str:
    """
    Aggressive Latin normalization for fuzzy matching.

    - Lowercase
    - Normalize ʿayn variants to canonical form
    - Strip diacritics (accents, macrons, etc.)
    - Collapse whitespace
    """
    # Lowercase
    text = text.lower()

    # Normalize ʿayn variants
    text = normalise_ayn(text)

    # Strip diacritics using NFD decomposition
    # (separates base characters from combining marks)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")

    # Collapse whitespace
    text = " ".join(text.split())

    return text


def _normalize_latin_embedding(text: str) -> str:
    """
    Moderate Latin normalization for embedding similarity.

    - Lowercase only
    - Preserve diacritics (models often trained on them)
    - Preserve case structure information would be lost
    """
    return text.lower()


def _normalize_latin_raw(text: str) -> str:
    """No normalization."""
    return text


# ============================================================================
# ARABIC NORMALIZATION
# ============================================================================

def _normalize_arabic_fuzzy(text: str) -> str:
    """
    Arabic normalization for fuzzy matching.

    Uses openiti.normalize_ara_light:
    - Strips vocalization (diacritics)
    - Preserves hamza (useful signal for matching)
    - Normalizes Persian letters

    WARNING: openiti functions strip digits and Latin script. Only feed pure Arabic.
    """
    if not text:
        return text

    # Verify text is pure Arabic before passing to openiti
    if _contains_latin_or_digit(text):
        raise ValueError(
            f"Arabic normalization requires pure Arabic text. "
            f"Text contains Latin/digits: {text[:50]}"
        )

    return normalize_ara_light(text)


def _normalize_arabic_embedding(text: str) -> str:
    """
    Arabic normalization for embedding similarity.

    Strips vocalization only (diacritics), preserving:
    - Hamza (phonetic signal)
    - Case structure
    - All base characters

    Uses a simple diacritic removal (NFD decomposition) rather than
    full openiti normalization to avoid losing structure signals.
    """
    if not text:
        return text

    # Verify text is pure Arabic before processing
    if _contains_latin_or_digit(text):
        raise ValueError(
            f"Arabic normalization requires pure Arabic text. "
            f"Text contains Latin/digits: {text[:50]}"
        )

    # Use NFD to decompose vocalization marks, then remove them
    text = unicodedata.normalize("NFD", text)
    # Keep only base characters (category != Mn for combining marks)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")

    return text


def _normalize_arabic_raw(text: str) -> str:
    """No normalization."""
    return text


# ============================================================================
# HELPERS
# ============================================================================

def _contains_latin_or_digit(text: str) -> bool:
    """Check if text contains Latin letters or digits."""
    return bool(re.search(r"[A-Za-z0-9]", text))


# ============================================================================
# PUBLIC API
# ============================================================================

def normalize(text: str, script: str, strategy: str = "fuzzy") -> str:
    """
    Normalize text for matching or embedding.

    Parameters
    ----------
    text : str
        Input text to normalize.
    script : str
        Script type: "lat" (Latin) or "ara" (Arabic).
    strategy : str
        Normalization density: "fuzzy", "embedding", or "raw".
        - "fuzzy": aggressive (use for fuzzy matching thresholds)
        - "embedding": moderate (use for embedding similarity ranking)
        - "raw": no normalization

    Returns
    -------
    str
        Normalized text.

    Raises
    ------
    ValueError
        If Arabic text contains Latin letters or digits (data contamination).
    """
    if not text:
        return text

    if script == "lat":
        normalizers = {
            "fuzzy": _normalize_latin_fuzzy,
            "embedding": _normalize_latin_embedding,
            "raw": _normalize_latin_raw,
        }
    elif script == "ara":
        normalizers = {
            "fuzzy": _normalize_arabic_fuzzy,
            "embedding": _normalize_arabic_embedding,
            "raw": _normalize_arabic_raw,
        }
    else:
        raise ValueError(f"Unknown script: {script}. Use 'lat' or 'ara'.")

    if strategy not in normalizers:
        raise ValueError(f"Unknown strategy: {strategy}. Use 'fuzzy', 'embedding', or 'raw'.")

    return normalizers[strategy](text)


def normalize_candidates(
    candidates: list[str], script: str, strategy: str = "fuzzy"
) -> list[str]:
    """
    Normalize a list of candidates (convenience wrapper).

    Parameters
    ----------
    candidates : list[str]
        List of candidate strings.
    script : str
        Script type: "lat" or "ara".
    strategy : str
        Normalization strategy: "fuzzy", "embedding", or "raw".

    Returns
    -------
    list[str]
        Normalized candidates (preserves order, no deduplication).
    """
    return [normalize(cand, script, strategy) for cand in candidates]
