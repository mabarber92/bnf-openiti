"""
Fuzzy matching scorer for candidate similarity.

Wraps fuzzywuzzy with caching to avoid rescoring identical pairs.
Used by all matching stages to compute normalized candidate similarity.
"""

from functools import lru_cache
from fuzzywuzzy import fuzz


class FuzzyScorer:
    """Caching fuzzy scorer for candidate matching."""

    def __init__(self):
        """Initialize the scorer with an empty cache."""
        self._cache = {}

    def score(self, str1: str, str2: str) -> float:
        """
        Compute fuzzy similarity score between two strings.

        Uses token_sort_ratio from fuzzywuzzy, which is robust to
        word order variations (important for author names and titles).

        Parameters
        ----------
        str1 : str
            First string (typically from BNF)
        str2 : str
            Second string (typically from OpenITI)

        Returns
        -------
        float
            Similarity score (0.0–100.0)
        """
        if not str1 or not str2:
            return 0.0

        # Create cache key (order-independent for symmetry)
        key = tuple(sorted([str1, str2]))

        if key in self._cache:
            return self._cache[key]

        # Compute score using token_sort_ratio (handles word order)
        score_val = float(fuzz.token_sort_ratio(str1, str2))
        self._cache[key] = score_val

        return score_val

    def cache_size(self) -> int:
        """Return number of cached scores."""
        return len(self._cache)

    def clear_cache(self) -> None:
        """Clear the cache."""
        self._cache.clear()
