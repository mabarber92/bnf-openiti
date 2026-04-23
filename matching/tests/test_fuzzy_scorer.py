"""Unit tests for FuzzyScorer."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from matching.fuzzy_scorer import FuzzyScorer


def test_exact_match():
    """Test exact match scores 100."""
    scorer = FuzzyScorer()
    score = scorer.score("test author", "test author")
    assert score == 100.0, f"Exact match should score 100, got {score}"
    print("  Exact match: 100.0 [OK]")


def test_high_similarity():
    """Test similar strings score high."""
    scorer = FuzzyScorer()
    score = scorer.score("ibn sina", "ibn sina")
    assert score >= 95.0, f"Similar strings should score >= 95, got {score}"
    print(f"  High similarity: {score:.1f} [OK]")


def test_low_similarity():
    """Test dissimilar strings score low."""
    scorer = FuzzyScorer()
    score = scorer.score("completely different text", "xyz123abc")
    assert score < 40.0, f"Dissimilar strings should score < 40, got {score}"
    print(f"  Low similarity: {score:.1f} [OK]")


def test_word_order_invariant():
    """Test token_sort_ratio handles word order."""
    scorer = FuzzyScorer()
    score1 = scorer.score("ibn sina", "sina ibn")
    # token_sort_ratio normalizes word order, so these should score high
    assert score1 >= 80.0, f"Should handle word order, got {score1}"
    print(f"  Word order invariance: {score1:.1f} [OK]")


def test_empty_strings():
    """Test edge case: empty strings."""
    scorer = FuzzyScorer()
    score = scorer.score("", "test")
    assert score == 0.0, f"Empty string should score 0, got {score}"
    print("  Empty string: 0.0 [OK]")


def test_caching():
    """Test cache avoids rescoring."""
    scorer = FuzzyScorer()

    # First call
    score1 = scorer.score("test", "test")
    cache_size_1 = scorer.cache_size()

    # Second call (should hit cache)
    score2 = scorer.score("test", "test")
    cache_size_2 = scorer.cache_size()

    assert score1 == score2, "Cached scores should match"
    assert cache_size_2 == cache_size_1, "Cache size should not grow on duplicate"
    print(f"  Caching: {cache_size_1} entries [OK]")


def test_cache_symmetry():
    """Test cache is symmetric (order doesn't matter)."""
    scorer = FuzzyScorer()

    score1 = scorer.score("apple", "banana")
    cache_after_1 = scorer.cache_size()

    score2 = scorer.score("banana", "apple")
    cache_after_2 = scorer.cache_size()

    assert score1 == score2, "Scores should be symmetric"
    assert cache_after_1 == cache_after_2, "Cache should not grow for reverse order"
    print(f"  Cache symmetry: {cache_after_2} entries [OK]")


def main():
    """Run all tests."""
    print("=" * 60)
    print("FUZZY SCORER UNIT TESTS")
    print("=" * 60)

    try:
        test_exact_match()
        test_high_similarity()
        test_low_similarity()
        test_word_order_invariant()
        test_empty_strings()
        test_caching()
        test_cache_symmetry()

        print("\n" + "=" * 60)
        print("ALL FUZZY SCORER TESTS PASSED")
        print("=" * 60)
        return 0

    except AssertionError as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
