"""
Stage 3: Filter title matches by matched authors (intersection).

Keeps only books whose author is in the Stage 1 matched authors.
This stage significantly improves precision by enforcing that both
author and title must match together.
"""

from tqdm import tqdm


class CombinedMatcher:
    """Filter title matches by matched authors (intersection)."""

    def __init__(self, verbose: bool = True, use_confidence_filtering: bool = False):
        """
        Initialize the combined matcher.

        Parameters
        ----------
        verbose : bool
            Print progress information
        use_confidence_filtering : bool
            If True, apply confidence-dependent filtering to reduce false positives.
            Marginal author matches (0.80-0.85) require higher title scores.
        """
        self.verbose = verbose
        self.use_confidence_filtering = use_confidence_filtering

    def execute(self, pipeline) -> None:
        """
        Stage 3: Combined scoring on matched author+book pairs.

        For each BNF record:
        1. Get Stage 1 results (matched author URIs) with scores
        2. Get Stage 2 results (matched book URIs) with scores
        3. For each valid author+book pair (author_uri matches book's author):
           - Check if both scores >= COMBINED_FLOOR (0.80)
           - Check if title_score >= TITLE_FLOOR (ensures rare-token boosted titles dominate weak-author cases)
           - Check if combined score (author+book)/2 >= COMBINED_THRESHOLD
           - If all pass, keep the pair
        4. Store filtered results as Stage 3 result

        Scoring logic:
        - Common-only matches (no rare tokens): fuzzy_score kept as-is
        - Matches with rare tokens: fuzzy_score * RARE_TOKEN_BOOST_FACTOR
        - Combined threshold ensures we don't accept pairs where both stages are weak
        - Floors ensure no single weak stage dominates and rare-token boosts matter

        Example:
        - Author 85% (common name) + Title 95% (rare title) → both >= 80%, title >= 90%, combined=90% ✓ PASS
        - Author 85% (common name) + Title 85% (common title) → title < 90% ✗ FAIL (title not strong enough)
        - Author 50% (weak) + Title 95% (strong) → author < 80% ✗ FAIL (can't rely on weak author)

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with loaded indices
        """
        from matching.config import COMBINED_THRESHOLD, COMBINED_FLOOR, TITLE_FLOOR

        if self.verbose:
            print("\n--- Stage 3: Combined Scoring ---")
            print(f"  Floor: {COMBINED_FLOOR:.2%} (both author and title must meet this)")
            print(f"  Title floor: {TITLE_FLOOR:.2%} (title must meet this; incentivizes rare-token boosts)")
            print(f"  Threshold: {COMBINED_THRESHOLD:.2%} (combined score must meet this)")

        # Iterate through all BNF records
        bnf_ids = list(pipeline.bnf_records.keys())

        for bnf_id in tqdm(
            bnf_ids,
            desc="Combined scoring",
            disable=not self.verbose,
        ):
            # Get Stage 1 and Stage 2 results with scores
            stage1_authors = pipeline.get_stage1_result(bnf_id) or []
            stage2_books = pipeline.get_stage2_result(bnf_id) or []

            if not stage1_authors or not stage2_books:
                # No intersection possible
                pipeline.set_stage3_result(bnf_id, [])
                continue

            # Get scores for combined threshold check
            stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
            stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}

            # Collect all candidate pairs that pass floor checks
            candidate_pairs = []  # List of (book_uri, author_score, title_score) tuples
            for book_uri in stage2_books:
                book = pipeline.openiti_index.get_book(book_uri)
                if book is None:
                    continue

                # Get author_uri from book (handle dict and dataclass)
                if isinstance(book, dict):
                    book_author_uri = book.get("author_uri")
                else:
                    book_author_uri = book.author_uri

                # Gate 1: Author URI must be in stage 1 results (valid pairing)
                if book_author_uri not in stage1_authors:
                    continue

                # Gate 2: Get scores (should exist, but default to 1.0 if missing)
                author_score = stage1_scores.get(book_author_uri, 1.0)
                title_score = stage2_scores.get(book_uri, 1.0)

                # Gate 3: Both scores must be >= COMBINED_FLOOR
                if author_score < COMBINED_FLOOR or title_score < COMBINED_FLOOR:
                    continue

                # Gate 4: Title must be >= TITLE_FLOOR to ensure rare-token boosted matches dominate weak-author cases
                # This filters pairs where both scores are in the mediocre range (0.80-0.90)
                # and incentivizes high-quality title matches (with rare tokens) over weak author matches
                if title_score < TITLE_FLOOR:
                    continue

                candidate_pairs.append((book_uri, author_score, title_score))

            # Normalize combined scores and apply threshold
            combined_matches = []
            if candidate_pairs:
                # Calculate combined scores and find max
                combined_scores = [(author_score + title_score) / 2.0 for _, author_score, title_score in candidate_pairs]
                max_combined = max(combined_scores)

                # Filter by threshold; only normalize if max > 1.0 to avoid upweighting poor candidates
                for (book_uri, _, _), combined_score in zip(candidate_pairs, combined_scores):
                    if max_combined > 1.0:
                        # Normalize only if top score exceeds 1.0 (rare token boost caused high score)
                        normalized_score = combined_score / max_combined
                    else:
                        # Keep raw score (already in [0,1] range)
                        normalized_score = combined_score

                    if normalized_score >= COMBINED_THRESHOLD:
                        combined_matches.append((book_uri, normalized_score))

            # Sort by normalized score descending (best first)
            if combined_matches:
                combined_matches.sort(key=lambda x: x[1], reverse=True)
                ranked_uris = [uri for uri, _ in combined_matches]
                pipeline.set_stage3_result(bnf_id, ranked_uris)
                # Store normalized combined scores for debugging
                pipeline.set_stage3_scores(bnf_id, {uri: score for uri, score in combined_matches})
            else:
                pipeline.set_stage3_result(bnf_id, [])

        if self.verbose:
            print("Stage 3 complete. Combined scoring applied.")
