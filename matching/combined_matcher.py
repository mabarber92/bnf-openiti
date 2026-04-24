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
        Stage 3: Filter title matches by matched authors.

        For each BNF record:
        1. Get Stage 1 results (matched author URIs)
        2. Get Stage 2 results (matched book URIs)
        3. Keep only books whose author_uri is in matched authors
        4. (Optional) Apply confidence-dependent filtering to reduce marginal false positives
        5. Store intersection as Stage 3 result

        If use_confidence_filtering is True:
        - Author score >= 0.90: Accept any title match
        - Author score 0.85-0.89: Require title score >= 0.90
        - Author score 0.80-0.84: Require title score >= 0.95

        This helps eliminate false positives from marginal author matches when
        combined with average title matches.

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with loaded indices
        """
        if self.verbose:
            print("\n--- Stage 3: Combined Matching (Intersection) ---")
            if self.use_confidence_filtering:
                print("  (Using confidence-dependent filtering)")

        # Iterate through all BNF records
        bnf_ids = list(pipeline.bnf_records.keys())

        for bnf_id in tqdm(
            bnf_ids,
            desc="Intersection filtering",
            disable=not self.verbose,
        ):
            # Get Stage 1 and Stage 2 results
            stage1_authors = pipeline.get_stage1_result(bnf_id) or []
            stage2_books = pipeline.get_stage2_result(bnf_id) or []

            if not stage1_authors or not stage2_books:
                # No intersection possible
                pipeline.set_stage3_result(bnf_id, [])
                continue

            # Get scores if using confidence filtering
            stage1_scores = {}
            stage2_scores = {}
            if self.use_confidence_filtering:
                stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
                stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}

            # Get book data to access author URIs
            intersection = []
            for book_uri in stage2_books:
                book = pipeline.openiti_index.get_book(book_uri)
                if book is None:
                    continue

                # Get author_uri from book (handle dict and dataclass)
                if isinstance(book, dict):
                    book_author_uri = book.get("author_uri")
                else:
                    book_author_uri = book.author_uri

                # Check if author matches
                if book_author_uri not in stage1_authors:
                    continue

                # If not using confidence filtering, accept the match
                if not self.use_confidence_filtering:
                    intersection.append(book_uri)
                    continue

                # Apply confidence-dependent filtering
                author_score = stage1_scores.get(book_author_uri, 1.0)
                title_score = stage2_scores.get(book_uri, 1.0)

                keep_match = False
                if author_score >= 0.90:
                    # High confidence author match - accept any title match
                    keep_match = True
                elif author_score >= 0.85:
                    # Moderate confidence - require strong title confirmation
                    if title_score >= 0.90:
                        keep_match = True
                elif author_score >= 0.80:
                    # Low confidence author match - require very strong title
                    if title_score >= 0.95:
                        keep_match = True

                if keep_match:
                    intersection.append(book_uri)

            pipeline.set_stage3_result(bnf_id, intersection)

        if self.verbose:
            print("Stage 3 complete. Intersection filtering applied.")
