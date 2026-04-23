"""
Stage 3: Filter title matches by matched authors (intersection).

Keeps only books whose author is in the Stage 1 matched authors.
This stage significantly improves precision by enforcing that both
author and title must match together.
"""

from tqdm import tqdm


class CombinedMatcher:
    """Filter title matches by matched authors (intersection)."""

    def __init__(self, verbose: bool = True):
        """
        Initialize the combined matcher.

        Parameters
        ----------
        verbose : bool
            Print progress information
        """
        self.verbose = verbose

    def execute(self, pipeline) -> None:
        """
        Stage 3: Filter title matches by matched authors.

        For each BNF record:
        1. Get Stage 1 results (matched author URIs)
        2. Get Stage 2 results (matched book URIs)
        3. Keep only books whose author_uri is in matched authors
        4. Store intersection as Stage 3 result

        This enforces that matched books must have authors that were
        matched in Stage 1, eliminating false positives where title
        matches but author differs.

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with loaded indices
        """
        if self.verbose:
            print("\n--- Stage 3: Combined Matching (Intersection) ---")

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

                # Keep only if author matches
                if book_author_uri in stage1_authors:
                    intersection.append(book_uri)

            pipeline.set_stage3_result(bnf_id, intersection)

        if self.verbose:
            print("Stage 3 complete. Intersection filtering applied.")
