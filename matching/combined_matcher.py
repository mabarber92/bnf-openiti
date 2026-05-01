"""
Stage 3: Filter title matches by matched authors (intersection).

Keeps only books whose author is in the Stage 1 matched authors.
Computes a normalised weighted combined score: author and title scores are each
independently normalised by their per-record maximum, then combined as a weighted
sum. Title is weighted more heavily because it is the stronger discriminator once
the author stage has already filtered candidates.
"""

from tqdm import tqdm


class CombinedMatcher:
    """Filter title matches by matched authors (intersection)."""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose

    def execute(self, pipeline) -> None:
        """
        Stage 3: Combined scoring on matched author+book pairs.

        For each BNF record:
        1. Get Stage 1 results (matched author URIs) with scores
        2. Get Stage 2 results (matched book URIs) with scores
        3. Collect all valid (author, book) pairs that pass floor checks
        4. Normalise: divide each author score by the max author score for this
           record; divide each title score by the max title score for this record.
           This puts both axes on an equal [0,1] footing regardless of absolute
           score magnitudes (IDF boosts can push raw scores above 1.0).
        5. Compute weighted combined: COMBINED_AUTHOR_WEIGHT * auth_norm
                                    + COMBINED_TITLE_WEIGHT  * title_norm
        6. Accept pairs with combined >= COMBINED_THRESHOLD

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with loaded indices
        """
        from matching.config import (
            COMBINED_THRESHOLD, COMBINED_FLOOR, TITLE_FLOOR,
            COMBINED_AUTHOR_WEIGHT, COMBINED_TITLE_WEIGHT,
        )

        if self.verbose:
            print("\n--- Stage 3: Combined Scoring ---")
            print(f"  Floor: {COMBINED_FLOOR:.2%} (both author and title must meet this)")
            print(f"  Title floor: {TITLE_FLOOR:.2%} (title must independently meet this)")
            print(f"  Weights: author={COMBINED_AUTHOR_WEIGHT}, title={COMBINED_TITLE_WEIGHT}")
            print(f"  Threshold: {COMBINED_THRESHOLD:.2%} (normalised weighted combined score)")

        bnf_ids = list(pipeline.bnf_records.keys())

        for bnf_id in tqdm(bnf_ids, desc="Combined scoring", disable=not self.verbose):
            stage1_authors = pipeline.get_stage1_result(bnf_id) or []
            stage2_books = pipeline.get_stage2_result(bnf_id) or []

            if not stage1_authors or not stage2_books:
                pipeline.set_stage3_result(bnf_id, [])
                continue

            stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
            stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}

            # Collect candidate pairs that pass the hard floor checks
            candidate_pairs = []  # [(book_uri, author_score, title_score)]
            for book_uri in stage2_books:
                book = pipeline.openiti_index.get_book(book_uri)
                if book is None:
                    continue

                book_author_uri = (book.get("author_uri") if isinstance(book, dict)
                                   else book.author_uri)

                if book_author_uri not in stage1_authors:
                    continue

                author_score = stage1_scores.get(book_author_uri, 1.0)
                title_score = stage2_scores.get(book_uri, 1.0)

                if author_score < COMBINED_FLOOR or title_score < COMBINED_FLOOR:
                    continue

                if title_score < TITLE_FLOOR:
                    continue

                candidate_pairs.append((book_uri, author_score, title_score))

            combined_matches = []
            if candidate_pairs:
                # Normalise each axis independently so IDF boost magnitudes don't
                # dominate. Each score becomes a fraction of the best candidate on
                # that axis for this record.
                max_author = max(a for _, a, _ in candidate_pairs)
                max_title  = max(t for _, _, t in candidate_pairs)

                for book_uri, author_score, title_score in candidate_pairs:
                    auth_norm  = author_score / max_author if max_author > 0 else 0.0
                    title_norm = title_score  / max_title  if max_title  > 0 else 0.0
                    combined   = COMBINED_AUTHOR_WEIGHT * auth_norm + COMBINED_TITLE_WEIGHT * title_norm

                    if combined >= COMBINED_THRESHOLD:
                        combined_matches.append((book_uri, combined))

            if combined_matches:
                combined_matches.sort(key=lambda x: x[1], reverse=True)
                pipeline.set_stage3_result(bnf_id, [uri for uri, _ in combined_matches])
                pipeline.set_stage3_scores(bnf_id, {uri: score for uri, score in combined_matches})
            else:
                pipeline.set_stage3_result(bnf_id, [])

        if self.verbose:
            print("Stage 3 complete.")
