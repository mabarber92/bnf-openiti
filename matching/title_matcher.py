"""
Stage 2: Match BNF title candidates to OpenITI book titles.

Uses global deduplication: each unique normalized title candidate is scored
once against all OpenITI book titles. Matches above TITLE_THRESHOLD are mapped
back to all BNF records that contain the candidate.

Parallelized across CPU cores for efficiency on large candidate sets.
"""

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm
from matching.config import TITLE_THRESHOLD
from matching.fuzzy_scorer import FuzzyScorer


def _match_title_candidate(candidate, books_dict, threshold):
    """
    Standalone function for parallel processing of a single title candidate.

    Parameters
    ----------
    candidate : str
        Normalized title candidate
    books_dict : dict
        {book_uri: book_title} mapping
    threshold : float
        Matching threshold

    Returns
    -------
    tuple
        (candidate, [matching_book_uris])
    """
    from matching.fuzzy_scorer import FuzzyScorer

    scorer = FuzzyScorer()
    matches = []

    for book_uri, book_title in books_dict.items():
        if not book_title:
            continue
        score = scorer.score(candidate, book_title)
        if score >= threshold:
            matches.append(book_uri)

    return (candidate, matches)


class TitleMatcher:
    """Match BNF title candidates to OpenITI book titles."""

    def __init__(self, verbose: bool = True, num_workers: int = None):
        """
        Initialize the title matcher.

        Parameters
        ----------
        verbose : bool
            Print progress information
        num_workers : int, optional
            Number of parallel workers. If None, uses CPU count.
        """
        self.verbose = verbose
        self.num_workers = num_workers

    def execute(self, pipeline) -> None:
        """
        Stage 2: Match BNF title candidates to OpenITI books (parallel).

        For each unique normalized title candidate from BNFCandidateIndex:
        1. Score against all OpenITI book titles (fuzzy matching)
        2. Keep matches above TITLE_THRESHOLD
        3. Map matched book URIs to all BNF records with this candidate

        Uses multiprocessing to parallelize candidate matching across CPU cores.

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with loaded indices
        """
        if self.verbose:
            print("\n--- Stage 2: Title Matching (Parallel) ---")
            total_candidates = pipeline.bnf_index.title_candidate_count()
            print(f"Matching {total_candidates} unique title candidates...")

        # Prepare book data for parallel processing
        books_dict = {}
        for book_uri, book_data in pipeline.openiti_index.books.items():
            if isinstance(book_data, dict):
                book_title = book_data.get("title_slug", "")
            else:
                book_title = book_data.title_slug
            if book_title:
                books_dict[book_uri] = book_title

        # Prepare candidates and BNF mapping
        candidates_list = []
        candidate_to_bnf_ids = {}
        for candidate, bnf_ids in pipeline.bnf_index.title_candidates_iter():
            candidates_list.append(candidate)
            candidate_to_bnf_ids[candidate] = bnf_ids

        # Parallel matching
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            match_fn = partial(_match_title_candidate, books_dict=books_dict, threshold=TITLE_THRESHOLD)
            results = list(
                tqdm(
                    executor.map(match_fn, candidates_list, chunksize=10),
                    desc="Title matching",
                    disable=not self.verbose,
                    total=len(candidates_list),
                )
            )

        # Store results in pipeline
        for candidate, matched_books in results:
            bnf_ids = candidate_to_bnf_ids[candidate]
            for bnf_id in bnf_ids:
                current = pipeline.get_stage2_result(bnf_id)
                if current is None:
                    current = []
                current = list(set(current + matched_books))
                pipeline.set_stage2_result(bnf_id, current)

        if self.verbose:
            print(f"Stage 2 complete.")

