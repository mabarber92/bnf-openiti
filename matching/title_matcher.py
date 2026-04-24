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
from matching.candidate_builders import build_book_candidates_by_script


def _match_title_candidate(candidate, books_candidates, threshold, norm_strategy="fuzzy"):
    """
    Standalone function for parallel processing of a single BNF title candidate.

    Scores against all title candidates for each OpenITI book (matching original test logic).

    Parameters
    ----------
    candidate : str
        Raw title candidate from BNF
    books_candidates : dict
        {book_uri: {"lat": [...], "ara": [...]}} - title candidates by script per book
    threshold : float
        Matching threshold (0-1 range)
    norm_strategy : str
        Normalization strategy used

    Returns
    -------
    tuple
        (candidate, [matching_book_uris])
    """
    from fuzzywuzzy import fuzz
    from matching.normalize import normalize_transliteration

    matches = []

    # Normalize the BNF candidate once (using same function as original test)
    norm_candidate = normalize_transliteration(candidate)

    if not norm_candidate:
        return (candidate, matches)

    # Score against all title candidates for each book
    for book_uri, book_title_by_script in books_candidates.items():
        book_matched = False

        # Try both scripts (matching author matcher logic)
        for script in ["lat", "ara"]:
            if not book_title_by_script.get(script):
                continue

            for book_title in book_title_by_script[script]:
                if not book_title:
                    continue

                # Normalize the OpenITI book title the same way BNF candidates were normalized
                norm_book_title = normalize_transliteration(book_title)

                if not norm_book_title:
                    continue

                # Use token_set_ratio exactly like author matcher
                score = fuzz.token_set_ratio(norm_candidate, norm_book_title)
                if score >= threshold * 100:  # threshold is 0-1 range, score is 0-100
                    book_matched = True
                    break

            if book_matched:
                break

        if book_matched:
            matches.append(book_uri)

    return (candidate, matches)


class TitleMatcher:
    """Match BNF title candidates to OpenITI book titles."""

    def __init__(self, verbose: bool = True, num_workers: int = None, use_parallel: bool = True):
        """
        Initialize the title matcher.

        Parameters
        ----------
        verbose : bool
            Print progress information
        num_workers : int, optional
            Number of parallel workers. If None, uses CPU count.
        use_parallel : bool
            Use parallelization. If False, run sequentially for debugging.
        """
        self.verbose = verbose
        self.num_workers = num_workers
        self.use_parallel = use_parallel

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

        # Prepare book title candidates (extract title parts by script)
        books_candidates = {}
        for book_uri, book_data in pipeline.openiti_index.books.items():
            candidates = build_book_candidates_by_script(book_data)
            if candidates["lat"] or candidates["ara"]:
                books_candidates[book_uri] = candidates

        # Prepare candidates and BNF mapping
        candidates_list = []
        candidate_to_bnf_ids = {}
        for candidate, bnf_ids in pipeline.bnf_index.title_candidates_iter():
            candidates_list.append(candidate)
            candidate_to_bnf_ids[candidate] = bnf_ids

        # Matching (parallel or sequential)
        if self.use_parallel:
            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                match_fn = partial(_match_title_candidate, books_candidates=books_candidates, threshold=TITLE_THRESHOLD, norm_strategy=pipeline.norm_strategy)
                results = list(
                    tqdm(
                        executor.map(match_fn, candidates_list, chunksize=10),
                        desc="Title matching",
                        disable=not self.verbose,
                        total=len(candidates_list),
                    )
                )
        else:
            # Sequential processing for debugging
            results = []
            match_fn = partial(_match_title_candidate, books_candidates=books_candidates, threshold=TITLE_THRESHOLD, norm_strategy=pipeline.norm_strategy)
            for candidate in tqdm(candidates_list, desc="Title matching", disable=not self.verbose):
                result = match_fn(candidate)
                results.append(result)

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

