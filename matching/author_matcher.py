"""
Stage 1: Match BNF author candidates to OpenITI authors.

Uses global deduplication: each unique normalized author candidate is scored
once against all OpenITI authors. Matches above AUTHOR_THRESHOLD are mapped
back to all BNF records that contain the candidate.

Parallelized across CPU cores for efficiency on large candidate sets.
"""

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm
from matching.config import AUTHOR_THRESHOLD
from matching.fuzzy_scorer import FuzzyScorer


def _match_author_candidate(candidate, authors_dict, threshold):
    """
    Standalone function for parallel processing of a single author candidate.

    Parameters
    ----------
    candidate : str
        Normalized author candidate
    authors_dict : dict
        {author_uri: author_name} mapping
    threshold : float
        Matching threshold

    Returns
    -------
    tuple
        (candidate, [matching_author_uris])
    """
    from matching.fuzzy_scorer import FuzzyScorer

    scorer = FuzzyScorer()
    matches = []

    for author_uri, author_name in authors_dict.items():
        if not author_name:
            continue
        score = scorer.score(candidate, author_name)
        if score >= threshold:
            matches.append(author_uri)

    return (candidate, matches)


class AuthorMatcher:
    """Match BNF author candidates to OpenITI authors."""

    def __init__(self, verbose: bool = True, num_workers: int = None):
        """
        Initialize the author matcher.

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
        Stage 1: Match BNF author candidates to OpenITI authors (parallel).

        For each unique normalized author candidate from BNFCandidateIndex:
        1. Score against all OpenITI authors (fuzzy matching)
        2. Keep matches above AUTHOR_THRESHOLD
        3. Map matched author URIs to all BNF records with this candidate

        Uses multiprocessing to parallelize candidate matching across CPU cores.

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with loaded indices
        """
        if self.verbose:
            print("\n--- Stage 1: Author Matching (Parallel) ---")
            total_candidates = pipeline.bnf_index.author_candidate_count()
            print(f"Matching {total_candidates} unique author candidates...")

        # Prepare author data for parallel processing
        authors_dict = {}
        for author_uri, author_data in pipeline.openiti_index.authors.items():
            if isinstance(author_data, dict):
                author_name = author_data.get("name_slug", "")
            else:
                author_name = author_data.name_slug
            if author_name:
                authors_dict[author_uri] = author_name

        # Prepare candidates and BNF mapping
        candidates_list = []
        candidate_to_bnf_ids = {}
        for candidate, bnf_ids in pipeline.bnf_index.author_candidates_iter():
            candidates_list.append(candidate)
            candidate_to_bnf_ids[candidate] = bnf_ids

        # Parallel matching
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            match_fn = partial(_match_author_candidate, authors_dict=authors_dict, threshold=AUTHOR_THRESHOLD)
            results = list(
                tqdm(
                    executor.map(match_fn, candidates_list, chunksize=10),
                    desc="Author matching",
                    disable=not self.verbose,
                    total=len(candidates_list),
                )
            )

        # Store results in pipeline
        for candidate, matched_authors in results:
            bnf_ids = candidate_to_bnf_ids[candidate]
            for bnf_id in bnf_ids:
                current = pipeline.get_stage1_result(bnf_id)
                if current is None:
                    current = []
                current = list(set(current + matched_authors))
                pipeline.set_stage1_result(bnf_id, current)

        if self.verbose:
            print(f"Stage 1 complete.")

