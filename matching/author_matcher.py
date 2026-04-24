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
from matching.candidate_builders import build_author_candidates_by_script


def _match_author_candidate(candidate, authors_candidates, threshold, norm_strategy="fuzzy"):
    """
    Standalone function for parallel processing of a single BNF author candidate.

    Replicates exact scoring logic from test_fuzzy_with_author_comprehensive.py.

    Parameters
    ----------
    candidate : str
        Raw author candidate from BNF
    authors_candidates : dict
        {author_uri: {"lat": [...], "ara": [...]}} - per-script candidates
    threshold : float
        Matching threshold (0-1 range)
    norm_strategy : str
        Normalization strategy used

    Returns
    -------
    tuple
        (candidate, {matching_author_uri: score}) - matches with confidence scores
    """
    from fuzzywuzzy import fuzz
    from matching.normalize import normalize_transliteration

    matches = {}  # {author_uri: score}

    # Normalize BNF candidate (using same function as original test)
    norm_candidate = normalize_transliteration(candidate)

    if not norm_candidate:
        return (candidate, matches)

    # Score against all author candidates for each author (exact logic from original test)
    for author_uri, author_candidates_by_script in authors_candidates.items():
        best_score = 0

        # Try both scripts (matching original test exactly)
        for script in ["lat", "ara"]:
            if not author_candidates_by_script.get(script):
                continue

            for author_str in author_candidates_by_script[script]:
                if not author_str:
                    continue

                # Normalize author candidate
                norm_author_str = normalize_transliteration(author_str)
                if not norm_author_str:
                    continue

                # Use token_set_ratio exactly like original test
                score = fuzz.token_set_ratio(norm_candidate, norm_author_str)
                if score > best_score:
                    best_score = score

        # Store match with its score (0-100 range, convert to 0-1)
        if best_score >= threshold * 100:
            matches[author_uri] = best_score / 100.0

    return (candidate, matches)


class AuthorMatcher:
    """Match BNF author candidates to OpenITI authors."""

    def __init__(self, verbose: bool = True, num_workers: int = None, use_parallel: bool = True):
        """
        Initialize the author matcher.

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
        authors_candidates = {}
        for author_uri, author_data in pipeline.openiti_index.authors.items():
            candidates = build_author_candidates_by_script(author_data)
            if candidates["lat"] or candidates["ara"]:
                authors_candidates[author_uri] = candidates

        # Prepare candidates and BNF mapping
        candidates_list = []
        candidate_to_bnf_ids = {}
        for candidate, bnf_ids in pipeline.bnf_index.author_candidates_iter():
            candidates_list.append(candidate)
            candidate_to_bnf_ids[candidate] = bnf_ids

        # Matching (parallel or sequential)
        if self.use_parallel:
            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                match_fn = partial(_match_author_candidate, authors_candidates=authors_candidates, threshold=AUTHOR_THRESHOLD, norm_strategy=pipeline.norm_strategy)
                results = list(
                    tqdm(
                        executor.map(match_fn, candidates_list, chunksize=10),
                        desc="Author matching",
                        disable=not self.verbose,
                        total=len(candidates_list),
                    )
                )
        else:
            # Sequential processing for debugging
            results = []
            match_fn = partial(_match_author_candidate, authors_candidates=authors_candidates, threshold=AUTHOR_THRESHOLD, norm_strategy=pipeline.norm_strategy)
            for candidate in tqdm(candidates_list, desc="Author matching", disable=not self.verbose):
                result = match_fn(candidate)
                results.append(result)

        # Store results in pipeline
        for candidate, matched_authors_dict in results:
            bnf_ids = candidate_to_bnf_ids[candidate]
            for bnf_id in bnf_ids:
                # Update author URIs list
                current = pipeline.get_stage1_result(bnf_id)
                if current is None:
                    current = []
                current = list(set(current + list(matched_authors_dict.keys())))
                pipeline.set_stage1_result(bnf_id, current)

                # Update scores (keep max score for each author)
                current_scores = pipeline.get_stage1_scores(bnf_id)
                for author_uri, score in matched_authors_dict.items():
                    if author_uri not in current_scores or score > current_scores[author_uri]:
                        current_scores[author_uri] = score
                pipeline.set_stage1_scores(bnf_id, current_scores)

        if self.verbose:
            print(f"Stage 1 complete.")

