"""
Stage 2: Match BNF title candidates to OpenITI book titles.

Uses global deduplication: each unique normalized title candidate is scored
once against all OpenITI book titles. Matches above TITLE_THRESHOLD are mapped
back to all BNF records that contain the candidate.

Token-level IDF weighting suppresses false positives on common title words.
Parallelized across CPU cores for efficiency on large candidate sets.
"""

import math
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm
from matching.config import TITLE_THRESHOLD
from matching.fuzzy_scorer import FuzzyScorer
from matching.candidate_builders import build_book_candidates_by_script


def _build_token_idf_weights(books_candidates):
    """
    Build IDF (Inverse Document Frequency) weights for all tokens in title candidates.

    Rarer tokens (appearing in few titles) get higher weights.
    Common tokens (appearing in many titles) get lower weights.

    CRITICAL: Normalizes title strings before tokenizing (matching pipeline behavior).

    Returns dict: {token: idf_weight, ...}
    """
    from matching.normalize import normalize_for_matching

    token_doc_freq = defaultdict(set)
    total_docs = 0

    for book_uri, book_candidates_by_script in books_candidates.items():
        total_docs += 1
        tokens_seen = set()

        for script in ["lat", "ara"]:
            for title_str in book_candidates_by_script.get(script, []):
                if title_str:
                    # Normalize first (remove diacritics, apply conversions)
                    # This matches what the matching pipeline does
                    # Use split_camelcase=True because these are OpenITI book titles
                    norm_str = normalize_for_matching(title_str, split_camelcase=True, is_openiti=True)
                    if norm_str:
                        # Then split on whitespace to get tokens
                        for token in norm_str.lower().split():
                            tokens_seen.add(token)

        for token in tokens_seen:
            token_doc_freq[token].add(book_uri)

    idf_weights = {}
    for token, doc_set in token_doc_freq.items():
        doc_freq = len(doc_set)
        idf_weights[token] = math.log(1.0 + total_docs / max(doc_freq, 1))

    return idf_weights


def _score_with_token_weighting(norm_candidate, norm_title_str, idf_weights, fuzzy_score):
    """
    Apply continuous rare-token IDF boost to a fuzzy title score.

    Mirrors the author matching design: only tokens above TOKEN_RARITY_THRESHOLD
    contribute to the boost, so common title words ("kitab", "sharh") provide no
    lift while specific tokens ("futuh", "sham") do.

    boost = 1 + min(rare_idf_sum / TITLE_IDF_BOOST_SCALE, TITLE_MAX_BOOST - 1)

    A single rare token (e.g. "Muhammad" IDF=6.1) gets a modest boost (~1.31×).
    Two rare tokens (e.g. "Futuh"=7.4 + "Sham"=6.9) approach TITLE_MAX_BOOST.
    Zero rare tokens → boost=1.0 (no change; common-only matches pass through
    but are not amplified, so they remain below TITLE_FLOOR at stage 3).

    Blocks matches with no token overlap entirely (returns 0).
    """
    from matching.config import TOKEN_RARITY_THRESHOLD, TITLE_IDF_BOOST_SCALE, TITLE_MAX_BOOST

    candidate_tokens = set(norm_candidate.lower().split())
    title_tokens = set(norm_title_str.lower().split())

    if not candidate_tokens:
        return 0

    matched_tokens = candidate_tokens & title_tokens

    if not matched_tokens:
        return 0

    rare_idf = sum(idf_weights.get(t, 0.0) for t in matched_tokens
                   if idf_weights.get(t, 0.0) >= TOKEN_RARITY_THRESHOLD)
    boost = 1 + min(rare_idf / TITLE_IDF_BOOST_SCALE, TITLE_MAX_BOOST - 1)

    return max(fuzzy_score * boost, 0)


def _match_title_candidate(candidate, books_candidates, threshold, norm_strategy="fuzzy", idf_weights=None):
    """
    Standalone function for parallel processing of a single BNF title candidate.

    Combines fuzzy scores across multiple title variants per book using geometric mean.
    This rewards matches where multiple title candidates align, not just single common words.

    Pipeline:
    1. Collect fuzzy scores for all title candidates per book
    2. Combine with geometric mean: (score1 × score2 × ... × scoreN)^(1/N)
    3. Apply token-level IDF weighting for rare token boost

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
    idf_weights : dict, optional
        {token: idf_weight, ...} for token weighting. If None, uses raw fuzzy scores.

    Returns
    -------
    tuple
        (candidate, {matching_book_uri: score}) - matches with confidence scores
    """
    from fuzzywuzzy import fuzz
    from matching.normalize import normalize_for_matching

    matches = {}  # {book_uri: score}

    # Normalize the BNF candidate (without camelcase splitting - it's a regular title, not an OpenITI slug)
    norm_candidate = normalize_for_matching(candidate, split_camelcase=False, is_openiti=False)

    if not norm_candidate:
        return (candidate, matches)

    # Score against all title candidates for each book
    for book_uri, book_title_by_script in books_candidates.items():
        best_score = 0

        # Try both scripts: concatenate all title variants within each script
        for script in ["lat", "ara"]:
            candidates = book_title_by_script.get(script)
            if not candidates:
                continue
            # Filter and normalize all candidates for this script
            normalized_candidates = []
            for book_title in candidates:
                if not book_title:
                    continue
                # Normalize the OpenITI book title (with camelcase splitting - it may be a slug)
                norm_title = normalize_for_matching(book_title, split_camelcase=True, is_openiti=True)
                if norm_title:
                    normalized_candidates.append(norm_title)

            if not normalized_candidates:
                continue

            # Concatenate all title variants for this script into a single string
            # This allows matching against the full book title profile, not individual variants
            combined_book_title = " ".join(normalized_candidates)

            # Get raw fuzzy score
            raw_score = fuzz.token_set_ratio(norm_candidate, combined_book_title)

            # Apply token-level IDF weighting if available
            if idf_weights:
                score = _score_with_token_weighting(norm_candidate, combined_book_title, idf_weights, raw_score)
            else:
                score = raw_score

            if score > best_score:
                best_score = score

        # Store match with its score (0-100 range, convert to 0-1)
        if best_score >= threshold * 100:
            matches[book_uri] = best_score / 100.0

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
        Token-level IDF weighting is computed from full BNF and OpenITI datasets
        to suppress false positives on common title words.

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with loaded indices
        """
        if self.verbose:
            print("\n--- Stage 2: Title Matching (Parallel) ---")
            total_candidates = pipeline.bnf_index.title_candidate_count()
            print(f"Matching {total_candidates} unique title candidates...")

        # Prepare OpenITI book title candidates
        books_candidates = {}
        for book_uri, book_data in pipeline.openiti_index.books.items():
            candidates = build_book_candidates_by_script(book_data)
            if candidates["lat"] or candidates["ara"]:
                books_candidates[book_uri] = candidates

        # Build token-level IDF weights if enabled
        from matching.config import USE_TITLE_IDF_WEIGHTING

        if USE_TITLE_IDF_WEIGHTING:
            if self.verbose:
                print("  Building IDF weights from OpenITI book data only...")

            # Use only OpenITI books for IDF to measure rarity in our target domain
            # This prevents false boosting on tokens that are common in book titles
            idf_weights = _build_token_idf_weights(books_candidates)

            if self.verbose:
                print(f"  Built IDF weights for {len(idf_weights)} unique tokens from {len(books_candidates)} OpenITI book records")
        else:
            idf_weights = None

        # Prepare candidates and BNF mapping
        candidates_list = []
        candidate_to_bnf_ids = {}
        for candidate, bnf_ids in pipeline.bnf_index.title_candidates_iter():
            candidates_list.append(candidate)
            candidate_to_bnf_ids[candidate] = bnf_ids

        # Matching (parallel or sequential)
        if self.use_parallel:
            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                match_fn = partial(_match_title_candidate, books_candidates=books_candidates, threshold=TITLE_THRESHOLD, norm_strategy=pipeline.norm_strategy, idf_weights=idf_weights)
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
            match_fn = partial(_match_title_candidate, books_candidates=books_candidates, threshold=TITLE_THRESHOLD, norm_strategy=pipeline.norm_strategy, idf_weights=idf_weights)
            for candidate in tqdm(candidates_list, desc="Title matching", disable=not self.verbose):
                result = match_fn(candidate)
                results.append(result)

        # Store results in pipeline
        for candidate, matched_books_dict in results:
            bnf_ids = candidate_to_bnf_ids[candidate]
            for bnf_id in bnf_ids:
                # Update book URIs list
                current = pipeline.get_stage2_result(bnf_id)
                if current is None:
                    current = []
                current = list(set(current + list(matched_books_dict.keys())))
                pipeline.set_stage2_result(bnf_id, current)

                # Update scores (keep max score for each book)
                current_scores = pipeline.get_stage2_scores(bnf_id)
                for book_uri, score in matched_books_dict.items():
                    if book_uri not in current_scores or score > current_scores[book_uri]:
                        current_scores[book_uri] = score
                pipeline.set_stage2_scores(bnf_id, current_scores)

        if self.verbose:
            print(f"Stage 2 complete.")

