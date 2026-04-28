"""
Stage 1: Match BNF author candidates to OpenITI authors.

Uses global deduplication: each unique normalized author candidate is scored
once against all OpenITI authors. Matches above AUTHOR_THRESHOLD are mapped
back to all BNF records that contain the candidate.

Token-level IDF weighting suppresses false positives on common name parts
by weighting rare tokens (e.g., "al-Quduri") more heavily than common ones
(e.g., "Ahmad", "ibn", "Muhammad").

Parallelized across CPU cores for efficiency on large candidate sets.
"""

import math
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from tqdm import tqdm
from matching.config import AUTHOR_THRESHOLD
from matching.fuzzy_scorer import FuzzyScorer
from matching.candidate_builders import build_author_candidates_by_script


def _build_token_idf_weights(authors_candidates):
    """
    Build IDF (Inverse Document Frequency) weights for all tokens in author candidates.

    Rarer tokens (appearing in few author records) get higher weights.
    Common tokens (appearing in many records) get lower weights.

    CRITICAL: Normalizes author strings before tokenizing (matching pipeline behavior).

    Returns dict: {token: idf_weight, ...}
    """
    from matching.normalize import normalize_for_matching

    token_doc_freq = defaultdict(set)  # {token: set of author_uris containing it}
    total_docs = 0

    # Count document frequency for each token
    for author_uri, author_candidates_by_script in authors_candidates.items():
        total_docs += 1
        tokens_seen = set()

        for script in ["lat", "ara"]:
            for author_str in author_candidates_by_script.get(script, []):
                if author_str:
                    # Normalize first (remove diacritics, apply conversions)
                    # This matches what the matching pipeline does
                    # Use split_camelcase=True because these are OpenITI author names
                    norm_str = normalize_for_matching(author_str, split_camelcase=True)
                    if norm_str:
                        # Then split on whitespace to get tokens
                        for token in norm_str.lower().split():
                            tokens_seen.add(token)

        for token in tokens_seen:
            token_doc_freq[token].add(author_uri)

    # Calculate IDF: log(total_docs / document_frequency)
    idf_weights = {}
    for token, doc_set in token_doc_freq.items():
        doc_freq = len(doc_set)
        # Add 1 to avoid division by zero; use log(1+) to scale appropriately
        idf_weights[token] = math.log(1.0 + total_docs / max(doc_freq, 1))

    return idf_weights


def _score_with_token_weighting(norm_candidate, norm_author_str, idf_weights, fuzzy_score, debug=False):
    """
    Weight a fuzzy score based on presence of rare tokens in the match.

    If any matched token has IDF >= rarity_threshold: boost the score.
    If no rare tokens: keep score as-is (no penalty, no boost).

    This allows common-only matches to pass through, but prioritizes matches
    with rare (specific) tokens.

    Parameters
    ----------
    norm_candidate : str
        Normalized BNF candidate
    norm_author_str : str
        Normalized OpenITI author string
    idf_weights : dict
        {token: idf_weight, ...}
    fuzzy_score : float
        Raw fuzzy match score (0-100)
    debug : bool
        If True, print debug info about rare token detection

    Returns
    -------
    float
        Weighted fuzzy score (0-100 range)
    """
    candidate_tokens = set(norm_candidate.lower().split())
    author_tokens = set(norm_author_str.lower().split())

    if not candidate_tokens:
        return 0

    # Find which candidate tokens matched author tokens
    matched_tokens = candidate_tokens & author_tokens

    if not matched_tokens:
        # No token overlap - block completely
        return 0

    # Check if any matched token is rare (IDF >= rarity_threshold)
    from matching.config import TOKEN_RARITY_THRESHOLD, RARE_TOKEN_BOOST_FACTOR

    rare_tokens_found = [t for t in matched_tokens if idf_weights.get(t, 0.1) >= TOKEN_RARITY_THRESHOLD]
    has_rare_token = len(rare_tokens_found) > 0

    if debug and has_rare_token:
        print(f"  [RARE TOKEN BOOST] candidate='{norm_candidate[:40]}' author='{norm_author_str[:40]}' fuzzy={fuzzy_score:.0f}")
        for t in rare_tokens_found:
            idf = idf_weights.get(t, 0.1)
            print(f"    rare_token='{t}' idf={idf:.3f} (threshold={TOKEN_RARITY_THRESHOLD})")

    if has_rare_token:
        # Rare tokens present - boost the score to reward specificity
        weighted_score = fuzzy_score * RARE_TOKEN_BOOST_FACTOR
    else:
        # Only common tokens matched - accept score as-is, no penalty
        weighted_score = fuzzy_score

    return max(weighted_score, 0)  # Allow scores > 100 for rare token matches


def _match_author_candidate(candidate, authors_candidates, threshold, norm_strategy="fuzzy", idf_weights=None):
    """
    Standalone function for parallel processing of a single BNF author candidate.

    Combines fuzzy scores across multiple name components per author using geometric mean.
    This rewards matches where multiple name components (ism, nasab, shuhra, etc.) align,
    not just single common tokens like "Muhammad" or "Ahmad".

    Pipeline:
    1. Collect fuzzy scores for all name component candidates per author
    2. Combine with geometric mean: (score1 × score2 × ... × scoreN)^(1/N)
    3. Apply token-level IDF weighting for rare token boost

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
    idf_weights : dict, optional
        {token: idf_weight, ...} for token weighting. If None, uses raw fuzzy scores.

    Returns
    -------
    tuple
        (candidate, {matching_author_uri: score}) - matches with confidence scores (0-1)
    """
    from fuzzywuzzy import fuzz
    from matching.normalize import normalize_for_matching

    matches = {}  # {author_uri: score}

    # Normalize BNF candidate (without camelcase splitting - it's a regular name, not an OpenITI slug)
    norm_candidate = normalize_for_matching(candidate, split_camelcase=False)

    if not norm_candidate:
        return (candidate, matches)

    # Score against all author candidates for each author
    for author_uri, author_candidates_by_script in authors_candidates.items():
        best_score = 0

        # Try both scripts: concatenate all name components within each script
        for script in ["lat", "ara"]:
            candidates = author_candidates_by_script.get(script)
            if not candidates:
                continue

            # Filter and normalize all candidates for this script
            normalized_candidates = []
            for author_str in candidates:
                if not author_str:
                    continue
                # Normalize OpenITI author candidate (with camelcase splitting - it may be a slug like IbnKhayyat)
                norm_str = normalize_for_matching(author_str, split_camelcase=True)
                if norm_str:
                    normalized_candidates.append(norm_str)

            if not normalized_candidates:
                continue

            # Concatenate all components for this script into a single string
            # This allows matching against the full author profile, not individual name parts
            combined_author_str = " ".join(normalized_candidates)

            # Get raw fuzzy score
            raw_score = fuzz.token_set_ratio(norm_candidate, combined_author_str)

            # Apply token-level IDF weighting if available
            if idf_weights:
                score = _score_with_token_weighting(norm_candidate, combined_author_str, idf_weights, raw_score, debug=False)
            else:
                score = raw_score

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
        Token-level IDF weighting is computed from full BNF and OpenITI datasets
        to suppress false positives on common author name fragments.

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with loaded indices
        """
        if self.verbose:
            print("\n--- Stage 1: Author Matching (Parallel) ---")
            total_candidates = pipeline.bnf_index.author_candidate_count()
            print(f"Matching {total_candidates} unique author candidates...")

        # Prepare OpenITI author data for matching
        authors_candidates = {}
        for author_uri, author_data in pipeline.openiti_index.authors.items():
            candidates = build_author_candidates_by_script(author_data)
            if candidates["lat"] or candidates["ara"]:
                authors_candidates[author_uri] = candidates

        # Build token-level IDF weights if enabled
        from matching.config import USE_AUTHOR_IDF_WEIGHTING

        if USE_AUTHOR_IDF_WEIGHTING:
            if self.verbose:
                print("  Building IDF weights from OpenITI author data only...")

            # Use only OpenITI authors for IDF to measure rarity in our target domain
            # This prevents false boosting on tokens that are common in Islamic manuscripts
            idf_weights = _build_token_idf_weights(authors_candidates)

            if self.verbose:
                print(f"  Built IDF weights for {len(idf_weights)} unique tokens from {len(authors_candidates)} OpenITI author records")
        else:
            idf_weights = None

        # Prepare candidates and BNF mapping
        candidates_list = []
        candidate_to_bnf_ids = {}
        for candidate, bnf_ids in pipeline.bnf_index.author_candidates_iter():
            candidates_list.append(candidate)
            candidate_to_bnf_ids[candidate] = bnf_ids

        # Matching (parallel or sequential)
        if self.use_parallel:
            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                match_fn = partial(_match_author_candidate, authors_candidates=authors_candidates, threshold=AUTHOR_THRESHOLD, norm_strategy=pipeline.norm_strategy, idf_weights=idf_weights)
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
            match_fn = partial(_match_author_candidate, authors_candidates=authors_candidates, threshold=AUTHOR_THRESHOLD, norm_strategy=pipeline.norm_strategy, idf_weights=idf_weights)
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

