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

    Returns dict: {token: idf_weight, ...}
    """
    token_doc_freq = defaultdict(set)  # {token: set of author_uris containing it}
    total_docs = 0

    # Count document frequency for each token
    for author_uri, author_candidates_by_script in authors_candidates.items():
        total_docs += 1
        tokens_seen = set()

        for script in ["lat", "ara"]:
            for author_str in author_candidates_by_script.get(script, []):
                if author_str:
                    # Split on whitespace to get tokens
                    for token in author_str.lower().split():
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


def _score_with_token_weighting(norm_candidate, norm_author_str, idf_weights, fuzzy_score):
    """
    Weight a fuzzy score by the rarity of tokens that contributed to the match.

    Aggressively penalizes matches with only common tokens.
    Does not boost—relies on fuzzy matching for good candidates.

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

    Returns
    -------
    float
        Weighted fuzzy score (0-100 range, aggressively penalized for common-token matches)
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

    # Average IDF of matched tokens
    matched_idf_values = [idf_weights.get(t, 0.1) for t in matched_tokens]
    avg_matched_idf = sum(matched_idf_values) / len(matched_tokens)

    # IDF threshold: ~1.1 is where tokens become reasonably specific
    # Below this: mostly common tokens (ibn, ahmad, muhammad, al, fi, etc.)
    rarity_threshold = 1.1

    if avg_matched_idf < rarity_threshold:
        # Matched tokens are mostly common words - aggressively penalize
        # Cubic penalty: score *= (avg_idf / threshold)^3
        penalty_factor = (avg_matched_idf / rarity_threshold) ** 3
        weighted_score = fuzzy_score * penalty_factor
    else:
        # Matched tokens include rare ones - accept fuzzy score as-is
        # Don't boost, just don't penalize
        weighted_score = fuzzy_score

    return min(max(weighted_score, 0), 100)  # Clamp to [0, 100]


def _match_author_candidate(candidate, authors_candidates, threshold, norm_strategy="fuzzy", idf_weights=None):
    """
    Standalone function for parallel processing of a single BNF author candidate.

    Uses token-level IDF weighting to suppress false positives: rare tokens
    (like "al-Quduri") contribute more to the score than common tokens
    (like "Ahmad", "ibn", "Muhammad").

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

    # Normalize BNF candidate
    norm_candidate = normalize_for_matching(candidate)

    if not norm_candidate:
        return (candidate, matches)

    # Score against all author candidates for each author
    for author_uri, author_candidates_by_script in authors_candidates.items():
        best_score = 0

        # Try both scripts
        for script in ["lat", "ara"]:
            if not author_candidates_by_script.get(script):
                continue

            for author_str in author_candidates_by_script[script]:
                if not author_str:
                    continue

                # Normalize author candidate
                norm_author_str = normalize_for_matching(author_str)
                if not norm_author_str:
                    continue

                # Get raw fuzzy score
                raw_score = fuzz.token_set_ratio(norm_candidate, norm_author_str)

                # Apply token-level IDF weighting if available
                if idf_weights:
                    score = _score_with_token_weighting(norm_candidate, norm_author_str, idf_weights, raw_score)
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

        # Build token-level IDF weights from combined BNF + OpenITI datasets
        # This ensures IDF reflects true token rarity across both corpora
        if self.verbose:
            print("  Building IDF weights from full BNF + OpenITI datasets...")

        # Load full BNF dataset for IDF computation
        from matching.config import BNF_FULL_PATH
        from parsers.bnf import load_bnf_records
        full_bnf_records = load_bnf_records(BNF_FULL_PATH)

        # Build BNF author candidates from all records
        bnf_candidates_for_idf = {}
        for bnf_id, bnf_record in full_bnf_records.items():
            # Extract author names from BNF record (dataclass attributes)
            creators_lat = getattr(bnf_record, "creator_lat", []) or []
            creators_ara = getattr(bnf_record, "creator_ara", []) or []

            # Create entries for IDF computation
            for creator in creators_lat:
                if creator:
                    key = f"bnf_{bnf_id}_lat_{len(bnf_candidates_for_idf)}"
                    bnf_candidates_for_idf[key] = {"lat": [creator], "ara": []}

            for creator in creators_ara:
                if creator:
                    key = f"bnf_{bnf_id}_ara_{len(bnf_candidates_for_idf)}"
                    bnf_candidates_for_idf[key] = {"lat": [], "ara": [creator]}

        # Combine BNF and OpenITI candidates for IDF computation
        combined_candidates = {**authors_candidates, **bnf_candidates_for_idf}
        idf_weights = _build_token_idf_weights(combined_candidates)

        if self.verbose:
            print(f"  Built IDF weights for {len(idf_weights)} unique tokens from {len(combined_candidates)} total candidate sources")

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

