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
                    norm_str = normalize_for_matching(author_str, split_camelcase=True, is_openiti=True)
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


def _score_with_token_weighting(norm_candidate, norm_author_str, idf_weights, fuzzy_score):
    """
    Return (fuzzy_score, combined_idf, rare_idf) for matched tokens.

    combined_idf — sum of IDF weights across ALL matched tokens.
                   Used for the stage 1 entry threshold (permissive recall).
    rare_idf     — sum of IDF weights for matched tokens above TOKEN_RARITY_THRESHOLD only.
                   Used for stored scores passed to stage 3 (precision).

    Two-tier design: common-name authors (e.g. "Muhammad Rida") can still cross
    the entry threshold via combined_idf boost, but their stored rare_idf=0 leaves
    their final score below COMBINED_FLOOR, so they fail at stage 3.

    Blocks matches with no token overlap entirely (returns (0, 0.0, 0.0)).
    """
    from matching.config import TOKEN_RARITY_THRESHOLD

    candidate_tokens = set(norm_candidate.lower().split())
    author_tokens = set(norm_author_str.lower().split())

    if not candidate_tokens:
        return (0, 0.0, 0.0)

    matched_tokens = candidate_tokens & author_tokens

    if not matched_tokens:
        return (0, 0.0, 0.0)

    combined_idf = sum(idf_weights.get(t, 0.0) for t in matched_tokens)
    rare_idf = sum(idf_weights.get(t, 0.0) for t in matched_tokens
                   if idf_weights.get(t, 0.0) >= TOKEN_RARITY_THRESHOLD)

    return (fuzzy_score, combined_idf, rare_idf)


def _match_author_candidate(candidate, authors_candidates, threshold=0.80, norm_strategy="fuzzy", idf_weights=None):
    """
    Standalone function for parallel processing of a single BNF author candidate.

    Scores BNF author candidate against all OpenITI author name variants using
    token_sort_ratio (resistant to subset inflation). Each variant is scored
    individually; best score per author wins.

    Returns raw scores and combined IDF scores — boost is applied in Phase 2
    after creator field reweighting:
        base = raw_score * w1 + creator_score * w2
        final = base * (1 + min(combined_idf / SCALE, MAX_BOOST - 1))

    Returns
    -------
    tuple
        (candidate, {author_uri: (raw_score, combined_idf)})
        raw_score is in 0-1 range; boost is not yet applied.
    """
    from fuzzywuzzy import fuzz
    from matching.normalize import normalize_for_matching
    from matching.config import AUTHOR_IDF_BOOST_SCALE, AUTHOR_MAX_BOOST

    matches = {}  # {author_uri: (raw_score, rare_idf)}

    norm_candidate = normalize_for_matching(candidate, split_camelcase=False, is_openiti=False)
    if not norm_candidate:
        return (candidate, matches)

    for author_uri, author_candidates_by_script in authors_candidates.items():
        best_score = 0
        best_combined_idf = 0.0  # all matched tokens — used for threshold entry
        best_rare_idf = 0.0      # rare matched tokens only — stored for stage 3 scoring

        for script in ["lat", "ara"]:
            variants = author_candidates_by_script.get(script)
            if not variants:
                continue

            # Concatenate all variants into one string so token_set_ratio sees the
            # full author name space and IDF intersection is comprehensive.
            norm_parts = [normalize_for_matching(s, split_camelcase=True, is_openiti=True) for s in variants if s]
            norm_str = ' '.join(p for p in norm_parts if p)
            if not norm_str:
                continue

            raw_score = fuzz.token_set_ratio(norm_candidate, norm_str)

            if idf_weights:
                score, combined_idf, rare_idf = _score_with_token_weighting(norm_candidate, norm_str, idf_weights, raw_score)
            else:
                score, combined_idf, rare_idf = raw_score, 0.0, 0.0

            if score > best_score:
                best_score = score
                best_combined_idf = combined_idf
                best_rare_idf = rare_idf

        # Threshold uses full combined_idf boost for recall (lets common-name authors through).
        # Stored value uses rare_idf only — weak matches fail COMBINED_FLOOR at stage 3.
        boost = 1 + min(best_combined_idf / AUTHOR_IDF_BOOST_SCALE, AUTHOR_MAX_BOOST - 1)
        if best_score * boost >= threshold * 100:
            matches[author_uri] = (best_score / 100.0, best_rare_idf)

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

        # Build token-level IDF weights — needed for stage 1 boost and creator token filtering
        from matching.config import USE_AUTHOR_IDF_WEIGHTING, USE_AUTHOR_CREATOR_FIELD_MATCHING

        if USE_AUTHOR_IDF_WEIGHTING or USE_AUTHOR_CREATOR_FIELD_MATCHING:
            if self.verbose:
                print("  Building IDF weights from OpenITI author data only...")
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

        from matching.config import (
            USE_AUTHOR_CREATOR_FIELD_MATCHING,
            AUTHOR_CREATOR_IDF_THRESHOLD,
            AUTHOR_FULL_STRING_WEIGHT,
            AUTHOR_CREATOR_FIELD_WEIGHT,
            AUTHOR_IDF_BOOST_SCALE,
            AUTHOR_MAX_BOOST,
        )
        from fuzzywuzzy import fuzz
        from matching.normalize import normalize_for_matching

        # ── Phase 1: collect raw scores across all candidates ──────────────────
        # Each candidate may contribute different authors to the same bnf_id.
        # We merge here (max wins) so Phase 2 sees the full author set per record.
        # Structure: {bnf_id: {author_uri: (raw_score, combined_idf)}}
        raw_data = {}  # keyed by bnf_id

        for candidate, matched_authors_dict in results:
            for bnf_id in candidate_to_bnf_ids[candidate]:
                if bnf_id not in raw_data:
                    raw_data[bnf_id] = {}
                for author_uri, (raw_score, combined_idf) in matched_authors_dict.items():
                    existing = raw_data[bnf_id].get(author_uri)
                    if existing is None:
                        raw_data[bnf_id][author_uri] = (raw_score, combined_idf)
                    else:
                        new_boost = 1 + min(combined_idf / AUTHOR_IDF_BOOST_SCALE, AUTHOR_MAX_BOOST - 1)
                        ex_boost = 1 + min(existing[1] / AUTHOR_IDF_BOOST_SCALE, AUTHOR_MAX_BOOST - 1)
                        if raw_score * new_boost > existing[0] * ex_boost:
                            raw_data[bnf_id][author_uri] = (raw_score, combined_idf)

        # Store pre-reweighting scores for export/analysis
        if not hasattr(pipeline, '_stage1_scores_pre_reweighting'):
            pipeline._stage1_scores_pre_reweighting = {}
        for bnf_id, author_map in raw_data.items():
            pipeline._stage1_scores_pre_reweighting[bnf_id] = {
                uri: score for uri, (score, _) in author_map.items()
            }

        # ── Phase 2: per-record creator reweighting then boost ─────────────────
        # Now each bnf_id has its complete author set, so the threshold decision
        # and reweighting are made over all candidates together.
        for bnf_id, author_map in raw_data.items():

            # Gather BNF creator fields
            all_bnf_creators = []
            if USE_AUTHOR_CREATOR_FIELD_MATCHING:
                bnf_record = pipeline.bnf_records.get(bnf_id)
                if bnf_record:
                    bnf_creator_lat = bnf_record.get('creator_lat') if isinstance(bnf_record, dict) else getattr(bnf_record, 'creator_lat', None)
                    bnf_creator_ara = bnf_record.get('creator_ara') if isinstance(bnf_record, dict) else getattr(bnf_record, 'creator_ara', None)
                    bnf_creators_lat = bnf_creator_lat if isinstance(bnf_creator_lat, list) else ([bnf_creator_lat] if bnf_creator_lat else [])
                    bnf_creators_ara = bnf_creator_ara if isinstance(bnf_creator_ara, list) else ([bnf_creator_ara] if bnf_creator_ara else [])
                    all_bnf_creators = bnf_creators_lat + bnf_creators_ara

            # Score every matched author against BNF creator fields using rare-token overlap.
            # Strategy: filter the OpenITI author's tokens by IDF (rare tokens only), then
            # count how many of those rare OpenITI tokens appear in the BNF creator token set.
            # Score per script independently, take max — avoids penalising records
            # where BNF or OpenITI has data for only one script.
            # Per script: score = |openiti_rare_script ∩ bnf_creator_tokens| / |openiti_rare_script|
            # Final author score = max(lat_score, ara_score).
            # Trigger (both-sides check): BNF has creator fields AND at least one OpenITI
            # candidate has > 1 matching rare token in either script.
            # {author_uri: (matching_count, score)} — count kept for trigger check
            author_creator_data = {}

            if all_bnf_creators and idf_weights:
                # Build a single flat token set from all BNF creator strings
                bnf_creator_tokens = set()
                for c in all_bnf_creators:
                    if not c:
                        continue
                    norm = normalize_for_matching(c, split_camelcase=False, is_openiti=False)
                    if norm:
                        bnf_creator_tokens.update(norm.lower().split())

                if bnf_creator_tokens:
                    for author_uri in author_map:
                        author_obj = pipeline.openiti_index.authors.get(author_uri)
                        if not author_obj:
                            author_creator_data[author_uri] = (0, 0.0)
                            continue

                        author_candidates = build_author_candidates_by_script(author_obj)
                        best_matching = 0
                        best_score = 0.0

                        # Score each variant independently — accumulating across variants
                        # inflates the denominator, diluting scores for richly-named authors.
                        # Best-variant score wins.
                        for script in ['lat', 'ara']:
                            for n in author_candidates.get(script, []):
                                if not n:
                                    continue
                                norm_n = normalize_for_matching(n, split_camelcase=True, is_openiti=True)
                                if not norm_n:
                                    continue
                                variant_rare = {t for t in norm_n.lower().split() if idf_weights.get(t, 0.0) >= AUTHOR_CREATOR_IDF_THRESHOLD}
                                if not variant_rare:
                                    continue
                                matching = len(variant_rare & bnf_creator_tokens)
                                if matching == 0:
                                    continue
                                score = matching / len(variant_rare)
                                if score > best_score:
                                    best_score = score
                                    best_matching = matching

                        author_creator_data[author_uri] = (best_matching, best_score)

            # Both-sides trigger: BNF has creator fields and at least one candidate
            # has > 1 matching rare token.
            apply_creator_reweighting = any(
                count > 1 for count, _ in author_creator_data.values()
            )

            # Compute final scores and write to pipeline.
            # Intermediate scores stored for export/analysis:
            #   _stage1_scores_pre_reweighting : raw fuzzy score (set in Phase 1)
            #   _stage1_scores_post_idf        : raw * idf_boost (before creator reweighting)
            #   set_stage1_scores              : final (creator reweighted then idf boosted)
            # When reweighting is triggered, ALL candidates for this record get the formula
            # applied — zero-match candidates are intentionally penalised (raw * w1 + 0 * w2),
            # which is what separates them from the creator-matched candidate.
            final_scores = {}
            idf_scores = {}
            for author_uri, (raw_score, combined_idf) in author_map.items():
                boost = 1 + min(combined_idf / AUTHOR_IDF_BOOST_SCALE, AUTHOR_MAX_BOOST - 1)
                idf_scores[author_uri] = raw_score * boost
                if apply_creator_reweighting:
                    _, creator_score = author_creator_data.get(author_uri, (0, 0.0))
                    base = raw_score * AUTHOR_FULL_STRING_WEIGHT + creator_score * AUTHOR_CREATOR_FIELD_WEIGHT
                else:
                    base = raw_score
                final_scores[author_uri] = base * boost

            if not hasattr(pipeline, '_stage1_scores_post_idf'):
                pipeline._stage1_scores_post_idf = {}
            pipeline._stage1_scores_post_idf[bnf_id] = idf_scores

            pipeline.set_stage1_result(bnf_id, list(final_scores.keys()))
            pipeline.set_stage1_scores(bnf_id, final_scores)

        if self.verbose:
            print(f"Stage 1 complete.")

