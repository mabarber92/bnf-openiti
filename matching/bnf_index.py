"""
BNF candidate index builder for global deduplication.

Builds precomputed indices of normalized candidates across the entire BNF dataset,
enabling each unique normalized candidate string to be scored once and the results
applied to all BNF records that contain it.

Structure:
- author_index: {normalized_author_string: [BNF_IDs]}
- title_index: {normalized_title_string: [BNF_IDs]}
"""

import re
from typing import Optional
from tqdm import tqdm


class BNFCandidateIndex:
    """Global deduplication index for BNF matching candidates."""

    def __init__(self, bnf_records: dict, norm_strategy: str = "fuzzy"):
        """
        Build indices from all BNF records.

        Parameters
        ----------
        bnf_records : dict
            {bnf_id: BNFRecord} from parsed BNF corpus
        norm_strategy : str
            Normalization strategy: "fuzzy", "embedding", or "raw"
        """
        self.bnf_records = bnf_records
        self.norm_strategy = norm_strategy
        self.author_index = {}  # {norm_candidate: [BNF_IDs]}
        self.title_index = {}   # {norm_candidate: [BNF_IDs]}
        self._build_indices()

    def _build_indices(self) -> None:
        """Build author and title indices from all records."""
        from matching.normalize import normalize_for_matching

        print(f"Building BNF candidate indices ({len(self.bnf_records)} records)...")

        for bnf_id, record in tqdm(self.bnf_records.items(), desc="Indexing BNF"):
            try:
                author_cands = record.matching_candidates(norm_strategy="raw")
            except Exception:
                continue

            for raw_candidate in author_cands.get("ara", []):
                self._process_candidate(raw_candidate, bnf_id, "author", normalize_for_matching)

            for raw_candidate in author_cands.get("lat", []):
                self._process_candidate(raw_candidate, bnf_id, "author", normalize_for_matching)

            try:
                title_cands = record.matching_candidates(norm_strategy="raw")
            except Exception:
                continue

            for raw_candidate in title_cands.get("ara", []):
                self._process_candidate(raw_candidate, bnf_id, "title", normalize_for_matching)

            for raw_candidate in title_cands.get("lat", []):
                self._process_candidate(raw_candidate, bnf_id, "title", normalize_for_matching)


    def _process_candidate(self, raw: str, bnf_id: str, cand_type: str, normalize_fn) -> None:
        """Process a single candidate using parametrized normalization (diacritics or legacy)."""
        raw = raw.strip()
        if not raw:
            return

        # Normalize candidate using same function as benchmark test
        norm = normalize_fn(raw)
        if norm:
            self._add_to_index(norm, bnf_id, cand_type)

    def _add_to_index(self, norm_candidate: str, bnf_id: str, cand_type: str) -> None:
        """Add normalized candidate to appropriate index."""
        if cand_type == "author":
            if norm_candidate not in self.author_index:
                self.author_index[norm_candidate] = []
            if bnf_id not in self.author_index[norm_candidate]:
                self.author_index[norm_candidate].append(bnf_id)
        elif cand_type == "title":
            if norm_candidate not in self.title_index:
                self.title_index[norm_candidate] = []
            if bnf_id not in self.title_index[norm_candidate]:
                self.title_index[norm_candidate].append(bnf_id)

    def get_bnf_records_with_author_candidate(self, candidate: str) -> list[str]:
        """Get BNF IDs that have this normalized author candidate."""
        return self.author_index.get(candidate, [])

    def get_bnf_records_with_title_candidate(self, candidate: str) -> list[str]:
        """Get BNF IDs that have this normalized title candidate."""
        return self.title_index.get(candidate, [])

    def author_candidate_count(self) -> int:
        """Total number of unique normalized author candidates."""
        return len(self.author_index)

    def title_candidate_count(self) -> int:
        """Total number of unique normalized title candidates."""
        return len(self.title_index)

    def author_candidates_iter(self):
        """Iterate over (candidate, bnf_ids) for author matching."""
        return self.author_index.items()

    def title_candidates_iter(self):
        """Iterate over (candidate, bnf_ids) for title matching."""
        return self.title_index.items()

    def remove_author_candidates(self, candidates: list[str]) -> None:
        """
        Remove author candidates from the index.

        After stage 1 matches author candidates, remove them from the index
        so they don't get matched again as title candidates in stage 2.

        Parameters
        ----------
        candidates : list[str]
            Normalized author candidate strings to remove
        """
        for candidate in candidates:
            if candidate in self.author_index:
                del self.author_index[candidate]

    def remove_title_candidates(self, candidates: list[str]) -> None:
        """Remove title candidates from the index."""
        for candidate in candidates:
            if candidate in self.title_index:
                del self.title_index[candidate]
