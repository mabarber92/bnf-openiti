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
        from utils.normalize import normalize

        print(f"Building BNF candidate indices ({len(self.bnf_records)} records)...")

        for bnf_id, record in tqdm(self.bnf_records.items(), desc="Indexing BNF"):
            # Get raw candidates split by script (may contain mixed-script contamination)
            try:
                author_cands = record.matching_candidates(norm_strategy="raw")
            except Exception:
                # If even raw extraction fails, skip this record
                continue

            # Process Arabic candidates, detecting and splitting mixed-script
            for raw_candidate in author_cands.get("ara", []):
                self._process_candidate(raw_candidate, "ara", bnf_id, "author", normalize)

            # Process Latin candidates, detecting and splitting mixed-script
            for raw_candidate in author_cands.get("lat", []):
                self._process_candidate(raw_candidate, "lat", bnf_id, "author", normalize)

            # Title candidates (same logic)
            try:
                title_cands = record.matching_candidates(norm_strategy="raw")
            except Exception:
                continue

            for raw_candidate in title_cands.get("ara", []):
                self._process_candidate(raw_candidate, "ara", bnf_id, "title", normalize)

            for raw_candidate in title_cands.get("lat", []):
                self._process_candidate(raw_candidate, "lat", bnf_id, "title", normalize)

    def _process_candidate(self, raw: str, script: str, bnf_id: str, cand_type: str, normalize_fn) -> None:
        """Process a single candidate, handling mixed-script splitting."""
        raw = raw.strip()
        if not raw:
            return

        # Try to normalize as-is first
        try:
            norm = normalize_fn(raw, script, self.norm_strategy)
            if norm:
                self._add_to_index(norm, bnf_id, cand_type)
            return
        except ValueError:
            # Mixed-script contamination detected; split and re-route
            pass

        # Split mixed-script text and route appropriately
        ara_pattern = r"[\u0600-\u06FF\u0750-\u077F]+"
        lat_pattern = r"[A-Za-z0-9\u0100-\u017F\u0180-\u024F]+"

        # Extract Arabic segments
        for match in re.finditer(ara_pattern, raw):
            segment = match.group().strip()
            if segment:
                try:
                    norm = normalize_fn(segment, "ara", self.norm_strategy)
                    if norm:
                        self._add_to_index(norm, bnf_id, cand_type)
                except ValueError:
                    pass

        # Extract Latin segments
        for match in re.finditer(lat_pattern, raw):
            segment = match.group().strip()
            if segment:
                try:
                    norm = normalize_fn(segment, "lat", self.norm_strategy)
                    if norm:
                        self._add_to_index(norm, bnf_id, cand_type)
                except ValueError:
                    pass

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
