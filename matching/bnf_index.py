"""
BNF candidate index builder for global deduplication.

Builds precomputed indices of normalized candidates across the entire BNF dataset,
enabling each unique normalized candidate string to be scored once and the results
applied to all BNF records that contain it.

Structure:
- author_index: {normalized_author_string: [BNF_IDs]}
- title_index: {normalized_title_string: [BNF_IDs]}
"""

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
        print(f"Building BNF candidate indices ({len(self.bnf_records)} records)...")

        for bnf_id, record in tqdm(self.bnf_records.items(), desc="Indexing BNF"):
            # Author candidates
            author_cands = record.matching_candidates(norm_strategy=self.norm_strategy)
            for candidate in author_cands.get("lat", []) + author_cands.get("ara", []):
                if candidate not in self.author_index:
                    self.author_index[candidate] = []
                if bnf_id not in self.author_index[candidate]:
                    self.author_index[candidate].append(bnf_id)

            # Title candidates (same set as author, but indexed separately for clarity)
            title_cands = record.matching_candidates(norm_strategy=self.norm_strategy)
            for candidate in title_cands.get("lat", []) + title_cands.get("ara", []):
                if candidate not in self.title_index:
                    self.title_index[candidate] = []
                if bnf_id not in self.title_index[candidate]:
                    self.title_index[candidate].append(bnf_id)

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
