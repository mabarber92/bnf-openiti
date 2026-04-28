"""
Main matching orchestrator.

Coordinates fuzzy matching pipeline:
1. Load BNF and OpenITI data
2. Build global dedup indices
3. Register and execute stages (author → title → combined → classify)
4. Aggregate results and write output
"""

import json
from pathlib import Path
from typing import Optional

from matching.config import (
    OPENITI_CORPUS_PATH, BNF_SAMPLE_PATH, BNF_FULL_PATH,
    AUTHOR_THRESHOLD, TITLE_THRESHOLD, get_run_dir, get_output_files
)
from matching.openiti_index import OpenITIIndex
from matching.bnf_index import BNFCandidateIndex
from utils.config import load_config


class MatchingPipeline:
    """Main orchestrator for fuzzy matching pipeline."""

    def __init__(
        self,
        bnf_records: dict,
        openiti_data: dict,
        run_id: str = "default",
        norm_strategy: str = "fuzzy",
        verbose: bool = True,
    ):
        """
        Initialize pipeline.

        Parameters
        ----------
        bnf_records : dict
            {bnf_id: BNFRecord} from parsed JSON
        openiti_data : dict
            {"books": {...}, "authors": {...}} from parsed JSON
        run_id : str
            Identifier for this run (used for output directory)
        norm_strategy : str
            Normalization strategy: "fuzzy", "embedding", "raw"
        verbose : bool
            Print progress messages
        """
        self.config = load_config()
        self.bnf_records = bnf_records
        self.openiti_books = openiti_data["books"]
        self.openiti_authors = openiti_data["authors"]
        self.run_id = run_id
        self.norm_strategy = norm_strategy
        self.verbose = verbose

        # Build indices
        self.openiti_index = OpenITIIndex(self.openiti_books, self.openiti_authors)
        self.bnf_index = BNFCandidateIndex(self.bnf_records, norm_strategy=norm_strategy)

        # Result state (stages write here)
        self._stage1_results = {}  # {BNF_ID: [author_URIs]}
        self._stage1_scores = {}   # {BNF_ID: {author_URI: score}}
        self._stage1_matched_candidates = {}  # {BNF_ID: [candidate_strings that matched]}
        self._stage2_results = {}  # {BNF_ID: [book_URIs]}
        self._stage2_scores = {}   # {BNF_ID: {book_URI: score}}
        self._stage3_results = {}  # {BNF_ID: [book_URIs where author matches]}
        self._classified = {}      # {BNF_ID: tier}

        # Pipeline stages (pluggable)
        self.stages = []

    def register_stage(self, stage):
        """Register a stage to run in sequence."""
        self.stages.append(stage)
        if self.verbose:
            print(f"Registered stage: {stage.__class__.__name__}")

    def run(self) -> None:
        """Execute all registered stages."""
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"MATCHING PIPELINE: {self.run_id}")
            print(f"{'='*60}")
            print(f"BNF records: {len(self.bnf_records)}")
            print(f"OpenITI books: {self.openiti_index.book_count()}")
            print(f"OpenITI authors: {self.openiti_index.author_count()}")
            print(f"Unique author candidates: {self.bnf_index.author_candidate_count()}")
            print(f"Unique title candidates: {self.bnf_index.title_candidate_count()}")

        # Execute stages in order
        for stage in self.stages:
            if self.verbose:
                print(f"\n--- {stage.__class__.__name__} ---")
            stage.execute(self)

        if self.verbose:
            print(f"\n{'='*60}")
            print("PIPELINE COMPLETE")
            print(f"{'='*60}")

    def run_with_candidate_filtering(self) -> None:
        """
        Execute all registered stages with author string stripping and reindexing.

        After stage 1 (author matching), strips matched author name strings from BNF
        records' title/creator/description fields, then rebuilds the candidate index
        for stage 2. This prevents matched author names from triggering rare-token
        boosts in title matching.

        Only use this when running the full 3-stage pipeline (author → title → combined).
        """
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"MATCHING PIPELINE (with author string stripping): {self.run_id}")
            print(f"{'='*60}")
            print(f"BNF records: {len(self.bnf_records)}")
            print(f"OpenITI books: {self.openiti_index.book_count()}")
            print(f"OpenITI authors: {self.openiti_index.author_count()}")
            print(f"Unique author candidates: {self.bnf_index.author_candidate_count()}")
            print(f"Unique title candidates: {self.bnf_index.title_candidate_count()}")

        for i, stage in enumerate(self.stages):
            if self.verbose:
                print(f"\n--- {stage.__class__.__name__} ---")
            stage.execute(self)

            # After stage 1 (AuthorMatcher), strip matched author strings and rebuild candidate index
            if i == 0 and stage.__class__.__name__ == "AuthorMatcher":
                self._strip_matched_author_strings_and_reindex()

        if self.verbose:
            print(f"\n{'='*60}")
            print("PIPELINE COMPLETE")
            print(f"{'='*60}")

    # =========================================================================
    # Stage interface: get/set results
    # =========================================================================

    def set_stage1_result(self, bnf_id: str, author_uris: list[str]) -> None:
        """Stage 1 writes author matches."""
        self._stage1_results[bnf_id] = author_uris

    def get_stage1_result(self, bnf_id: str) -> list[str]:
        """Retrieve Stage 1 results for a BNF record."""
        return self._stage1_results.get(bnf_id, [])

    def set_stage1_scores(self, bnf_id: str, scores: dict) -> None:
        """Stage 1 writes match confidence scores."""
        self._stage1_scores[bnf_id] = scores

    def get_stage1_scores(self, bnf_id: str) -> dict:
        """Retrieve Stage 1 scores for a BNF record."""
        return self._stage1_scores.get(bnf_id, {})

    def set_stage1_matched_candidates(self, bnf_id: str, candidates: list[str]) -> None:
        """Stage 1 records which author candidate strings matched (high confidence)."""
        self._stage1_matched_candidates[bnf_id] = candidates

    def get_stage1_matched_candidates(self, bnf_id: str) -> list[str]:
        """Retrieve matched author candidate strings for a BNF record."""
        return self._stage1_matched_candidates.get(bnf_id, [])

    def set_stage2_result(self, bnf_id: str, book_uris: list[str]) -> None:
        """Stage 2 writes title matches."""
        self._stage2_results[bnf_id] = book_uris

    def get_stage2_result(self, bnf_id: str) -> list[str]:
        """Retrieve Stage 2 results for a BNF record."""
        return self._stage2_results.get(bnf_id, [])

    def set_stage2_scores(self, bnf_id: str, scores: dict) -> None:
        """Stage 2 writes match confidence scores."""
        self._stage2_scores[bnf_id] = scores

    def get_stage2_scores(self, bnf_id: str) -> dict:
        """Retrieve Stage 2 scores for a BNF record."""
        return self._stage2_scores.get(bnf_id, {})

    def set_stage3_result(self, bnf_id: str, book_uris: list[str]) -> None:
        """Stage 3 writes combined (intersected) matches."""
        self._stage3_results[bnf_id] = book_uris

    def get_stage3_result(self, bnf_id: str) -> list[str]:
        """Retrieve Stage 3 results for a BNF record."""
        return self._stage3_results.get(bnf_id, [])

    def set_classification(self, bnf_id: str, tier: str) -> None:
        """Classifier writes confidence tier."""
        self._classified[bnf_id] = tier

    def get_classification(self, bnf_id: str) -> Optional[str]:
        """Retrieve classification for a BNF record."""
        return self._classified.get(bnf_id)

    def _strip_matched_author_strings_and_reindex(self) -> None:
        """
        Strip matched author names from BNF records and rebuild candidate index.

        After stage 1:
        1. For each BNF record that matched an author
        2. Get the author names from OpenITI
        3. Strip those names from the BNF record's title/creator/description fields
        4. Rebuild the candidate index for stage 2 with the modified records

        This prevents matched author names from appearing in title candidates for
        stage 2, avoiding double-counting of matching signals.
        Uses normalization to match author names despite diacritical differences.
        """
        from matching.normalize import normalize_for_matching
        import re

        stripped_count = 0
        records_with_authors = 0

        for bnf_id, bnf_record in self.bnf_records.items():
            stage1_authors = self.get_stage1_result(bnf_id)
            if not stage1_authors:
                continue

            records_with_authors += 1
            # Collect author name variants to strip
            author_names_to_strip = []

            for author_uri in stage1_authors:
                author_data = self.openiti_index.get_author(author_uri)
                if not author_data:
                    continue

                # Extract author names (handle dict and dataclass)
                if isinstance(author_data, dict):
                    if author_data.get("lat_name"):
                        author_names_to_strip.append(author_data["lat_name"])
                    if author_data.get("ar_name"):
                        author_names_to_strip.append(author_data["ar_name"])
                else:
                    if hasattr(author_data, "lat_name") and author_data.lat_name:
                        author_names_to_strip.append(author_data.lat_name)
                    if hasattr(author_data, "ar_name") and author_data.ar_name:
                        author_names_to_strip.append(author_data.ar_name)

            if not author_names_to_strip:
                continue

            # Strip from title/creator/description fields
            # These are the fields BNF title candidates are extracted from
            for field in ["title_lat", "title_ara", "creator_lat", "creator_ara",
                         "description_candidates_lat", "description_candidates_ara"]:
                if field not in bnf_record or not isinstance(bnf_record[field], list):
                    continue

                for i, value in enumerate(bnf_record[field]):
                    if not isinstance(value, str):
                        continue

                    # Normalize both the field value and author names for comparison
                    norm_value = normalize_for_matching(value, split_camelcase=False, is_openiti=False)

                    # Try to remove each author name
                    for author_name in author_names_to_strip:
                        norm_author = normalize_for_matching(author_name, split_camelcase=True, is_openiti=True)

                        # If normalized author name matches part of normalized value, try to remove
                        if norm_author and norm_author.lower() in norm_value.lower():
                            # Use regex for case-insensitive removal
                            pattern = re.compile(re.escape(author_name), re.IGNORECASE)
                            new_value = pattern.sub("", value).strip()
                            if new_value != value:
                                bnf_record[field][i] = new_value
                                stripped_count += 1

        if self.verbose:
            print(f"  Author string stripping: {records_with_authors} BNF records matched authors")
            print(f"  Stripped {stripped_count} author name occurrences from title/creator fields")

        # Always rebuild candidate index after stage 1 to reflect any stripping
        if stripped_count > 0 or records_with_authors > 0:
            if self.verbose:
                print(f"  Rebuilding candidate index for stage 2...")

            # Clear and rebuild the index
            self.bnf_index = BNFCandidateIndex(self.bnf_records, norm_strategy=self.norm_strategy)

            if self.verbose:
                print(f"  Title candidates after stripping: {self.bnf_index.title_candidate_count()}")

    # =========================================================================
    # Output writing
    # =========================================================================

    def write_results(self) -> None:
        """Write final results to output files."""
        run_dir = get_run_dir(self.run_id)
        output_files = get_output_files(run_dir)

        if self.verbose:
            print(f"\nWriting results to {run_dir}")

        # Separate results by confidence tier
        high_confidence = []
        author_only = []
        title_only = []
        unmatched = []

        for bnf_id in self.bnf_records.keys():
            tier = self.get_classification(bnf_id)
            matches = self.get_stage3_result(bnf_id)

            result = {
                "bnf_id": bnf_id,
                "tier": tier,
                "matches": matches,
                "author_matches": self.get_stage1_result(bnf_id),
                "title_matches": self.get_stage2_result(bnf_id),
            }

            if tier == "high_confidence":
                high_confidence.append(result)
            elif tier == "author_only":
                author_only.append(result)
            elif tier == "title_only":
                title_only.append(result)
            else:
                unmatched.append(result)

        # Write individual tier files
        for tier_name, results, output_file in [
            ("high_confidence", high_confidence, output_files["high_confidence"]),
            ("author_only", author_only, output_files["author_only"]),
            ("title_only", title_only, output_files["title_only"]),
            ("unmatched", unmatched, output_files["unmatched"]),
        ]:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            if self.verbose:
                print(f"  {tier_name}: {len(results)} records → {output_file}")

        # Write summary
        summary = {
            "run_id": self.run_id,
            "total_bnf_records": len(self.bnf_records),
            "high_confidence": len(high_confidence),
            "author_only": len(author_only),
            "title_only": len(title_only),
            "unmatched": len(unmatched),
        }
        with open(output_files["summary"], "w", encoding="utf-8") as f:
            for key, value in summary.items():
                f.write(f"{key}: {value}\n")
        if self.verbose:
            print(f"\n  Summary: {output_files['summary']}")
