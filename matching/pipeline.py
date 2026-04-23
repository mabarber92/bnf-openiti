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
        self._stage2_results = {}  # {BNF_ID: [book_URIs]}
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

    # =========================================================================
    # Stage interface: get/set results
    # =========================================================================

    def set_stage1_result(self, bnf_id: str, author_uris: list[str]) -> None:
        """Stage 1 writes author matches."""
        self._stage1_results[bnf_id] = author_uris

    def get_stage1_result(self, bnf_id: str) -> list[str]:
        """Retrieve Stage 1 results for a BNF record."""
        return self._stage1_results.get(bnf_id, [])

    def set_stage2_result(self, bnf_id: str, book_uris: list[str]) -> None:
        """Stage 2 writes title matches."""
        self._stage2_results[bnf_id] = book_uris

    def get_stage2_result(self, bnf_id: str) -> list[str]:
        """Retrieve Stage 2 results for a BNF record."""
        return self._stage2_results.get(bnf_id, [])

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
