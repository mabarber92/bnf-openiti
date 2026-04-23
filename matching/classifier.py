"""
Final stage: Classify BNF records by match confidence.

Assigns each BNF record to a confidence tier based on which stages produced matches:
- high_confidence: Stage 3 (both author and title matched)
- author_only: Stage 1 only (author matched but no Stage 3)
- title_only: Stage 2 only (title matched but no Stage 3)
- unmatched: No matches at any stage
"""

from tqdm import tqdm


class Classifier:
    """Assign confidence tiers to matched records."""

    def __init__(self, verbose: bool = True):
        """
        Initialize the classifier.

        Parameters
        ----------
        verbose : bool
            Print progress information
        """
        self.verbose = verbose

    def execute(self, pipeline) -> None:
        """
        Classify BNF records by match confidence tier.

        For each BNF record, assigns to confidence tier based on stages:
        - high_confidence: Stage 3 has matches (author AND title)
        - author_only: Stage 1 has matches, Stage 3 empty
        - title_only: Stage 2 has matches, Stage 3 empty
        - unmatched: No matches at any stage

        Parameters
        ----------
        pipeline : MatchingPipeline
            Pipeline orchestrator with stage results
        """
        if self.verbose:
            print("\n--- Final Stage: Classification ---")

        bnf_ids = list(pipeline.bnf_records.keys())
        tier_counts = {
            "high_confidence": 0,
            "author_only": 0,
            "title_only": 0,
            "unmatched": 0,
        }

        for bnf_id in tqdm(
            bnf_ids,
            desc="Classification",
            disable=not self.verbose,
        ):
            # Get stage results
            stage1 = pipeline.get_stage1_result(bnf_id) or []
            stage2 = pipeline.get_stage2_result(bnf_id) or []
            stage3 = pipeline.get_stage3_result(bnf_id) or []

            # Assign tier based on which stages have results
            if stage3:
                tier = "high_confidence"
            elif stage1 and not stage3:
                tier = "author_only"
            elif stage2 and not stage3:
                tier = "title_only"
            else:
                tier = "unmatched"

            pipeline.set_classification(bnf_id, tier)
            tier_counts[tier] += 1

        if self.verbose:
            print(f"Classification complete:")
            print(f"  High confidence: {tier_counts['high_confidence']}")
            print(f"  Author only:     {tier_counts['author_only']}")
            print(f"  Title only:      {tier_counts['title_only']}")
            print(f"  Unmatched:       {tier_counts['unmatched']}")
