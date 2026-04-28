"""
Validate pipeline on 500-record BNF sample.
Outputs detailed results for manual review.
"""

import json
import sys
from collections import defaultdict

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
import matching.config as cfg
from tqdm import tqdm

def main():
    # Load data
    print("Loading BNF and OpenITI data...")
    all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
    openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

    # Load 500-record sample
    print("Loading 500-record BNF sample...")
    with open('matching/sampling/bnf_sample_500.json') as f:
        sample_records = json.load(f)

    sample_bnf_ids = [item['bnf_id'] for item in sample_records]
    sample_bnf = {bnf_id: all_bnf[bnf_id] for bnf_id in sample_bnf_ids if bnf_id in all_bnf}
    print(f"Loaded {len(sample_bnf)} sample BNF records\n")

    # Run pipeline
    print("Running matching pipeline...")
    pipeline = MatchingPipeline(sample_bnf, openiti_data, verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Evaluate
    print("\nEvaluating results...\n")

    results_by_id = {}
    matched = 0
    unmatched = 0

    for bnf_id in sample_bnf_ids:
        if bnf_id not in sample_bnf:
            continue

        classification = pipeline.get_classification(bnf_id)
        stage3_books = pipeline.get_stage3_result(bnf_id) or []
        stage1_authors = pipeline.get_stage1_result(bnf_id) or []
        stage2_books = pipeline.get_stage2_result(bnf_id) or []

        has_match = classification is not None

        results_by_id[bnf_id] = {
            'classification': classification,
            'stage3_books': stage3_books,
            'stage1_authors': stage1_authors,
            'stage2_books': stage2_books,
            'matched': has_match
        }

        if has_match:
            matched += 1
        else:
            unmatched += 1

    # Print summary
    total = len(sample_bnf)
    print("="*100)
    print("500-RECORD SAMPLE VALIDATION RESULTS")
    print("="*100)
    print(f"\nDataset size: {total} BNF records")
    print(f"\nResults:")
    print(f"  Matched:   {matched:>4} ({matched/total*100:>5.1f}%)")
    print(f"  Unmatched: {unmatched:>4} ({unmatched/total*100:>5.1f}%)")

    # Output detailed results to JSON for manual review
    output_file = "validation_500_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            'config': {
                'AUTHOR_THRESHOLD': cfg.AUTHOR_THRESHOLD,
                'TITLE_THRESHOLD': cfg.TITLE_THRESHOLD,
                'COMBINED_THRESHOLD': cfg.COMBINED_THRESHOLD,
                'COMBINED_FLOOR': cfg.COMBINED_FLOOR,
                'TOKEN_RARITY_THRESHOLD': cfg.TOKEN_RARITY_THRESHOLD,
                'RARE_TOKEN_BOOST_FACTOR': cfg.RARE_TOKEN_BOOST_FACTOR,
            },
            'summary': {
                'total_records': total,
                'matched': matched,
                'unmatched': unmatched,
                'match_rate': matched / total
            },
            'results': results_by_id
        }, f, indent=2, default=str)

    print(f"\n✓ Detailed results saved to: {output_file}")

    # Print configuration
    print(f"\n" + "="*100)
    print(f"Config settings used:")
    print(f"  AUTHOR_THRESHOLD: {cfg.AUTHOR_THRESHOLD}")
    print(f"  TITLE_THRESHOLD: {cfg.TITLE_THRESHOLD}")
    print(f"  COMBINED_THRESHOLD: {cfg.COMBINED_THRESHOLD}")
    print(f"  COMBINED_FLOOR: {cfg.COMBINED_FLOOR}")
    print(f"  TOKEN_RARITY_THRESHOLD: {cfg.TOKEN_RARITY_THRESHOLD}")
    print(f"  RARE_TOKEN_BOOST_FACTOR: {cfg.RARE_TOKEN_BOOST_FACTOR}")
    print("="*100)


if __name__ == "__main__":
    main()
