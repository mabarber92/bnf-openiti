"""
Parameter sweeper: test all combinations of author/title thresholds with/without IDF.

Tests 75 configurations (5 author × 5 title × 3 IDF variants) in parallel.
Each configuration runs the matching pipeline and records Precision, Recall, F1.

Output: parameter_optimization/results/sweep_results.csv
"""

import json
import sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from itertools import product

import pandas as pd
from tqdm import tqdm

# Add repo root to path
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH


def run_single_config(config_params):
    """
    Run pipeline with specific threshold and IDF settings.

    Returns: (config_params, precision, recall, f1, matched_book_uris, extra_count)
    """
    author_threshold, title_threshold, idf_enabled, penalty_exp = config_params

    # Load data
    bnf_records = load_bnf_records(BNF_FULL_PATH)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

    # Load test correspondences
    with open(repo_root / "data_samplers" / "correspondence.json", encoding="utf-8") as f:
        correspondences = json.load(f)

    test_pairs = {}
    for item in correspondences:
        for openiti_uri, bnf_id in item.items():
            if bnf_id not in test_pairs:
                test_pairs[bnf_id] = []
            test_pairs[bnf_id].append(openiti_uri)

    # Filter to test records
    test_bnf_ids = [bid for bid in test_pairs.keys() if bid in bnf_records]
    bnf_records_test = {bid: bnf_records[bid] for bid in test_bnf_ids}

    # Temporarily modify config
    import matching.config as config_module
    old_author_thresh = config_module.AUTHOR_THRESHOLD
    old_title_thresh = config_module.TITLE_THRESHOLD
    old_idf_enabled = config_module.USE_TOKEN_IDF_WEIGHTING
    old_penalty_exp = getattr(config_module, 'TOKEN_IDF_PENALTY_EXPONENT', 3)

    try:
        # Set new config
        config_module.AUTHOR_THRESHOLD = author_threshold
        config_module.TITLE_THRESHOLD = title_threshold
        config_module.USE_TOKEN_IDF_WEIGHTING = idf_enabled
        config_module.TOKEN_IDF_PENALTY_EXPONENT = penalty_exp

        # Run pipeline (no parallelization to avoid nesting)
        pipeline = MatchingPipeline(
            bnf_records_test,
            openiti_data,
            run_id=f"sweep_{author_threshold}_{title_threshold}_{idf_enabled}_{penalty_exp}",
            verbose=False
        )
        pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
        pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
        pipeline.register_stage(CombinedMatcher(verbose=False))
        pipeline.register_stage(Classifier(verbose=False))
        pipeline.run()

        # Calculate metrics
        correct = 0
        extra_total = 0
        for bnf_id, expected_uris in test_pairs.items():
            result = pipeline.get_final_result(bnf_id)
            matched = set(result) if result else set()
            expected = set(expected_uris)

            if matched == expected:
                correct += 1

            extra = matched - expected
            extra_total += len(extra)

        recall = correct / len(test_pairs) if test_pairs else 0
        total_matched = sum(len(pipeline.get_final_result(bid) or []) for bid in test_pairs.keys())
        expected_count = sum(len(uris) for uris in test_pairs.values())
        precision = (total_matched - extra_total) / total_matched if total_matched > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        return (
            author_threshold,
            title_threshold,
            idf_enabled,
            penalty_exp,
            precision,
            recall,
            f1,
            extra_total,
            correct
        )

    finally:
        # Restore config
        config_module.AUTHOR_THRESHOLD = old_author_thresh
        config_module.TITLE_THRESHOLD = old_title_thresh
        config_module.USE_TOKEN_IDF_WEIGHTING = old_idf_enabled
        if hasattr(config_module, 'TOKEN_IDF_PENALTY_EXPONENT'):
            config_module.TOKEN_IDF_PENALTY_EXPONENT = old_penalty_exp


def main():
    """Run parameter sweep."""
    print("=" * 80)
    print("PARAMETER SWEEP: Author/Title Thresholds × IDF Variants")
    print("=" * 80)

    # Define parameter ranges
    author_thresholds = [0.75, 0.80, 0.85, 0.90, 0.95]
    title_thresholds = [0.75, 0.80, 0.85, 0.90, 0.95]
    idf_variants = [
        (False, 3),      # No IDF
        (True, 3),       # IDF with ^3
        (True, 4),       # IDF with ^4
    ]

    # Generate all combinations
    configs = list(product(
        author_thresholds,
        title_thresholds,
        idf_variants
    ))

    # Flatten: (author, title, (idf_bool, penalty)) -> (author, title, idf_bool, penalty)
    configs = [
        (author, title, idf_bool, penalty)
        for author, title, (idf_bool, penalty) in configs
    ]

    print(f"\nTesting {len(configs)} configurations...")
    print(f"  Author thresholds: {author_thresholds}")
    print(f"  Title thresholds: {title_thresholds}")
    print(f"  IDF variants: No IDF, IDF^3, IDF^4\n")

    # Run in parallel
    results = []
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(run_single_config, cfg): cfg for cfg in configs}

        for future in tqdm(futures, desc="Running configs", total=len(configs)):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                config = futures[future]
                print(f"ERROR in config {config}: {e}")

    # Convert to DataFrame
    df = pd.DataFrame(results, columns=[
        'author_threshold', 'title_threshold', 'idf_enabled', 'penalty_exponent',
        'precision', 'recall', 'f1', 'extra_matches', 'correct_matches'
    ])

    # Add timestamp and IDF label
    df['idf_label'] = df.apply(
        lambda row: f"IDF^{int(row['penalty_exponent'])}" if row['idf_enabled'] else "No IDF",
        axis=1
    )
    df['timestamp'] = datetime.now().isoformat()

    # Save to CSV
    output_path = Path(__file__).parent / "results" / "sweep_results.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"\n✓ Results saved to: {output_path}")
    print(f"\nSummary statistics:")
    print(f"  Total configs tested: {len(df)}")
    print(f"  Precision range: {df['precision'].min():.3f} - {df['precision'].max():.3f}")
    print(f"  Recall range: {df['recall'].min():.3f} - {df['recall'].max():.3f}")
    print(f"  F1 range: {df['f1'].min():.3f} - {df['f1'].max():.3f}")

    return df


if __name__ == "__main__":
    df = main()
