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
from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH


def run_single_config(config_params):
    """
    Run pipeline with specific threshold and IDF settings.

    Returns: (config_params, precision, recall, f1, matched_book_uris, extra_count)
    """
    author_threshold, title_threshold, author_idf, title_idf, penalty_exp = config_params

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
    old_author_idf = config_module.USE_AUTHOR_IDF_WEIGHTING
    old_title_idf = config_module.USE_TITLE_IDF_WEIGHTING
    old_penalty_exp = getattr(config_module, 'TOKEN_IDF_PENALTY_EXPONENT', 3)

    try:
        # Set new config
        config_module.AUTHOR_THRESHOLD = author_threshold
        config_module.TITLE_THRESHOLD = title_threshold
        config_module.USE_AUTHOR_IDF_WEIGHTING = author_idf
        config_module.USE_TITLE_IDF_WEIGHTING = title_idf
        config_module.TOKEN_IDF_PENALTY_EXPONENT = penalty_exp

        # Import matchers AFTER config is set
        # Import them inside try block to pick up modified config values
        import importlib

        # Clear cached matcher modules so they reimport and pick up new config
        for mod in ['matching.pipeline', 'matching.author_matcher', 'matching.title_matcher', 'matching.combined_matcher', 'matching.classifier']:
            if mod in sys.modules:
                del sys.modules[mod]

        from matching.pipeline import MatchingPipeline
        from matching.author_matcher import AuthorMatcher
        from matching.title_matcher import TitleMatcher
        from matching.combined_matcher import CombinedMatcher
        from matching.classifier import Classifier

        # Run pipeline (no parallelization to avoid nesting)
        idf_label = f"a{int(author_idf)}t{int(title_idf)}"
        pipeline = MatchingPipeline(
            bnf_records_test,
            openiti_data,
            run_id=f"sweep_{author_threshold}_{title_threshold}_{idf_label}_{penalty_exp}",
            verbose=False
        )
        pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
        pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
        pipeline.register_stage(CombinedMatcher(verbose=False))
        pipeline.register_stage(Classifier(verbose=False))
        pipeline.run()

        # Calculate metrics (using "found any" metric like validation script, not "exact match")
        correct = 0
        extra_total = 0
        for bnf_id, expected_uris in test_pairs.items():
            result = pipeline.get_stage3_result(bnf_id)
            matched = set(result) if result else set()
            expected = set(expected_uris)

            # Correct if ANY expected URI is found (not all)
            if any(uri in matched for uri in expected_uris):
                correct += 1

            extra = matched - expected
            extra_total += len(extra)

        recall = correct / len(test_pairs) if test_pairs else 0
        total_matched = sum(len(pipeline.get_stage3_result(bid) or []) for bid in test_pairs.keys())
        expected_count = sum(len(uris) for uris in test_pairs.values())
        precision = (total_matched - extra_total) / total_matched if total_matched > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        # Create IDF label
        if not author_idf and not title_idf:
            idf_label = "No IDF"
        elif author_idf and not title_idf:
            idf_label = f"AuthorIDF^{penalty_exp}"
        elif not author_idf and title_idf:
            idf_label = f"TitleIDF^{penalty_exp}"
        else:
            idf_label = f"BothIDF^{penalty_exp}"

        return (
            author_threshold,
            title_threshold,
            author_idf,
            title_idf,
            penalty_exp,
            precision,
            recall,
            f1,
            extra_total,
            correct,
            idf_label
        )

    finally:
        # Restore config
        config_module.AUTHOR_THRESHOLD = old_author_thresh
        config_module.TITLE_THRESHOLD = old_title_thresh
        config_module.USE_AUTHOR_IDF_WEIGHTING = old_author_idf
        config_module.USE_TITLE_IDF_WEIGHTING = old_title_idf
        if hasattr(config_module, 'TOKEN_IDF_PENALTY_EXPONENT'):
            config_module.TOKEN_IDF_PENALTY_EXPONENT = old_penalty_exp


def main(test_mode=False):
    """Run parameter sweep.

    Parameters
    ----------
    test_mode : bool
        If True, only test one configuration (author=0.80, title=0.80, no IDF)
    """
    print("=" * 80)
    print("PARAMETER SWEEP: Author/Title Thresholds × IDF Variants")
    if test_mode:
        print("(TEST MODE: single configuration)")
    print("=" * 80)

    if test_mode:
        # Single test config (baseline with author IDF)
        configs = [(0.80, 0.85, True, False, 3)]
        print(f"\nTesting {len(configs)} configuration...")
        print(f"  Author=0.80, Title=0.85, AuthorIDF^3, NoTitleIDF\n")
    else:
        # Define parameter ranges
        author_thresholds = [0.75, 0.80, 0.85, 0.90, 0.95]
        title_thresholds = [0.75, 0.80, 0.85, 0.90, 0.95]
        idf_variants = [
            (False, False, 3),  # No IDF
            (True, False, 3),   # Author IDF ^3 only
            (False, True, 3),   # Title IDF ^3 only
            (True, True, 3),    # Both IDF ^3
            (True, True, 4),    # Both IDF ^4
        ]

        # Generate all combinations
        configs = list(product(
            author_thresholds,
            title_thresholds,
            idf_variants
        ))

        # Flatten: (author, title, (author_idf, title_idf, penalty)) -> (author, title, author_idf, title_idf, penalty)
        configs = [
            (author, title, author_idf, title_idf, penalty)
            for author, title, (author_idf, title_idf, penalty) in configs
        ]

        print(f"\nTesting {len(configs)} configurations...")
        print(f"  Author thresholds: {author_thresholds}")
        print(f"  Title thresholds: {title_thresholds}")
        print(f"  IDF variants: None, AuthorOnly, TitleOnly, Both^3, Both^4\n")

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
        'author_threshold', 'title_threshold', 'author_idf', 'title_idf', 'penalty_exponent',
        'precision', 'recall', 'f1', 'extra_matches', 'correct_matches', 'idf_label'
    ])

    # Add timestamp
    df['timestamp'] = datetime.now().isoformat()

    # Save to CSV
    output_path = Path(__file__).parent / "results" / "sweep_results.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"\n[OK] Results saved to: {output_path}")
    print(f"\nSummary statistics:")
    print(f"  Total configs tested: {len(df)}")
    print(f"  Precision range: {df['precision'].min():.3f} - {df['precision'].max():.3f}")
    print(f"  Recall range: {df['recall'].min():.3f} - {df['recall'].max():.3f}")
    print(f"  F1 range: {df['f1'].min():.3f} - {df['f1'].max():.3f}")

    return df


if __name__ == "__main__":
    import sys
    test_mode = "--test" in sys.argv
    df = main(test_mode=test_mode)
