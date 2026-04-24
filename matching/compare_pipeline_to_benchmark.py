"""
Validation script: Compare pipeline output to benchmark test.

Ensures the production pipeline produces identical results to the canonical
fuzzy matching benchmark test (test_surface_matching.py).

This script should be run after any changes to the matching pipeline to verify
that the results remain consistent with the benchmark.

Usage:
    python matching/compare_pipeline_to_benchmark.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier

# Import benchmark functions
sys.path.insert(0, str(Path(__file__).parent / "final_fuzzy_benchmarking"))
from test_surface_matching import search_authors, search_titles
from matching.normalize import normalize_transliteration
from fuzzywuzzy import fuzz


def load_correspondence():
    """Load test correspondences."""
    with open("data_samplers/correspondence.json", encoding='utf-8') as f:
        correspondences = json.load(f)

    test_pairs = {}
    for item in correspondences:
        for openiti_uri, bnf_id in item.items():
            if bnf_id not in test_pairs:
                test_pairs[bnf_id] = []
            test_pairs[bnf_id].append(openiti_uri)

    return test_pairs


def benchmark_search(bnf_id, expected_uris, author_threshold, title_threshold):
    """Run benchmark search for one record at specified thresholds."""
    # Load locally to avoid circular imports
    with open(BNF_FULL_PATH, encoding='utf-8') as f:
        bnf_records = json.load(f)["records"]
    with open(OPENITI_CORPUS_PATH, encoding='utf-8') as f:
        openiti_data = json.load(f)

    bnf_record = bnf_records.get(bnf_id)
    if not bnf_record:
        return None, None, None

    # Call benchmark functions with separate thresholds
    matched_authors, _ = search_authors(bnf_id, author_threshold)
    matched_books, _ = search_titles(bnf_id, title_threshold)

    # Stage 3: Combine
    combined = []
    for book_uri in matched_books:
        book = openiti_data["books"].get(book_uri)
        if book:
            author_uri = book.get("author_uri")
            if author_uri in matched_authors:
                combined.append(book_uri)

    return set(matched_authors), set(matched_books), set(combined)


def pipeline_search(pipeline, bnf_id):
    """Get pipeline results for one record."""
    stage1 = pipeline.get_stage1_result(bnf_id) or []
    stage2 = pipeline.get_stage2_result(bnf_id) or []
    stage3 = pipeline.get_stage3_result(bnf_id) or []

    return set(stage1), set(stage2), set(stage3)


def run_validation():
    """Run full validation."""
    print("="*70)
    print("PIPELINE VALIDATION: Compare to Benchmark Test")
    print("="*70)

    # Load data
    print("\nLoading data...")
    bnf_records = load_bnf_records(BNF_FULL_PATH)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
    test_pairs = load_correspondence()

    print(f"  BNF records: {len(bnf_records)}")
    print(f"  OpenITI books: {len(openiti_data['books'])}")
    print(f"  Test pairs: {len(test_pairs)}")

    # Run pipeline
    print("\nRunning pipeline...")
    test_bnf_ids = [bid for bid in test_pairs.keys() if bid in bnf_records]
    bnf_records_test = {bid: bnf_records[bid] for bid in test_bnf_ids}

    pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="validation", verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
    pipeline.register_stage(CombinedMatcher(verbose=False))
    pipeline.register_stage(Classifier(verbose=False))

    pipeline.run()

    # Compare results
    print("\nComparing results...")
    mismatches = []

    # Import thresholds from config
    from matching.config import AUTHOR_THRESHOLD, TITLE_THRESHOLD

    for bnf_id in test_bnf_ids:
        expected_uris = test_pairs[bnf_id]

        # Benchmark (use same thresholds as pipeline)
        bench_authors, bench_books, bench_combined = benchmark_search(
            bnf_id, expected_uris, AUTHOR_THRESHOLD, TITLE_THRESHOLD
        )

        # Pipeline (use fixed optimal thresholds)
        pipe_authors, pipe_books, pipe_combined = pipeline_search(pipeline, bnf_id)

        # Compare Stage 1
        if bench_authors != pipe_authors:
            mismatches.append({
                "bnf_id": bnf_id,
                "stage": 1,
                "benchmark_count": len(bench_authors),
                "pipeline_count": len(pipe_authors),
                "benchmark": sorted(bench_authors)[:3],  # First 3 for display
                "pipeline": sorted(pipe_authors)[:3],
                "in_bench_not_pipe": sorted(set(bench_authors) - set(pipe_authors))[:3],
                "in_pipe_not_bench": sorted(set(pipe_authors) - set(bench_authors))[:3],
            })

        # Compare Stage 2
        if bench_books != pipe_books:
            mismatches.append({
                "bnf_id": bnf_id,
                "stage": 2,
                "benchmark_count": len(bench_books),
                "pipeline_count": len(pipe_books),
                "benchmark": sorted(bench_books)[:3],
                "pipeline": sorted(pipe_books)[:3],
                "in_bench_not_pipe": sorted(set(bench_books) - set(pipe_books))[:3],
                "in_pipe_not_bench": sorted(set(pipe_books) - set(bench_books))[:3],
            })

        # Compare Stage 3
        if bench_combined != pipe_combined:
            mismatches.append({
                "bnf_id": bnf_id,
                "stage": 3,
                "benchmark_count": len(bench_combined),
                "pipeline_count": len(pipe_combined),
                "benchmark": sorted(bench_combined),
                "pipeline": sorted(pipe_combined),
            })

    # Report
    print("\n" + "="*70)
    if mismatches:
        print(f"VALIDATION FAILED: {len(mismatches)} mismatches found")
        print("="*70)

        # Group by stage
        for stage in [1, 2, 3]:
            stage_mismatches = [m for m in mismatches if m["stage"] == stage]
            if stage_mismatches:
                print(f"\nStage {stage} Mismatches ({len(stage_mismatches)}):")
                for m in stage_mismatches[:5]:  # Show first 5
                    print(f"  {m['bnf_id']}: benchmark {m['benchmark_count']} vs pipeline {m['pipeline_count']}")
                    if stage < 3:
                        print(f"    In benchmark not in pipeline: {m.get('in_bench_not_pipe', [])}")
                        print(f"    In pipeline not in benchmark: {m.get('in_pipe_not_bench', [])}")

        return False
    else:
        print("VALIDATION PASSED: Pipeline matches benchmark results")
        print("="*70)
        return True


if __name__ == "__main__":
    success = run_validation()
    sys.exit(0 if success else 1)
