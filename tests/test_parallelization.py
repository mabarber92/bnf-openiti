"""
Test parallelization: Verify sequential and parallel pipeline produce identical results.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH

def run_pipeline(use_parallel: bool) -> dict:
    """Run validation with given parallelization setting."""

    # Load data
    bnf_records = load_bnf_records(BNF_FULL_PATH)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

    with open("data_samplers/correspondence.json", encoding="utf-8") as f:
        correspondences = json.load(f)

    test_pairs = {}
    for item in correspondences:
        for openiti_uri, bnf_id in item.items():
            if bnf_id not in test_pairs:
                test_pairs[bnf_id] = []
            test_pairs[bnf_id].append(openiti_uri)

    # Filter to test records only
    test_bnf_ids = [bid for bid in test_pairs.keys() if bid in bnf_records]
    bnf_records_test = {bid: bnf_records[bid] for bid in test_bnf_ids}

    # Run pipeline
    run_id = "parallel_test" if use_parallel else "sequential_test"
    pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id=run_id, verbose=False)
    pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=use_parallel))
    pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=use_parallel))
    pipeline.register_stage(CombinedMatcher(verbose=False, use_confidence_filtering=False))
    pipeline.register_stage(Classifier(verbose=False))
    pipeline.run()

    # Collect results
    results = {}
    for bnf_id in sorted(test_bnf_ids):
        expected_uris = test_pairs[bnf_id]
        stage1 = sorted(pipeline.get_stage1_result(bnf_id) or [])
        stage2 = sorted(pipeline.get_stage2_result(bnf_id) or [])
        stage3 = sorted(pipeline.get_stage3_result(bnf_id) or [])

        results[bnf_id] = {
            "expected": expected_uris,
            "stage1_count": len(stage1),
            "stage2_count": len(stage2),
            "stage3": stage3,
            "stage3_count": len(stage3),
            "found": any(uri in stage3 for uri in expected_uris),
        }

    return results

if __name__ == "__main__":
    print("="*80)
    print("PARALLELIZATION TEST")
    print("="*80)

    print("\nRunning SEQUENTIAL pipeline...")
    sequential_results = run_pipeline(use_parallel=False)

    print("Running PARALLEL pipeline...")
    parallel_results = run_pipeline(use_parallel=True)

    # Compare results
    print("\n" + "="*80)
    print("COMPARISON")
    print("="*80)

    all_match = True
    differences = []

    for bnf_id in sorted(sequential_results.keys()):
        seq = sequential_results[bnf_id]
        par = parallel_results[bnf_id]

        # Compare Stage 3 results (the most important)
        if seq["stage3"] != par["stage3"]:
            all_match = False
            differences.append({
                "bnf_id": bnf_id,
                "seq_stage3": seq["stage3"],
                "par_stage3": par["stage3"],
                "seq_count": seq["stage3_count"],
                "par_count": par["stage3_count"],
            })

        # Also check stage1 and stage2 counts
        if seq["stage1_count"] != par["stage1_count"] or seq["stage2_count"] != par["stage2_count"]:
            if bnf_id not in [d["bnf_id"] for d in differences]:
                all_match = False
                differences.append({
                    "bnf_id": bnf_id,
                    "seq_stage1": seq["stage1_count"],
                    "par_stage1": par["stage1_count"],
                    "seq_stage2": seq["stage2_count"],
                    "par_stage2": par["stage2_count"],
                })

    if all_match:
        print("\n[SUCCESS] Sequential and parallel pipelines produce IDENTICAL results")
        print("\nAll 10 test records match perfectly:")
        for bnf_id in sorted(sequential_results.keys()):
            seq = sequential_results[bnf_id]
            status = "FOUND" if seq["found"] else "MISSED"
            print(f"  {bnf_id}: {status} ({seq['stage3_count']} matches)")
    else:
        print("\n[FAILURE] Results differ between sequential and parallel!")
        print(f"\nFound {len(differences)} differences:")
        for diff in differences:
            print(f"\n  {diff['bnf_id']}:")
            if "seq_stage3" in diff:
                print(f"    Sequential Stage 3: {diff['seq_stage3']}")
                print(f"    Parallel Stage 3:   {diff['par_stage3']}")
            else:
                print(f"    Sequential S1/S2: {diff['seq_stage1']}/{diff['seq_stage2']}")
                print(f"    Parallel S1/S2:   {diff['par_stage1']}/{diff['par_stage2']}")

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    seq_correct = sum(1 for r in sequential_results.values() if r["found"])
    par_correct = sum(1 for r in parallel_results.values() if r["found"])

    print(f"\nSequential: {seq_correct}/10 records found")
    print(f"Parallel:   {par_correct}/10 records found")

    if all_match and seq_correct == par_correct:
        print("\nCONCLUSION: Parallelization is SAFE - can be used in production")
    else:
        print("\nCONCLUSION: Parallelization has issues - use sequential mode")
