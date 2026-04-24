"""Compare fuzzywuzzy vs PolyFuzz on correspondence.json test set."""

import json
from matching.config import AUTHOR_THRESHOLD
from matching.bnf_index import BNFCandidateIndex
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.pipeline import MatchingPipeline
from utils.parse_openiti import load_openiti_corpus
from utils.parse_bnf import load_bnf_corpus

def run_test(fuzzy_backend):
    """Run matching pipeline with specified fuzzy backend."""
    import matching.config as cfg
    cfg.FUZZY_MATCHER = fuzzy_backend
    
    print(f"\n{'='*70}")
    print(f"Testing with FUZZY_MATCHER = {fuzzy_backend}")
    print(f"{'='*70}")
    
    # Load data
    openiti_books, openiti_authors = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
    bnf_records = load_bnf_corpus(cfg.BNF_SAMPLE_PATH)
    
    # Create pipeline
    pipeline = MatchingPipeline(bnf_records, openiti_books, openiti_authors, norm_strategy="fuzzy")
    
    # Run stages
    AuthorMatcher(verbose=True, use_parallel=False).execute(pipeline)
    TitleMatcher(verbose=True, use_parallel=False).execute(pipeline)
    
    # Load expected matches
    with open("data_samplers/correspondence.json", encoding="utf-8") as f:
        correspondence = json.load(f)
    
    expected = {}
    for pair in correspondence:
        for openiti_uri, bnf_id in pair.items():
            if bnf_id not in expected:
                expected[bnf_id] = []
            expected[bnf_id].append(openiti_uri)
    
    # Evaluate
    correct = 0
    missed = 0
    false_positives = 0
    
    for bnf_id, expected_uris in expected.items():
        matched = pipeline.get_stage1_result(bnf_id) or []
        
        if set(expected_uris) & set(matched):
            correct += 1
        else:
            missed += 1
            print(f"  MISSED: {bnf_id}")
        
        extra = set(matched) - set(expected_uris)
        false_positives += len(extra)
    
    recall = correct / len(expected) * 100 if expected else 0
    precision = correct / (correct + false_positives) * 100 if (correct + false_positives) > 0 else 0
    
    print(f"\nResults with {fuzzy_backend}:")
    print(f"  Recall: {correct}/{len(expected)} ({recall:.1f}%)")
    print(f"  Precision: {precision:.1f}%")
    print(f"  False positives: {false_positives}")
    
    return correct, len(expected), false_positives

if __name__ == "__main__":
    print("Comparing fuzzy matching backends...\n")
    
    # Test with fuzzywuzzy (baseline)
    fuzz_correct, fuzz_total, fuzz_fp = run_test("fuzzywuzzy")
    
    # Test with polyfuzz
    poly_correct, poly_total, poly_fp = run_test("polyfuzz")
    
    print(f"\n{'='*70}")
    print("COMPARISON SUMMARY")
    print(f"{'='*70}")
    print(f"fuzzywuzzy: {fuzz_correct}/{fuzz_total} recall, {fuzz_fp} false positives")
    print(f"polyfuzz:   {poly_correct}/{poly_total} recall, {poly_fp} false positives")
    print(f"Δ recall:   {poly_correct - fuzz_correct:+d} records")
    print(f"Δ FP:       {poly_fp - fuzz_fp:+d} false positives")
