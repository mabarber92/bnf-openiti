# Pipeline Validation Plan

This document describes what changes are needed to ensure the matching pipeline produces equivalent results to the benchmark test, and proposes a validation mechanism.

---

## Part 1: Current Pipeline Architecture vs. Benchmark

### Current Pipeline (matching/pipeline.py + stage classes)

The current pipeline uses:

1. **Global Deduplication Indices** (matching/bnf_index.py)
   - Pre-computes all unique normalized candidates from BNF
   - Maps each unique candidate to BNF IDs that contain it
   - Goal: Score each candidate once, apply results to all containing records

2. **Stage 1: AuthorMatcher** (matching/author_matcher.py)
   - Iterates through unique BNF author candidates (from BNF index)
   - For each candidate, scores against ALL OpenITI authors
   - Returns author URIs that match each candidate
   - Maps results back to BNF records via the index

3. **Stage 2: TitleMatcher** (matching/title_matcher.py)
   - Iterates through unique BNF title candidates
   - Scores against OpenITI books
   - Returns book URIs

4. **Stage 3: CombinedMatcher** (matching/combined_matcher.py)
   - Takes Stage 1 author URIs and Stage 2 book URIs
   - Filters Stage 2 books to only those whose author appears in Stage 1

### Benchmark Test (matching/final_fuzzy_benchmarking/test_surface_matching.py)

The benchmark uses:

1. **Search Authors** (`search_authors(bnf_id, threshold)`)
   - For a single BNF record, extracts ALL author candidates (normalized, deduplicated)
   - Iterates through ALL OpenITI authors (not candidates)
   - Returns author URIs that match at threshold

2. **Search Titles** (`search_titles(bnf_id, threshold)`)
   - For a single BNF record, extracts ALL title candidates
   - Iterates through ALL OpenITI books
   - Returns book URIs that match at threshold

3. **Stage 3: Combine** (in analyze_threshold_combinations.py)
   - For each book in Stage 2, check if its author is in Stage 1
   - Return books where both signals match

### Key Differences

| Aspect | Pipeline | Benchmark |
|--------|----------|-----------|
| **Author Search** | Searches for each BNF author candidate against authors | Searches for each BNF author candidate against authors ✓ |
| **Candidate Extraction** | Per-BNF-record | Per-BNF-record ✓ |
| **Author Candidates** | From index (normalized) | Built fresh each search ✓ |
| **Title Candidates** | From index (normalized) | Built fresh each search ✓ |
| **Deduplication** | Global (candidates deduplicated across BNF) | Per-BNF (candidates built per record) |
| **Search Scope** | Author candidates → authors; title candidates → books | Author candidates → all authors; title candidates → all books ✓ |
| **Threshold Handling** | Single threshold for both stages | Separate thresholds for each stage ✓ |

**Status**: Pipeline should be functionally equivalent IF the deduplication doesn't introduce bugs.

---

## Part 2: Potential Issues to Verify

### Issue 1: Candidate Normalization
**Risk**: Pipeline normalizes candidates during index building. Benchmark normalizes during search.
- **Impact**: Should not differ if normalize_transliteration() is deterministic
- **Verification**: Compare normalized candidates from index vs. fresh normalization

### Issue 2: Language Splitting
**Status**: BNF index has mixed-script splitting disabled (per notes)
- **Current**: _process_candidate() skips on normalization failure (doesn't split)
- **Benchmark**: No mixed-script splitting logic
- **Action**: VERIFY the current implementation matches this expectation

### Issue 3: Threshold Application
**Risk**: Pipeline may apply thresholds differently
- **Current Code**: Line 72 in author_matcher.py: `if score >= threshold * 100`
- **Benchmark**: Line 76 in test_surface_matching.py: `if score >= threshold * 100`
- **Status**: ✓ Matches

### Issue 4: Author Candidate Extraction
**Critical**: Must include ALL sources (creators, contributors, descriptions, titles)

**Current** (matching/candidate_builders.py):
```python
def build_author_candidates_by_script(bnf_record):
    candidates = {"lat": [], "ara": []}
    
    # Creators
    for creator in bnf_record.get("creator_lat", []):
        candidates["lat"].append(creator)
    # Contributor
    for contrib in bnf_record.get("contributor_lat", []):
        candidates["lat"].append(contrib)
    # Titles (contain author names)
    for title in bnf_record.get("title_lat", []):
        for part in title.split(". "):
            candidates["lat"].append(part)
    # Description candidates
    for desc in bnf_record.get("description_candidates_lat", []):
        candidates["lat"].append(desc)
    return candidates
```

**Benchmark** (test_surface_matching.py line 81-130):
```python
def build_bnf_author_candidates(bnf_record):
    candidates = {"lat": [], "ara": []}
    for creator in bnf_record.get("creator_lat", []):
        candidates["lat"].append(creator)
    for contrib in bnf_record.get("contributor_lat", []):
        candidates["lat"].append(contrib)
    for title in bnf_record.get("title_lat", []):
        for part in title.split(". "):
            candidates["lat"].append(part)
    for desc in bnf_record.get("description_candidates_lat", []):
        candidates["lat"].append(desc)
    return candidates
```

**Status**: ✓ Functionally equivalent

### Issue 5: Title Candidate Extraction
**Current** (candidate_builders.py):
```python
def build_book_candidates_by_script(book):
    candidates = {"lat": [], "ara": []}
    
    if book.get("title_lat"):
        title_list = book["title_lat"] if isinstance(book["title_lat"], list) else [book["title_lat"]]
        for title in title_list:
            for part in title.split(". "):
                candidates["lat"].append(part)
    return candidates
```

**Benchmark** (test_surface_matching.py line 132-160):
```python
def build_openiti_title_candidates(book):
    candidates = {"lat": [], "ara": []}
    
    for title in book.get("title_lat", []):
        for part in title.split(". "):
            candidates["lat"].append(part)
    return candidates
```

**Issue**: Pipeline handles list/string type checking. Benchmark assumes list.
- **Verification**: Current data (data/openiti_corpus_2025_1_9.json) has titles as lists (from e32c745 refactoring)
- **Status**: Pipeline is more robust; functionally equivalent

### Issue 6: Stage 3 Combination Logic
**Current** (combined_matcher.py):
- Should filter Stage 2 books to only those whose author matches Stage 1

**Benchmark** (analyze_threshold_combinations.py):
```python
for book_uri in matched_books:
    book = openiti_books.get(book_uri)
    author_uri = book.get("author_uri")
    if author_uri in matched_authors:
        combined_uris.add(book_uri)
```

**Verification**: Code review needed to confirm CombinedMatcher does this correctly

---

## Part 3: Validation Script Proposal

### Objective
Create a script that:
1. Runs the benchmark test on the test set
2. Runs the pipeline on the same test set
3. Compares Stage 1, Stage 2, and Stage 3 results
4. Reports discrepancies with details

### Script Structure: compare_pipeline_to_benchmark.py

```python
"""
Validation script: Compare pipeline output to benchmark test.

Ensures the production pipeline produces identical results to the canonical
fuzzy matching benchmark test.
"""

import json
from pathlib import Path
from difflib import unified_diff

# Import benchmark
sys.path.insert(0, str(Path(__file__).parent / "final_fuzzy_benchmarking"))
from test_surface_matching import search_authors, search_titles
from analyze_threshold_combinations import combine_results_at_thresholds

# Import pipeline
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.config import AUTHOR_THRESHOLD, TITLE_THRESHOLD, BNF_FULL_PATH, OPENITI_CORPUS_PATH

def validate_pipeline():
    """Run validation on test set."""
    
    # Load data
    with open(OPENITI_CORPUS_PATH) as f:
        openiti_data = json.load(f)
    with open(BNF_FULL_PATH) as f:
        bnf_data = json.load(f)
    with open("data_samplers/correspondence.json") as f:
        correspondence = json.load(f)
    
    test_pairs = {item[list(item.keys())[0]]: list(item.keys())[0] 
                  for item in correspondence}
    
    # Benchmark results
    benchmark_stage1 = {}
    benchmark_stage2 = {}
    benchmark_stage3 = {}
    
    # Pipeline results
    pipeline = MatchingPipeline(bnf_data["records"], openiti_data, 
                                run_id="validation", verbose=False)
    pipeline.register_stage(AuthorMatcher())
    pipeline.register_stage(TitleMatcher())
    pipeline.register_stage(CombinedMatcher())
    pipeline.run()
    
    # Compare results for each record and threshold combination
    report = []
    
    for bnf_id in test_pairs.keys():
        for threshold in [0.80, 0.85]:  # Focus on optimal range
            # Benchmark
            bench_authors, _ = search_authors(bnf_id, threshold)
            bench_books, _ = search_titles(bnf_id, threshold)
            bench_combined, _, _, _ = combine_results_at_thresholds(
                author_results, title_results, openiti_data["books"],
                title_threshold=threshold, author_threshold=threshold
            )
            
            # Pipeline
            pipe_authors = pipeline.get_stage1_result(bnf_id) or []
            pipe_books = pipeline.get_stage2_result(bnf_id) or []
            pipe_combined = pipeline.get_stage3_result(bnf_id) or []
            
            # Compare
            if set(bench_authors) != set(pipe_authors):
                report.append({
                    "bnf_id": bnf_id,
                    "threshold": threshold,
                    "stage": 1,
                    "mismatch": "author",
                    "benchmark": bench_authors,
                    "pipeline": pipe_authors,
                })
            
            # Similar for stage 2 and 3...
    
    return report

if __name__ == "__main__":
    mismatches = validate_pipeline()
    if mismatches:
        print(f"VALIDATION FAILED: {len(mismatches)} mismatches")
        for m in mismatches[:10]:
            print(f"  {m['bnf_id']} Stage {m['stage']}: benchmark {len(m['benchmark'])} vs pipeline {len(m['pipeline'])}")
    else:
        print("VALIDATION PASSED: Pipeline matches benchmark results")
```

### What This Script Validates

1. **Stage 1 Output**: Author URIs match for each BNF record at each threshold
2. **Stage 2 Output**: Book URIs match for each BNF record at each threshold
3. **Stage 3 Output**: Combined results match (same books selected)
4. **Threshold Behavior**: Both systems apply thresholds identically
5. **Candidate Extraction**: Both systems build the same candidate sets

### How to Use

```bash
# Run validation
python matching/compare_pipeline_to_benchmark.py

# Output format:
# VALIDATION PASSED: Pipeline matches benchmark results
# OR
# VALIDATION FAILED: 3 mismatches
#   OAI_11000434 Stage 1: benchmark 227 vs pipeline 230
#   OAI_10030933 Stage 2: benchmark 8 vs pipeline 9
#   OAI_10884186 Stage 3: benchmark 1 vs pipeline 0
```

---

## Part 4: Changes Needed (if any)

Before implementing the validation script, verify:

1. **BNF Index Configuration**
   - [ ] Language splitter is disabled (should be)
   - [ ] Normalization matches benchmark (normalize_transliteration)

2. **CombinedMatcher Implementation**
   - [ ] Correctly filters Stage 2 results by Stage 1 author URIs
   - [ ] Returns intersection (books where author matches)

3. **Threshold Application**
   - [ ] All stages apply threshold as `score >= threshold * 100`
   - [ ] Thresholds are independently configurable (title ≠ author)

4. **Candidate Extraction**
   - [ ] All sources included (creators, contributors, titles, descriptions)
   - [ ] Script separation handles lists and strings

5. **Data Compatibility**
   - [ ] Current data has titles as lists (from e32c745)
   - [ ] Pipeline handles both list and string formats

---

## Next Steps

1. **Immediate**: Create and run `compare_pipeline_to_benchmark.py`
2. **If failures**: Investigate and fix discrepancies in pipeline stages
3. **Ongoing**: Run validation on each release to prevent regressions
4. **Documentation**: Update README with validation instructions
