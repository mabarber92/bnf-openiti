# Final Fuzzy Matching Benchmarking Suite

This folder contains the **canonical benchmark tests** for the BNF-OpenITI fuzzy matching system.

## Scripts

### 1. test_surface_matching.py
**Purpose**: Perform fuzzy matching at all tested thresholds (0.70, 0.75, 0.80, 0.85, 0.90)

**Outputs**:
- `matching/matching_results_author.csv` - Stage 1 (author matching) results
- `matching/matching_results_title.csv` - Stage 2 (title matching) results

**Run**: `python test_surface_matching.py`

**Critical Implementation Details**:
- **Stage 1** (`search_authors()`): Iterates through OpenITI **authors**, not books. Returns author URIs.
- **Stage 2** (`search_titles()`): Iterates through OpenITI **books**. Returns book URIs.
- Uses `normalize_transliteration()` for candidate normalization
- Uses `fuzz.token_set_ratio()` for fuzzy matching

### 2. analyze_threshold_combinations.py
**Purpose**: Combine Stage 1 and Stage 2 results to test all 25 threshold combinations

**Outputs**:
- `matching/threshold_combination_analysis.csv` - Summary of all combinations
- Console output with rankings

**Run**: `python analyze_threshold_combinations.py`

**Critical Logic**:
- Stage 3 combination: For each book in Stage 2 results, check if its author is in Stage 1 results
- Both conditions must be true: author matched AND title matched

## Benchmark Results

**Optimal threshold combination: title=0.85, author=0.80**

| Metric | Value |
|--------|-------|
| Recall | 90% |
| Precision | 100% |
| F1-Score | 0.947 |
| False Positives | 0 |

**Test Set**: 10 BNF records from `data_samplers/correspondence.json`

## Key Architectural Points

1. **Why author matching first?** Author candidates are more specific. This narrows scope before title matching, preventing false positive cascades.

2. **Why separate thresholds?** Title field has more false positive potential than author. Title needs stricter threshold (0.85 vs 0.80).

3. **Critical bug to avoid**: Author CSV must contain author URIs (`0685NasirDinBaydawi`), not book URIs (`0685NasirDinBaydawi.AnwarTanzil`). The analyze script depends on this for Stage 3 combination.

4. **One record limitation**: OAI_11000928 has no title data and cannot be matched (architectural limitation).

## For Pipeline Validation

See `matching/PIPELINE_VALIDATION.md` for a comparison script that validates the matching pipeline against this benchmark.

## Historical References

- **Original Analysis**: Commit 2df2547
- **Parser Refactoring**: Commit e32c745 (resolved Daraqutni metadata issues)
- **Full Documentation**: `matching/BENCHMARKING_REPORT.md`
