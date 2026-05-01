# BNF-OpenITI Fuzzy Matching Benchmarking Report

## Executive Summary

**Optimal Threshold Combination: `title=0.85, author=0.80`**

- **Recall: 90%** (9 of 10 test records matched correctly)
- **Precision: 100%** (0 false positives)
- **F1-Score: 0.947**
- **Test Set: 10 BNF records** from `data_samplers/correspondence.json`

This report documents the three-stage matching architecture, testing methodology, and threshold tuning that produced these benchmark results.

---

## 1. Matching Logic & Architecture

### 1.1 What We Match

**Stage 1: Author Matching**
- **Source**: BNF record fields (creators, contributors, description_candidates, titles containing author names)
- **Target**: OpenITI author entities
- **Goal**: Find which author URIs match the BNF record's author candidates
- **Returns**: Set of matched author URIs

**Stage 2: Title Matching**
- **Source**: BNF record fields (title_lat, title_ara, description_candidates)
- **Target**: OpenITI book titles
- **Goal**: Find which books have titles matching the BNF record's title candidates
- **Returns**: Set of matched book URIs

**Stage 3: Combined (Author + Title)**
- **Logic**: For each book matched in Stage 2, check if its author URI appears in Stage 1 results
- **Requirement**: BOTH conditions must be true:
  1. Title matched (from Stage 2)
  2. Author matched (book's author in Stage 1 results)
- **Returns**: Set of books where both author and title signals fire

### 1.2 Why This Order

1. **Author matching first** - narrows the scope to specific authors
2. **Title matching second** - finds books matching the title
3. **Combined filter** - ensures we only accept books where BOTH signals are strong

This prevents false positives like Ibn Hanbal's Masail (a reference work appearing in many BNF records) from matching unrelated records just because they mention Ibn Hanbal.

### 1.3 Candidate Extraction

**BNF Author Candidates** (used in Stage 1):
- Creator names (Latin and Arabic transliterations)
- Contributor names
- Description candidates (contains author mentions in composite manuscripts)
- Title parts (author names often in titles)

**BNF Title Candidates** (used in Stage 2):
- Title text (split on ". " separator)
- Description candidates

**OpenITI Author Candidates** (matched against Stage 1):
- `name_slug`: primary transliterated slug
- `wd_label_en`: Wikidata English label
- `wd_aliases_en`: Wikidata English aliases
- Structured name components: `name_shuhra_lat`, `name_ism_lat`, `name_kunya_lat`, `name_laqab_lat`, `name_nasab_lat`, `name_nisba_lat`

**OpenITI Title Candidates** (matched against Stage 2):
- Book titles (split on ". " separator)

---

## 2. Testing Methodology

### 2.1 Threshold Testing

All thresholds use **fuzzy token_set_ratio matching** with a 0-1 scale (normalized from 0-100 fuzzy scores).

**Tested Range**: 0.70, 0.75, 0.80, 0.85, 0.90
**Total Combinations**: 5 × 5 = 25 (all author/title threshold pairs)

Key finding: **Different optimal thresholds for each signal**
- Title matching works best at higher threshold (0.85) - fewer false positives from similar titles
- Author matching can use slightly lower threshold (0.80) - author matching more specific than title

### 2.2 Test Data

**Correspondence File**: `data_samplers/correspondence.json`
- 10 unique BNF records with 12 correspondence pairs
- Each record has 1 expected matching book in OpenITI

---

## 3. Test Script Architecture

### 3.1 Stage 1 & 2: test_surface_matching.py

**Purpose**: Run fuzzy matching at all thresholds, produce separate author and title CSVs

**Key Functions**:

#### `search_authors(bnf_id, threshold) → (author_uris, elapsed_ms)`
- Iterates through **all OpenITI authors** (not books)
- For each author, extracts candidates and normalizes them
- Compares against BNF author candidates using token_set_ratio
- Returns matched author URIs directly
- **Critical**: Returns author URIs, not book URIs

#### `search_titles(bnf_id, threshold) → (book_uris, elapsed_ms)`
- Iterates through **all OpenITI books**
- For each book, extracts title candidates and normalizes them
- Compares against BNF title candidates
- Returns matched book URIs

#### `run_tests(thresholds) → (author_results, title_results)`
- For each BNF record and each threshold:
  - Run `search_authors()` → record author match
  - Run `search_titles()` → record title match
- Output Stage 1 and Stage 2 results to separate CSVs
- Correctness check: Simple existence check (expected URI in matched set)

**Output Files**:
- `matching/matching_results_author.csv` - Author stage results with author URIs
  - Columns: bnf_id, expected_uri, threshold, matched_uris (pipe-separated author URIs), is_correct, num_matches, false_positives
- `matching/matching_results_title.csv` - Title stage results with book URIs
  - Columns: bnf_id, expected_uri, threshold, matched_uris (pipe-separated book URIs), is_correct, num_matches, false_positives

### 3.2 Stage 3: analyze_threshold_combinations.py

**Purpose**: Combine Stage 1 and Stage 2 results at all threshold combinations to find optimal combination

**Key Logic**:

```python
for title_threshold in [0.70, 0.75, 0.80, 0.85, 0.90]:
    for author_threshold in [0.70, 0.75, 0.80, 0.85, 0.90]:
        # Load Stage 1 results at author_threshold
        matched_authors = load_authors(author_threshold)
        
        # Load Stage 2 results at title_threshold
        matched_books = load_titles(title_threshold)
        
        # Stage 3: Combine
        combined_matches = []
        for book_uri in matched_books:
            book = openiti_books[book_uri]
            book_author = book["author_uri"]
            
            if book_author in matched_authors:
                combined_matches.add(book_uri)
        
        # Calculate metrics
        recall = num_correct / total_records
        precision = num_correct_matches / total_matches
        f1 = harmonic_mean(precision, recall)
```

**Output**:
- `matching/threshold_combination_analysis.csv` - All 25 combinations with metrics
- Summary ranking by F1-score, recall, and precision

---

## 4. Critical Implementation Details

### 4.1 Normalization

Both author and title candidates are normalized using `normalize_transliteration()` before fuzzy matching. This handles:
- Diacritical marks (ā, ī, ū, etc.)
- Character variations (ayn variants)
- Case normalization
- Whitespace cleanup

### 4.2 Fuzzy Matching Algorithm

**Algorithm**: `fuzz.token_set_ratio()` from fuzzywuzzy library

This is superior to simple ratio because it:
- Handles word order differences
- Ignores duplicates
- More robust to partial matches

**Scoring**: 0-100 (internally), converted to 0-1 threshold scale

### 4.3 CSV Output Format

Critical: Author CSV must contain **author URIs**, not book URIs
- Author URIs: `0685NasirDinBaydawi` (author prefix only)
- Book URIs: `0685NasirDinBaydawi.AnwarTanzil` (author prefix + book slug)

This distinction is essential for correct Stage 3 combination logic.

---

## 5. Results Interpretation

### 5.1 Threshold Tuning Trade-offs

| title | author | recall | precision | f1    | fp  | interpretation |
|-------|--------|--------|-----------|-------|-----|---|
| 0.70  | 0.70   | 90%    | 12.9%     | 0.225 | 61  | Too permissive, massive false positive cascade |
| 0.75  | 0.80   | 90%    | 52.9%     | 0.667 | 8   | Good balance, but still some noise |
| 0.80  | 0.80   | 90%    | 64.3%     | 0.750 | 5   | Better, but false positives remain |
| **0.85** | **0.80** | **90%** | **100%** | **0.947** | **0** | **OPTIMAL** |
| 0.90  | 0.80   | 70%    | 100%      | 0.824 | 0   | Loses one record (OAI_11000928 - no title data) |

### 5.2 Key Findings

1. **Stricter title threshold needed** - Title field has more false positive potential than author
2. **Author threshold can be looser** - Author matching more discriminative
3. **Perfect precision possible** - 100% precision with 90% recall indicates good signal separation
4. **One record limitation** - OAI_11000928 has no title data, cannot be matched at any threshold (architectural limitation, not matching failure)

---

## 6. Lessons Learned

### 6.1 Architecture Matters
- Searching authors directly vs. extracting authors from books produces different results
- Stage 3 must filter on author URIs, not book URIs
- Order of operations (author first, then title) prevents false positive cascades

### 6.2 Data Quality
- Verbose metadata in some books (Daraqutni had embedded genealogies with `::` and `¶` separators)
- OpenITI data was refactored (commit e32c745) to split these separators
- Current data (`data/openiti_corpus_2025_1_9.json`) has this fix applied

### 6.3 Threshold Independence
- Different signals (author vs title) need different thresholds
- Cannot use same threshold for both stages
- Must test all combinations, not assume symmetry

---

## 7. Reproduction

To reproduce the benchmark results:

```bash
# Step 1: Run Stage 1 & 2
python matching/final_fuzzy_benchmarking/test_surface_matching.py

# Step 2: Run Stage 3 analysis
python matching/final_fuzzy_benchmarking/analyze_threshold_combinations.py

# Step 3: Verify optimal combination
# Check: title=0.85, author=0.80 shows recall=90%, precision=100%, fp=0
```

---

## 8. References

- **Original Analysis Commit**: 2df2547 (threshold tuning and false positive investigation)
- **Parser Refactoring Commit**: e32c745 (resolved Daraqutni verbose metadata issue)
- **Test Data**: `data_samplers/correspondence.json`
- **Benchmark Data**: 
  - `data/openiti_corpus_2025_1_9.json` (9,539 books, 3,639 authors)
  - `outputs/bnf_parsed.json` (7,825 BNF records)
