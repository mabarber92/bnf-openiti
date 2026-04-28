# BNF–OpenITI Manuscript Matching Pipeline

This document describes the architectural decisions and parameter choices for fuzzy-matching BNF manuscript records to OpenITI books.

## Overview

The pipeline matches BNF bibliographic records to OpenITI manuscripts through a three-stage process:

1. **Stage 1 (Author Matching)**: Fuzzy-match BNF creator names to OpenITI author URIs
2. **Stage 2 (Title Matching)**: Fuzzy-match BNF titles to OpenITI book URIs
3. **Stage 3 (Combined Scoring)**: Form valid author+book pairs and apply combined thresholds

Each stage uses parametrized fuzzy matching (token set ratio from fuzzywuzzy) with optional IDF-weighted boosting for rare tokens.

## Fuzzy Matching Strategy

### Token Set Ratio

We use `fuzzywuzzy.fuzz.token_set_ratio()` for all matching because it:
- Handles partial matches (e.g., "ibn Muhammad Ahmad al-Quduri" matches "Ahmad al-Quduri")
- Ignores token order (e.g., "al-Quduri Ahmad" ≈ "Ahmad al-Quduri")
- Provides consistent scores across different name/title orderings

### Component Concatenation

Rather than matching individual name components separately, we concatenate all variants within a script into a single string:

**Old (per-component) approach:**
```
BNF author: "Ahmad ibn Muhammad al-Quduri"
OpenITI authors: ["Ahmad", "Muhammad", "al-Quduri", "al-Qudu_ri"]
→ Best match: "Muhammad" @ 100% (too permissive—common name)
```

**New (concatenated) approach:**
```
BNF author: "Ahmad ibn Muhammad al-Quduri" 
OpenITI author variants: ["Ahmad", "Muhammad", "al-Quduri", "al-Qudu_ri"]
→ Concatenated: "ahmad muhammad al quduri al qudu ri"
→ Best match: Full concatenation @ 100% (captures full profile)
```

The concatenation approach:
1. Collects all name/title variants (Latin and Arabic separately)
2. Normalizes each variant (diacritics, CamelCase splitting, lowercasing)
3. Joins them with spaces into a single string
4. Performs one fuzzy match against the normalized BNF record
5. Applies IDF boost to the final score

**Why this matters**: Matching against full concatenated profiles prevents common name fragments (e.g., "Muhammad") from generating false positives. The full profile name is rarer and more distinctive, enabling true discrimination.

## Normalization

Normalization is applied consistently at both BNF and OpenITI sides before fuzzy matching:

1. **CamelCase splitting** (OpenITI only): "PolyMorphism" → "poly morphism"
   - Applied only to OpenITI author/book slugs
   - **Never** applied to BNF fields (prevents mangling all-caps author names into individual characters)
   
2. **OpenITI-specific conversions**: "Ayn" → "ayn", "Alif-Lam" → "al"

3. **Diacritic removal**: "Qudūrī" → "qudu ri"

4. **Lowercasing**: Normalize to single case

**Critical fix**: The IDF weighting script must apply the same normalization as the matching pipeline before tokenizing. Without this, rare-token detection fails (e.g., "quduri" not found if only "Qudūrī" was tokenized).

## Token-Level IDF Weighting

### Philosophy

IDF (Inverse Document Frequency) weighting boosts matches containing rare tokens while penalizing nothing:
- **If rare tokens matched**: Multiply score by boost factor (e.g., 85 * 1.15 = 97.75)
- **If only common tokens matched**: Keep score unchanged (no penalty)

This asymmetric approach encourages distinctive matches while allowing legitimate common-name-only matches through.

### Configuration

From `config.py`:
```python
USE_AUTHOR_IDF_WEIGHTING = True
USE_TITLE_IDF_WEIGHTING = True
TOKEN_RARITY_THRESHOLD = 2.5  # IDF ≥ 2.5 triggers boost (≈8% document frequency)
RARE_TOKEN_BOOST_FACTOR = 1.15  # 15% boost
```

### Implementation Details

- **IDF Scope**: Computed from full OpenITI corpus (all 8,000+ authors and 200,000+ books), not test subset
- **Token Definition**: Whitespace-separated terms after normalization
- **Rarity Check**: After fuzzy matching, tokenize both BNF record and matched OpenITI record, check if any matched token has IDF ≥ threshold
- **Score Clamping**: **Removed**. Scores can exceed 100 when rare tokens apply boost (e.g., 87 * 1.15 = 100.05). This is intentional—it creates discrimination between marginally-boosted and significantly-boosted matches.

### Why It Works

On the 11-record test set:
- Without IDF boost (boost=1.05): 8 correct, 2 FP (80% precision)
- With IDF boost (boost=1.15): 9 correct, 1 FP (90% precision)

The single FP (Nasai book) is suppressed by rare-token weighting on the correct author (Maqrizi). By boosting the already-strong Maqrizi match, it edges past the Nasai alternative in combined scoring.

## Threshold Configuration

### Stage 1 & 2 (Author & Title)

```python
AUTHOR_THRESHOLD = 0.80  # Minimum fuzzy match score for author candidates
TITLE_THRESHOLD = 0.85   # Minimum fuzzy match score for title candidates
```

These are soft gates that filter obvious non-matches but allow marginal candidates forward. Real filtering happens at Stage 3.

**Rationale**: 
- 0.80 for authors allows for typos, transliteration variants, and abbreviated names
- 0.85 for titles is stricter (titles are more distinctive) but still permits variant spellings

### Stage 3 (Combined Scoring)

```python
COMBINED_THRESHOLD = 0.92  # Average of author+title scores must meet this
COMBINED_FLOOR = 0.80      # Both author AND title must be ≥ this individually
```

Combined scoring prevents weak-author/strong-title or weak-title/strong-author pairs from matching. Both dimensions must have reasonable confidence.

## Parameter Optimization Results

### Test Set (11 records from correspondence.json)

Tested 25 combinations of:
- AUTHOR_THRESHOLD: [0.75, 0.80, 0.85, 0.90, 0.95]
- RARE_TOKEN_BOOST_FACTOR: [1.05, 1.10, 1.15, 1.20, 1.30]

**Key finding**: Boost factor is the only meaningful variable. Author threshold has zero effect.

**Results by boost factor:**
| Boost | Correct | FP | Precision | Recall |  F1  |
|-------|---------|----|-----------|---------| -----|
| 1.05  |    8    | 2  |   80.0%   |  72.7%  | 76.2%|
| 1.10  |    8    | 2  |   80.0%   |  72.7%  | 76.2%|
| **1.15** | **9**    | **1**  |   **90.0%**   |  **81.8%**  | **85.7%**|
| 1.20  |    9    | 1  |   90.0%   |  81.8%  | 85.7%|
| 1.30  |    9    | 1  |   90.0%   |  81.8%  | 85.7%|

**Selected**: AUTHOR_THRESHOLD=0.80, RARE_TOKEN_BOOST_FACTOR=1.15

- Achieves 90% precision with 81.8% recall
- Suppresses 1 false positive (Nasai) without losing any correct matches
- No benefit from pushing boost beyond 1.15 on this test set
- No benefit from adjusting author threshold (problem is title discrimination, not author matching)

### Why Optimization Plateaued

Both failing records have root causes at the title-matching stage, not author:
- **OAI_10884186 (FP Nasai)**: Correct author (Maqrizi) identified, but no matching Maqrizi titles in BNF. Nasai book matches on title, forming a false positive pair.
- **OAI_11000928 (Missing)**: Correct author identified, but no stage-3 pairs formed because no titles matched.

Increasing author thresholds would not help—the weak authors pass through at even 0.95 because they're strong enough. The only way to fix these is to improve title matching or add title-side confidence checks.

## Implementation Notes

### File Structure

- **`config.py`**: Centralized configuration for all thresholds and parameters
- **`author_matcher.py`**: Stage 1 matching with concatenated components and IDF weighting
- **`title_matcher.py`**: Stage 2 matching (same architecture as author_matcher)
- **`combined_matcher.py`**: Stage 3 pair formation and combined scoring
- **`classifier.py`**: Final classification (matched / unmatched / low confidence)
- **`normalize.py`**: Normalization pipeline with optional CamelCase splitting

### Key Classes

**AuthorMatcher**:
- `_match_author_candidate()`: Concatenates all OpenITI author variants, performs single fuzzy match
- `_build_token_idf_weights()`: Computes IDF for all author tokens in OpenITI corpus
- `_apply_idf_boost()`: Checks for rare tokens and boosts score if found

**TitleMatcher**: Identical to AuthorMatcher but operates on book titles and URIs

### Parallelization

All stages support parallel processing via `use_parallel=True`, but for validation and debugging, set `use_parallel=False` for reproducible sequential execution.

## Future Work

### Title-Stage Improvements

Current approach matches titles character-by-character. Potential improvements:
1. **Semantic similarity**: Embed BNF titles and OpenITI summaries, use cosine distance
2. **Subject-field matching**: Compare Dublin Core subject fields (language, era, topic)
3. **Minimum length filtering**: Reject matches on very short titles (e.g., "History" or "Commentary")

### Remaining FP Cases

The single remaining FP on the 11-record set is likely fixable by:
- Adding a confidence floor on title matches (only combine strong author + strong title)
- Implementing subject-field matching to reject cross-topical pairs

### Broader Validation

These parameters were optimized on 11 ground-truth records. Before deployment:
1. Validate on the 500-record BNF sample for broader coverage
2. Perform manual review of matched and false-positive pairs
3. Adjust thresholds if systematic patterns emerge (e.g., all FPs are translation works)

## References

- **fuzzywuzzy documentation**: https://github.com/seatgeek/fuzzywuzzy
- **Token Set Ratio**: Implemented as set comparison of tokens, ignoring duplicates and order
- **IDF Formula**: `log(total_documents / documents_containing_token)`
