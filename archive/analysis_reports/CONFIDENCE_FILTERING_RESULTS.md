# Confidence-Dependent Filtering: Analysis & Recommendations

## Test Results

Confidence-dependent filtering was implemented and tested on the 10-record test set.

**Result: No improvement in precision**
- Recall: 90.0% (unchanged)
- Precision: 90.0% (unchanged)  
- False Positives: 1 (unchanged - still Ibn Hanbal)

## Why It Didn't Work

The Ibn Hanbal false positive **cannot be eliminated** by confidence-dependent filtering because:

1. **Author match is perfect (1.0)** - One of the BNF author candidates normalizes identically to Ibn Hanbal's name
2. **Title match is strong (0.92)** - The book titles also match well
3. **Confidence filtering only targets marginal matches** - The filter requires high title scores for authors scoring 0.80-0.85, but Ibn Hanbal scores 1.0

```
Confidence Filtering Logic:
├─ Author score >= 0.90: Accept ANY title match ← Ibn Hanbal falls here
├─ Author score 0.85-0.89: Require title >= 0.90
└─ Author score 0.80-0.84: Require title >= 0.95
```

## Root Cause Analysis

The 1.0 author score indicates **generic name fragment matching**:
- BNF record OAI_11001068 likely contains author candidates like "Ahmad b."
- After normalization, this becomes something very short (e.g., "Ahmad")
- Ibn Hanbal's structured name field also contains "Ahmad" (common Islamic name)
- Perfect fuzzy score: 100% match on identical normalized strings

## Alternative Solutions

Since confidence-dependent filtering doesn't help, here are better approaches:

### 1. **Minimum Specificity Filter** (Recommended)
Only match author candidates above a minimum length (e.g., >= 8 characters after normalization).
- **Pros**: Simple, removes matching on single names like "Ahmad"
- **Cons**: May miss some legitimate short-name matches for less common authors
- **Implementation**: Add length check in AuthorMatcher before scoring

### 2. **Common Name Exclusion**
Maintain a list of very common Islamic name parts (Ahmad, Muhammad, Ali, Hassan, etc.) and don't match independently.
- **Pros**: Targeted, specific to the domain
- **Cons**: Requires manual curation of common names; may be incomplete

### 3. **Require Higher Min Author Score**
Require author matches >= 0.85 or 0.90 (not 0.80) for combined matching.
- **Pros**: Eliminates marginal author matches; simpler than filtering
- **Cons**: Might miss legitimate matches; affects recall

### 4. **Context-Based Filtering**
Only combine author+title if:
- Author match is strong (>= 0.90), OR
- Both author AND title come from the same BNF candidate (not independent matching)
- **Pros**: More sophisticated, preserves specificity
- **Cons**: Complex to implement; requires tracking candidate sources

### 5. **Sequence Position Analysis**
If BNF candidate is very short and appears in the middle of a longer sequence (not a complete name), downweight or skip it.
- **Pros**: Captures the "Ahmad b." as a fragment issue
- **Cons**: Requires parsing BNF structure during candidate extraction

## Recommendation for Production

**For the 7,825 full BNF corpus:**

1. **Run as-is first**: 90% precision on test set is acceptable
2. **Monitor false positives** on sample of full results
3. **Implement Approach #1** if false positive rate becomes problematic:
   - Add `MIN_AUTHOR_CANDIDATE_LENGTH = 8` to config
   - Apply length filter in AuthorMatcher
   - Re-validate on test set

**Confidence-dependent filtering is ready as a configurable option** but will not solve the Ibn Hanbal problem. It should be **DISABLED by default** (already set in `matching/config.py`).

## Code Status

- ✅ Score tracking implemented through pipeline stages
- ✅ Confidence-dependent filtering implemented in CombinedMatcher
- ✅ Configuration option added (`USE_CONFIDENCE_FILTERING`)
- ✅ Validation script created and tested
- ⚠️ **No effect on test set** (as expected based on analysis above)

## Next Steps

If false positives become an issue on the full corpus:
1. Profile the most common false positive patterns
2. Implement Approach #1 (minimum length filter)
3. Re-validate on test set
4. Run full corpus again
