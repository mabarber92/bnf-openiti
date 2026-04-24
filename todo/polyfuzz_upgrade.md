# PolyFuzz Upgrade Proposal

## Context

Current pipeline uses per-candidate fuzzy matching (fuzzywuzzy token_set_ratio). While this achieves 90% recall on test set, it generates false positives from generic name fragments matching multiple authors (e.g., "Ahmad b. Muhammad" matching Ibn Hanbal incorrectly).

We explored integrating PolyFuzz (TF-IDF weighted token matching) to suppress these false positives via:
1. Token frequency analysis across the entire corpus
2. Rare tokens (e.g., "al-Quduri") weighted heavily, common tokens (e.g., "Ahmad") weighted lightly
3. Score normalization to 0-100 range

**Problem:** PolyFuzz is designed for batch matching, not per-candidate scoring in parallel loops. Current integration attempt failed due to API mismatch.

---

## Proposed Architecture

### Current Pipeline (Per-Candidate)
```
For each BNF candidate:
  For each OpenITI author:
    score = fuzz.token_set_ratio(norm_bnf, norm_author)  ← In parallel, per-candidate
  Keep scores > threshold
```

**Issue:** ProcessPoolExecutor + per-candidate scoring = can't build TF-IDF index efficiently.

### Proposed PolyFuzz Pipeline (Batch)
```
Stage 0: Build indices
  - Extract all author candidates from OpenITI
  - Build PolyFuzz TF-IDF model from corpus (once, at pipeline init)

Stage 1: Batch match
  - Collect all unique BNF author candidates
  - Pass batch to PolyFuzz.match() → get all scores at once
  - Map scores back to BNF records
  ← No ProcessPoolExecutor; PolyFuzz handles vectorization internally

Stage 2+: Title/combined matching (unchanged)
```

---

## Benefits

1. **Better disambiguation:** TF-IDF weighting suppresses false positives on common name parts
2. **Simpler code:** No parallel complexity; batch processing is clearer
3. **Better performance:** PolyFuzz uses sparse matrix math; faster than O(n²) per-candidate comparisons
4. **Maintainability:** Delegates fuzzy logic to well-maintained library vs custom token weighting

---

## Implementation Steps

1. **Create `matching/author_matcher_batch.py`** — Parallel-free version using PolyFuzz batch API
   - Init: Build TF-IDF index from all OpenITI author candidates
   - Execute: Call `pf.match(bnf_candidates_list)` once
   - Map results back to BNF records

2. **Create `matching/title_matcher_batch.py`** — Same for titles

3. **Config flag:** `USE_BATCH_MATCHING = True/False` to toggle between implementations
   - True: Use PolyFuzz batch (new, better but requires library)
   - False: Use fuzzywuzzy per-candidate (current, works everywhere)

4. **Test:** Validate on correspondence.json
   - Target: Maintain ≥90% recall, reduce false positives vs current

5. **Future:** If custom token weighting (current plan) doesn't work well enough, revert to this

---

## Trade-offs

### PolyFuzz Approach
| Pro | Con |
|-----|-----|
| TF-IDF weighting built-in | Requires library + batch architecture |
| Faster (sparse matrix) | Less control over weighting formula |
| Cleaner code | Harder to debug scoring logic |

### Custom Token Weighting (Current Plan)
| Pro | Con |
|-----|-----|
| Full control over logic | More code to maintain |
| Works with existing parallel architecture | Have to implement TF-IDF ourselves |
| No new dependencies | Slower (per-candidate scoring) |

---

## Decision

**Current approach:** Implement custom token-level IDF weighting (lower friction, leverages existing architecture).

**Fallback:** If custom approach doesn't suppress false positives adequately, execute PolyFuzz upgrade (higher effort but better long-term).

**Hybrid option:** Use PolyFuzz for edge cases (Stage 4 re-scoring of borderline matches) while keeping batch architecture separate.

---

## Files to Create

- `matching/author_matcher_batch.py` — PolyFuzz-based author matching
- `matching/title_matcher_batch.py` — PolyFuzz-based title matching  
- `matching/batch_pipeline.py` — Orchestrator for batch-first architecture (or extend `pipeline.py`)

---

## Estimated Effort

- **Implementation:** ~200 lines of code
- **Testing:** ~2 hours
- **Risk:** Medium (PolyFuzz API is stable, but batch architecture is different)
