# Refactoring Plan

## Tier 1 — High impact, low risk

### 1. Extract shared IDF utilities to `matching/idf_utils.py`

**Problem:** `_build_token_idf_weights()` and `_score_with_token_weighting()` are implemented twice — once in `author_matcher.py` (lines 25–103) and once in `title_matcher.py` (lines 22–99). The implementations are nearly identical; the only differences are variable names and the config constants they reference.

**Solution:** Create `matching/idf_utils.py` with parametrized versions:

```python
def build_token_idf_weights(documents: dict[str, list[str]]) -> dict[str, float]:
    """Compute IDF from a dict of {doc_id: [token_strings]}."""
    ...

def apply_idf_boost(
    norm_query: str,
    norm_target: str,
    idf_weights: dict[str, float],
    fuzzy_score: float,
    rarity_threshold: float,
    boost_scale: float,
    max_boost: float,
    return_tiers: bool = False,
) -> float | tuple[float, float, float]:
    """Apply continuous IDF boost. If return_tiers=True, returns (score, combined_idf, rare_idf)."""
    ...
```

Both matchers import from `idf_utils.py` rather than defining their own copies. This saves ~80 LOC of duplicated logic and means any future tuning to the IDF formula only needs to happen in one place.

**Files affected:** `matching/author_matcher.py`, `matching/title_matcher.py`
**New file:** `matching/idf_utils.py`
**Risk:** Low — purely mechanical extraction; behaviour is identical

---

### 2. Fix `matching/bnf_index.py` method reference

**Problem:** `bnf_index.py` calls `record.matching_candidates()` (lines ~46, 57, 62) — a method that does not exist on `BNFRecord`. This code path is broken.

**Solution:** Replace with calls to `candidate_builders.build_author_candidates_by_script()` and `candidate_builders.build_book_candidates_by_script()`, which are the correct extraction functions.

**Files affected:** `matching/bnf_index.py`
**Risk:** Low — fix a broken reference; verify with a test run

---

### 3. Shared pipeline-loading boilerplate across validate scripts

**Problem:** All validate/debug scripts in `matching/scripts/` duplicate the same ~30 lines of pipeline setup:

```python
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches ...}
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(...))
pipeline.register_stage(TitleMatcher(...))
pipeline.register_stage(CombinedMatcher(...))
pipeline.register_stage(Classifier(...))
pipeline.run()
```

**Solution:** Add a helper function to `matching/scripts/` (e.g. `_helpers.py`):

```python
def build_and_run_pipeline(bnf_records, verbose=False, use_parallel=False):
    pipeline = MatchingPipeline(bnf_records, load_openiti_corpus(...), verbose=verbose)
    pipeline.register_stage(AuthorMatcher(verbose=verbose, use_parallel=use_parallel))
    pipeline.register_stage(TitleMatcher(verbose=verbose, use_parallel=use_parallel))
    pipeline.register_stage(CombinedMatcher(verbose=verbose))
    pipeline.register_stage(Classifier(verbose=verbose))
    pipeline.run()
    return pipeline
```

**Files affected:** All files in `matching/scripts/`
**Risk:** Low — reduces maintenance burden; no logic change

---

## Tier 2 — Medium impact, medium risk

### 4. Clarify normalization strategy

**Problem:** `matching/normalize.py` exposes two parallel paths (heavy NFD decomposition, and table-driven) that are merged via a flag (`USE_DIACRITIC_CONVERSION_TABLE`). The two-path design is functional but hard to reason about: it's not obvious which path is active or why both exist.

**Solution:** Document the design explicitly at the top of `normalize.py`. Add a comment block explaining:
- Path 1 (table-driven, default): diacritic_conversions.csv → heavy NFD. This is the production path.
- Path 2 (NFD only): fallback when no conversion table is available (e.g. fresh install before Stage 3 has been run).

Consider renaming internal functions to `_normalize_with_table` and `_normalize_without_table` to make the branching self-documenting.

**Risk:** Low for documentation; Medium if renaming (check all call sites)

---

### 5. Define an explicit stage interface

**Problem:** All four matching stages implement `execute(pipeline)` by convention, but there is no formal interface. This makes it easy to accidentally register a non-stage object without a clear error.

**Solution:** Add a simple ABC in `matching/pipeline.py`:

```python
from abc import ABC, abstractmethod

class MatchingStage(ABC):
    @abstractmethod
    def execute(self, pipeline) -> None:
        ...
```

All stage classes (`AuthorMatcher`, `TitleMatcher`, `CombinedMatcher`, `Classifier`) inherit from it. `pipeline.register_stage()` checks `isinstance(stage, MatchingStage)`.

**Risk:** Low — additive change; no behaviour change

---

## Tier 3 — Lower priority

### 6. Config dependency injection

**Problem:** All stage classes import `matching.config` directly at the top of their functions. This makes it harder to test stages in isolation with different parameter values.

**Solution (future):** Pass a config object to each stage's `__init__`. This would enable:
```python
pipeline.register_stage(AuthorMatcher(config=my_test_config))
```

**Risk:** Medium — requires changing all stage constructors and callers. Not worth doing until test coverage is higher.

---

### 7. Remove `parameter_optimization/` stale references

`parameter_optimization/sweep_thresholds.py` references `TOKEN_IDF_PENALTY_EXPONENT` which no longer exists. Before the next parameter sweep:
- Update all config references to use current parameter names
- Update the sweep grid to include `COMBINED_AUTHOR_WEIGHT` / `COMBINED_TITLE_WEIGHT`
- Add `COMBINED_THRESHOLD` to the sweep (was fixed at 0.93 in old sweep)

---

## Summary

| ID | Change | LOC saved | Risk | Priority |
|----|--------|-----------|------|---------|
| 1 | Extract IDF utilities | ~80 | Low | High |
| 2 | Fix bnf_index.py | ~5 | Low | High |
| 3 | Shared pipeline loader | ~25/script | Low | Medium |
| 4 | Document normalisation paths | 0 | Low | Medium |
| 5 | Explicit stage interface (ABC) | ~10 | Low | Low |
| 6 | Config dependency injection | — | Medium | Low |
| 7 | Update parameter_optimization/ | ~20 | Low | Before next sweep |
