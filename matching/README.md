# BNF–OpenITI Manuscript Matching: Stage 6

This document describes the design decisions and parameter choices for the fuzzy-matching pipeline (Stage 6) that matches BNF manuscript records to OpenITI books.

For end-to-end pipeline documentation including CLI commands, output format, and validation, see [PIPELINE.md](../PIPELINE.md#stage-6--matching-pipeline).

---

## Fuzzy Matching Strategy

### Token Set Ratio

All fuzzy matching uses `fuzzywuzzy.fuzz.token_set_ratio()`, which:
- Handles partial name matches (a short query fully contained in a longer string scores 100)
- Is insensitive to token order
- Works well against long concatenated candidate strings

The tradeoff is that `token_set_ratio` scores any short query 100 against a longer string that contains it — "Muhammad" scores 100 against any author name. IDF weighting (see below) is the primary mechanism for suppressing these common-token false positives.

### Variant Concatenation

All OpenITI name/title variants within a script (Latin or Arabic) are concatenated into a single string before scoring. This means the full author/title profile is matched at once, not individual fragments:

```
BNF author:  "Ahmad ibn Muhammad al-Quduri"
OpenITI variants (Latin): ["Ahmad", "Muhammad", "Quduri", "Qudu_ri"]
→ Concatenated: "ahmad muhammad quduri qudu ri"
→ Fuzzy score against full profile; IDF boost applied based on matched rare tokens
```

---

## Two-Tier IDF Design

The core precision mechanism. IDF (Inverse Document Frequency) measures token rarity: tokens appearing in few author names / book titles have high IDF and are strongly discriminative.

### The Problem Being Solved

`token_set_ratio("muhammad", "ahmad muhammad al-quduri") = 100` — common tokens cause false positives. A raw threshold cannot distinguish a genuine match on a distinctive name from one that only shares "Muhammad".

### The Solution: Two Tiers

**Tier 1 — Recall (entry threshold):**
Uses `combined_idf` = sum of IDF weights for **all** matched tokens. An author passes Stage 1 if `raw_score × combined_idf_boost ≥ AUTHOR_THRESHOLD`. This keeps common-name authors (e.g. "Muhammad ibn Ahmad") in the pool so they can be disambiguated by title at Stage 2.

**Tier 2 — Precision (stored score):**
The score forwarded to Stage 3 uses `rare_idf` = sum of IDF for tokens with `IDF ≥ TOKEN_RARITY_THRESHOLD` only. Authors who matched only on common tokens store `rare_idf = 0 → boost = 1.0`. Their score remains at the raw fuzzy level and fails `COMBINED_FLOOR` at Stage 3.

**Boost formula** (same structure for both tiers):

```
boost = 1 + min(idf_sum / SCALE, MAX_BOOST - 1)
```

| Parameter | Author | Title |
|-----------|--------|-------|
| `IDF_BOOST_SCALE` | 15.0 | 20.0 |
| `MAX_BOOST` | 1.3 | 1.4 |

Title scale is wider (20 vs 15) because title-domain IDF values are higher, preventing a single token from immediately hitting the ceiling.

### IDF Reference Values

**Author name domain** (IDF computed from OpenITI author names):

| Token | IDF | Status |
|-------|-----|--------|
| ibn | ≈1.06 | common — no boost |
| al | ≈1.25 | common — no boost |
| muhammad | ≈1.69 | common — no boost |
| abd | ≈3.92 | borderline — excluded at threshold 3.5 |
| ali | ≈4.01 | borderline — excluded at threshold 3.5 |
| khatib | ≈5.56 | rare — boost applies |
| hajar | ≈6.59 | rare — boost applies |
| waqidi | ≈7.51 | rare — boost applies |

**Title domain** (IDF computed from OpenITI book titles):

| Token | IDF | Status |
|-------|-----|--------|
| kitab | ≈3.70 | below threshold — no boost |
| sharh | ≈3.68 | below threshold — no boost |
| muhammad | ≈6.12 | rare in titles — boost applies |
| futuh | ≈7.37 | rare — boost applies |
| sham | ≈6.86 | rare — boost applies |

Note: "waqidi" does not appear in OpenITI book titles (IDF_title = 0), so author-domain rarity does not carry over to title matching.

---

## Creator Field Reweighting (Stage 1, Phase 2)

After initial author scoring, a second pass re-blends scores using the BNF `creator_lat`/`creator_ara` fields as an additional signal.

**Trigger:** At least one matched author must share >1 rare token with the BNF creator field. Single-token triggers are too noisy; genuine attribution matches typically share a distinctive name fragment *and* a toponym or epithet.

**Per-variant scoring:** Each OpenITI name variant is scored independently:
```
variant_score = |openiti_rare_tokens ∩ bnf_creator_tokens| / |openiti_rare_tokens|
```
Best-variant wins. This prevents denominator inflation from accumulating tokens across unrelated nisba/laqab variants.

**Blended score:**
```
score = raw_score × AUTHOR_FULL_STRING_WEIGHT (0.6)
      + creator_overlap × AUTHOR_CREATOR_FIELD_WEIGHT (0.4)
```
Applied before the rare-IDF boost, so the rare-token signal still governs the final Stage 3 score.

`AUTHOR_CREATOR_IDF_THRESHOLD = 4.5` (higher than `TOKEN_RARITY_THRESHOLD = 3.5`) excludes tokens like "abd" and "ali" which add noise to creator matching despite being above the general rarity threshold.

---

## Combined Scoring (Stage 3)

Forms (author, book) pairs and applies four sequential gates:

1. Book's `author_uri` must appear in Stage 1 results
2. Both `author_score` and `title_score` must be `≥ COMBINED_FLOOR (0.80)` (raw, pre-normalisation)
3. `title_score ≥ TITLE_FLOOR (0.90)`
4. Normalised weighted score `≥ COMBINED_THRESHOLD (0.94)`

**Normalised weighted score:**

Both scores are normalised by their per-record maximum first, then combined:
```
combined = 0.3 × (author_score / max_author) + 0.7 × (title_score / max_title)
```

Title is weighted 0.7 because it is the stronger discriminator once the author stage has already narrowed the candidate pool. Normalisation ensures IDF boost magnitudes on different records do not distort the comparison.

---

## Validation

```bash
python matching/scripts/validate_correspondences_only.py
```

Evaluates against `data_samplers/correspondence.json` (16 known BNF–OpenITI pairs including composite manuscripts). Outputs per-record status and aggregate P/R/F1. Correctly handles records with multiple expected matches (majāmiʿ).

**Current results:** 100% precision; recall limited to records with usable title evidence.

---

## File Structure

```
matching/
├── config.py                  All tunable parameters with inline documentation
├── pipeline.py                Orchestrator: registers and runs stages
├── author_matcher.py          Stage 1: two-tier IDF author matching
├── title_matcher.py           Stage 2: continuous IDF title matching
├── combined_matcher.py        Stage 3: normalised weighted combined scoring
├── classifier.py              Stage 4: confidence tier assignment
├── normalize.py               Normalization (diacritic stripping, CamelCase split)
├── normalize_diacritics.py    Table-driven diacritic conversion (generated by Stage 3)
├── candidate_builders.py      Extract author/book name candidates by script
├── fuzzy_scorer.py            Caching wrapper for fuzzywuzzy
├── bnf_index.py               Global BNF candidate deduplication index
├── openiti_index.py           OpenITI book/author lookup index
└── scripts/
    ├── validate_correspondences_only.py   Primary validation script
    ├── validate_author_matching.py        Stage 1 standalone validation
    ├── export_author_scores.py            Book-centric CSV: per-stage scores + IDF flags
    ├── export_combined_scores.py          Stage 3 scores to CSV
    ├── debug_author_scores.py             Per-record Stage 1 inspection
    └── debug_combined_scores.py           Per-record Stage 3 inspection
```
