"""
Matching pipeline configuration.

Loads from config.yml (via utils/config.py) and provides matching-specific
parameters. All tunable thresholds and parallelization settings are centralised
here for easy adjustment and reproducibility.

Design philosophy
-----------------
The pipeline has three scored stages (author → title → combined). Each stage
uses a fuzzy base score boosted by rare-token IDF weighting. The key design
principle is a two-tier IDF approach:

  Tier 1 (entry / recall): full combined_idf boost — ensures authors/titles
    that share ANY token with the candidate are considered, even if the match
    is weak. This keeps recall high at stage 1.

  Tier 2 (scoring / precision): rare-token-only IDF boost — only tokens above
    TOKEN_RARITY_THRESHOLD count toward the stored score. Matches that only
    share common tokens (e.g. "Muhammad" alone) receive no boost, leaving their
    score below COMBINED_FLOOR and failing at stage 3.

This means parameter tuning should mainly be needed once per corpus type (OpenITI
Arabic manuscript names). Switching to a different target corpus may require
revisiting TOKEN_RARITY_THRESHOLD and the floor/threshold values.
"""

from pathlib import Path
from utils.config import load_config

_PIPELINE_CONFIG = load_config()

# ============================================================================
# DATA PATHS (derived from config.yml)
# ============================================================================

DATA_DIR = Path("data")


def _get_openiti_corpus_path() -> Path:
    corpus_files = list(DATA_DIR.glob("openiti_corpus_*.json"))
    if corpus_files:
        return corpus_files[0]
    raise FileNotFoundError("No openiti_corpus_*.json found in data/")


OPENITI_CORPUS_PATH = _get_openiti_corpus_path()
BNF_SAMPLE_PATH = Path("matching/sampling/bnf_sample_500.json")
BNF_FULL_PATH = Path(_PIPELINE_CONFIG.pipeline_out_dir) / "bnf_parsed.json"

# ============================================================================
# OUTPUT CONFIGURATION
# ============================================================================

MATCHES_DIR = Path(_PIPELINE_CONFIG.pipeline_out_dir) / "matches"


def get_run_dir(run_id: str) -> Path:
    run_dir = MATCHES_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def get_output_files(run_dir: Path) -> dict:
    return {
        'high_confidence': run_dir / "matches_high_confidence.json",
        'author_only': run_dir / "matches_author_only.json",
        'title_only': run_dir / "matches_title_only.json",
        'unmatched': run_dir / "matches_unmatched.json",
        'summary': run_dir / "matching_summary.txt",
        'manifest': run_dir / "manifest.json",
    }

# ============================================================================
# NORMALIZATION
# ============================================================================

# If True, applies the parametrised diacritic conversion table
# (matching/normalize_diacritics.py → bnf_diacritic_conversions.csv).
# Handles ayn representation, diacritic mappings, hyphen normalisation, etc.
USE_DIACRITIC_CONVERSION_TABLE = True

# ============================================================================
# MATCHING BEHAVIOUR
# ============================================================================

# Strip author name tokens from OpenITI book fields at index build time.
# Prevents author name fragments from inflating title match scores.
CULL_AUTHOR_DATA_FROM_BOOKS = True

# ============================================================================
# IDF RARITY THRESHOLD  (shared across all stages)
# ============================================================================

# Tokens with IDF >= this value are considered "rare" and eligible for the
# precision-tier IDF boost. Reference values (author name domain):
#   ibn  ≈ 1.06   al   ≈ 1.25   muhammad ≈ 1.69   (very common — no boost)
#   abd  ≈ 3.92   ali  ≈ 4.01   (borderline — excluded at current threshold)
#   khatib ≈ 5.56   hajar ≈ 6.59   waqidi ≈ 7.51   (rare — boost applies)
# Raising this makes the pipeline more conservative (fewer boosts);
# lowering it admits noisier tokens into the precision tier.
TOKEN_RARITY_THRESHOLD = 3.5

# ============================================================================
# STAGE 1: AUTHOR MATCHING
# ============================================================================

# Entry threshold — an author enters stage 1 if:
#   raw_score × full_combined_idf_boost  >=  AUTHOR_THRESHOLD
# Intentionally permissive so common-name authors reach title disambiguation.
# Precision is enforced downstream by COMBINED_FLOOR in stage 3.
AUTHOR_THRESHOLD = 0.80

USE_AUTHOR_IDF_WEIGHTING = True

# Two-tier IDF boost for author matching:
#
#   Recall tier (threshold check):
#     Uses combined_idf = sum of ALL matched token IDFs.
#     Allows authors matching on any token to pass the entry threshold.
#
#   Precision tier (stored score passed to stage 3):
#     Uses rare_idf = sum of IDFs for tokens >= TOKEN_RARITY_THRESHOLD only.
#     Authors matching only on common tokens (e.g. "Muhammad" IDF=1.7) store
#     a rare_idf of 0 → boost=1.0 → their score stays below COMBINED_FLOOR.
#
# Boost formula: boost = 1 + min(idf_sum / SCALE, MAX_BOOST - 1)
#
# AUTHOR_IDF_BOOST_SCALE — normalises the IDF sum. Higher = gentler curve.
#   At scale=15: a single rare token (IDF≈7.5) gives boost ≈ 1.3 (near cap).
#   Two rare tokens (IDF≈15) hit the cap exactly.
# AUTHOR_MAX_BOOST — hard ceiling on the score multiplier.
AUTHOR_IDF_BOOST_SCALE = 15.0
AUTHOR_MAX_BOOST = 1.3

# Maximum author candidates forwarded from stage 1.
# Composite manuscripts can legitimately have many authors.
MAX_AUTHOR_CANDIDATES = 50

# Creator field reweighting — blends the full fuzzy score with a rare-token
# overlap score against BNF creator_lat/creator_ara fields. Rewards candidates
# whose rare name tokens match the BNF attribution (e.g. "khatib", "nasiriyya").
#
# Trigger: at least one matched author must have > 1 matching rare token.
# Single-token triggers are too noisy; genuine matches typically share a
# distinctive name fragment AND a toponym or epithet.
#
# Per-variant scoring (best variant wins — avoids denominator inflation
# from accumulating unrelated nisba/laqab tokens across all variants):
#   score = |openiti_rare_tokens ∩ bnf_creator_tokens| / |openiti_rare_tokens|
#
# Final score formula when triggered:
#   base  = raw_score × AUTHOR_FULL_STRING_WEIGHT + creator_score × AUTHOR_CREATOR_FIELD_WEIGHT
#   final = base × rare_idf_boost
USE_AUTHOR_CREATOR_FIELD_MATCHING = True

# IDF threshold for creator token filtering. Higher than TOKEN_RARITY_THRESHOLD
# to exclude moderately common name parts that add noise.
# Tokens excluded: abd=3.92, ali=4.01.
# Tokens admitted: khatib=5.56, hajar=6.59, waqidi=7.51, asqalani=7.50.
AUTHOR_CREATOR_IDF_THRESHOLD = 4.5

# Weights must sum to 1.0 to keep scores in [0, 1] before boosting.
AUTHOR_FULL_STRING_WEIGHT = 0.6    # Weight for full fuzzy string score
AUTHOR_CREATOR_FIELD_WEIGHT = 0.4  # Weight for creator field rare-token overlap

# ============================================================================
# STAGE 2: TITLE MATCHING
# ============================================================================

# Entry threshold — a book enters stage 2 if its boosted title score meets this.
TITLE_THRESHOLD = 0.85

USE_TITLE_IDF_WEIGHTING = True

# Continuous rare-token IDF boost for title matching. Same precision-tier design
# as author matching: only tokens >= TOKEN_RARITY_THRESHOLD contribute to the boost.
# (Title IDF is computed from the OpenITI book corpus, not the author corpus.)
#
# Boost formula: boost = 1 + min(rare_idf_sum / TITLE_IDF_BOOST_SCALE, TITLE_MAX_BOOST - 1)
#
# Example title-domain IDF values (for reference):
#   kitab ≈ 3.70   sharh ≈ 3.68   (below threshold — no boost contribution)
#   muhammad ≈ 6.12   sham ≈ 6.86   futuh ≈ 7.37   rida ≈ 7.78   (rare — boost applies)
#
# TITLE_IDF_BOOST_SCALE — wider than author (20 vs 15) because title tokens have
#   higher base IDF values. This prevents a single rare title token from
#   immediately hitting the ceiling:
#     one token  (rare_idf ≈ 6–7):  boost ≈ 1.31–1.35×
#     two tokens (rare_idf ≈ 14):   boost ≈ 1.4× (near cap)
# TITLE_MAX_BOOST — slightly higher ceiling than author (1.4 vs 1.3) to reward
#   multi-token title matches more strongly than single-token ones.
TITLE_IDF_BOOST_SCALE = 20.0
TITLE_MAX_BOOST = 1.4

# ============================================================================
# STAGE 3: COMBINED MATCHING
# ============================================================================

# All three conditions must pass for a match to be accepted:
#
# COMBINED_FLOOR — both author AND title scores must be >= this (raw, pre-normalisation).
#   Authors that only matched on common tokens store a rare_idf-boosted score
#   that remains below this floor, filtering them even if they passed the
#   permissive AUTHOR_THRESHOLD at stage 1. At 0.80, a raw author score of
#   0.76 with no rare tokens (boost=1.0) correctly fails.
#
# TITLE_FLOOR — title score must be >= this independently (raw, pre-normalisation).
#   Ensures the title is a genuine specific match, not a weak coincidence
#   amplified by a strong author score.
#
# COMBINED_AUTHOR_WEIGHT / COMBINED_TITLE_WEIGHT — weights for the normalised
#   combined score. Title is weighted more heavily because it is the stronger
#   discriminator once stage 1 has already filtered by author. Both raw scores are
#   normalised by their per-record maximum before weighting, so the magnitudes of
#   IDF boosts don't distort the balance. Weights must sum to 1.0.
#
# COMBINED_THRESHOLD — normalised weighted combined score must be >= this.
#   Final gate: rejects pairs where the weighted combination falls below the bar.
#   At 0.94, a title-heavy match needs near-best title AND reasonable author.
COMBINED_FLOOR = 0.80
TITLE_FLOOR = 0.90
COMBINED_AUTHOR_WEIGHT = 0.3
COMBINED_TITLE_WEIGHT = 0.7
COMBINED_THRESHOLD = 0.94

# ============================================================================
# PARALLELIZATION
# ============================================================================

BATCH_SIZE = 200
NUM_WORKERS = 12

# ============================================================================
# LOGGING & PROGRESS
# ============================================================================

VERBOSE = True
USE_PROGRESS_BARS = True

# ============================================================================
# FUZZY MATCHING BACKEND
# ============================================================================

# "fuzzywuzzy" only — PolyFuzz requires a batch architecture incompatible with
# the current parallel loop design.
FUZZY_MATCHER = "fuzzywuzzy"
