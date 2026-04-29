"""
Matching pipeline configuration.

Loads from config.yml (via utils/config.py) and provides matching-specific
parameters. All tunable thresholds and parallelization settings centralized here
for easy adjustment and reproducibility.
"""

from pathlib import Path
from utils.config import load_config

# Load pipeline config (required; will fail if config.yml missing)
_PIPELINE_CONFIG = load_config()

# ============================================================================
# THRESHOLDS
# ============================================================================

# Stage 1 & 2 thresholds: filter out weak candidates early
# Real filtering happens at Stage 3 (combined threshold)
AUTHOR_THRESHOLD = 0.80  # Stage 1: BNF author → OpenITI author URIs
TITLE_THRESHOLD = 0.85   # Stage 2: BNF titles → OpenITI book URIs

# Stage 3 combined scoring: final filtering on author+title pairs
# Optimized via threshold validation on 11-record validation set
COMBINED_THRESHOLD = 0.93  # Combined (author_score + title_score) / 2 must meet this (balances recall vs FP suppression)
COMBINED_FLOOR = 0.80      # Both author AND title scores must be >= this
TITLE_FLOOR = 0.90         # Title score must be >= this to ensure rare-token boosted matches dominate weak-author cases

# ============================================================================
# DATA PATHS (derived from config.yml)
# ============================================================================

DATA_DIR = Path("data")

# OpenITI corpus path — infer from data/ directory
def _get_openiti_corpus_path() -> Path:
    """Find the OpenITI corpus JSON in data/ (e.g., openiti_corpus_2025_1_9.json)."""
    corpus_files = list(DATA_DIR.glob("openiti_corpus_*.json"))
    if corpus_files:
        return corpus_files[0]
    raise FileNotFoundError("No openiti_corpus_*.json found in data/")

OPENITI_CORPUS_PATH = _get_openiti_corpus_path()

# BNF paths
BNF_SAMPLE_PATH = Path("matching/sampling/bnf_sample_500.json")
BNF_FULL_PATH = Path(_PIPELINE_CONFIG.pipeline_out_dir) / "bnf_parsed.json"

# ============================================================================
# OUTPUT CONFIGURATION (nested under pipeline_out_dir)
# ============================================================================

MATCHES_DIR = Path(_PIPELINE_CONFIG.pipeline_out_dir) / "matches"

def get_run_dir(run_id: str) -> Path:
    """Return the output directory for a specific matching run."""
    run_dir = MATCHES_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir

def get_output_files(run_dir: Path) -> dict:
    """Return the output file paths for a matching run."""
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

# Use parametrized diacritic conversion table (bnf_diacritic_conversions.csv)
# If True: applies conversions (ayn handling, diacritic mappings, etc.)
# If False: simple character removal (legacy behavior)
USE_DIACRITIC_CONVERSION_TABLE = True  # Disabled: new normalizer breaks matching (hyphen handling, uppercase diacritics, ayn representation)

# ============================================================================
# MATCHING BEHAVIOR
# ============================================================================

# Remove author data from OpenITI book fields at index build time
# Prevents author name tokens from boosting title matches
# If True: cull author tokens from book title/description during indexing
# If False: keep book data as-is (original behavior)
CULL_AUTHOR_DATA_FROM_BOOKS = True

# Token-level IDF weighting: reward matches with rare tokens, allow common-only matches through
# If any matched token has IDF >= TOKEN_RARITY_THRESHOLD, boost the fuzzy score
# If no rare tokens matched, keep fuzzy score as-is (no penalty)
USE_AUTHOR_IDF_WEIGHTING = True
USE_TITLE_IDF_WEIGHTING = True

# IDF rarity threshold: tokens with IDF >= this are considered "rare" and trigger boost
# Measured against OpenITI domain (authors/books only, not BNF)
# Common tokens (Ahmad, Muhammad, ibn, al): IDF 0.99 (appearing in nearly all authors)
# Rare tokens (unique surnames, book titles): IDF 7.3–8.0 (appearing in <1% of corpus)
# 2.5 = catch semi-uncommon tokens that help disambiguate (surnames, descriptive titles like "Khatib")
TOKEN_RARITY_THRESHOLD = 3.5

# Separate boost factors for author and title matching
# Optimized via parameter sweep (validation: 56.2% precision, 81.8% recall, 66.7% F1)
# Authors: conservative boost (many cluster at 100% on common names)
# Titles: stronger boost (discriminative, rare tokens matter more)
AUTHOR_RARE_TOKEN_BOOST_FACTOR = 1.10  # (unchanged from previous)
TITLE_RARE_TOKEN_BOOST_FACTOR = 1.20   # (up from 1.15)

# Legacy parameter (deprecated, kept for compatibility)
RARE_TOKEN_BOOST_FACTOR = 1.15

# Fuzzy matching backend: "fuzzywuzzy" or "polyfuzz"
# Note: PolyFuzz requires batch architecture; current parallel loop uses fuzzywuzzy
FUZZY_MATCHER = "fuzzywuzzy"

# Maximum author candidates to pass forward from fuzzy matching
# (Composite manuscripts can legitimately have many authors)
MAX_AUTHOR_CANDIDATES = 50

# # Confidence-dependent filtering in Stage 3 (Combined Matching)
# # If True, marginal author matches require higher title scores to be combined
# # Helps reduce false positives from generic name fragments matching multiple authors
# USE_CONFIDENCE_FILTERING = False  # Set to True to enable

# ============================================================================
# PARALLELIZATION
# ============================================================================

BATCH_SIZE = 100  # Process BNF records in batches of N
NUM_WORKERS = 10   # Number of parallel processes

# ============================================================================
# LOGGING & PROGRESS
# ============================================================================

VERBOSE = True          # Print detailed progress
USE_PROGRESS_BARS = True  # Use tqdm for long operations
