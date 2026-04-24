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
# THRESHOLDS (from optimal parameter analysis: 0.85/0.80)
# ============================================================================

AUTHOR_THRESHOLD = 0.80  # Stage 1: BNF author → OpenITI author URIs
TITLE_THRESHOLD = 0.85   # Stage 2: BNF titles → OpenITI book URIs

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
USE_DIACRITIC_CONVERSION_TABLE = False  # Disabled: new normalizer breaks matching (hyphen handling, uppercase diacritics, ayn representation)

# ============================================================================
# MATCHING BEHAVIOR
# ============================================================================

# Fuzzy matching backend: "fuzzywuzzy" or "polyfuzz"
# Note: PolyFuzz requires batch architecture; current parallel loop uses fuzzywuzzy
FUZZY_MATCHER = "fuzzywuzzy"

# Maximum author candidates to pass forward from fuzzy matching
# (Composite manuscripts can legitimately have many authors)
MAX_AUTHOR_CANDIDATES = 50

# Confidence-dependent filtering in Stage 3 (Combined Matching)
# If True, marginal author matches require higher title scores to be combined
# Helps reduce false positives from generic name fragments matching multiple authors
USE_CONFIDENCE_FILTERING = False  # Set to True to enable

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
