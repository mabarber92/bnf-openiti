# BNF–OpenITI Matching Pipeline

End-to-end pipeline for matching BNF Gallica manuscript records against
OpenITI corpus entries.  Each stage produces artifact files that are consumed
by the next stage.

The pipeline is split into two tracks:

- **OpenITI preparation (Stages 1–2)** — shared infrastructure, run once
  per corpus version.  Outputs live in `data/` and are committed to the
  repo so any new manuscript library can skip these steps.
- **Manuscript library preparation (Stages 3–5)** — library-specific.
  Run for each new collection (BNF, or any future library).
- **Matching and resolution (Stages 6–7)** — produces the final output.

---

## Setup

1. Copy `config.example.yml` → `config.yml` and fill in your local paths:

```yaml
bnf_data_path:    /path/to/BNF_data   # OAI-PMH XML files
pipeline_out_dir: outputs              # relative or absolute
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Ensure `outputs/` (or your `pipeline_out_dir`) is gitignored — it will
   contain large intermediate files and must not be committed.

---

## Stage overview

```
── OpenITI preparation (one-off per corpus version; outputs committed to repo) ──

Stage 1  parse OpenITI    →  data/openiti_<version>.json   (structural parse)
         ↓
Stage 2  enrich Wikidata  →  data/openiti_<version>.json   (wd_* fields added in-place)

── Manuscript library preparation (per collection) ──────────────────────────────

Stage 3  survey build     →  outputs/bnf_survey/summary.json
                             outputs/bnf_survey/ngrams.json
                             outputs/bnf_survey/boilerplate_review.csv
         ↓
         [MANUAL REVIEW: boilerplate_review.csv]
         ↓
Stage 4  apply-review     →  outputs/bnf_survey/boilerplate.json
         ↓
Stage 5  parse BNF        →  outputs/bnf_parsed.json

── Matching and resolution ───────────────────────────────────────────────────────

Stage 6  match            →  outputs/matches/{run_id}/matches_*.json
         ↓
Stage 7  cluster/resolve  →  outputs/resolutions.json               [not yet implemented]
```

---

## Stage 1 — Parse OpenITI

> **Most users:** check `data/` for an existing `openiti_<corpus_version>.json`.
> If present, both stages are already done — skip to Stage 3.
> The file is committed to the repo; you only need to run Stages 1–2 for a
> new corpus version.

### Build (first-time / new corpus version)

```bash
python utils/parse_openiti.py build --dir /path/to/corpus_2025_1_9
```

Walks the corpus directory recursively, parses every `.yml` file, and writes
`data/openiti_<version>.json`.  Typical corpus (~10,000 books) completes in
under a minute.  **Run Stage 2 immediately after** — do not commit the file
until both stages are complete.

### Update (after a corpus version bump)

```bash
python utils/parse_openiti.py update --dir /path/to/corpus_2025_1_9
```

Re-parses the full corpus and overwrites the output file.  Remember to also
bump `corpus_version` in `openiti.yml` before running, then run Stage 2.

**Output file:** `data/openiti_<corpus_version>.json`  
Shared with Stage 2 — Stage 1 writes the structural skeleton; Stage 2 adds
`wd_*` fields to author records in-place.  Commit the file after both stages.

---

## Stage 2 — Wikidata enrichment (optional)

> **Most users:** check `data/` for an existing
> `openiti_wikidata_<corpus_version>.json`.  If present, skip to Stage 3.
> The file is committed to the repo — you only need to run this if you are
> working with a new corpus version.

**Why it exists:**  
~48 % of OpenITI authors carry a verified Wikidata QID in their YML `EXTID`
field.  Wikidata provides Arabic-script name labels and English
transliteration aliases beyond what the YML contains, improving author-name
matching coverage for that subset.  The Wikidata SPARQL endpoint is fully
open and explicitly supports programmatic access.

### Build (first-time / new corpus version)

```bash
python utils/enrich_wikidata.py build
```

Reads `data/openiti_<corpus_version>.json` (created by Stage 1), batches
~1,700 author QIDs into 5 SPARQL queries, and writes `wd_*` fields directly
into each author record.  Typically completes in under two minutes.  Safe to
resume — already-enriched authors are skipped.

### Update (after a corpus version bump)

```bash
python utils/enrich_wikidata.py update
```

Re-fetches only authors whose QID has changed since the last enrichment.

**Output:** updates `data/openiti_<corpus_version>.json` in-place.

**Author record after Stage 2:**

```json
"0110HasanBasri": {
  "uri": "0110HasanBasri",
  "death_year_ah": 110,
  "name_slug": "HasanBasri",
  "name_shuhra_ar": "al-Ḥasan al-Baṣrī",
  "name_ism_ar": "al-Ḥasan",
  "name_kunya_ar": "Abū Saʿīd",
  "name_nasab_ar": "b. Yasār",
  "name_nisba_ar": "al-Baṣrī",
  "wikidata_id": "Q293500",
  "wd_label_ar":   "الحسن البصري",
  "wd_label_en":   "Hasan al-Basri",
  "wd_aliases_ar": ["حسن البصري", "الحسن البصرى"],
  "wd_aliases_en": ["Hasan of Basra", "al-Ḥasan al-Baṣrī"],
  "wd_death_year": 728,
  "wd_fetched_at": "..."
}
```

**Loading in downstream stages:**

```python
from utils.parse_openiti import load_openiti_corpus

books, authors = load_openiti_corpus("data/openiti_corpus_2025_1_9.json")
author = authors.get("0110HasanBasri")
print(author.wd_label_ar)    # "الحسن البصري"
print(author.wd_aliases_en)  # ["Hasan of Basra", ...]
```

---

## Stage 3 — Survey build

**Command:**
```bash
python utils/survey_bnf.py build
```

**What it does:**  
Scans all `OAI_*.xml` files in `bnf_data_path`, computes field coverage
statistics and n-gram frequencies across configured DC fields, then applies
per-field threshold criteria to generate a list of boilerplate candidates
for review.

**Parameters (set in `config.yml`, override with CLI flags):**

| config key | CLI flag | default | effect |
|---|---|---|---|
| `survey.max_n` | `--max-n` | `4` | largest n-gram size (bigrams–quadgrams) |
| `survey.keep_abbrev_dots` | `--keep-abbrev-dots` | `true` | retain dots on abbreviation tokens (e.g. `cf.`, `ms.`) so abbreviation phrases form distinct n-grams |

**Per-field boilerplate thresholds (set in `config.yml` only):**

N-gram scanning and boilerplate candidate generation is per-field.  Each
field in `boilerplate.fields` is scanned independently and applies its own
mode and thresholds.  Both Latin-script and Arabic-script n-grams are
detected independently for every listed field.

| mode | criteria applied | suitable for |
|---|---|---|
| `full` | `doc_freq_pct >= min` AND `repeats_per_doc <= max` | mixed-content fields (`dc:description`) where boilerplate and content coexist |
| `freq_only` | `doc_freq_pct >= min` only | predominantly-boilerplate fields (`dc:format`, `dc:rights`, `dc:source`) |

```yaml
boilerplate:
  fields:
    description:
      mode: full
      min_doc_freq_pct: 2.0
      max_repeats_per_doc: 1.1
    creator:
      mode: freq_only
      min_doc_freq_pct: 10.0
    subject:
      mode: freq_only
      min_doc_freq_pct: 15.0
    # --- further fields are available; see config.example.yml ---
```

**Sampling (experimentation only — do not use for production runs):**
```bash
python utils/survey_bnf.py build --sample 500 --seed 42
```

**Optional display flags:**
```bash
python utils/survey_bnf.py build --print-summary   # field coverage report
python utils/survey_bnf.py build --print-ngrams     # top n-gram rankings
python utils/survey_bnf.py build --print-ngrams --top-n 30
```

**Outputs** (written to `bnf_survey_dir`, default `outputs/bnf_survey/`):

| file | contents |
|---|---|
| `summary.json` | field coverage statistics (records present, script breakdown, samples) |
| `ngrams.json` | full n-gram vocabulary — term_freq, doc_freq, TF-IDF for every observed n-gram per field; untruncated |
| `boilerplate_review.csv` | candidate boilerplate phrases for manual review |
| `manifest.json` | run record: timestamp, config snapshot, parameters used |

**Re-running:**  
Re-running `build` overwrites all four files and clears the `apply_review`
stage entry from the manifest (since the review CSV has changed).  If you
change only the boilerplate thresholds, `ngrams.json` contains the full
vocabulary — you can regenerate the review CSV without re-scanning by
calling `_suggest_boilerplate()` directly in a script rather than
re-running `build`.

---

## Manual review — `boilerplate_review.csv`

Open `outputs/bnf_survey/boilerplate_review.csv` in a spreadsheet tool.
Rows are sorted by `source_field` then `repeats_per_doc` ascending — true
boilerplate clusters at 1.0 (appears exactly once per record) and
name/content fragments appear further down with higher values.

**Columns:**

| column | description |
|---|---|
| `ngram` | the phrase |
| `source_field` | DC field the phrase was found in (`description`, `creator`, `subject`, etc.) |
| `script` | `latin` or `arabic` |
| `n` | n-gram size (2=bigram, 3=trigram, 4=quadgram) |
| `doc_freq_pct` | % of records containing this phrase |
| `repeats_per_doc` | average occurrences per record — ~1.0 = structural template |
| `keep` | `yes` = filter this phrase; `no` = leave in field text as content |
| `signal_type` | if non-empty, phrase is a structural signal rather than pure noise (see below) |

**`keep` column:**
- `yes` — phrase is boilerplate or a signal; will be filtered from field text
- `no` — phrase is content (e.g. a name fragment that slipped through); leave it in

**`signal_type` column (optional):**

Leave empty for pure boilerplate (digitisation notices, physical description
templates).  Fill in for phrases that mark a structural role or textual
relationship — these are forwarded to relation detection rather than silently
discarded:

| value | meaning |
|---|---|
| `agent:copyist` | phrase marks a copyist name (e.g. "copié par", "katabahu") |
| `agent:commentator` | phrase marks a commentator as person (e.g. "sharḥ by") |
| `agent:owner` | phrase marks a previous owner name |
| `relation:commentary` | phrase marks that *this text* is a commentary on another work |
| `relation:abridgement` | phrase marks abridgement of a source work |
| `relation:continuation` | phrase marks continuation of a source work |
| `date:copy` | phrase marks a colophon copy date (e.g. "copié en", "katabahu fī") |

> **Note on ambiguity:** `relation:commentary` means the manuscript *is* a
> commentary — what follows is the referenced *work*.  `agent:commentator`
> means a *person* wrote a commentary — what follows is a *name*.  These are
> distinct and must not be conflated; downstream matching uses them
> differently.

---

## Stage 4 — Apply review

**Command:**
```bash
python utils/survey_bnf.py apply-review
```

**What it does:**  
Reads the reviewed `boilerplate_review.csv` and writes `boilerplate.json`
with two sections:

```json
{
  "boilerplate": [
    {"ngram": "numérisation effectuée", "field": "description"},
    {"ngram": "effectuée partir",       "field": "description"},
    {"ngram": "auteur du texte",        "field": "creator"},
    ...
  ],
  "signals": [
    {"ngram": "lieu de copie", "field": "description", "signal_type": "agent:copyist"},
    {"ngram": "sharḥ ʿalā",   "field": "description", "signal_type": "relation:commentary"},
    {"ngram": "copié en",     "field": "description", "signal_type": "date:copy"}
  ]
}
```

- `boilerplate` — n-grams stripped from the specified field during parsing
- `signals` — n-grams that trigger relation detection; forwarded to
  `BNFRecord.detected_relations` and annotated in `matching_data()`

**Re-running:**  
Safe to re-run after editing the CSV.  Updates `boilerplate.json` and the
manifest in place.

---

## Stage 5 — Parse BNF records

Parse OAI-PMH XML files into structured JSON records using the curated
boilerplate (from Stage 4).

**Prerequisite:**  
Stage 4 must be completed first — `outputs/bnf_survey/boilerplate.json` is required.

### Build (full collection)

```bash
python utils/parse_bnf.py build
```

Parses all `OAI_*.xml` files in `bnf_data_path`, extracts:
- Titles (Latin and Arabic, split on '. ')
- Creators (dates and role suffixes cleaned)
- Description candidates (boilerplate-filtered; now labeled with relation types)
- Subject, contributor, format, coverage
- Detected relations (e.g. commentary, date:copy)

For each `dc:description` string:
1. **Signal matching** (Pass 1): Finds relation markers ("daté de", "lieu de copie") and marks them as covered spans, tracking the signal type
2. **Boilerplate matching** (Pass 2): Finds remaining boilerplate n-grams (from curated list) and marks them covered
3. **Between-phrase extraction**: Extracts uncovered token runs using character boundaries of adjacent covered phrases, capturing inter-token content (digits, punctuation)
4. **Label assignment**: Each candidate inherits the nearest signal's relation type

**Outputs:**

| file | contents |
|---|---|
| `outputs/bnf_parsed.json` | all parsed BNF records as JSON (no `_matching_data`); reused across matching runs to avoid re-parsing |

Typical collection (7,825 records) parses in under 30 seconds.

### Update (incremental)

```bash
python utils/parse_bnf.py update
```

Re-parses only XML files not yet present in the existing `bnf_parsed.json`.
Safe to run when new records are added to the collection.

### Sample (exploration and validation)

```bash
python utils/parse_bnf.py sample --n 50 --seed 42
```

**Purpose:** Validate parser output during development; inspect description
candidates and relation detection; run regression checks.

**Key difference:** The sample output includes `_matching_data` (matching
candidates for each record), while the final `bnf_parsed.json` does **not**.

```json
{
  "OAI_11002945": {
    "bnf_id": "OAI_11002945",
    "title_lat": [...],
    "description_candidates_lat": ["Fabricius...", "1666"],
    "description_candidate_labels_lat": ["date:copy", "date:copy"],
    "_matching_data": {"lat": [...], "ar": [...]}     // ← sample only; not in bnf_parsed.json
  }
}
```

**Options:**
- `--n` — sample size (default 50)
- `--seed` — random seed for reproducibility
- `--output` — override output path

---

### Programmatic access

For threshold experimentation or embedding-based downstream stages:

```python
import parsers.bnf as bnf
from utils.config import load_config

cfg = load_config()

# Load curated boilerplate (production)
bnf.BOILERPLATE_NGRAMS = bnf.load_boilerplate_file(
    f"{cfg.resolved_bnf_survey_dir()}/boilerplate.json"
)

# Parse full collection
metadata = bnf.BNFMetadata(cfg.bnf_data_path)

# Access a record
record = metadata.get("OAI_11000434")
print(record.matching_data())   # {"lat": [...], "ar": [...]}
print(record.description_candidates_lat)
print(record.description_candidate_labels_lat)
```

**Threshold experimentation (bypass manual review):**
```python
# Apply thresholds directly to raw vocabulary — no reviewed CSV needed
bnf.BOILERPLATE_NGRAMS = bnf.load_boilerplate_ngrams(
    f"{cfg.resolved_bnf_survey_dir()}/ngrams.json",
    min_doc_freq_pct=2.0,
    max_repeats_per_doc=1.1,
)
```

---

## Manifest and audit trail

Each BNF survey stage writes its completion record to
`outputs/bnf_survey/manifest.json`:

```json
{
  "created": "...",
  "last_updated": "...",
  "config_snapshot": { ... },
  "stages": {
    "build":        { "completed": true, "timestamp": "...", "parameters": { ... } },
    "apply_review": { "completed": true, "timestamp": "...", "boilerplate_count": 45, "signal_count": 3 }
  }
}
```

The `config_snapshot` records the full resolved configuration at the time
`build` was run.  If you update `config.yml` and re-run `build`, the new
snapshot is written — the previous run's parameters are preserved in git
history if the manifest was committed, or lost if not.  For full
reproducibility of a specific run, copy the manifest to a safe location
before overwriting.

---

## Adding a new manuscript library

1. Set the appropriate data path key in `config.yml` (e.g. `bnf_data_path`).
2. Set `bnf_survey_dir` to a new output directory
   (e.g. `outputs/new_collection_survey/`) so artifacts are not overwritten.
3. Run Stage 3 (survey build) → review → Stage 4 (apply-review) → Stage 5 (parse).
4. The `boilerplate.json` produced is independent of any other collection.
5. Stages 1–2 (OpenITI parse and Wikidata enrichment) do **not** need to be
   repeated — the committed `data/` files are reused directly.

---

## Key files reference

| file | in repo? | description |
|---|---|---|
| `config.yml` | no (gitignored) | local paths and pipeline parameters |
| `config.example.yml` | yes | template — copy to `config.yml` |
| `requirements.txt` | yes | Python dependencies |
| `utils/config.py` | yes | typed config loader; all defaults defined here |
| `utils/tokens.py` | yes | shared tokenisation (Latin + Arabic) |
| `utils/survey_bnf.py` | yes | BNF survey pipeline: `build` and `apply-review` subcommands |
| `utils/parse_bnf.py` | yes | BNF parsing CLI: `build`, `update`, and `sample` subcommands |
| `utils/parse_openiti.py` | yes | OpenITI parse script: `build` and `update` subcommands |
| `utils/enrich_wikidata.py` | yes | Wikidata enrichment: `build` and `update` subcommands |
| `parsers/bnf.py` | yes | BNF XML parser: `BNFXml`, `BNFMetadata`, `BNFRecord` |
| `parsers/openiti.py` | yes | OpenITI YML parser: `OpenITIMetaYmls`, `OpenITIBookData`, etc. |
| `data/openiti_<version>.json` | yes | Parsed corpus + Wikidata enrichment; single source of truth; committed |
| `outputs/bnf_survey/summary.json` | no | field coverage report |
| `outputs/bnf_survey/ngrams.json` | no | full n-gram vocabulary (~250 MB for 7,825 records) |
| `outputs/bnf_survey/boilerplate_review.csv` | no | manual review artifact |
| `outputs/bnf_survey/boilerplate.json` | no | curated boilerplate + signals |
| `outputs/bnf_survey/manifest.json` | no | stage completion and config audit trail |

---

## Diacritic Normalization Setup (Part of Stage 3)

As part of Stage 3 (survey build), a diacritic conversion table is automatically generated. This maps special characters (diacritics, transliteration marks) to OpenITI equivalents for consistent fuzzy matching.

### Why it matters

Different manuscript libraries use different diacritical marks and transliteration conventions. Without normalization:
- al-Dabbāǧī vs al-Dhahabi match incorrectly (false positives due to unequal diacritics)
- Accents from different sources (French é, transliterated ē) aren't normalized consistently
- C/c from OpenITI slugs and ʿ from transliterations need unified representation

The conversion table ensures both BNF and OpenITI data normalize to compatible forms for fuzzy matching.

### Normalization pipeline (three phases)

The matching pipeline normalizes text in three phases:

1. **Hardcoded OpenITI conversions** — Fixed transformations for OpenITI conventions:
   - C/c (OpenITI ayn representation) → ʿ (unified intermediary)
   - Long vowels: ā → a, ī → i, ū → u
   - Emphatics: ḥ → h, ḍ → d, ṭ → t, ẓ → z, ṣ → s
   - Consonants with marks: ḏ → dh, ṯ → th, ḫ → kh, ǧ → j, š → sh, ġ → gh
   - ta marbuta ŧ → a

2. **Parametrized diacritic table** (optional) — Library-specific character mappings from `diacritic_conversions.csv`:
   - User-defined conversions for characters found in BNF data
   - Applied only if enabled in config

3. **Legacy normalizer** — Handles remaining normalization:
   - Strips diacritics (ī → i, ā → a, etc.) via NFD decomposition
   - Lowers case
   - Normalizes hyphens to spaces
   - Collapses whitespace

### Workflow: Stage 3 generates two review files

When you run `python utils/survey_bnf.py build`, it generates two files for you to review in parallel:

1. **boilerplate_review.csv** — n-gram boilerplate candidates (existing)
2. **diacritic_conversions.csv** — special characters to normalize (NEW)

Both files are output to `outputs/bnf_survey/`.

### Review and configure conversions

After running Stage 3 build, the diacritic conversion table documents ALL special characters found in the BNF dataset. You configure how each character should be handled:

1. Open `outputs/bnf_survey/diacritic_conversions.csv` in a spreadsheet editor
2. For each special character, fill in the `openiti_equivalent` column:
   - **Convert:** Enter the replacement character(s) (e.g., `gh` for ǧ, `dh` for ḏ)
   - **Preserve:** Enter the character itself (e.g., `ʿ` for ayn — only if character should be preserved through matching)
   - **Remove:** Leave blank (character will be stripped)
3. Save the file (will be read by matching pipeline)
4. No need to commit to git; it lives in `outputs/` with your library data

**Example entries:**
| character | openiti_equivalent | notes |
|-----------|-------------------|-------|
| ʿ | ʿ | Ayn - preserve for consistency with hardcoded OpenITI conversions |
| ǧ | gh | G with caron → simplify to two-character form |
| É | e | Accented E → strip accent |
| ◊ | | Special symbol → remove |

**Note:** The hardcoded OpenITI conversions (C/c → ʿ, long vowels → short, emphatics → base forms) are applied automatically and cannot be overridden via the table.

### Reference template

For reference, a populated BNF conversion table is available at:
- `data/bnf_diacritic_conversions_reference.csv` — Example of what a completed table looks like

You can copy values from this if you're working with a similar library.

### Enable/disable normalization

Toggle diacritic normalization in `matching/config.py`:

```python
USE_DIACRITIC_CONVERSION_TABLE = True  # Enable (default)
USE_DIACRITIC_CONVERSION_TABLE = False # Disable (legacy mode)
```

### Files involved

| file | purpose | generated by |
|------|---------|--------------|
| `outputs/bnf_survey/diacritic_conversions.csv` | Conversion table (unpopulated after generation) | `survey_bnf.py build` |
| `data/bnf_diacritic_conversions_reference.csv` | Reference/template for completed table | — (reference only) |
| `matching/normalize_diacritics.py` | Normalizer that applies the conversion table | — (used by matcher) |
| `matching/config.py` | Toggle flag: `USE_DIACRITIC_CONVERSION_TABLE` | — (configuration) |

### Warning if conversion table not populated

If the conversion table exists but is empty (all `openiti_equivalent` cells blank), the pipeline will warn:

```
WARNING: Conversion table has not been populated.
         All non-standard Latin characters will be normalised to their standard variants.
```

This means:
- All non-ASCII characters will be stripped
- No character preservation or custom conversions will be applied
- The pipeline continues normally (no error)

---

## Stage 6 — Matching Pipeline

The fuzzy matching pipeline matches BNF manuscript records against the OpenITI corpus through three scored stages followed by a confidence classifier.

### Prerequisites

1. Stage 5 complete: `outputs/bnf_parsed.json` exists
2. OpenITI corpus JSON available in `data/`
3. Diacritic conversion table populated: `outputs/bnf_survey/diacritic_conversions.csv` (see [Diacritic Normalization Setup](#diacritic-normalization-setup))

### Quick Start

```bash
# 500-record sample
python run_matching_pipeline.py --sample

# Full BNF corpus (~7,800 records), parallelized
python run_matching_pipeline.py --full --parallel
```

### Architecture

Four sequential stages are registered with and executed by `MatchingPipeline`:

---

**Stage 1: Author Matching** (`AuthorMatcher`)

Extracts author name candidates from BNF `dc:creator` and description fields, then fuzzy-matches against OpenITI author name variants using `fuzz.token_set_ratio`. All name variants for a given script (Latin or Arabic) are concatenated into one string before scoring, so the full name profile is matched rather than individual fragments.

**Two-tier IDF design:**

- *Recall tier (entry threshold)*: A candidate author passes if `raw_score × full_idf_boost ≥ AUTHOR_THRESHOLD`. The boost uses `combined_idf` — the sum of IDF weights for **all** matched tokens. This keeps common-name authors (e.g. "Muhammad") in the candidate pool so they can be disambiguated by title.

- *Precision tier (stored score)*: The score forwarded to Stage 3 uses `rare_idf` — only tokens with `IDF ≥ TOKEN_RARITY_THRESHOLD` contribute. Authors who matched only on common tokens store a boost of 1.0; their score stays below `COMBINED_FLOOR` and is rejected at Stage 3.

**Boost formula** (same for both tiers, different `idf_sum`):

```
boost = 1 + min(idf_sum / AUTHOR_IDF_BOOST_SCALE, AUTHOR_MAX_BOOST - 1)
```

**Creator field reweighting (Phase 2):** After initial scoring, if any matched author shares more than one rare token with the BNF `creator_lat`/`creator_ara` fields, all author scores for that record are re-blended:

```
score = raw_score × AUTHOR_FULL_STRING_WEIGHT
      + creator_overlap × AUTHOR_CREATOR_FIELD_WEIGHT
```

where `creator_overlap` is scored per OpenITI name variant (best variant wins, avoiding denominator inflation).

Returns up to `MAX_AUTHOR_CANDIDATES` author URIs per record.

---

**Stage 2: Title Matching** (`TitleMatcher`)

Extracts title candidates from BNF `dc:title` and description fields, then fuzzy-matches against all OpenITI book title variants. Same concatenation approach as Stage 1.

**Continuous rare-IDF boost** (precision tier only — no dual-tier needed here since title entry is already stricter):

```
boost = 1 + min(rare_idf / TITLE_IDF_BOOST_SCALE, TITLE_MAX_BOOST - 1)
```

Title IDF is computed from the OpenITI book corpus (not author names), so token rarity is calibrated for the title domain. "kitab" and "sharh" are common in titles (no boost); "futuh", "sham", "rida" are rare (boost applies).

Returns matching book URIs per record with boosted scores.

---

**Stage 3: Combined Scoring** (`CombinedMatcher`)

Forms (author, book) pairs from Stage 1 × Stage 2 results and applies sequential gates:

1. Book's `author_uri` must appear in Stage 1 results (valid pair)
2. Both `author_score` and `title_score` must be `≥ COMBINED_FLOOR` (raw scores; filters common-token-only author matches)
3. `title_score ≥ TITLE_FLOOR` (ensures a strong, specific title signal)
4. Normalised weighted combined score must be `≥ COMBINED_THRESHOLD`

**Normalised weighted combined score:**

Author and title scores are each normalised by their per-record maximum (putting both axes on [0,1] regardless of IDF boost magnitude), then combined as:

```
combined = COMBINED_AUTHOR_WEIGHT × author_norm + COMBINED_TITLE_WEIGHT × title_norm
         = 0.3 × author_norm + 0.7 × title_norm
```

Title is weighted more heavily because it is the stronger discriminator once the author stage has already filtered candidates.

---

**Stage 4: Classification** (`Classifier`)

Assigns each record to a confidence tier:
- `high_confidence` — Stage 3 produced a match
- `author_only` — Stage 1 matched but Stage 2/3 did not
- `title_only` — Stage 2 matched but no valid author pairing
- `unmatched` — no signal found

---

### Configuration

All parameters are in `matching/config.py` with inline documentation.

```python
# Shared IDF rarity gate (both stages)
TOKEN_RARITY_THRESHOLD = 3.5       # IDF reference: ibn≈1.06, al≈1.25,
                                    # muhammad≈1.69, khatib≈5.56, waqidi≈7.51

# Stage 1: Author matching
AUTHOR_THRESHOLD = 0.80            # Entry: raw × combined_idf_boost must meet this
AUTHOR_IDF_BOOST_SCALE = 15.0      # Normalises IDF sum for boost formula
AUTHOR_MAX_BOOST = 1.3             # Ceiling on author boost
MAX_AUTHOR_CANDIDATES = 50

# Stage 1: Creator field reweighting
AUTHOR_CREATOR_IDF_THRESHOLD = 4.5 # Higher than TOKEN_RARITY_THRESHOLD; excludes abd, ali
AUTHOR_FULL_STRING_WEIGHT = 0.6
AUTHOR_CREATOR_FIELD_WEIGHT = 0.4  # Must sum to 1.0

# Stage 2: Title matching
TITLE_THRESHOLD = 0.85             # Entry threshold
TITLE_IDF_BOOST_SCALE = 20.0       # Wider scale than author (title IDFs are higher)
TITLE_MAX_BOOST = 1.4

# Stage 3: Combined scoring
COMBINED_FLOOR = 0.80              # Both author AND title raw score must meet this
TITLE_FLOOR = 0.90                 # Title must independently be this strong
COMBINED_AUTHOR_WEIGHT = 0.3
COMBINED_TITLE_WEIGHT = 0.7        # Must sum to 1.0
COMBINED_THRESHOLD = 0.94          # Normalised weighted score must meet this

# Parallelization
NUM_WORKERS = 10
BATCH_SIZE = 100
```

### CLI Commands

```bash
# Sample set (500 records)
python run_matching_pipeline.py --sample

# Full corpus
python run_matching_pipeline.py --full

# Parallel processing (recommended for production)
python run_matching_pipeline.py --full --parallel

# Custom input / run ID
python run_matching_pipeline.py --bnf /path/to/custom_data.json --run-id my_test

# All options
python run_matching_pipeline.py --help
```

### Output Format

Results are written to `outputs/matches/{run_id}/`:

| File | Contents |
|------|----------|
| `matches_high_confidence.json` | Records with a Stage 3 match (author + title both confirmed) |
| `matches_author_only.json` | Author matched but no title match |
| `matches_title_only.json` | Title matched but no valid author pairing |
| `matches_unmatched.json` | No signal found |
| `matching_summary.txt` | Human-readable counts |
| `manifest.json` | Machine-readable run metadata for reproducibility |

**matches_high_confidence.json** sample:
```json
[
  {
    "bnf_id": "OAI_10030933",
    "bnf_title": "Bughyat al-Talab",
    "matches": ["0660IbnCadim.BughyatTalab", "0843IbnKhatibNasiriyya.DurrMuntakhab"]
  }
]
```

Note: composite manuscripts (majāmiʿ) legitimately produce multiple matches per record.

### Performance

Tested on Windows 11, Python 3.12:

| Dataset | Sequential | Parallel (10 workers) |
|---------|-----------|----------------------|
| Sample (500) | ~15s | ~5s |
| Full (~7,800) | ~90s | ~20s |

Parallelization is safe — sequential and parallel produce identical results (validated on correspondence test set).

### Validation

Run against the known-pairs test set (`data_samplers/correspondence.json`):

```bash
python matching/scripts/validate_correspondences_only.py
```

**Current results (16-record test set including composite manuscripts):**
- **Precision: 100%** — no false positives returned
- **Recall:** limited only by records where BNF has author evidence but no usable title — structurally unavoidable with the current combined gate design

Run per-stage debugging scripts in `matching/scripts/`:

```bash
python matching/scripts/export_author_scores.py    # book-centric CSV: per-stage scores + IDF flags
python matching/scripts/debug_combined_scores.py   # per-record Stage 3 score breakdown
python matching/scripts/validate_author_matching.py # Stage 1 standalone validation
```

### Known Limitations

- **Author-only records:** Records with BNF author evidence but no matching title fail Stage 3 by design. The combined gate requires both signals; author-only evidence is preserved in `matches_author_only.json` for manual review.
- **Composite manuscripts:** One BNF record legitimately matching multiple OpenITI books (majāmiʿ) is supported — the pipeline returns all matching pairs.
- **Author URI correspondence entries:** `data_samplers/correspondence.json` contains one entry (`0845Maqrizi`, OAI_10884186) where the expected value is an author URI rather than a book URI. This is a data quality issue in the ground truth, not a pipeline limitation.

### Programmatic API

```python
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier

bnf_records  = load_bnf_records("outputs/bnf_parsed.json")
openiti_data = load_openiti_corpus("data/openiti_corpus_*.json")

pipeline = MatchingPipeline(bnf_records, openiti_data, run_id="custom")
pipeline.register_stage(AuthorMatcher(use_parallel=False))
pipeline.register_stage(TitleMatcher(use_parallel=False))
pipeline.register_stage(CombinedMatcher())
pipeline.register_stage(Classifier())
pipeline.run()

# Access results
authors        = pipeline.get_stage1_result("OAI_10030933")       # [author_URIs]
author_scores  = pipeline.get_stage1_scores("OAI_10030933")        # {author_URI: score}
books          = pipeline.get_stage2_result("OAI_10030933")        # [book_URIs]
book_scores    = pipeline.get_stage2_scores("OAI_10030933")        # {book_URI: score}
matches        = pipeline.get_stage3_result("OAI_10030933")        # [book_URIs] ranked
combined       = pipeline.get_stage3_scores("OAI_10030933")        # {book_URI: combined_score}
classification = pipeline.get_classification("OAI_10030933")       # "high_confidence" | ...
```

---

## Stage 7 — Clustering and Resolution

> **Status:** Not yet implemented.
>
> This stage will perform record-level clustering and manual resolution of
> ambiguous or multi-match cases. Planned as future work after initial
> validation of Stage 6 results on the full corpus.
