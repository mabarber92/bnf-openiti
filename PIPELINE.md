# BNF–OpenITI Matching Pipeline

End-to-end pipeline for matching BNF Gallica manuscript records against
OpenITI corpus entries.  Each stage produces artifact files that are consumed
by the next stage.  All artifact directories are outside the repo (or
gitignored) — see **Setup** below.

---

## Setup

1. Copy `config.example.yml` → `config.yml` and fill in your local paths:

```yaml
bnf_data_path:     /path/to/BNF_data      # OAI-PMH XML files
openiti_data_path: /path/to/OpenITI_data
pipeline_out_dir:  outputs                 # relative or absolute
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
Stage 1  survey build     →  summary.json, ngrams.json, boilerplate_review.csv
         ↓
         [MANUAL REVIEW: boilerplate_review.csv]
         ↓
Stage 2  apply-review     →  boilerplate.json
         ↓
Stage 3  parse BNF        →  bnf_parsed.json
         ↓
Stage 4  parse OpenITI    →  openiti_parsed.json   [not yet implemented]
         ↓
Stage 5  match            →  matches.json          [not yet implemented]
         ↓
Stage 6  cluster/resolve  →  resolutions.json      [not yet implemented]
```

---

## Stage 1 — Survey build

**Command:**
```bash
python utils/survey_bnf.py build
```

**What it does:**  
Scans all `OAI_*.xml` files in `bnf_data_path`, computes field coverage
statistics and n-gram frequencies across `dc:description` text, then applies
threshold criteria to generate a list of boilerplate candidates for review.

**Parameters (set in `config.yml`, override with CLI flags):**

| config key | CLI flag | default | effect |
|---|---|---|---|
| `survey.max_n` | `--max-n` | `4` | largest n-gram size (bigrams–quadgrams) |
| `survey.keep_abbrev_dots` | `--keep-abbrev-dots` | `true` | retain dots on abbreviation tokens (e.g. `cf.`, `ms.`) so abbreviation phrases form distinct n-grams |

**Per-field boilerplate thresholds (set in `config.yml` only):**

N-gram scanning and boilerplate candidate generation is now per-field.  Each
field in `boilerplate.fields` is scanned independently and applies its own
mode and thresholds.

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
    format:
      mode: freq_only
      min_doc_freq_pct: 10.0
    subject:
      mode: freq_only
      min_doc_freq_pct: 15.0
    rights:
      mode: freq_only
      min_doc_freq_pct: 50.0
    source:
      mode: freq_only
      min_doc_freq_pct: 50.0
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
| `ngrams.json` | full n-gram vocabulary — term_freq, doc_freq, TF-IDF for every observed n-gram; untruncated so threshold filtering can be applied downstream |
| `boilerplate_review.csv` | candidate boilerplate phrases for manual review |
| `manifest.json` | run record: timestamp, config snapshot, parameters used |

**Re-running:**  
Re-running `build` overwrites all four files and clears the `apply_review`
stage entry from the manifest (since the review CSV has changed).  If you
change only the boilerplate thresholds (`min_doc_freq_pct` /
`max_repeats_per_doc`), `ngrams.json` contains the full vocabulary — you can
regenerate the review CSV without re-scanning by calling
`_suggest_boilerplate()` directly in a script rather than re-running `build`.

---

## Manual review — `boilerplate_review.csv`

Open `outputs/bnf_survey/boilerplate_review.csv` in a spreadsheet tool.
Rows are sorted by `repeats_per_doc` ascending — true boilerplate clusters
at 1.0 (appears exactly once per record) and name/content fragments appear
further down with higher values.

**Columns:**

| column | description |
|---|---|
| `ngram` | the phrase |
| `source_field` | DC field the phrase was found in (`description`, `format`, `subject`, etc.) |
| `script` | `latin` or `arabic` |
| `n` | n-gram size (2=bigram, 3=trigram, 4=quadgram) |
| `doc_freq_pct` | % of records containing this phrase |
| `repeats_per_doc` | average occurrences per record — ~1.0 = structural template |
| `keep` | `yes` = filter this phrase; `no` = leave in field text as content |
| `signal_type` | if non-empty, phrase is a structural signal rather than pure noise (see below) |

**`keep` column:**
- `yes` — phrase is boilerplate or a signal; will be filtered from descriptions
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

## Stage 2 — Apply review

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
    {"ngram": "naskhi maghribi",        "field": "format"},
    ...
  ],
  "signals": [
    {"ngram": "lieu de copie", "field": "description", "signal_type": "agent:copyist"},
    {"ngram": "sharḥ ʿalā",   "field": "description", "signal_type": "relation:commentary"},
    {"ngram": "copié en",     "field": "description", "signal_type": "date:copy"}
  ]
}
```

- `boilerplate` — n-grams stripped from descriptions during parsing
- `signals` — n-grams that trigger relation detection; forwarded to
  `BNFRecord.detected_relations` and annotated in `matching_data()`

**Re-running:**  
Safe to re-run after editing the CSV.  Updates `boilerplate.json` and the
manifest in place.

---

## Stage 3 — Parse BNF records

**Not yet a CLI stage** — called programmatically.

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

**Outputs** (planned, not yet implemented):

| file | contents |
|---|---|
| `outputs/bnf_survey/bnf_parsed.json` | all parsed BNF records as JSON; reused across matching runs to avoid re-parsing |

---

## Manifest and audit trail

Each stage writes its completion record to `outputs/bnf_survey/manifest.json`:

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

## Adding a new collection

1. Set `bnf_data_path` (or the equivalent path key) in `config.yml` to point
   at the new collection's XML directory.
2. Set `bnf_survey_dir` to a new output directory
   (e.g. `outputs/new_collection_survey/`) so BNF artifacts are not overwritten.
3. Run `build` → review → `apply-review` as above.
4. The new `boilerplate.json` is independent of the BNF one — pass it
   separately to the parser for that collection.

---

## Key files reference

| file | in repo? | description |
|---|---|---|
| `config.yml` | no (gitignored) | local paths and pipeline parameters |
| `config.example.yml` | yes | template — copy to `config.yml` |
| `requirements.txt` | yes | Python dependencies; planned future deps listed as comments |
| `utils/config.py` | yes | typed config loader; all defaults defined here |
| `utils/tokens.py` | yes | shared tokenisation (Latin + Arabic); used by survey and parser |
| `utils/survey_bnf.py` | yes | survey pipeline: `build` and `apply-review` subcommands |
| `parsers/bnf.py` | yes | BNF XML parser: `BNFXml`, `BNFMetadata`, `BNFRecord` |
| `outputs/bnf_survey/summary.json` | no | field coverage report |
| `outputs/bnf_survey/ngrams.json` | no | full n-gram vocabulary (~250 MB for 7,825 records) |
| `outputs/bnf_survey/boilerplate_review.csv` | no | manual review artifact |
| `outputs/bnf_survey/boilerplate.json` | no | curated boilerplate + signals |
| `outputs/bnf_survey/manifest.json` | no | stage completion and config audit trail |
