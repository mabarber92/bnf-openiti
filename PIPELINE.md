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
── OpenITI preparation (one-off per corpus version; outputs committed to repo) ──

Stage 1  parse OpenITI    →  data/openiti_parsed_<version>.json    [not yet implemented]
         ↓
Stage 2  enrich WorldCat  →  data/openiti_worldcat_<version>.json  [optional]

── Manuscript library preparation (per collection) ──────────────────────────────

Stage 3  survey build     →  outputs/bnf_survey/summary.json
                             outputs/bnf_survey/ngrams.json
                             outputs/bnf_survey/boilerplate_review.csv
         ↓
         [MANUAL REVIEW: boilerplate_review.csv]
         ↓
Stage 4  apply-review     →  outputs/bnf_survey/boilerplate.json
         ↓
Stage 5  parse BNF        →  outputs/bnf_parsed.json               [not yet implemented]

── Matching and resolution ───────────────────────────────────────────────────────

Stage 6  match            →  outputs/matches.json                   [not yet implemented]
         ↓
Stage 7  cluster/resolve  →  outputs/resolutions.json               [not yet implemented]
```

---

## Stage 1 — Parse OpenITI

**Not yet implemented** — will produce `data/openiti_parsed_<corpus_version>.json`
and be committed to the repo.

---

## Stage 2 — WorldCat enrichment (optional)

> **Most users:** check `data/` for an existing
> `openiti_worldcat_<corpus_version>.json`.  If it is present, skip to
> **Update** below.  The file is committed to the repo so you should not
> need to run `build` unless you are working with a new corpus version.

**Why it exists:**  
Only ~13 % of OpenITI book YMLs have a `TITLEA` field.  The remaining
87 % carry only a CamelCase URI slug (e.g. `DalalatHairin`).  WorldCat
provides Arabic-script titles and ALA-LC transliterations for ~18 % of
books, improving Arabic-script matching coverage for that subset.

---

### Build (first-time / new corpus version)

```bash
python utils/enrich_worldcat.py build
python utils/enrich_worldcat.py build --delay 2.0   # more conservative
```

Fetches WorldCat data for all ~1,300 books with OCLC links in the current
corpus.  At 1 req/s this takes ~25 minutes.  Safe to interrupt — re-running
resumes from where it stopped.  Commit the output to `data/` once complete.

---

### Update (after a corpus version bump or new YML links)

```bash
python utils/enrich_worldcat.py update
```

Loads the existing enrichment file and fetches **only**:
- Books not yet in the file (new OCLC links added in a corpus update)
- Books whose OCLC ID in the YML has changed since last fetch

The stored `oclc_id` per record is the canonical change key — if it
matches the current YML link, no fetch is made even if the previous fetch
errored.  To retry errors for a specific book, remove that URI from the
JSON and re-run `update`.

---

**Rate limiting:**  
Default: 1 req/s.  Do not set `--delay` below 1.0.  The User-Agent
header identifies this as a research tool.

**Output:**

| file | contents |
|---|---|
| `data/openiti_worldcat_<corpus_version>.json` | WorldCat title/author data keyed by book URI; committed to repo |

**Output format:**

```json
{
  "_meta": {
    "schema_version": 1,
    "corpus_version": "corpus_2025_1_9",
    "generated_at": "...",
    "total_records": 1283,
    "total_fetched": 1278,
    "total_failed": 5
  },
  "records": {
    "0110HasanBasri.FadailMakka": {
      "oclc_id":          "8850761",
      "title_ar":         "فضائل مكة والسكن فيها",
      "title_lat":        null,
      "author_names_ar":  ["الحسن البصري", "حسن البصري"],
      "author_names_lat": ["Ḥasan al-Baṣrī"],
      "language":         "ara",
      "fetched_at":       "..."
    },
    "0601MusaIbnMaymun.DalalatHairin": {
      "oclc_id":    "...",
      "error":      "HTTP 404: Not Found",
      "fetched_at": "..."
    }
  }
}
```

Records with an `"error"` key are retained so `update` skips them unless
the OCLC ID changes.

**Loading in downstream stages:**

```python
from utils.enrich_worldcat import load_worldcat_enrichment

wc = load_worldcat_enrichment("data/openiti_worldcat_corpus_2025_1_9.json")
rec = wc.get("0110HasanBasri.FadailMakka")
if rec and not rec.get("error"):
    arabic_title = rec["title_ar"]   # "فضائل مكة والسكن فيها"
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
| `outputs/bnf_parsed.json` | all parsed BNF records as JSON; reused across matching runs to avoid re-parsing |

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
5. Stages 1–2 (OpenITI parse and WorldCat enrichment) do **not** need to be
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
| `utils/enrich_worldcat.py` | yes | WorldCat enrichment: `build` and `update` subcommands |
| `parsers/bnf.py` | yes | BNF XML parser: `BNFXml`, `BNFMetadata`, `BNFRecord` |
| `parsers/openiti.py` | yes | OpenITI YML parser: `OpenITIMetaYmls`, `OpenITIBookData`, etc. |
| `data/openiti_worldcat_<version>.json` | yes | WorldCat enrichment; committed, version-stamped |
| `data/openiti_parsed_<version>.json` | yes (planned) | Parsed OpenITI corpus; committed, version-stamped |
| `outputs/bnf_survey/summary.json` | no | field coverage report |
| `outputs/bnf_survey/ngrams.json` | no | full n-gram vocabulary (~250 MB for 7,825 records) |
| `outputs/bnf_survey/boilerplate_review.csv` | no | manual review artifact |
| `outputs/bnf_survey/boilerplate.json` | no | curated boilerplate + signals |
| `outputs/bnf_survey/manifest.json` | no | stage completion and config audit trail |
